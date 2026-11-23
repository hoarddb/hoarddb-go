// Copyright 2026 Boeger IT Sh.p.k
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ring

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
)

const (
	defaultDialTimeout    = 10 * time.Second
	defaultRequestTimeout = 30 * time.Second
	clientName            = "hoarddb-go"
)

// errClosed is why a Client that Close was called on refuses calls.
var errClosed = errors.New("ring: the client is closed")

// loginRequest is the AUTH body. It is the client-side twin of the server's LoginRequest,
// declared here so this package does not import the server.
type loginRequest struct {
	Username        string   `bson:"username"`
	Password        string   `bson:"password"`
	ProtocolVersion uint32   `bson:"protocol_version"`
	Capabilities    []string `bson:"capabilities"`
	Client          string   `bson:"client"`
}

// authFailure is the body of an AUTH_FAIL frame.
type authFailure struct {
	Reason  uint8  `bson:"reason"`
	Message string `bson:"message,omitempty"`
}

// Client is one authenticated connection to a node. It is safe for concurrent use: requests
// from many goroutines are pipelined on the connection and each reply is matched to its
// request by request id.
type Client struct {
	opts Options
	conn net.Conn

	writeLock sync.Mutex

	state   sync.Mutex
	pending map[[16]byte]chan hoarddbwire.Frame
	failure error // why the connection ended; set once, under state

	reader sync.WaitGroup

	idPrefix  [8]byte
	idCounter atomic.Uint64

	headers headerCache
}

// trustPolicy picks the one way the server is checked. The strictest option that is set
// decides: Fingerprint, else KnownServers, else Insecure.
func trustPolicy(opts Options) (trust.Policy, error) {
	switch {
	case opts.Fingerprint != "":
		pin, err := trust.ParseFingerprint(opts.Fingerprint)
		if err != nil {
			return trust.Policy{}, fmt.Errorf("ring: Options.Fingerprint: %w", err)
		}
		return trust.Policy{Pin: pin}, nil
	case opts.KnownServers != "":
		// No Prompt: a library has no terminal to ask on, so a server the file does not
		// record is refused and the file is never written.
		return trust.Policy{Known: trust.NewKnownServers(opts.KnownServers)}, nil
	case opts.Insecure:
		return trust.Policy{InsecureReason: "ring.Options.Insecure, local development only"}, nil
	}
	return trust.Policy{}, errors.New("ring: Options trusts no server. Set Fingerprint to the value the server prints when it starts, " +
		"or set KnownServers to the file that records the server's key (the hoardDB command line tool writes ~/.hoarddb/known_servers), " +
		"or set Insecure for a node you started yourself on this machine (it skips certificate checks and is for local development only)")
}

// connectHint is the fix that goes with a failed connect. A server the known-servers file
// does not vouch for is a decision for the caller, not a server that is down, so it gets
// its own text with the two ways to make it.
func connectHint(opts Options, err error) string {
	var unknown *trust.UnknownServerError
	var changed *trust.KeyChangedError
	switch {
	case errors.As(err, &unknown):
		return fmt.Sprintf("Options.KnownServers is %s and it has no line for %s. If the fingerprint above is the one that server printed when it started, "+
			"set Options.Fingerprint to it, or run the hoardDB command line tool once against %s and answer y so the file records it",
			opts.KnownServers, opts.Addr, opts.Addr)
	case errors.As(err, &changed):
		return fmt.Sprintf("Options.KnownServers recorded another key for %s. If the server was reinstalled and the presented fingerprint is the one it printed when it started, "+
			"delete its line from %s and trust it again; if not, something else is answering at that address", opts.Addr, changed.Path)
	}
	return "Check that hoarddb-server is running there and, with Fingerprint, that it matches the one the server printed"
}

// Dial connects to opts.Addr over TLS, logs in and returns the ready client. The context
// bounds the connect and the login only; it does not outlive the call.
func Dial(ctx context.Context, opts Options) (*Client, error) {
	policy, err := trustPolicy(opts)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := trust.NewTLSConfig(opts.Addr, policy)
	if err != nil {
		return nil, fmt.Errorf("ring: building the TLS config for %s: %w", opts.Addr, err)
	}
	timeout := opts.DialTimeout
	if timeout <= 0 {
		timeout = defaultDialTimeout
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dialer := tls.Dialer{NetDialer: &net.Dialer{}, Config: tlsConfig}
	raw, err := dialer.DialContext(dialCtx, "tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("ring: connecting to %s: %w. %s", opts.Addr, err, connectHint(opts, err))
	}
	client := &Client{opts: opts, conn: raw, pending: map[[16]byte]chan hoarddbwire.Frame{}}
	if _, err := rand.Read(client.idPrefix[:]); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("ring: reading random bytes for request ids: %w", err)
	}
	reader := bufio.NewReader(raw)
	if err := client.login(dialCtx, reader); err != nil {
		_ = raw.Close()
		return nil, err
	}
	client.reader.Add(1)
	go client.readLoop(reader)
	return client, nil
}

// login runs AUTH before the reader goroutine starts, so it reads the reply itself.
func (c *Client) login(ctx context.Context, reader *bufio.Reader) error {
	body, err := bson.Marshal(loginRequest{Username: c.opts.User, Password: c.opts.Password, Client: clientName})
	if err != nil {
		return fmt.Errorf("ring: encoding the login: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("ring: setting the login deadline: %w", err)
		}
		defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	}
	id, err := hoarddbwire.NewRequestID()
	if err != nil {
		return fmt.Errorf("ring: making the login's request id: %w", err)
	}
	if err := hoarddbwire.WriteFrame(c.conn, hoarddbwire.Frame{Op: hoarddbwire.OpAuth, ID: id, Payload: body}); err != nil {
		return fmt.Errorf("ring: sending the login to %s: %w", c.opts.Addr, err)
	}
	reply, err := hoarddbwire.ReadFrame(reader)
	if err != nil {
		return fmt.Errorf("ring: reading the login reply from %s: %w", c.opts.Addr, err)
	}
	switch reply.Op {
	case hoarddbwire.OpAuthOK:
		return nil
	case hoarddbwire.OpAuthFail:
		var failure authFailure
		detail := "wrong user or password"
		if len(reply.Payload) > 0 && bson.Unmarshal(reply.Payload, &failure) == nil && failure.Message != "" {
			detail = failure.Message
		}
		return fmt.Errorf("ring: %s rejected the login of user %q: %s. Check the user name and the password in Options", c.opts.Addr, c.opts.User, detail)
	case hoarddbwire.OpError:
		return fmt.Errorf("ring: %s refused the login of user %q: %w", c.opts.Addr, c.opts.User, refusalError(reply.Payload))
	}
	return fmt.Errorf("ring: %s answered the login with opcode %s", c.opts.Addr, reply.Op)
}

// NextRequestID returns a request id no other request of this client uses: 8 random bytes
// chosen at Dial and an 8-byte counter. Keep it to retry a write with [WithRequestID].
func (c *Client) NextRequestID() [16]byte {
	var id [16]byte
	copy(id[:8], c.idPrefix[:])
	binary.BigEndian.PutUint64(id[8:], c.idCounter.Add(1))
	return id
}

// readLoop hands every reply frame to the request that is waiting for it. It exits when the
// connection ends, and Close waits for it.
func (c *Client) readLoop(reader *bufio.Reader) {
	defer c.reader.Done()
	for {
		frame, err := hoarddbwire.ReadFrame(reader)
		if err != nil {
			c.fail(fmt.Errorf("ring: the connection to %s ended: %w", c.opts.Addr, err))
			return
		}
		c.state.Lock()
		waiter, ok := c.pending[frame.ID]
		delete(c.pending, frame.ID)
		c.state.Unlock()
		if ok {
			waiter <- frame // buffered, and this goroutine is its only sender
		}
	}
}

// fail records why the connection ended and wakes every waiter. Only the first reason is kept.
func (c *Client) fail(reason error) {
	c.state.Lock()
	if c.failure == nil {
		c.failure = reason
	}
	waiters := c.pending
	c.pending = map[[16]byte]chan hoarddbwire.Frame{}
	c.state.Unlock()
	for _, waiter := range waiters {
		close(waiter) // removed from the map above, so the reader can no longer send on it
	}
	_ = c.conn.Close()
}

// endedError is what a call gets once the connection is over.
func (c *Client) endedError() error {
	c.state.Lock()
	defer c.state.Unlock()
	if c.failure == nil {
		return errClosed
	}
	return c.failure
}

// Close ends the connection, wakes every request still waiting (they fail with an error) and
// waits for the reader goroutine to exit. It is safe to call more than once.
func (c *Client) Close() error {
	c.fail(errClosed)
	c.reader.Wait()
	return nil
}

// roundtrip sends one frame under id and waits for the frame that answers it.
func (c *Client) roundtrip(ctx context.Context, op hoarddbwire.Opcode, id [16]byte, payload []byte) (hoarddbwire.Frame, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeout := c.opts.RequestTimeout
		if timeout <= 0 {
			timeout = defaultRequestTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	waiter := make(chan hoarddbwire.Frame, 1)
	c.state.Lock()
	if c.failure != nil {
		failure := c.failure
		c.state.Unlock()
		return hoarddbwire.Frame{}, failure
	}
	if _, inFlight := c.pending[id]; inFlight {
		c.state.Unlock()
		return hoarddbwire.Frame{}, fmt.Errorf("ring: request id %x is already in flight on this client; ids must be unique among the requests waiting for an answer", id)
	}
	c.pending[id] = waiter
	c.state.Unlock()

	if err := c.write(ctx, hoarddbwire.Frame{Op: op, ID: id, Payload: payload}); err != nil {
		c.forget(id)
		return hoarddbwire.Frame{}, err
	}
	select {
	case frame, ok := <-waiter:
		if !ok {
			return hoarddbwire.Frame{}, c.endedError()
		}
		return frame, nil
	case <-ctx.Done():
		c.forget(id)
		return hoarddbwire.Frame{}, fmt.Errorf("ring: no answer to %s (request id %x) before the deadline, so the server may or may not have applied it. "+
			"To retry a write safely, pass WithRequestID(%x): %w", op, id, id, ctx.Err())
	}
}

func (c *Client) forget(id [16]byte) {
	c.state.Lock()
	delete(c.pending, id)
	c.state.Unlock()
}

// write sends one frame. A write that fails or times out may have sent half a frame, so the
// connection is ended: the stream cannot be trusted afterwards. A frame over the protocol's
// size limit is refused before a byte is sent, and the connection stays open.
func (c *Client) write(ctx context.Context, frame hoarddbwire.Frame) error {
	encoded, err := frame.AppendTo(make([]byte, 0, hoarddbwire.HeaderSize+len(frame.Payload)))
	if err != nil {
		return fmt.Errorf("ring: the request was not sent: %w. Send it as several smaller requests", err)
	}
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			c.fail(fmt.Errorf("ring: setting the write deadline on the connection to %s: %w", c.opts.Addr, err))
			return c.endedError()
		}
	}
	if _, err := c.conn.Write(encoded); err != nil {
		c.fail(fmt.Errorf("ring: writing to %s: %w", c.opts.Addr, err))
		return c.endedError()
	}
	return nil
}

// call sends a request and returns the payload of its RING_REPLY, or the server's refusal as
// an *Error.
func (c *Client) call(ctx context.Context, op hoarddbwire.Opcode, id [16]byte, payload []byte) ([]byte, error) {
	frame, err := c.roundtrip(ctx, op, id, payload)
	if err != nil {
		return nil, err
	}
	switch frame.Op {
	case hoarddbwire.OpRingReply:
		return frame.Payload, nil
	case hoarddbwire.OpError:
		return nil, refusalError(frame.Payload)
	}
	return nil, frameError(frame.Op, op.String())
}
