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

package hoarddbwire

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// Errors a [Conn] reports. Match them with errors.Is.
var (
	// ErrConnClosed is a request on a connection that was closed by its owner.
	ErrConnClosed = errors.New("connection closed")
	// ErrAbandoned is a connection closed because a request on it was cancelled
	// or timed out: the server cannot be told to stop, and its late reply would be
	// mistaken for another request's.
	ErrAbandoned = errors.New("connection closed after a cancelled request")
	// ErrUnexpectedResponse is a response whose request ID matches no request in
	// flight. It is a protocol violation: the connection is torn down and not
	// reused.
	ErrUnexpectedResponse = errors.New("the server sent a response to a request that is not in flight")
)

// ErrBadRequestID is a request ID [Conn.RoundTripWithID] refuses before it sends
// anything: an all-zero ID, or one already in flight on the connection.
var ErrBadRequestID = errors.New("request id")

// RefusedError is a connection-level refusal: the server completed the TLS
// handshake, sent one OpError frame with an all-zero request ID, and closed
// (too many connections, or ALPN not served). It is distinguishable from a TLS
// misconfiguration for that reason.
type RefusedError struct {
	Response ErrorResponse
}

func (err *RefusedError) Error() string {
	return fmt.Sprintf("the server refused the connection (%s): %s", err.Response.Code, err.Response.Message)
}

// writeFailureGrace is how long a failed write waits for the read side to
// explain it. A server that refuses a connection writes its reason and closes,
// and the reason is more useful than "broken pipe".
const writeFailureGrace = 250 * time.Millisecond

// Conn is one authenticated-or-not TCP+TLS connection to a server.
//
// It is a request-ID demultiplexer from the start: a writer lock serialises
// frame writes and one reader goroutine delivers each response to the request
// waiting for its ID. With the default of one request in flight the table
// never holds more than one entry; allowing more is a constant (see
// [ConnOptions.MaxInFlight]), not a rewrite. The reader is also what notices
// a server that closed an idle connection, before a request is sent into it.
//
// A Conn is safe for concurrent use. Cancelling a request's context closes the
// connection, because the server cannot be told to stop and a late reply would
// otherwise be mistaken for another request's.
type Conn struct {
	raw     net.Conn
	slots   chan struct{}
	writeMu sync.Mutex

	mu      sync.Mutex
	pending map[RequestID]chan Frame

	done       chan struct{}
	readerDone chan struct{}
	closeOnce  sync.Once
	err        error // set once, before done is closed
}

// ConnOptions tunes a [Conn].
type ConnOptions struct {
	// MaxInFlight is how many requests may be outstanding at once. The default
	// is 1. More than 1 is only safe against a server that advertises
	// query_pipelining, and then responses and execution order are unspecified.
	MaxInFlight int
}

// NewConn takes ownership of raw, which must already be connected and past its
// TLS handshake, and starts the reader goroutine. [Conn.Close] stops it.
func NewConn(raw net.Conn, options ConnOptions) *Conn {
	maxInFlight := options.MaxInFlight
	if maxInFlight < 1 {
		maxInFlight = 1
	}
	conn := &Conn{
		raw:        raw,
		slots:      make(chan struct{}, maxInFlight),
		pending:    make(map[RequestID]chan Frame),
		done:       make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	go conn.readLoop()
	return conn
}

// DialOptions tunes [Dial].
type DialOptions struct {
	// Conn is passed to the new connection.
	Conn ConnOptions
	// KeepAlive is the TCP keepalive period; zero means 30 seconds.
	KeepAlive time.Duration
}

// Dial opens a TCP connection to address, completes the TLS handshake with
// tlsConfig (which carries the trust policy; see package trust), and confirms
// the server negotiated the hoarddb/1 protocol. ctx bounds the whole dial.
// Dial does not authenticate: the first frame on the connection must be an
// OpAuth.
func Dial(ctx context.Context, address string, tlsConfig *tls.Config, options DialOptions) (*Conn, error) {
	keepAlive := options.KeepAlive
	if keepAlive == 0 {
		keepAlive = 30 * time.Second
	}
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{KeepAlive: keepAlive},
		Config:    tlsConfig,
	}
	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", address, err)
	}
	if negotiated := raw.(*tls.Conn).ConnectionState().NegotiatedProtocol; negotiated != ALPNv1 {
		_ = raw.Close()
		return nil, fmt.Errorf("the server at %s negotiated protocol %q, not %q", address, negotiated, ALPNv1)
	}
	return NewConn(raw, options.Conn), nil
}

// RemoteAddr is the server's address.
func (conn *Conn) RemoteAddr() net.Addr { return conn.raw.RemoteAddr() }

// Done is closed when the connection is no longer usable, for any reason.
func (conn *Conn) Done() <-chan struct{} { return conn.done }

// Err is why the connection stopped being usable, or nil while it is usable.
func (conn *Conn) Err() error {
	select {
	case <-conn.done:
		return conn.err
	default:
		return nil
	}
}

// Close closes the connection and waits for the reader goroutine to exit. It
// is safe to call more than once and from any goroutine.
func (conn *Conn) Close() error {
	conn.closeWith(ErrConnClosed)
	<-conn.readerDone
	return nil
}

// Do marshals request as the payload of one frame with opcode op, sends it,
// waits for the reply and, unless the reply is an OpError or OpAuthFail,
// unmarshals its payload into response. A nil request sends an empty payload; a
// nil response discards the payload.
//
// An OpError or OpAuthFail reply is returned as the frame with a nil error: it
// is the caller's to judge, because what an error means depends on the
// operation. A non-nil error means the exchange itself failed.
func (conn *Conn) Do(ctx context.Context, op Opcode, request, response any) (Frame, error) {
	var payload []byte
	if request != nil {
		encoded, err := codec.Marshal(request)
		if err != nil {
			return Frame{}, fmt.Errorf("preparing %s request: %w", op, err)
		}
		payload = encoded
	}
	reply, err := conn.RoundTrip(ctx, Frame{Op: op, Payload: payload})
	if err != nil {
		return reply, err
	}
	if response != nil && reply.Op != OpError && reply.Op != OpAuthFail {
		if err := codec.Unmarshal(reply.Payload, response); err != nil {
			return reply, fmt.Errorf("reading the %s reply to %s: %w", reply.Op, op, err)
		}
	}
	return reply, nil
}

// RoundTrip sends request, whose ID is replaced with a fresh one, and returns
// the reply frame. See [Conn.Do] for the contract.
func (conn *Conn) RoundTrip(ctx context.Context, request Frame) (Frame, error) {
	return conn.roundTrip(ctx, request, true)
}

// RoundTripWithID is [Conn.RoundTrip] with the request ID the caller chose. A
// ring write keeps one ID across its resends, so the server answers a second
// send from its record of the first (entry-ring-conventions.md section 18). It
// refuses, before it takes a slot or writes, an all-zero ID (a connection-level
// refusal carries that ID, so a reply under it could not be told from one) and
// an ID already in flight on this connection.
func (conn *Conn) RoundTripWithID(ctx context.Context, request Frame) (Frame, error) {
	if request.ID.IsZero() {
		return Frame{}, fmt.Errorf("%s: %w: the ID is all zeros, which a connection-level refusal carries; mint one with NewRequestID", request.Op, ErrBadRequestID)
	}
	conn.mu.Lock()
	_, busy := conn.pending[request.ID]
	conn.mu.Unlock()
	if busy {
		return Frame{}, inFlightRefusal(request)
	}
	return conn.roundTrip(ctx, request, false)
}

// inFlightRefusal is why a call under an ID still in flight is not sent: two
// replies under one ID could not be told apart.
func inFlightRefusal(request Frame) error {
	return fmt.Errorf("%s: %w: ID %s is already in flight on this connection, and two replies under one ID could not be told apart; wait for that call to return, or use a new ID", request.Op, ErrBadRequestID, request.ID)
}

// roundTrip is the body of [Conn.RoundTrip] and [Conn.RoundTripWithID]; mint
// says whether the ID is replaced with a fresh one.
func (conn *Conn) roundTrip(ctx context.Context, request Frame, mint bool) (Frame, error) {
	// Refuse an oversized frame before it takes a slot or touches the socket.
	if len(request.Payload) > MaxPayload {
		return Frame{}, fmt.Errorf("%s payload of %d bytes: %w (the most one frame carries is %d)",
			request.Op, len(request.Payload), ErrFrameTooLarge, MaxPayload)
	}
	select {
	case conn.slots <- struct{}{}:
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-conn.done:
		return Frame{}, conn.err
	}
	defer func() { <-conn.slots }()

	if mint {
		id, err := NewRequestID()
		if err != nil {
			return Frame{}, err
		}
		request.ID = id
	}
	replies := make(chan Frame, 1)
	conn.mu.Lock()
	select {
	case <-conn.done:
		conn.mu.Unlock()
		return Frame{}, conn.err
	default:
	}
	// The check before the slot refuses a busy ID without waiting; this one
	// closes the gap for two callers that chose the same ID at once.
	if _, busy := conn.pending[request.ID]; busy && !mint {
		conn.mu.Unlock()
		return Frame{}, inFlightRefusal(request)
	}
	conn.pending[request.ID] = replies
	conn.mu.Unlock()
	defer conn.forget(request.ID)

	// Cancellation closes the connection, which unblocks a write and the wait
	// below alike.
	stopWatching := context.AfterFunc(ctx, func() { conn.closeWith(ErrAbandoned) })
	defer stopWatching()

	if err := conn.write(request); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Frame{}, ctxErr
		}
		return Frame{}, err
	}
	select {
	case reply := <-replies:
		return reply, nil
	case <-conn.done:
		// A reply that raced the close still counts.
		select {
		case reply := <-replies:
			return reply, nil
		default:
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Frame{}, ctxErr
		}
		return Frame{}, conn.err
	}
}

func (conn *Conn) write(frame Frame) error {
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	if err := WriteFrame(conn.raw, frame); err != nil {
		select {
		case <-conn.done:
			return conn.err
		case <-time.After(writeFailureGrace):
		}
		conn.closeWith(err)
		return err
	}
	return nil
}

func (conn *Conn) forget(id RequestID) {
	conn.mu.Lock()
	delete(conn.pending, id)
	conn.mu.Unlock()
}

func (conn *Conn) closeWith(reason error) {
	conn.closeOnce.Do(func() {
		conn.err = reason
		close(conn.done)
		_ = conn.raw.Close()
	})
}

func (conn *Conn) readLoop() {
	defer close(conn.readerDone)
	for {
		frame, err := ReadFrame(conn.raw)
		if err != nil {
			conn.closeWith(fmt.Errorf("connection to %s lost: %w", conn.raw.RemoteAddr(), err))
			return
		}
		conn.mu.Lock()
		replies, expected := conn.pending[frame.ID]
		if expected {
			delete(conn.pending, frame.ID)
		}
		conn.mu.Unlock()
		switch {
		case expected:
			replies <- frame
		case frame.ID.IsZero() && frame.Op == OpError:
			conn.closeWith(&RefusedError{Response: DecodeError(frame.Payload)})
			return
		default:
			conn.closeWith(fmt.Errorf("%s from %s with request ID %s: %w", frame.Op, conn.raw.RemoteAddr(), frame.ID, ErrUnexpectedResponse))
			return
		}
	}
}
