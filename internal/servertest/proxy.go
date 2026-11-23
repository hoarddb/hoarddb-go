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

package servertest

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// Proxy is a relay in front of a real [Server] that shows a test what a driver
// put on the wire: every connection it opened, every AUTH it sent on each one,
// and what the server answered. The server's answers are the real ones; the
// proxy only reads the frames it forwards.
//
// It terminates TLS with a key of its own, so a driver pins [Proxy.Fingerprint],
// and dials the server pinned to the server's fingerprint.
type Proxy struct {
	// Address is where a driver connects, host:port.
	Address string
	// Fingerprint is the proxy's key: the pin a driver uses for Address.
	Fingerprint string

	upstream *Server
	listener net.Listener
	group    sync.WaitGroup

	mu          sync.Mutex
	connections []*relay
	// afterRequest runs after each data request is forwarded; forgeAuths is how
	// many fresh-connection AUTH frames the proxy still answers itself.
	afterRequest func(hoarddbwire.Frame)
	forgeAuths   int
}

// AuthAttempt is one AUTH frame a driver sent and the server's answer to it.
type AuthAttempt struct {
	// Method is "password" or "token": a non-empty token makes the server
	// ignore the password (handbook/auth.md section 1).
	Method string
	// OnFreshConnection is true when the AUTH was the first frame on its
	// connection; a later one on an authenticated connection is a refresh.
	OnFreshConnection bool
	// Answer is AUTH_OK, AUTH_FAIL or ERROR; zero while no answer has arrived.
	Answer hoarddbwire.Opcode
	// Reason is the AUTH_FAIL reason.
	Reason hoarddbwire.AuthFailReason
}

// Connection is what the proxy saw on one connection a driver opened.
type Connection struct {
	// Auths are the AUTH frames on the connection, in the order they were sent.
	Auths []AuthAttempt
	// Frames counts every frame the driver sent, AUTH included.
	Frames int
	// ClosedBy is "driver", "server" or "proxy" once the connection has ended,
	// naming the side that closed it first; empty while it is open.
	ClosedBy string
	// Failure is why the proxy could not reach the server for this connection,
	// when it could not.
	Failure string
}

type relay struct {
	driverSide net.Conn
	serverSide net.Conn
	record     Connection
	pending    map[hoarddbwire.RequestID]int // request ID to index in record.Auths
	exchanges  []Exchange
	waiting    map[hoarddbwire.RequestID]int // request ID to index in exchanges
	closeOnce  sync.Once
}

// StartProxy starts a proxy in front of upstream and stops it when the test
// ends.
func StartProxy(t testing.TB, upstream *Server) *Proxy {
	t.Helper()
	certificate, fingerprint := proxyCertificate(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{hoarddbwire.ALPNv1},
	})
	if err != nil {
		t.Fatal(err)
	}
	proxy := &Proxy{Address: listener.Addr().String(), Fingerprint: fingerprint, upstream: upstream, listener: listener}
	proxy.group.Add(1)
	go proxy.accept()
	t.Cleanup(proxy.stop)
	return proxy
}

func proxyCertificate(t testing.TB) (tls.Certificate, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "hoarddb-servertest-proxy"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, trust.Fingerprint(public)
}

func (proxy *Proxy) accept() {
	defer proxy.group.Done()
	for {
		accepted, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		connection := &relay{driverSide: accepted, pending: map[hoarddbwire.RequestID]int{}, waiting: map[hoarddbwire.RequestID]int{}}
		proxy.mu.Lock()
		proxy.connections = append(proxy.connections, connection)
		proxy.mu.Unlock()
		proxy.group.Add(1)
		go proxy.serve(connection)
	}
}

// serve dials the server and relays frames both ways until either side ends.
func (proxy *Proxy) serve(connection *relay) {
	defer proxy.group.Done()
	config, err := trust.NewTLSConfig(proxy.upstream.Address, trust.Policy{Pin: proxy.upstream.Fingerprint})
	if err != nil {
		proxy.fail(connection, fmt.Errorf("trust policy for %s: %w", proxy.upstream.Address, err))
		return
	}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: config}
	upstream, err := dialer.DialContext(context.Background(), "tcp", proxy.upstream.Address)
	if err != nil {
		proxy.fail(connection, fmt.Errorf("dialling the server at %s: %w", proxy.upstream.Address, err))
		return
	}
	proxy.mu.Lock()
	connection.serverSide = upstream
	proxy.mu.Unlock()

	var directions sync.WaitGroup
	directions.Add(2)
	go func() {
		defer directions.Done()
		proxy.forwardRequests(connection)
	}()
	go func() {
		defer directions.Done()
		proxy.forwardAnswers(connection)
	}()
	directions.Wait()
}

func (proxy *Proxy) forwardRequests(connection *relay) {
	for {
		frame, err := hoarddbwire.ReadFrame(connection.driverSide)
		if err != nil {
			proxy.end(connection, "driver")
			return
		}
		proxy.mu.Lock()
		connection.record.Frames++
		if frame.Op == hoarddbwire.OpAuth && connection.record.Frames == 1 && proxy.forgeAuths > 0 {
			proxy.forgeAuths--
			connection.record.Auths = append(connection.record.Auths, AuthAttempt{Method: "forged by the proxy", OnFreshConnection: true, Answer: hoarddbwire.OpAuthOK})
			proxy.mu.Unlock()
			if err := proxy.forgeAuthOK(connection, frame); err != nil {
				proxy.end(connection, "driver")
				return
			}
			continue
		}
		hook := proxy.afterRequest
		if frame.Op == hoarddbwire.OpAuth {
			var request hoarddbwire.AuthRequest
			method := "unreadable"
			if codec.Unmarshal(frame.Payload, &request) == nil {
				method = "password"
				if request.Token != "" {
					method = "token"
				}
			}
			connection.pending[frame.ID] = len(connection.record.Auths)
			connection.record.Auths = append(connection.record.Auths, AuthAttempt{
				Method: method, OnFreshConnection: connection.record.Frames == 1,
			})
		} else {
			connection.waiting[frame.ID] = len(connection.exchanges)
			connection.exchanges = append(connection.exchanges, Exchange{Request: frame})
		}
		proxy.mu.Unlock()
		if err := hoarddbwire.WriteFrame(connection.serverSide, frame); err != nil {
			proxy.end(connection, "server")
			return
		}
		if hook != nil && frame.Op != hoarddbwire.OpAuth {
			hook(frame)
		}
	}
}

// forgeAuthOK answers an AUTH on the proxy's own authority, without the server
// seeing it, so the server's side of the connection stays unauthenticated.
func (proxy *Proxy) forgeAuthOK(connection *relay, request hoarddbwire.Frame) error {
	payload, err := codec.Marshal(hoarddbwire.AuthResponse{Success: true, ProtocolVersion: 1, ServerVersion: "servertest-proxy", Capabilities: []string{}})
	if err != nil {
		return fmt.Errorf("encoding the forged AUTH_OK: %w", err)
	}
	if err := hoarddbwire.WriteFrame(connection.driverSide, hoarddbwire.Frame{Op: hoarddbwire.OpAuthOK, ID: request.ID, Payload: payload}); err != nil {
		return fmt.Errorf("writing the forged AUTH_OK: %w", err)
	}
	return nil
}

// AfterEachRequest makes the proxy call hook with every data request (not
// AUTH) right after forwarding it to the server, before the server's answer is
// relayed: the moment to stop a server "during" a request. hook runs on the
// proxy's goroutine for that connection.
func (proxy *Proxy) AfterEachRequest(hook func(hoarddbwire.Frame)) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	proxy.afterRequest = hook
}

// ForgeNextAuth makes the proxy answer the next AUTH that opens a connection
// with an AUTH_OK of its own, and not forward it. The driver believes the
// connection is logged in; the server has seen no AUTH on it, so it answers the
// first request NOT_AUTHENTICATED (handbook/wire.md section 8). The forged
// answer advertises no capabilities and mints no token.
func (proxy *Proxy) ForgeNextAuth() {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	proxy.forgeAuths++
}

func (proxy *Proxy) forwardAnswers(connection *relay) {
	for {
		frame, err := hoarddbwire.ReadFrame(connection.serverSide)
		if err != nil {
			proxy.end(connection, "server")
			return
		}
		proxy.mu.Lock()
		if index, isAuth := connection.pending[frame.ID]; isAuth {
			delete(connection.pending, frame.ID)
			connection.record.Auths[index].Answer = frame.Op
			if frame.Op == hoarddbwire.OpAuthFail {
				connection.record.Auths[index].Reason = hoarddbwire.DecodeAuthFail(frame.Payload).Reason
			}
		} else if index, isData := connection.waiting[frame.ID]; isData {
			delete(connection.waiting, frame.ID)
			connection.exchanges[index].Response, connection.exchanges[index].Answered = frame, true
		}
		proxy.mu.Unlock()
		if err := hoarddbwire.WriteFrame(connection.driverSide, frame); err != nil {
			proxy.end(connection, "driver")
			return
		}
	}
}

// fail records why a connection could not be relayed, for Describe, and ends it.
func (proxy *Proxy) fail(connection *relay, cause error) {
	proxy.mu.Lock()
	connection.record.Failure = cause.Error()
	proxy.mu.Unlock()
	proxy.end(connection, "proxy")
}

// end closes both sides once, recording which side ended the connection first.
func (proxy *Proxy) end(connection *relay, closedBy string) {
	connection.closeOnce.Do(func() {
		proxy.mu.Lock()
		connection.record.ClosedBy = closedBy
		upstream := connection.serverSide
		proxy.mu.Unlock()
		_ = connection.driverSide.Close()
		if upstream != nil {
			_ = upstream.Close()
		}
	})
}

// Connections returns a copy of what the proxy has seen, one entry per
// connection in the order they were accepted.
func (proxy *Proxy) Connections() []Connection {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	seen := make([]Connection, len(proxy.connections))
	for index, connection := range proxy.connections {
		seen[index] = connection.record
		seen[index].Auths = slices.Clone(connection.record.Auths)
	}
	return seen
}

// AwaitClosed waits until connection index (0-based) has ended and returns its
// record. It fails the test after timeout.
func (proxy *Proxy) AwaitClosed(t testing.TB, index int, timeout time.Duration) Connection {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		seen := proxy.Connections()
		if index < len(seen) && seen[index].ClosedBy != "" {
			return seen[index]
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection %d through the proxy was still open after %v: %+v", index, timeout, seen)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// DropAll closes every open connection on both sides, as a server does to a
// user it dropped or a network does when it fails.
func (proxy *Proxy) DropAll() {
	proxy.mu.Lock()
	open := slices.Clone(proxy.connections)
	proxy.mu.Unlock()
	for _, connection := range open {
		proxy.end(connection, "proxy")
	}
}

func (proxy *Proxy) stop() {
	_ = proxy.listener.Close()
	proxy.DropAll()
	proxy.group.Wait()
}

// PasswordAuthsPerConnection reports the most password AUTH frames any one
// connection carried. A server accepts one per connection, spent even when it
// fails (handbook/auth.md section 2), so more than one is a driver bug.
func PasswordAuthsPerConnection(seen []Connection) int {
	most := 0
	for _, connection := range seen {
		count := 0
		for _, attempt := range connection.Auths {
			if attempt.Method == "password" {
				count++
			}
		}
		most = max(most, count)
	}
	return most
}

// Describe renders what the proxy saw, for a failure message.
func Describe(seen []Connection) string {
	text := fmt.Sprintf("%d connection(s)", len(seen))
	for index, connection := range seen {
		text += fmt.Sprintf("\n  #%d: %d frame(s), closed by %q", index+1, connection.Frames, connection.ClosedBy)
		if connection.Failure != "" {
			text += ", relay failed: " + connection.Failure
		}
		for _, attempt := range connection.Auths {
			answer := attempt.Answer.String()
			if attempt.Answer == hoarddbwire.OpAuthFail {
				answer += " " + attempt.Reason.String()
			}
			text += fmt.Sprintf("\n      AUTH by %s (first frame: %v) -> %s", attempt.Method, attempt.OnFreshConnection, answer)
		}
	}
	return text
}

// Exchange is one data request a driver sent through the proxy and the
// server's real answer to it; AUTH frames are not data and are in
// [Connection.Auths] instead.
type Exchange struct {
	// Request and Response are the frames as they crossed the proxy.
	Request, Response hoarddbwire.Frame
	// Answered is false until the server's answer arrived.
	Answered bool
}

// Exchanges returns the data requests seen so far, in the order the driver sent
// them across all connections.
func (proxy *Proxy) Exchanges() []Exchange {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	var seen []Exchange
	for _, connection := range proxy.connections {
		seen = append(seen, connection.exchanges...)
	}
	return seen
}
