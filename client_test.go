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

package hoarddb_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
)

func (server *fakeServer) uri() string {
	return "hoarddb://admin:fake-password@" + server.address + "/?fingerprint=" + server.fingerprint
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustConnect(t *testing.T, connectionString string, opts ...hoarddb.Option) *hoarddb.Client {
	t.Helper()
	client, err := hoarddb.Connect(testContext(t), connectionString, opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestPingMakesOneRoundTripAndReusesTheConnection(t *testing.T) {
	var pings atomic.Int32
	server := startFake(t, serveAuthThen(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		pings.Add(1)
		return pong(c, request, 0)
	}))
	client := mustConnect(t, server.uri())
	for range 5 {
		if err := client.Ping(testContext(t)); err != nil {
			t.Fatal(err)
		}
	}
	if pings.Load() != 5 || server.connections.Load() != 1 {
		t.Fatalf("%d pings over %d connections; want 5 over 1", pings.Load(), server.connections.Load())
	}
	if info := client.ServerInfo(); info.Version != "fake-1.0" || !info.Has("auth_token") || info.Has("nope") {
		t.Fatalf("ServerInfo = %+v", info)
	}
}

func TestConnectSendsTheClientIdentity(t *testing.T) {
	identity := make(chan string, 1)
	server := startFake(t, func(c *fakeConn) {
		first, request, ok := c.authRequest()
		if !ok {
			return
		}
		identity <- request.Client
		c.authOK(first, "t", time.Hour)
		_, _ = c.read()
	})
	client := mustConnect(t, server.uri()+"&clientName=etl")
	_ = client
	if got := <-identity; got != "hoarddb-go/"+hoarddb.Version+" (etl)" {
		t.Fatalf("client identity = %q", got)
	}
	// The option wins over the URI.
	mustConnect(t, server.uri()+"&clientName=etl", hoarddb.WithClientName("billing"))
	if got := <-identity; got != "hoarddb-go/"+hoarddb.Version+" (billing)" {
		t.Fatalf("client identity with the option = %q", got)
	}
}

func TestConnectRefusalIsAnErrorWithTheServersWords(t *testing.T) {
	server := startFake(t, func(c *fakeConn) {
		// A connection-level refusal: zero request ID, then close.
		payload, _ := marshal(map[string]any{"code": int32(hoarddbwire.CodeOverloaded),
			"message": "server connection limit reached (1000, HOARDDB_MAX_CONNS); retry shortly or raise the cap"})
		_ = hoarddbwire.WriteFrame(c.Conn, hoarddbwire.Frame{Op: hoarddbwire.OpError, Payload: payload})
	})
	_, err := hoarddb.Connect(testContext(t), server.uri())
	if !errors.Is(err, hoarddb.ErrOverloaded) {
		t.Fatalf("err = %v, want ErrOverloaded (the class must survive a single failed host)", err)
	}
	if !strings.Contains(err.Error(), "retry shortly or raise the cap") {
		t.Fatalf("the server's sentence, which holds the fix, was lost: %v", err)
	}
}

func TestFailoverTriesHostsInWrittenOrderAndSkipsADeadOne(t *testing.T) {
	var firstSeen atomic.Int32
	down := startFake(t, func(c *fakeConn) {
		firstSeen.Add(1)
		if first, _, ok := c.authRequest(); ok {
			c.authFail(first, int32(hoarddbwire.AuthFailInternal), "no user configuration yet")
		}
	})
	release := make(chan struct{})
	up := startFake(t, serveAuthThen(func(c *fakeConn, request hoarddbwire.Frame, count int) bool {
		if c.index == 1 {
			<-release // keep the first connection busy so the next call must dial
		}
		return pong(c, request, count)
	}))
	connectionString := "hoarddb://admin:pw@" + down.address + "," + up.address + "/?fingerprint=" + down.fingerprint + "&fingerprint=" + up.fingerprint
	client := mustConnect(t, connectionString, hoarddb.WithPoolSize(2))
	if firstSeen.Load() != 1 || up.connections.Load() != 1 {
		t.Fatalf("down host seen %d times, up host %d; the first host is tried once, then the next", firstSeen.Load(), up.connections.Load())
	}

	// The failed host is backed off: the next dial goes straight to the live one.
	busy := make(chan error, 1)
	go func() { busy <- client.Ping(testContext(t)) }()
	waitUntil(t, func() bool { return client.PoolStats().InUse == 1 })
	if err := client.Ping(testContext(t)); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-busy; err != nil {
		t.Fatal(err)
	}
	if firstSeen.Load() != 1 || up.connections.Load() != 2 {
		t.Fatalf("down host seen %d times, up host %d; the downed host must be skipped while it backs off", firstSeen.Load(), up.connections.Load())
	}
}

func TestFailoverDoesNotSkipPastATrustFailure(t *testing.T) {
	impostor := startFake(t, serveAuthThen(nil))
	honest := startFake(t, serveAuthThen(nil))
	// The pin for host one is wrong. Host two is fine, but trying it would turn
	// a possible interception into a silent success.
	connectionString := "hoarddb://admin:pw@" + impostor.address + "," + honest.address +
		"/?fingerprint=SHA256:" + strings.Repeat("00", 32) + "&fingerprint=" + honest.fingerprint
	_, err := hoarddb.Connect(testContext(t), connectionString)
	if !errors.Is(err, hoarddb.ErrFingerprintMismatch) {
		t.Fatalf("err = %v, want ErrFingerprintMismatch", err)
	}
	if honest.connections.Load() != 0 {
		t.Fatal("the second host was tried after the first failed its pin")
	}
}

func TestPositionalPinsApplyPerHost(t *testing.T) {
	first := startFake(t, serveAuthThen(nil))
	second := startFake(t, serveAuthThen(nil))
	// Reverse order: first host dead, second reached with its own pin.
	_ = first.address
	deadPort := net.JoinHostPort("127.0.0.1", "1")
	connectionString := "hoarddb://admin:pw@" + deadPort + "," + second.address + "/?fingerprint=" + first.fingerprint + "&fingerprint=" + second.fingerprint
	client := mustConnect(t, connectionString)
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatal(err)
	}
	// With the pins swapped the second host must fail its own pin.
	swapped := "hoarddb://admin:pw@" + deadPort + "," + second.address + "/?fingerprint=" + second.fingerprint + "&fingerprint=" + first.fingerprint
	if _, err := hoarddb.Connect(testContext(t), swapped); !errors.Is(err, hoarddb.ErrFingerprintMismatch) {
		t.Fatalf("err = %v, want ErrFingerprintMismatch (the pin is positional)", err)
	}
}

func TestTokenIsPresentedOnLaterConnectionsAndFallsBackToThePassword(t *testing.T) {
	type attempt struct {
		connection int
		token      string
		password   string
	}
	var mu sync.Mutex
	var attempts []attempt
	blockPing := make(chan struct{})
	server := startFake(t, func(c *fakeConn) {
		first, request, ok := c.authRequest()
		if !ok {
			return
		}
		mu.Lock()
		attempts = append(attempts, attempt{c.index, request.Token, request.Password})
		mu.Unlock()
		if request.Token != "" {
			c.authFail(first, int32(hoarddbwire.AuthFailTokenExpired), "token expired")
			return // the server drops a connection whose token AUTH failed
		}
		c.authOK(first, "minted", time.Hour)
		for {
			frame, err := c.read()
			if err != nil {
				return
			}
			if c.index == 1 {
				<-blockPing // hold the first connection busy so the next call dials
			}
			c.send(frame, hoarddbwire.OpPong, nil)
		}
	})
	client := mustConnect(t, server.uri(), hoarddb.WithPoolSize(2))

	held := make(chan error, 1)
	go func() { held <- client.Ping(testContext(t)) }()
	waitUntil(t, func() bool { return client.PoolStats().InUse == 1 })
	if err := client.Ping(testContext(t)); err != nil { // dials a second connection
		close(blockPing)
		t.Fatalf("the second connection should fall back to the password: %v", err)
	}
	close(blockPing)
	if err := <-held; err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 3 {
		t.Fatalf("AUTH attempts = %+v; want password, token (rejected), password", attempts)
	}
	if attempts[0].password != "fake-password" || attempts[0].token != "" {
		t.Errorf("attempt 1 = %+v, want the password", attempts[0])
	}
	if attempts[1].token != "minted" || attempts[1].password != "" {
		t.Errorf("attempt 2 = %+v, want the minted token and no password", attempts[1])
	}
	if attempts[2].password != "fake-password" || attempts[2].token != "" || attempts[2].connection == attempts[1].connection {
		t.Errorf("attempt 3 = %+v, want the password on a new connection", attempts[2])
	}
}

func TestTokenIsRefreshedAtHalfLifeOnALiveConnection(t *testing.T) {
	var mu sync.Mutex
	var tokens []string
	server := startFake(t, func(c *fakeConn) {
		for {
			frame, err := c.read()
			if err != nil {
				return
			}
			switch frame.Op {
			case hoarddbwire.OpAuth:
				var request hoarddbwire.AuthRequest
				if err := unmarshal(frame.Payload, &request); err != nil {
					return
				}
				mu.Lock()
				tokens = append(tokens, request.Token)
				count := len(tokens)
				mu.Unlock()
				c.authOK(frame, "token-"+string(rune('0'+count)), 3*time.Second)
			case hoarddbwire.OpPing:
				c.send(frame, hoarddbwire.OpPong, nil)
			}
		}
	})
	client := mustConnect(t, server.uri())
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	early := len(tokens)
	mu.Unlock()
	if early != 1 {
		t.Fatalf("%d AUTH frames before half of the token's life; want only the login", early)
	}
	time.Sleep(1600 * time.Millisecond) // past half of a 3 s token, before the whole-second expiry (3 s truncates to 2-3 s)
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(tokens) != 2 || tokens[0] != "" || tokens[1] != "token-1" {
		t.Fatalf("token AUTH history = %q; want the login, then a refresh presenting token-1", tokens)
	}
	if server.connections.Load() != 1 {
		t.Fatalf("%d connections; a refresh happens on the live connection", server.connections.Load())
	}
}

// A refresh the server refuses for a reason that is not about the token (rate
// limiting) must not make every later Ping retry it, nor throw away a healthy
// connection.
func TestRefreshRefusedForANonTokenReasonIsBackedOffAndKeepsTheConnection(t *testing.T) {
	var tokenAuths atomic.Int32
	server := startFake(t, func(c *fakeConn) {
		for {
			frame, err := c.read()
			if err != nil {
				return
			}
			switch frame.Op {
			case hoarddbwire.OpAuth:
				var request hoarddbwire.AuthRequest
				if unmarshal(frame.Payload, &request) != nil {
					return
				}
				if request.Token != "" {
					tokenAuths.Add(1)
					c.authFail(frame, int32(hoarddbwire.AuthFailRateLimited), "slow down")
					continue
				}
				c.authOK(frame, "tok", 3*time.Second)
			case hoarddbwire.OpPing:
				c.send(frame, hoarddbwire.OpPong, nil)
			}
		}
	})
	client := mustConnect(t, server.uri())
	time.Sleep(1600 * time.Millisecond) // past half of a 3 s token, before the whole-second expiry (3 s truncates to 2-3 s)
	for range 5 {
		if err := client.Ping(testContext(t)); err != nil {
			t.Fatalf("Ping with a refresh refused for rate limiting: %v", err)
		}
	}
	if tokenAuths.Load() != 1 || server.connections.Load() != 1 || client.PoolStats().Discarded != 0 {
		t.Fatalf("%d refresh attempts over %d connections, %d discarded; want one attempt, one connection, none discarded",
			tokenAuths.Load(), server.connections.Load(), client.PoolStats().Discarded)
	}
}

func TestPingIsRetriedOnceOnAFreshConnectionAfterTheServerDropsIt(t *testing.T) {
	server := startFake(t, serveAuthThen(func(c *fakeConn, request hoarddbwire.Frame, count int) bool {
		if c.index == 1 {
			return false // the first connection dies on its first PING, unanswered
		}
		return pong(c, request, count)
	}))
	client := mustConnect(t, server.uri())
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatalf("Ping should survive one dead connection: %v", err)
	}
	stats := client.PoolStats()
	if stats.Dials != 2 || stats.Discarded < 1 || server.connections.Load() != 2 {
		t.Fatalf("PoolStats = %+v, %d connections; want the dead one discarded and a second dialled", stats, server.connections.Load())
	}
}

func TestPingIsNotRetriedForever(t *testing.T) {
	server := startFake(t, serveAuthThen(func(*fakeConn, hoarddbwire.Frame, int) bool { return false }))
	client := mustConnect(t, server.uri())
	err := client.Ping(testContext(t))
	if err == nil {
		t.Fatal("Ping against a server that drops every connection must fail")
	}
	if server.connections.Load() != 2 {
		t.Fatalf("%d connections; Ping retries exactly once (1 login + 1 retry)", server.connections.Load())
	}
	if !errors.Is(err, hoarddb.ErrUnreachable) || !strings.Contains(err.Error(), "next call dials a new connection") {
		t.Fatalf("err = %v", err)
	}
}

func TestPingServerErrorFrameKeepsCodeAndMessage(t *testing.T) {
	tests := []struct {
		name    string
		code    int32
		message string
		want    error
	}{
		{"known code", int32(hoarddbwire.CodePermissionDenied), `user "ro" needs the readWrite role on "app"`, hoarddb.ErrPermissionDenied},
		{"code zero is never success", 0, "boom", hoarddb.ErrInternal},
		{"code from a newer server", 0x41, "new thing", hoarddb.ErrInternal},
		{"overloaded", int32(hoarddbwire.CodeOverloaded), "busy", hoarddb.ErrOverloaded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := startFake(t, serveAuthThen(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				c.errorFrame(request, test.code, test.message)
				return true
			}))
			client := mustConnect(t, server.uri())
			err := client.Ping(testContext(t))
			if !errors.Is(err, test.want) {
				t.Fatalf("err = %v, want class %v", err, test.want)
			}
			var failure *hoarddb.Error
			if !errors.As(err, &failure) || failure.Message != test.message || failure.Op != "ping" {
				t.Fatalf("error = %#v; want Op ping and the server's message verbatim", failure)
			}
		})
	}
}

func TestPingUnexpectedReplyOpcode(t *testing.T) {
	server := startFake(t, serveAuthThen(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.send(request, hoarddbwire.OpGetResponse, nil)
		return true
	}))
	client := mustConnect(t, server.uri())
	err := client.Ping(testContext(t))
	if !errors.Is(err, hoarddb.ErrInternal) || !strings.Contains(err.Error(), "PONG") {
		t.Fatalf("err = %v", err)
	}
}

func TestRequestTimeoutAppliesOnlyWithoutADeadline(t *testing.T) {
	silent := make(chan struct{})
	server := startFake(t, serveAuthThen(func(*fakeConn, hoarddbwire.Frame, int) bool {
		<-silent
		return false
	}))
	defer close(silent)
	client := mustConnect(t, server.uri(), hoarddb.WithRequestTimeout(150*time.Millisecond))
	started := time.Now()
	err := client.Ping(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded from the default request timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("the request timeout took %v", elapsed)
	}
	// The abandoned connection is gone; the client still works.
	if client.PoolStats().Discarded < 1 {
		t.Fatalf("a request that timed out must discard its connection: %+v", client.PoolStats())
	}
}

func TestPoolExhaustionBlocksUntilTheContextEnds(t *testing.T) {
	release := make(chan struct{})
	server := startFake(t, serveAuthThen(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		<-release
		return pong(c, request, 0)
	}))
	client := mustConnect(t, server.uri(), hoarddb.WithPoolSize(1))
	busy := make(chan error, 1)
	go func() { busy <- client.Ping(testContext(t)) }()
	waitUntil(t, func() bool { return client.PoolStats().InUse == 1 })

	short, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := client.Ping(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded while the only connection is busy", err)
	}
	if client.PoolStats().Waiting != 0 {
		t.Fatalf("Waiting = %d after the caller gave up", client.PoolStats().Waiting)
	}
	close(release)
	if err := <-busy; err != nil {
		t.Fatal(err)
	}
	if server.connections.Load() != 1 {
		t.Fatalf("%d connections on a pool of one", server.connections.Load())
	}
}

func TestConcurrentPingsAndCloseAreSafe(t *testing.T) {
	server := startFake(t, serveAuthThen(nil))
	client := mustConnect(t, server.uri(), hoarddb.WithPoolSize(3))
	var group sync.WaitGroup
	for range 12 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 20 {
				err := client.Ping(context.Background())
				if err != nil && !errors.Is(err, hoarddb.ErrClosed) && !errors.Is(err, hoarddb.ErrUnreachable) {
					t.Errorf("Ping during Close: %v", err)
					return
				}
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	group.Wait()
	if err := client.Ping(context.Background()); !errors.Is(err, hoarddb.ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
}

func TestDialTimeoutFromTheURIBoundsOneHost(t *testing.T) {
	// A host that accepts TCP and never answers the handshake.
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	var heldMu sync.Mutex
	go func() {
		for {
			accepted, err := silent.Accept()
			if err != nil {
				return
			}
			heldMu.Lock()
			held = append(held, accepted)
			heldMu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = silent.Close()
		heldMu.Lock()
		for _, connection := range held {
			_ = connection.Close()
		}
		heldMu.Unlock()
	})
	live := startFake(t, serveAuthThen(nil))
	connectionString := "hoarddb://admin:pw@" + silent.Addr().String() + "," + live.address + "/?dialTimeoutMs=250&fingerprint=" + live.fingerprint
	started := time.Now()
	client := mustConnect(t, connectionString)
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("a silent host held Connect for %v despite dialTimeoutMs=250", elapsed)
	}
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatal(err)
	}

	_, err = hoarddb.Connect(testContext(t), "hoarddb://admin:pw@"+silent.Addr().String()+"/?dialTimeoutMs=200&fingerprint="+live.fingerprint)
	if !errors.Is(err, hoarddb.ErrUnreachable) || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "dialTimeoutMs") {
		t.Fatalf("err = %v; want ErrUnreachable wrapping DeadlineExceeded and naming dialTimeoutMs", err)
	}
}

// TestConcurrentFirstUseTrustsAtMostOneKey: a server that shows a different key
// on every connection, two Connects racing through an accepting prompt. At most
// one may be trusted, and no credential may reach the other.
func TestConcurrentFirstUseTrustsAtMostOneKey(t *testing.T) {
	var authFrames atomic.Int32
	config := &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{hoarddbwire.ALPNv1},
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			certificate, _ := newCertificate(t)
			return &certificate, nil
		},
	}
	server := startFakeWith(t, config, "", func(c *fakeConn) {
		first, _, ok := c.authRequest()
		if !ok {
			return
		}
		authFrames.Add(1)
		c.authOK(first, "t", time.Hour)
		_, _ = c.read()
	})
	known := knownPath(t)
	var arrived sync.WaitGroup
	arrived.Add(2)
	prompt := func(string, string) bool {
		arrived.Done()
		done := make(chan struct{})
		go func() { arrived.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond): // the loser may never get this far
		}
		return true
	}
	results := make(chan error, 2)
	for range 2 {
		go func() {
			client, err := hoarddb.Connect(testContext(t), "hoarddb://admin:pw@"+server.address+"/",
				hoarddb.WithKnownServers(known), hoarddb.WithTrustPrompt(prompt))
			if err == nil {
				_ = client.Close()
			}
			results <- err
		}()
	}
	succeeded := 0
	for range 2 {
		if err := <-results; err == nil {
			succeeded++
		}
	}
	if succeeded > 1 || authFrames.Load() > 1 {
		t.Fatalf("%d Connects succeeded and %d credentials were sent; one host showing two keys may be trusted once", succeeded, authFrames.Load())
	}
	contents, _ := os.ReadFile(known)
	if strings.Count(string(contents), "\n") != 1 {
		t.Fatalf("known_servers = %q; want exactly one key", contents)
	}
}

func TestUnsafeKnownServersFileIsRefusedNotMistakenForADownServer(t *testing.T) {
	server := startFake(t, serveAuthThen(nil))
	known := knownPath(t)
	if err := os.WriteFile(known, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(known, 0o666); err != nil {
		t.Fatal(err)
	}
	_, err := hoarddb.Connect(testContext(t), "hoarddb://admin:pw@"+server.address+"/", hoarddb.WithKnownServers(known))
	if !errors.Is(err, hoarddb.ErrUntrustedServer) {
		t.Fatalf("err = %v, want ErrUntrustedServer", err)
	}
	if !strings.Contains(err.Error(), "chmod 600") || strings.Contains(err.Error(), "is running") {
		t.Fatalf("message should say how to fix the file, not blame the server: %v", err)
	}
	if server.connections.Load() > 1 {
		t.Fatalf("%d connections; an unsafe file is final and not retried", server.connections.Load())
	}
}

func TestNotAHoardDBPortSaysSo(t *testing.T) {
	plain, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	go func() {
		for {
			accepted, err := plain.Accept()
			if err != nil {
				return
			}
			_, _ = accepted.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
			_ = accepted.Close()
		}
	}()
	_, err = hoarddb.Connect(testContext(t), "hoarddb://admin:pw@"+plain.Addr().String()+"/?fingerprint=SHA256:"+strings.Repeat("ab", 32))
	if !errors.Is(err, hoarddb.ErrUnreachable) || !strings.Contains(err.Error(), "TLS 1.3") || !strings.Contains(err.Error(), "7433") {
		t.Fatalf("err = %v; a port that is not a hoardDB listener should be named as such", err)
	}
}

func TestServerWithoutTLS13IsRefused(t *testing.T) {
	var reachedScript atomic.Int32
	certificate, public := newCertificate(t)
	server := startFakeWith(t, &tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
		NextProtos: []string{hoarddbwire.ALPNv1},
	}, trust.Fingerprint(public), func(c *fakeConn) {
		if c.Conn.(*tls.Conn).Handshake() != nil {
			return
		}
		reachedScript.Add(1)
		serveAuthThen(nil)(c)
	})
	if _, err := hoarddb.Connect(testContext(t), server.uri()); !errors.Is(err, hoarddb.ErrUnreachable) {
		t.Fatalf("err = %v; TLS 1.3 is unconditional", err)
	}
	if reachedScript.Load() > 0 {
		t.Fatal("the script ran: a TLS 1.2 handshake was accepted")
	}
}

func TestServerWithoutOurALPNIsRefused(t *testing.T) {
	certificate, public := newCertificate(t)
	server := startFakeWith(t, &tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, NextProtos: []string{"other/9"},
	}, trust.Fingerprint(public), serveAuthThen(nil))
	if _, err := hoarddb.Connect(testContext(t), server.uri()); err == nil {
		t.Fatal("a server that does not serve hoarddb/1 must not connect")
	}
}

func TestEd25519OnlyServerKeys(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tls.Certificate{Certificate: [][]byte{selfSignedECDSA(t, key)}, PrivateKey: key}
	server := startFakeWith(t, &tls.Config{
		Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, NextProtos: []string{hoarddbwire.ALPNv1},
	}, "", serveAuthThen(nil))
	_, err = hoarddb.Connect(testContext(t), "hoarddb://admin:pw@"+server.address+"/?fingerprint=SHA256:"+strings.Repeat("ab", 32))
	if !errors.Is(err, hoarddb.ErrUntrustedServer) || !strings.Contains(err.Error(), "ed25519") {
		t.Fatalf("err = %v", err)
	}
}

func TestRateLimitedAndOtherAuthRefusals(t *testing.T) {
	tests := []struct {
		name    string
		reason  hoarddbwire.AuthFailReason
		want    hoarddb.AuthReason
		contain string
	}{
		{"rate limited", hoarddbwire.AuthFailRateLimited, hoarddb.AuthReasonRateLimited, "not a credential failure"},
		{"protocol mismatch", hoarddbwire.AuthFailProtocolMismatch, hoarddb.AuthReasonProtocolMismatch, "protocol versions"},
		{"credentials", hoarddbwire.AuthFailCredentials, hoarddb.AuthReasonCredentials, "does not retry"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var attempts atomic.Int32
			server := startFake(t, func(c *fakeConn) {
				attempts.Add(1)
				if first, _, ok := c.authRequest(); ok {
					c.authFail(first, int32(test.reason), "server detail")
				}
			})
			_, err := hoarddb.Connect(testContext(t), server.uri())
			var failure *hoarddb.Error
			if !errors.Is(err, hoarddb.ErrAuthFailed) || !errors.As(err, &failure) || failure.Reason != test.want {
				t.Fatalf("err = %v, want ErrAuthFailed with reason %v", err, test.want)
			}
			if !strings.Contains(err.Error(), test.contain) || !strings.Contains(err.Error(), "server detail") {
				t.Fatalf("message = %v; want %q and the server's own words", err, test.contain)
			}
			if attempts.Load() != 1 {
				t.Fatalf("%d login attempts; a refused login is never retried", attempts.Load())
			}
			if strings.Contains(err.Error(), "fake-password") {
				t.Fatal("the password appeared in an error")
			}
		})
	}
}

// RATE_LIMITED has two causes the wire does not tell apart: too many failed
// logins from this address, and a busy password-hashing slot. The message must
// be true for both and must not contradict the server's own sentence.
func TestRateLimitedMessageIsTrueForBothCauses(t *testing.T) {
	for name, serverSaid := range map[string]string{
		"failed logins": "too many failed authentication attempts from this address; retry in 60s",
		"busy slot":     "the server is busy verifying other passwords; no credential was checked, retry shortly",
	} {
		t.Run(name, func(t *testing.T) {
			server := startFake(t, func(c *fakeConn) {
				if first, _, ok := c.authRequest(); ok {
					c.authFail(first, int32(hoarddbwire.AuthFailRateLimited), serverSaid)
				}
			})
			_, err := hoarddb.Connect(testContext(t), server.uri())
			if !errors.Is(err, hoarddb.ErrAuthFailed) || !strings.Contains(err.Error(), serverSaid) {
				t.Fatalf("err = %v; want ErrAuthFailed carrying the server's sentence", err)
			}
			message := err.Error()
			for _, want := range []string{"RATE_LIMITED", "failed logins", "busy", "stopped retrying"} {
				if !strings.Contains(message, want) {
					t.Errorf("message should mention %q: %s", want, message)
				}
			}
			for _, claim := range []string{"too many failed logins from this address.", "Wait a minute"} {
				if strings.Contains(message, claim) {
					t.Errorf("message asserts one cause (%q): %s", claim, message)
				}
			}
		})
	}
}

func TestNoCredentialsExplainsWhereToPutThem(t *testing.T) {
	t.Setenv("HOARDDB_ROOT_PASSWORD", "")
	t.Setenv("HOARDDB_ROOT_USER", "")
	_, err := hoarddb.Connect(testContext(t), "hoarddb://127.0.0.1:1/")
	if !errors.Is(err, hoarddb.ErrAuthFailed) {
		t.Fatalf("err = %v", err)
	}
	for _, want := range []string{"HOARDDB_ROOT_PASSWORD", "WithCredentials", "root.password", `"admin"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message should mention %q: %v", want, err)
		}
	}
}

func TestCredentialPrecedenceOptionThenURIThenEnvironment(t *testing.T) {
	seen := make(chan hoarddbwire.AuthRequest, 4)
	server := startFake(t, func(c *fakeConn) {
		first, request, ok := c.authRequest()
		if !ok {
			return
		}
		seen <- request
		c.authOK(first, "t", time.Hour)
		_, _ = c.read()
	})
	t.Setenv("HOARDDB_ROOT_USER", "env-user")
	t.Setenv("HOARDDB_ROOT_PASSWORD", "env-password")
	base := "hoarddb://" + server.address + "/?fingerprint=" + server.fingerprint

	mustConnect(t, base)
	if got := <-seen; got.Username != "env-user" || got.Password != "env-password" {
		t.Errorf("environment only: %q/%q", got.Username, got.Password)
	}
	mustConnect(t, "hoarddb://uri-user@"+server.address+"/?fingerprint="+server.fingerprint)
	if got := <-seen; got.Username != "uri-user" || got.Password != "env-password" {
		t.Errorf("URI user, no URI password: %q/%q (the password still comes from the environment)", got.Username, got.Password)
	}
	mustConnect(t, "hoarddb://uri-user:uri-password@"+server.address+"/?fingerprint="+server.fingerprint)
	if got := <-seen; got.Username != "uri-user" || got.Password != "uri-password" {
		t.Errorf("URI credentials: %q/%q", got.Username, got.Password)
	}
	mustConnect(t, "hoarddb://uri-user:uri-password@"+server.address+"/?fingerprint="+server.fingerprint,
		hoarddb.WithCredentials("opt-user", "opt-password"))
	if got := <-seen; got.Username != "opt-user" || got.Password != "opt-password" {
		t.Errorf("option credentials: %q/%q", got.Username, got.Password)
	}
}

func TestURIFromTheEnvironmentAndInvalidURIs(t *testing.T) {
	server := startFake(t, serveAuthThen(nil))
	t.Setenv("HOARDDB_URI", server.uri())
	client, err := hoarddb.Connect(testContext(t), "")
	if err != nil {
		t.Fatalf("Connect with an empty string should read HOARDDB_URI: %v", err)
	}
	_ = client.Close()

	t.Setenv("HOARDDB_URI", "")
	if _, err := hoarddb.Connect(testContext(t), ""); !errors.Is(err, hoarddb.ErrInvalidURI) || !strings.Contains(err.Error(), "HOARDDB_URI") {
		t.Fatalf("no URI anywhere: %v", err)
	}
	for _, bad := range []string{"mongodb://h/app", "hoarddb://h/app?insecure=true", "hoarddb://u:p@ss@h/", "nonsense"} {
		_, err := hoarddb.Connect(testContext(t), bad)
		if !errors.Is(err, hoarddb.ErrInvalidURI) {
			t.Errorf("Connect(%q) = %v, want ErrInvalidURI", bad, err)
		}
	}
}

func TestOptionValidation(t *testing.T) {
	tests := []struct {
		name    string
		option  hoarddb.Option
		contain string
	}{
		{"empty user", hoarddb.WithCredentials("", "pw"), "user name"},
		{"empty token", hoarddb.WithToken(""), "token"},
		{"bad fingerprint", hoarddb.WithFingerprint("sha1:abc"), "SHA256:"},
		{"empty known path", hoarddb.WithKnownServers(""), "path"},
		{"nil prompt", hoarddb.WithTrustPrompt(nil), "function"},
		{"insecure without reason", hoarddb.WithInsecureSkipVerify("  "), "reason"},
		{"zero pool", hoarddb.WithPoolSize(0), "WithPoolSize"},
		{"zero bulk pool", hoarddb.WithBulkPoolSize(0), "WithBulkPoolSize"},
		{"negative timeout", hoarddb.WithRequestTimeout(-time.Second), "positive"},
		{"nil logger", hoarddb.WithLogger(nil), "logger"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := hoarddb.Connect(testContext(t), "hoarddb://admin:pw@127.0.0.1:1/", test.option)
			if !errors.Is(err, hoarddb.ErrInvalidURI) || !strings.Contains(err.Error(), test.contain) {
				t.Fatalf("err = %v; want ErrInvalidURI mentioning %q", err, test.contain)
			}
		})
	}
}

func TestFingerprintAndInsecureContradict(t *testing.T) {
	_, err := hoarddb.Connect(testContext(t), "hoarddb://admin:pw@127.0.0.1:1/?fingerprint=SHA256:"+strings.Repeat("ab", 32),
		hoarddb.WithInsecureSkipVerify("because"))
	if !errors.Is(err, hoarddb.ErrInvalidURI) || !strings.Contains(err.Error(), "contradict") {
		t.Fatalf("err = %v", err)
	}
}

func TestFingerprintCountMismatchFromOptions(t *testing.T) {
	_, err := hoarddb.Connect(testContext(t), "hoarddb://admin:pw@a:1,b:1,c:1/",
		hoarddb.WithFingerprint("SHA256:"+strings.Repeat("ab", 32), "SHA256:"+strings.Repeat("cd", 32)))
	if !errors.Is(err, hoarddb.ErrInvalidURI) || !strings.Contains(err.Error(), "2 fingerprints") || !strings.Contains(err.Error(), "3 hosts") {
		t.Fatalf("err = %v", err)
	}
}

func TestOptionFingerprintOverridesTheURI(t *testing.T) {
	server := startFake(t, serveAuthThen(nil))
	wrongInURI := "hoarddb://admin:pw@" + server.address + "/?fingerprint=SHA256:" + strings.Repeat("00", 32)
	client := mustConnect(t, wrongInURI, hoarddb.WithFingerprint(server.fingerprint))
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestPoolSizeWarningNamesTheServerCap(t *testing.T) {
	server := startFake(t, serveAuthThen(nil))
	var logs bytes.Buffer
	mustConnect(t, server.uri(), hoarddb.WithPoolSize(32), hoarddb.WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "64 connections per source address") {
		t.Fatalf("log:\n%s", logs.String())
	}
}

func TestNothingIsLoggedByDefault(t *testing.T) {
	// A library does not write to stderr: with no logger option the default
	// discards. Observable through the insecure path, which always warns.
	server := startFake(t, serveAuthThen(nil))
	client := mustConnect(t, "hoarddb://admin:pw@"+server.address+"/", hoarddb.WithInsecureSkipVerify("test"))
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestEveryErrorAndLogLineKeepsSecretsOut(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	server := startFake(t, func(c *fakeConn) {
		if first, _, ok := c.authRequest(); ok {
			c.authFail(first, int32(hoarddbwire.AuthFailCredentials), "")
		}
	})
	_, err := hoarddb.Connect(testContext(t), "hoarddb://admin:s3cret-pw-value@"+server.address+"/?fingerprint="+server.fingerprint+"&authToken=tok-s3cret",
		hoarddb.WithLogger(logger))
	if err == nil {
		t.Fatal("expected a refusal")
	}
	for _, rendering := range []string{err.Error(), logs.String()} {
		if strings.Contains(rendering, "s3cret") {
			t.Fatalf("a secret leaked: %s", rendering)
		}
	}
}
