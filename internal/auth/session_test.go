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

package auth

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// authServer is a fake server that answers AUTH frames with a script.
type authServer struct {
	t        *testing.T
	address  string
	handle   func(connection int, request hoarddbwire.AuthRequest) hoarddbwire.Frame
	mu       sync.Mutex
	requests []hoarddbwire.AuthRequest
	accepted atomic.Int32
	wait     sync.WaitGroup
}

func startAuthServer(t *testing.T, handle func(connection int, request hoarddbwire.AuthRequest) hoarddbwire.Frame) *authServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &authServer{t: t, address: listener.Addr().String(), handle: handle}
	server.wait.Add(1)
	go func() {
		defer server.wait.Done()
		for {
			accepted, err := listener.Accept()
			if err != nil {
				return
			}
			index := int(server.accepted.Add(1))
			server.wait.Add(1)
			go func() {
				defer server.wait.Done()
				defer func() { _ = accepted.Close() }()
				server.serve(accepted, index)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		server.wait.Wait()
	})
	return server
}

func (server *authServer) serve(connection net.Conn, index int) {
	for {
		frame, err := hoarddbwire.ReadFrame(connection)
		if err != nil {
			return
		}
		var request hoarddbwire.AuthRequest
		if frame.Op == hoarddbwire.OpAuth {
			if codec.Unmarshal(frame.Payload, &request) != nil {
				return
			}
			server.mu.Lock()
			server.requests = append(server.requests, request)
			server.mu.Unlock()
		}
		reply := server.handle(index, request)
		reply.ID = frame.ID
		if hoarddbwire.WriteFrame(connection, reply) != nil {
			return
		}
	}
}

func (server *authServer) seen() []hoarddbwire.AuthRequest {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]hoarddbwire.AuthRequest(nil), server.requests...)
}

func (server *authServer) dial(ctx context.Context) (*hoarddbwire.Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.address)
	if err != nil {
		return nil, err
	}
	return hoarddbwire.NewConn(raw, hoarddbwire.ConnOptions{}), nil
}

func okFrame(t *testing.T, response hoarddbwire.AuthResponse) hoarddbwire.Frame {
	t.Helper()
	response.Success = true
	payload, err := codec.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return hoarddbwire.Frame{Op: hoarddbwire.OpAuthOK, Payload: payload}
}

func failFrame(t *testing.T, reason int32, message string) hoarddbwire.Frame {
	t.Helper()
	payload, err := codec.Marshal(map[string]any{"reason": reason, "message": message})
	if err != nil {
		t.Fatal(err)
	}
	return hoarddbwire.Frame{Op: hoarddbwire.OpAuthFail, Payload: payload}
}

var epoch = time.Unix(1_800_000_000, 0)

// clock is a settable time source.
type clock struct{ now atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.now.Store(epoch.UnixNano())
	return c
}
func (c *clock) Now() time.Time          { return time.Unix(0, c.now.Load()) }
func (c *clock) Advance(d time.Duration) { c.now.Add(int64(d)) }

func tokenOK(t *testing.T, clk *clock, token string, ttl, chain time.Duration) hoarddbwire.Frame {
	return okFrame(t, hoarddbwire.AuthResponse{
		Token: token, TokenExpiresAt: clk.Now().Add(ttl).Unix(), TokenDeadline: clk.Now().Add(chain).Unix(),
		ProtocolVersion: 1, ServerVersion: "test-1", Capabilities: []string{"get_fast", hoarddbwire.CapabilityAuthToken},
	})
}

type fallbackRecorder struct {
	mu     sync.Mutex
	waits  []time.Duration
	limits []time.Duration
}

func (recorder *fallbackRecorder) config(clk *clock, base Config) Config {
	base.Now = clk.Now
	base.Jitter = func(limit time.Duration) time.Duration {
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		recorder.limits = append(recorder.limits, limit)
		return limit / 2
	}
	base.Sleep = func(_ context.Context, wait time.Duration) error {
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		recorder.waits = append(recorder.waits, wait)
		return nil
	}
	return base
}

func TestPasswordAuthRequestAndResult(t *testing.T) {
	clk := newClock()
	server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
		return tokenOK(t, clk, "tok-1", 15*time.Minute, 12*time.Hour)
	})
	session := NewSession(Config{Username: "root", Password: "hunter2", ClientName: "hoarddb-go/test", Now: clk.Now})
	conn, info, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if info.ServerVersion != "test-1" || info.ProtocolVersion != 1 || len(info.Capabilities) != 2 {
		t.Fatalf("info = %+v", info)
	}
	request := server.seen()[0]
	if request.Username != "root" || request.Password != "hunter2" || request.Token != "" ||
		request.ProtocolVersion != 1 || request.Client != "hoarddb-go/test" {
		t.Fatalf("password AUTH carried %+v", request)
	}
	for _, capability := range request.Capabilities {
		if strings.HasPrefix(capability, "pipeline_window") {
			t.Fatalf("pipeline_window was struck from the protocol and must not be sent: %v", request.Capabilities)
		}
	}
}

func TestLaterConnectionsPresentTheTokenNotThePassword(t *testing.T) {
	clk := newClock()
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		return tokenOK(t, clk, "tok-"+request.Username, 15*time.Minute, 12*time.Hour)
	})
	session := NewSession(Config{Username: "root", Password: "hunter2", Now: clk.Now})
	for range 3 {
		conn, _, err := session.Open(context.Background(), server.dial)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
	}
	requests := server.seen()
	if requests[0].Password != "hunter2" || requests[0].Token != "" {
		t.Fatalf("first AUTH = %+v, want the password", requests[0])
	}
	for index, request := range requests[1:] {
		if request.Token != "tok-root" || request.Password != "" {
			t.Fatalf("AUTH %d = %+v; later connections must carry the token and no password", index+2, request)
		}
	}
}

func TestTokenRejectionFallsBackToThePasswordOnceOnANewConnection(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason int32
	}{
		{"expired", int32(hoarddbwire.AuthFailTokenExpired)},
		{"invalid", int32(hoarddbwire.AuthFailTokenInvalid)},
		{"unsupported", int32(hoarddbwire.AuthFailTokenUnsupported)},
	} {
		t.Run(test.name, func(t *testing.T) {
			clk := newClock()
			server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
				if request.Token != "" {
					return failFrame(t, test.reason, "token no good")
				}
				return tokenOK(t, clk, "fresh", 15*time.Minute, 12*time.Hour)
			})
			var recorder fallbackRecorder
			session := NewSession(recorder.config(clk, Config{Username: "root", Password: "pw", Token: "stale"}))
			conn, _, err := session.Open(context.Background(), server.dial)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			requests := server.seen()
			if len(requests) != 2 || requests[0].Token != "stale" || requests[0].Password != "" ||
				requests[1].Password != "pw" || requests[1].Token != "" {
				t.Fatalf("requests = %+v; want a token AUTH then exactly one password AUTH", requests)
			}
			if got := server.accepted.Load(); got != 2 {
				t.Fatalf("%d connections accepted; the fallback must use a new connection", got)
			}
			if len(recorder.waits) != 1 || recorder.waits[0] != maxFallbackJitter/2 || recorder.limits[0] != maxFallbackJitter {
				t.Fatalf("jitter: waits %v limits %v; want one wait drawn from [0, %v)", recorder.waits, recorder.limits, maxFallbackJitter)
			}
		})
	}
}

func TestTokenUnsupportedIsRememberedAndTokensStopBeingOffered(t *testing.T) {
	clk := newClock()
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		if request.Token != "" {
			return failFrame(t, int32(hoarddbwire.AuthFailTokenUnsupported), "no tokens here")
		}
		// An older server: AUTH_OK without the auth_token capability and no token.
		return okFrame(t, hoarddbwire.AuthResponse{ServerVersion: "old", Capabilities: []string{"get_fast"}})
	})
	var recorder fallbackRecorder
	session := NewSession(recorder.config(clk, Config{Username: "root", Password: "pw", Token: "stale"}))
	for range 3 {
		conn, _, err := session.Open(context.Background(), server.dial)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
	}
	tokens := 0
	for _, request := range server.seen() {
		if request.Token != "" {
			tokens++
		}
	}
	if tokens != 1 {
		t.Fatalf("the token was offered %d times; after TOKEN_UNSUPPORTED it must never be offered again", tokens)
	}
}

func TestRejectionsThatMustNotBeRetried(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason int32
		want   hoarddbwire.AuthFailReason
	}{
		{"credentials", 1, hoarddbwire.AuthFailCredentials},
		{"protocol mismatch", 5, hoarddbwire.AuthFailProtocolMismatch},
		{"rate limited", 6, hoarddbwire.AuthFailRateLimited},
		{"already authed", 7, hoarddbwire.AuthFailAlreadyAuthed},
		{"internal", 8, hoarddbwire.AuthFailInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
				return failFrame(t, test.reason, "server says no")
			})
			session := NewSession(Config{Username: "root", Password: "pw"})
			conn, _, err := session.Open(context.Background(), server.dial)
			if conn != nil {
				t.Fatal("a refused login returned a connection")
			}
			var rejection *Rejection
			if !errors.As(err, &rejection) || rejection.Reason != test.want || rejection.Message != "server says no" ||
				rejection.Method != "password" || rejection.Username != "root" {
				t.Fatalf("err = %v, want a *Rejection with reason %v", err, test.want)
			}
			if got := len(server.seen()); got != 1 {
				t.Fatalf("%d AUTH attempts; a rejected login is never retried", got)
			}
		})
	}
}

func TestTokenRejectionOtherThanTheTokenReasonsDoesNotFallBack(t *testing.T) {
	// RATE_LIMITED on a token AUTH must not turn into a password AUTH: the
	// limiter is exactly what a fallback storm would trip.
	server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
		return failFrame(t, int32(hoarddbwire.AuthFailRateLimited), "slow down")
	})
	session := NewSession(Config{Username: "root", Password: "pw", Token: "tok"})
	_, _, err := session.Open(context.Background(), server.dial)
	var rejection *Rejection
	if !errors.As(err, &rejection) || rejection.Reason != hoarddbwire.AuthFailRateLimited {
		t.Fatalf("err = %v", err)
	}
	if got := len(server.seen()); got != 1 {
		t.Fatalf("%d AUTH attempts, want 1", got)
	}
}

func TestEmptyAuthFailPayloadIsCredentials(t *testing.T) {
	server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
		return hoarddbwire.Frame{Op: hoarddbwire.OpAuthFail}
	})
	_, _, err := NewSession(Config{Username: "root", Password: "pw"}).Open(context.Background(), server.dial)
	var rejection *Rejection
	if !errors.As(err, &rejection) || rejection.Reason != hoarddbwire.AuthFailCredentials {
		t.Fatalf("err = %v, want reason CREDENTIALS", err)
	}
}

func TestTokenOnlySessionHasNothingToFallBackTo(t *testing.T) {
	server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
		return failFrame(t, int32(hoarddbwire.AuthFailTokenExpired), "expired")
	})
	_, _, err := NewSession(Config{Username: "root", Token: "tok"}).Open(context.Background(), server.dial)
	var rejection *Rejection
	if !errors.As(err, &rejection) || rejection.Reason != hoarddbwire.AuthFailTokenExpired || rejection.Method != "token" {
		t.Fatalf("err = %v", err)
	}
	if got := len(server.seen()); got != 1 {
		t.Fatalf("%d attempts, want 1", got)
	}
}

func TestNoCredentials(t *testing.T) {
	dialed := false
	_, _, err := NewSession(Config{Username: "root"}).Open(context.Background(), func(context.Context) (*hoarddbwire.Conn, error) {
		dialed = true
		return nil, errors.New("must not dial")
	})
	if !errors.Is(err, ErrNoCredentials) || dialed {
		t.Fatalf("err = %v, dialed = %v; want ErrNoCredentials before any dial", err, dialed)
	}
}

func TestKnownExpiryDecidesWithoutAWastedRoundTrip(t *testing.T) {
	clk := newClock()
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		if request.Token != "" {
			return tokenOK(t, clk, "refreshed", 15*time.Minute, time.Hour)
		}
		return tokenOK(t, clk, "first", 15*time.Minute, time.Hour)
	})
	session := NewSession(Config{Username: "root", Password: "pw", Now: clk.Now})
	first, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Close()

	clk.Advance(16 * time.Minute) // past the token's own expiry, inside the chain
	second, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Close()
	requests := server.seen()
	if len(requests) != 2 || requests[1].Token != "" || requests[1].Password != "pw" {
		t.Fatalf("requests = %+v; an expired token should be skipped, not tried and rejected", requests)
	}

	clk.Advance(2 * time.Hour) // past the chain deadline
	third, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	_ = third.Close()
	if last := server.seen()[len(server.seen())-1]; last.Token != "" || last.Password != "pw" {
		t.Fatalf("after the deadline AUTH = %+v, want the password", last)
	}
}

func TestRefreshHappensAtHalfLifeAndIsSingleFlight(t *testing.T) {
	clk := newClock()
	var refreshes atomic.Int32
	release := make(chan struct{})
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		if request.Token != "" {
			refreshes.Add(1)
			<-release
			return tokenOK(t, clk, "second", 15*time.Minute, 12*time.Hour)
		}
		return tokenOK(t, clk, "first", 15*time.Minute, 12*time.Hour)
	})
	session := NewSession(Config{Username: "root", Password: "pw", Now: clk.Now})
	conn, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if session.NeedsRefresh() {
		t.Fatal("a fresh token needs no refresh")
	}
	clk.Advance(7 * time.Minute)
	if session.NeedsRefresh() {
		t.Fatal("before half of 15 minutes (7m30s) no refresh is due")
	}
	clk.Advance(31 * time.Second)
	if !session.NeedsRefresh() {
		t.Fatal("past half of the token's life a refresh is due")
	}

	// Two callers race; only one AUTH goes out.
	var group sync.WaitGroup
	results := make(chan error, 2)
	group.Add(2)
	go func() { defer group.Done(); results <- session.Refresh(context.Background(), conn) }()
	deadline := time.Now().Add(2 * time.Second)
	for refreshes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	go func() { defer group.Done(); results <- session.Refresh(context.Background(), conn) }()
	awaitResult := func() error {
		t.Helper()
		select {
		case err := <-results:
			return err
		case <-time.After(5 * time.Second):
			close(release) // let the server goroutine finish so the test can end
			t.Fatal("the second Refresh blocked behind the first: refresh is not single-flight")
			return nil
		}
	}
	if err := awaitResult(); err != nil { // the second caller returns at once
		t.Fatal(err)
	}
	close(release)
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	group.Wait()
	if got := refreshes.Load(); got != 1 {
		t.Fatalf("%d refresh AUTH frames; single-flight means one", got)
	}
	if session.NeedsRefresh() {
		t.Fatal("after refreshing the new token is fresh")
	}
}

func TestRefreshRejectionForgetsTheTokenSoTheNextConnectionUsesThePassword(t *testing.T) {
	clk := newClock()
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		if request.Token != "" {
			return failFrame(t, int32(hoarddbwire.AuthFailTokenInvalid), "revoked")
		}
		return tokenOK(t, clk, "first", 15*time.Minute, 12*time.Hour)
	})
	var recorder fallbackRecorder
	session := NewSession(recorder.config(clk, Config{Username: "root", Password: "pw"}))
	conn, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	clk.Advance(10 * time.Minute)
	var rejection *Rejection
	if err := session.Refresh(context.Background(), conn); !errors.As(err, &rejection) {
		t.Fatalf("err = %v, want the rejection", err)
	}
	next, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = next.Close() }()
	if last := server.seen()[len(server.seen())-1]; last.Password != "pw" || last.Token != "" {
		t.Fatalf("after a rejected refresh the next AUTH was %+v, want the password", last)
	}
}

func TestRefreshRefusedForANonTokenReasonIsPostponedNotRepeated(t *testing.T) {
	clk := newClock()
	var refreshes atomic.Int32
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		if request.Token != "" {
			refreshes.Add(1)
			return failFrame(t, int32(hoarddbwire.AuthFailRateLimited), "slow down")
		}
		return tokenOK(t, clk, "first", 15*time.Minute, 12*time.Hour)
	})
	session := NewSession(Config{Username: "root", Password: "pw", Now: clk.Now})
	conn, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	clk.Advance(10 * time.Minute)
	if err := session.Refresh(context.Background(), conn); err != nil {
		t.Fatalf("a rate-limited refresh must not fail the caller: %v", err)
	}
	if session.NeedsRefresh() {
		t.Fatal("NeedsRefresh stayed true, so every call would retry the refresh")
	}
	if err := session.Refresh(context.Background(), conn); err != nil || refreshes.Load() != 1 {
		t.Fatalf("second Refresh: err %v, %d attempts; want it postponed", err, refreshes.Load())
	}
	clk.Advance(refreshRetryDelay + time.Second)
	if !session.NeedsRefresh() {
		t.Fatal("the refresh should be due again after the postponement")
	}
	session.mu.Lock()
	kept := session.token
	session.mu.Unlock()
	if kept != "first" {
		t.Fatalf("token = %q; a rate-limited refresh must keep the token", kept)
	}
}

func TestOpenFailureModes(t *testing.T) {
	t.Run("dial error", func(t *testing.T) {
		dialErr := errors.New("connection refused")
		_, _, err := NewSession(Config{Username: "u", Password: "p"}).Open(context.Background(),
			func(context.Context) (*hoarddbwire.Conn, error) { return nil, dialErr })
		if !errors.Is(err, dialErr) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("server closes instead of answering", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = listener.Close() }()
		go func() {
			accepted, err := listener.Accept()
			if err == nil {
				_, _ = hoarddbwire.ReadFrame(accepted)
				_ = accepted.Close()
			}
		}()
		_, _, err = NewSession(Config{Username: "u", Password: "p", Token: "t"}).Open(context.Background(),
			func(ctx context.Context) (*hoarddbwire.Conn, error) {
				raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
				if err != nil {
					return nil, err
				}
				return hoarddbwire.NewConn(raw, hoarddbwire.ConnOptions{}), nil
			})
		var rejection *Rejection
		if err == nil || errors.As(err, &rejection) {
			t.Fatalf("err = %v; a transport failure is not a rejection and must not trigger the password fallback", err)
		}
	})
	t.Run("AUTH_OK with success false", func(t *testing.T) {
		server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
			payload, _ := codec.Marshal(hoarddbwire.AuthResponse{Success: false, Message: "locked"})
			return hoarddbwire.Frame{Op: hoarddbwire.OpAuthOK, Payload: payload}
		})
		_, _, err := NewSession(Config{Username: "u", Password: "p"}).Open(context.Background(), server.dial)
		var rejection *Rejection
		if !errors.As(err, &rejection) || rejection.Message != "locked" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("OpError instead of AUTH_OK", func(t *testing.T) {
		server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
			payload, _ := codec.Marshal(map[string]any{"code": int32(5), "message": "boom"})
			return hoarddbwire.Frame{Op: hoarddbwire.OpError, Payload: payload}
		})
		_, _, err := NewSession(Config{Username: "u", Password: "p"}).Open(context.Background(), server.dial)
		var unexpected *UnexpectedReplyError
		if !errors.As(err, &unexpected) || unexpected.Response.Message != "boom" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("cancelled during the fallback pause", func(t *testing.T) {
		server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
			return failFrame(t, int32(hoarddbwire.AuthFailTokenExpired), "expired")
		})
		ctx, cancel := context.WithCancel(context.Background())
		session := NewSession(Config{Username: "u", Password: "p", Token: "t",
			Sleep: func(ctx context.Context, _ time.Duration) error { cancel(); return sleepContext(ctx, time.Minute) }})
		_, _, err := session.Open(ctx, server.dial)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

func TestSessionNeverPrintsItsSecrets(t *testing.T) {
	session := NewSession(Config{Username: "root", Password: "hunter2-very-secret", Token: "token-very-secret"})
	for _, rendering := range []string{
		session.String(), fmt.Sprintf("%v", session), fmt.Sprintf("%+v", session), fmt.Sprintf("%#v", session), fmt.Sprint(session),
	} {
		if strings.Contains(rendering, "hunter2") || strings.Contains(rendering, "token-very-secret") {
			t.Fatalf("a secret leaked: %s", rendering)
		}
	}
	rejection := (&Rejection{Username: "root", Method: "password", Reason: hoarddbwire.AuthFailCredentials, Message: "bad"}).Error()
	if strings.Contains(rejection, "hunter2") {
		t.Fatalf("rejection leaked: %s", rejection)
	}
}

// A refusal that says the credentials or the protocol are wrong ends the
// session's logins: the next Open reports it without dialling, so a pool that
// keeps calling cannot turn one refusal into a failed-login loop
// (handbook/auth.md section 4).
func TestRefusalsThatEndTheSessionAreNotSentAgain(t *testing.T) {
	for _, test := range []struct {
		name   string
		method string
		reason hoarddbwire.AuthFailReason
	}{
		{"credentials for the password", "password", hoarddbwire.AuthFailCredentials},
		{"credentials for a token", "token", hoarddbwire.AuthFailCredentials},
		{"protocol mismatch", "password", hoarddbwire.AuthFailProtocolMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
				return failFrame(t, int32(test.reason), "server says no")
			})
			config := Config{Username: "root", Password: "pw"}
			if test.method == "token" {
				config.Token = "tok"
			}
			session := NewSession(config)
			if _, _, err := session.Open(context.Background(), server.dial); err == nil {
				t.Fatal("the first Open succeeded")
			}
			dialled := server.accepted.Load()
			for range 3 {
				_, _, err := session.Open(context.Background(), server.dial)
				var ended *EndedError
				var rejection *Rejection
				if !errors.As(err, &ended) || !errors.As(err, &rejection) || rejection.Reason != test.reason || rejection.Message != "server says no" {
					t.Fatalf("a later Open: err = %v; want *EndedError wrapping the first refusal", err)
				}
				if !strings.Contains(err.Error(), "not sent again") || strings.Contains(err.Error(), "pw") {
					t.Fatalf("a later Open: %q must say the credential is not sent again, and not show it", err)
				}
			}
			if after := server.accepted.Load(); after != dialled {
				t.Fatalf("%d more connection(s) after a refusal that ends the session", after-dialled)
			}
			session.mu.Lock()
			kept := session.password
			session.mu.Unlock()
			if kept != "" {
				t.Fatal("an ended session still holds the password; it will never send it again (handbook/auth.md section 3, item 1)")
			}
		})
	}
}

// RATE_LIMITED checked no credential, so it does not end the session; it holds
// every login off for a while and then lets one through.
func TestRateLimitedHoldsLoginsOffThenLetsOneThrough(t *testing.T) {
	clk := newClock()
	var limited atomic.Bool
	limited.Store(true)
	server := startAuthServer(t, func(int, hoarddbwire.AuthRequest) hoarddbwire.Frame {
		if limited.Load() {
			return failFrame(t, int32(hoarddbwire.AuthFailRateLimited), "slow down")
		}
		return tokenOK(t, clk, "tok", time.Hour, 12*time.Hour)
	})
	session := NewSession(Config{Username: "root", Password: "pw", Now: clk.Now})
	if _, _, err := session.Open(context.Background(), server.dial); err == nil {
		t.Fatal("the first Open succeeded")
	}
	// handbook/auth.md section 4 asks for a back-off; this driver holds logins
	// off for five seconds. The numbers are written out, not read from the
	// constant, so a hold of any other length fails here.
	clk.Advance(4 * time.Second)
	_, _, err := session.Open(context.Background(), server.dial)
	var holding *HoldingOffError
	var rejection *Rejection
	if !errors.As(err, &holding) || !errors.As(err, &rejection) || rejection.Reason != hoarddbwire.AuthFailRateLimited ||
		!strings.Contains(err.Error(), "holding off") {
		t.Fatalf("an Open inside the hold: err = %v; want *HoldingOffError wrapping RATE_LIMITED", err)
	}
	if server.accepted.Load() != 1 {
		t.Fatalf("%d connections; an Open inside the hold must not dial", server.accepted.Load())
	}
	limited.Store(false)
	clk.Advance(time.Second)
	conn, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatalf("an Open five seconds after RATE_LIMITED: %v", err)
	}
	_ = conn.Close()
	if server.accepted.Load() != 2 {
		t.Fatalf("%d connections; want the refused login and exactly one let through after the hold", server.accepted.Load())
	}
}

// A refresh refused as CREDENTIALS ends the session's logins exactly as Open's
// refusal does: Refresh returns it, so the caller drops the connection, and the
// next Open reports it without dialling.
func TestRefreshRefusedAsCredentialsEndsTheSession(t *testing.T) {
	clk := newClock()
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		if request.Token != "" {
			return failFrame(t, int32(hoarddbwire.AuthFailCredentials), "user dropped")
		}
		return tokenOK(t, clk, "first", 15*time.Minute, 12*time.Hour)
	})
	session := NewSession(Config{Username: "root", Password: "pw", Now: clk.Now})
	conn, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	clk.Advance(8 * time.Minute)
	var rejection *Rejection
	if err := session.Refresh(context.Background(), conn); !errors.As(err, &rejection) || rejection.Reason != hoarddbwire.AuthFailCredentials {
		t.Fatalf("Refresh: err = %v; want the CREDENTIALS refusal returned", err)
	}
	dialled := server.accepted.Load()
	var ended *EndedError
	if _, _, err := session.Open(context.Background(), server.dial); !errors.As(err, &ended) {
		t.Fatalf("Open after the refresh was refused: err = %v; want *EndedError", err)
	}
	if server.accepted.Load() != dialled {
		t.Fatal("Open dialled after the session ended")
	}
}

// A connection that chose the token must send the token it chose, even when
// another goroutine forgets the token before the frame goes out. Read at send
// time instead, the frame carries neither token nor password, the server
// answers CREDENTIALS, and a session with the right password ends for good.
func TestALoginSendsTheCredentialItChose(t *testing.T) {
	clk := newClock()
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		switch {
		case request.Token == "" && request.Password == "":
			return failFrame(t, int32(hoarddbwire.AuthFailCredentials), "invalid username or password")
		case request.Token != "":
			return failFrame(t, int32(hoarddbwire.AuthFailTokenExpired), "AUTH token expired; authenticate again with a password")
		default:
			return tokenOK(t, clk, "minted", 15*time.Minute, 12*time.Hour)
		}
	})
	session := NewSession(Config{Username: "root", Password: "pw", Now: clk.Now,
		Sleep: func(context.Context, time.Duration) error { return nil }})
	first, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	clk.Advance(8 * time.Minute) // the token is alive, and due for a refresh

	chosen, proceed := make(chan struct{}), make(chan struct{})
	held := func(ctx context.Context) (*hoarddbwire.Conn, error) {
		select {
		case <-chosen:
		default:
			close(chosen) // the first dial of this Open: the token is chosen
			<-proceed
		}
		return server.dial(ctx)
	}
	type outcome struct {
		conn *hoarddbwire.Conn
		err  error
	}
	opened := make(chan outcome, 1)
	go func() {
		conn, _, err := session.Open(context.Background(), held)
		opened <- outcome{conn, err}
	}()
	<-chosen
	// Meanwhile a refresh on the first connection is refused: the token is gone.
	var rejection *Rejection
	if err := session.Refresh(context.Background(), first); !errors.As(err, &rejection) || rejection.Reason != hoarddbwire.AuthFailTokenExpired {
		t.Fatalf("Refresh: err = %v, want TOKEN_EXPIRED", err)
	}
	close(proceed)
	result := <-opened
	if result.err != nil {
		t.Fatalf("Open with the right password: %v", result.err)
	}
	_ = result.conn.Close()

	for index, request := range server.seen() {
		if request.Token == "" && request.Password == "" {
			t.Fatalf("AUTH %d carried neither a token nor a password: %+v", index+1, server.seen())
		}
	}
	if err := session.refusedBefore(); err != nil {
		t.Fatalf("the session refuses logins with the right password: %v", err)
	}
}

// A connection whose token was refused forgets that token only: when a refresh
// on another connection has adopted a newer one meanwhile, the fallback
// presents the newer token and sends no password.
func TestARefusedTokenDoesNotDropANewerOne(t *testing.T) {
	clk := newClock()
	server := startAuthServer(t, func(connection int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		switch {
		case request.Token == "first" && connection == 1: // the refresh on the login connection
			return tokenOK(t, clk, "second", 15*time.Minute, 12*time.Hour)
		case request.Token == "first":
			return failFrame(t, int32(hoarddbwire.AuthFailTokenExpired), "AUTH token expired; authenticate again with a password")
		case request.Token != "":
			return tokenOK(t, clk, request.Token, 15*time.Minute, 12*time.Hour)
		default:
			return tokenOK(t, clk, "first", 15*time.Minute, 12*time.Hour)
		}
	})
	session := NewSession(Config{Username: "root", Password: "pw", Now: clk.Now,
		Sleep: func(context.Context, time.Duration) error { return nil }})
	first, _, err := session.Open(context.Background(), server.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	clk.Advance(8 * time.Minute)

	chosen, proceed := make(chan struct{}), make(chan struct{})
	held := func(ctx context.Context) (*hoarddbwire.Conn, error) {
		select {
		case <-chosen:
		default:
			close(chosen) // "first" is chosen; hold the frame until the refresh lands
			<-proceed
		}
		return server.dial(ctx)
	}
	opened := make(chan error, 1)
	go func() {
		conn, _, err := session.Open(context.Background(), held)
		if conn != nil {
			_ = conn.Close()
		}
		opened <- err
	}()
	<-chosen
	if err := session.Refresh(context.Background(), first); err != nil {
		t.Fatalf("the refresh: %v", err)
	}
	close(proceed)
	if err := <-opened; err != nil {
		t.Fatalf("Open: %v", err)
	}
	seen := server.seen()
	passwords := 0
	for _, request := range seen {
		if request.Password != "" {
			passwords++
		}
	}
	session.mu.Lock()
	kept := session.token
	session.mu.Unlock()
	if passwords != 1 || kept != "second" {
		t.Fatalf("%d password logins and token %q after the refusal; want only the first login's password and the newer token kept (AUTH frames: %+v)",
			passwords, kept, seen)
	}
}

// Reauthenticate sends exactly one AUTH on the connection it is given, with
// the credential a new connection would use, and never falls back on it. A
// rejected token is forgotten, so the next connection presents the password.
func TestReauthenticateSendsOneAuthOnTheSameConnection(t *testing.T) {
	server := startAuthServer(t, func(_ int, request hoarddbwire.AuthRequest) hoarddbwire.Frame {
		if request.Token != "" {
			return failFrame(t, int32(hoarddbwire.AuthFailTokenExpired), "token expired")
		}
		return okFrame(t, hoarddbwire.AuthResponse{ServerVersion: "fake"})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	session := NewSession(Config{Username: "ada", Password: "secret"})
	conn, err := server.dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := session.Reauthenticate(ctx, conn); err != nil {
		t.Fatalf("Reauthenticate with a password: %v", err)
	}
	if seen := server.seen(); len(seen) != 1 || seen[0].Password != "secret" || server.accepted.Load() != 1 {
		t.Fatalf("want one password AUTH on the one connection, saw %d AUTH frames on %d connections", len(seen), server.accepted.Load())
	}

	withToken := NewSession(Config{Username: "ada", Password: "secret", Token: "old-token"})
	_, err = withToken.Reauthenticate(ctx, conn)
	var rejection *Rejection
	if !errors.As(err, &rejection) || rejection.Reason != hoarddbwire.AuthFailTokenExpired {
		t.Fatalf("Reauthenticate with an expired token = %v; want the rejection, not a fallback", err)
	}
	if seen := server.seen(); len(seen) != 2 || seen[1].Token != "old-token" {
		t.Fatalf("want exactly one more AUTH, with the token: %d frames", len(seen))
	}
	if chosen := withToken.chooseCredential(); chosen.method != "password" {
		t.Fatalf("after the token was rejected the next login uses %q, want the password", chosen.method)
	}
}

func TestSecretHoldersRedactUnderEveryFormatVerb(t *testing.T) {
	config := Config{Username: "alice", Password: "Zq7xK9Wm3pL", Token: "Hy6cDRt5nB"}
	session := NewSession(config)
	for name, value := range map[string]any{"config": config, "config pointer": &config, "session": session} {
		for _, verb := range []string{"%s", "%v", "%+v", "%#v", "%q", "%x", "%d"} {
			text := fmt.Sprintf(verb, value)
			if strings.Contains(text, "Zq7xK9Wm3pL") || strings.Contains(text, "Hy6cDRt5nB") {
				t.Errorf("%s formatted with %s repeats a credential: %s", name, verb, text)
			}
		}
	}
}
