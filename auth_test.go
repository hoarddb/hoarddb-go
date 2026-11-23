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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// These are the component halves of conformance leg 8: what a real server
// cannot be made to do on demand (handbook/testing.md section 4).

type authLog struct {
	mu       sync.Mutex
	attempts []string // "password" or "token", one per AUTH frame
}

func (log *authLog) add(method string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.attempts = append(log.attempts, method)
}

func (log *authLog) snapshot() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.attempts...)
}

// pastPoolBackoff outlasts the pool's own dial back-off after the first few
// failed dials (100 ms doubling, internal/pool.DefaultBackoffBase), so a call
// made after it would dial again unless the session itself refuses to.
const pastPoolBackoff = 500 * time.Millisecond

// A dropped user's connections are closed by the server (handbook/wire.md
// section 8). The next call dials, the server refuses the token and then the
// password, and the call reports that refusal. Later calls report it again
// without sending the password a second time.
func TestDroppedUserPooledConnectionsCloseAndTheNextCallReportsTheRefusal(t *testing.T) {
	var log authLog
	dropped := make(chan struct{})
	server := startFake(t, func(c *fakeConn) {
		first, request, ok := c.authRequest()
		if !ok {
			return
		}
		method := "password"
		if request.Token != "" {
			method = "token"
		}
		log.add(method)
		select {
		case <-dropped:
			if method == "token" {
				c.authFail(first, int32(hoarddbwire.AuthFailTokenInvalid), "AUTH token is invalid; authenticate again with a password")
			} else {
				c.authFail(first, int32(hoarddbwire.AuthFailCredentials), "invalid username or password")
			}
			return
		default:
		}
		c.authOK(first, "minted", time.Hour)
		<-dropped // the user is dropped: the server closes the connection
	})
	client := mustConnect(t, server.uri())
	close(dropped)

	err := client.Ping(testContext(t))
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrAuthFailed) || !errors.As(err, &failure) || failure.Reason != hoarddb.AuthReasonCredentials {
		t.Fatalf("err = %v; want ErrAuthFailed with CREDENTIALS", err)
	}
	if !strings.Contains(err.Error(), "invalid username or password") {
		t.Errorf("the server's sentence is missing: %v", err)
	}
	if got := log.snapshot(); strings.Join(got, ",") != "password,token,password" {
		t.Fatalf("AUTH frames = %v; want the login, then the token on a new connection, then the password once", got)
	}

	for range 3 {
		time.Sleep(pastPoolBackoff)
		err = client.Ping(testContext(t))
		if !errors.Is(err, hoarddb.ErrAuthFailed) || !strings.Contains(err.Error(), "not sent again") {
			t.Fatalf("a later call: err = %v; want the recorded refusal, saying the password is not sent again", err)
		}
	}
	if got := log.snapshot(); len(got) != 3 || server.connections.Load() != 3 {
		t.Fatalf("later calls sent %v over %d connections; a refused password is never sent again", got, server.connections.Load())
	}
}

// RATE_LIMITED says no credential was checked; the driver backs off instead of
// dialling again on the next call (handbook/auth.md section 4).
func TestRateLimitedHoldsOffTheNextDial(t *testing.T) {
	var log authLog
	limited := make(chan struct{})
	server := startFake(t, func(c *fakeConn) {
		first, request, ok := c.authRequest()
		if !ok {
			return
		}
		method := "password"
		if request.Token != "" {
			method = "token"
		}
		log.add(method)
		select {
		case <-limited:
			c.authFail(first, int32(hoarddbwire.AuthFailRateLimited), "too many failed logins from this address")
			return
		default:
		}
		c.authOK(first, "minted", time.Hour)
		<-limited
	})
	client := mustConnect(t, server.uri())
	close(limited)

	err := client.Ping(testContext(t))
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Reason != hoarddb.AuthReasonRateLimited {
		t.Fatalf("err = %v; want RATE_LIMITED", err)
	}
	before := server.connections.Load()
	for range 3 {
		time.Sleep(pastPoolBackoff)
		err = client.Ping(testContext(t))
		if !errors.As(err, &failure) || failure.Reason != hoarddb.AuthReasonRateLimited || !strings.Contains(err.Error(), "holding off") {
			t.Fatalf("a call during the back-off: err = %v; want RATE_LIMITED, saying the driver is holding off", err)
		}
	}
	if after := server.connections.Load(); after != before {
		t.Fatalf("%d more connection(s) during the back-off (AUTH frames %v); RATE_LIMITED is not retried in a loop", after-before, log.snapshot())
	}
}

// PROTOCOL_MISMATCH is never retried: a later call on the client reports it
// without dialling.
func TestProtocolMismatchIsNotRetriedByLaterCalls(t *testing.T) {
	mismatch := make(chan struct{})
	server := startFake(t, func(c *fakeConn) {
		first, _, ok := c.authRequest()
		if !ok {
			return
		}
		select {
		case <-mismatch:
			c.authFail(first, int32(hoarddbwire.AuthFailProtocolMismatch), "server speaks protocol 2, client sent 1")
			return
		default:
		}
		c.authOK(first, "", time.Hour) // no token: every dial uses the password
		<-mismatch
	})
	client := mustConnect(t, server.uri())
	close(mismatch)
	err := client.Ping(testContext(t))
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Reason != hoarddb.AuthReasonProtocolMismatch {
		t.Fatalf("err = %v; want PROTOCOL_MISMATCH", err)
	}
	before := server.connections.Load()
	time.Sleep(pastPoolBackoff)
	err = client.Ping(testContext(t))
	if !errors.As(err, &failure) || failure.Reason != hoarddb.AuthReasonProtocolMismatch {
		t.Fatalf("a later call: err = %v; want the recorded PROTOCOL_MISMATCH", err)
	}
	if after := server.connections.Load(); after != before {
		t.Fatalf("a later call dialled again after PROTOCOL_MISMATCH")
	}
}
