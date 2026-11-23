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
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// These tests check the token behaviour the session relies on against a real
// server, because the unit tests only prove the driver agrees with itself.

func dialPinned(server *servertest.Server) func(context.Context) (*hoarddbwire.Conn, error) {
	return func(ctx context.Context) (*hoarddbwire.Conn, error) {
		config, err := trust.NewTLSConfig(server.Address, trust.Policy{Pin: server.Fingerprint})
		if err != nil {
			return nil, err
		}
		return hoarddbwire.Dial(ctx, server.Address, config, hoarddbwire.DialOptions{})
	}
}

func TestRealServerHonoursTokensAcrossConnections(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dial := dialPinned(server)

	session := NewSession(Config{Username: server.User, Password: server.Password})
	first, info, err := session.Open(ctx, dial)
	if err != nil {
		t.Fatalf("password login: %v", err)
	}
	defer func() { _ = first.Close() }()
	if !slices.Contains(info.Capabilities, hoarddbwire.CapabilityAuthToken) {
		t.Fatalf("capabilities %v lack auth_token", info.Capabilities)
	}
	session.mu.Lock()
	minted, expires, deadline := session.token, session.expiresAt, session.deadline
	session.password = "no-longer-the-password" // from here only the token can succeed
	session.mu.Unlock()
	if minted == "" || !expires.After(time.Now()) || !deadline.After(expires) {
		t.Fatalf("token %q expires %v deadline %v; want a token alive now whose chain outlives it", minted, expires, deadline)
	}

	second, _, err := session.Open(ctx, dial)
	if err != nil {
		t.Fatalf("a second connection with only the token: %v", err)
	}
	defer func() { _ = second.Close() }()

	// A password AUTH is spent once per connection, even on a connection that
	// already authenticated.
	reply, err := first.Do(ctx, hoarddbwire.OpAuth, hoarddbwire.AuthRequest{
		Username: server.User, Password: server.Password, ProtocolVersion: 1, Capabilities: []string{}, Client: "hoarddb-go/test",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if failure := hoarddbwire.DecodeAuthFail(reply.Payload); reply.Op != hoarddbwire.OpAuthFail || failure.Reason != hoarddbwire.AuthFailAlreadyAuthed {
		t.Fatalf("second password AUTH on one connection: op %v reason %v, want AUTH_FAIL ALREADY_AUTHED", reply.Op, failure.Reason)
	}

	// A refresh on a live connection is a token AUTH and is allowed.
	session.mu.Lock()
	session.refreshAt = time.Now().Add(-time.Second)
	session.mu.Unlock()
	if !session.NeedsRefresh() {
		t.Fatal("forced refresh time not honoured")
	}
	if err := session.Refresh(ctx, second); err != nil {
		t.Fatalf("token refresh on a live connection: %v", err)
	}
	third, _, err := session.Open(ctx, dial)
	if err != nil {
		t.Fatalf("a connection using the refreshed token: %v", err)
	}
	_ = third.Close()
}

func TestRealServerRejectsAGarbageTokenWithATokenReasonAndTheFallbackWorks(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dial := dialPinned(server)

	// Token alone: the rejection is a token reason, not CREDENTIALS.
	tokenOnly := NewSession(Config{Username: server.User, Token: "not-a-token"})
	_, _, err := tokenOnly.Open(ctx, dial)
	var rejection *Rejection
	if !errors.As(err, &rejection) || rejection.Method != "token" {
		t.Fatalf("err = %v, want a token rejection", err)
	}
	switch rejection.Reason {
	case hoarddbwire.AuthFailTokenInvalid, hoarddbwire.AuthFailTokenExpired:
	default:
		t.Fatalf("a garbage token was refused as %v (%q); the fallback logic expects TOKEN_INVALID or TOKEN_EXPIRED", rejection.Reason, rejection.Message)
	}

	// With the password too, the same session falls back and logs in.
	both := NewSession(Config{Username: server.User, Password: server.Password, Token: "not-a-token"})
	conn, _, err := both.Open(ctx, dial)
	if err != nil {
		t.Fatalf("fallback from a rejected token to the password: %v", err)
	}
	_ = conn.Close()
}
