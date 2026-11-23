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

package conformance_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// Conformance leg 8 (handbook/testing.md section 2): authentication against a
// real server. Every test starts its own server, because the failed-login
// limiter counts per source address (handbook/auth.md section 4), and puts a
// servertest.Proxy between the driver and the server, so it can count what the
// driver sent: connections, password and token AUTH frames, refreshes. The
// answers the proxy records are the server's own.

// shortTokens makes the server mint one-second tokens and judge their expiry
// without slack, as the vectors' token cases were captured
// (testdata/driver-vectors/v1.json, "auth refused: expired token").
var shortTokens = map[string]string{"HOARDDB_TOKEN_TTL": "1", "HOARDDB_AUTH_SKEW": "0"}

type authFixture struct {
	server *servertest.Server
	proxy  *servertest.Proxy
}

func startAuthServer(t *testing.T, env map[string]string) authFixture {
	t.Helper()
	server := servertest.Start(t, servertest.Options{Env: env})
	return authFixture{server: server, proxy: servertest.StartProxy(t, server)}
}

// uri is a connection string through the proxy with the given password.
func (fixture authFixture) uri(password string) string {
	return fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s", fixture.server.User, password, fixture.proxy.Address, fixture.proxy.Fingerprint)
}

func authContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// loginForToken opens a raw connection to the server, logs in with the
// password, and returns the token the server minted.
func loginForToken(t *testing.T, ctx context.Context, server *servertest.Server) (string, time.Time) {
	t.Helper()
	conn, err := server.DialUnauthenticated(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	var answer hoarddbwire.AuthResponse
	reply, err := conn.Do(ctx, hoarddbwire.OpAuth, hoarddbwire.AuthRequest{
		Username: server.User, Password: server.Password, ProtocolVersion: 1, Capabilities: []string{}, Client: "hoarddb-go/leg8",
	}, &answer)
	if err != nil || reply.Op != hoarddbwire.OpAuthOK || answer.Token == "" {
		t.Fatalf("minting a token: op %v, token %q, err %v", reply.Op, answer.Token, err)
	}
	return answer.Token, time.Unix(answer.TokenExpiresAt, 0)
}

// requireAttempts checks the AUTH frames the proxy saw on connection index.
func requireAttempts(t *testing.T, seen []servertest.Connection, index int, want ...servertest.AuthAttempt) {
	t.Helper()
	if index >= len(seen) {
		t.Fatalf("no connection %d: %s", index+1, servertest.Describe(seen))
	}
	got := seen[index].Auths
	if len(got) != len(want) {
		t.Fatalf("connection %d: want %d AUTH frame(s): %s", index+1, len(want), servertest.Describe(seen))
	}
	for position := range want {
		if got[position] != want[position] {
			t.Fatalf("connection %d, AUTH %d: got %+v, want %+v: %s", index+1, position+1, got[position], want[position], servertest.Describe(seen))
		}
	}
}

func passwordAttempt(answer hoarddbwire.Opcode, reason hoarddbwire.AuthFailReason) servertest.AuthAttempt {
	return servertest.AuthAttempt{Method: "password", OnFreshConnection: true, Answer: answer, Reason: reason}
}

func tokenAttempt(answer hoarddbwire.Opcode, reason hoarddbwire.AuthFailReason) servertest.AuthAttempt {
	return servertest.AuthAttempt{Method: "token", OnFreshConnection: true, Answer: answer, Reason: reason}
}

func requireAuthFailure(t *testing.T, err error, reason hoarddb.AuthReason, wants ...string) {
	t.Helper()
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrAuthFailed) || !errors.As(err, &failure) || failure.Reason != reason {
		t.Fatalf("err = %v; want ErrAuthFailed with reason %v", err, reason)
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error must contain %q: %v", want, err)
		}
	}
}

// A wrong password is CREDENTIALS, the driver closes the connection whose one
// password attempt it spent, and the next attempt is a new connection, so the
// server never answers ALREADY_AUTHED (failure 6 of handbook/build-go.md).
func TestWrongPasswordIsCredentialsAndRedials(t *testing.T) {
	fixture := startAuthServer(t, nil)
	ctx := authContext(t)

	_, err := hoarddb.Connect(ctx, fixture.uri("not-the-password"))
	requireAuthFailure(t, err, hoarddb.AuthReasonCredentials,
		"invalid username or password", `"admin"`, "does not retry")
	if strings.Contains(err.Error(), "not-the-password") {
		t.Fatalf("the password appeared in the error: %v", err)
	}
	if closedBy := fixture.proxy.AwaitClosed(t, 0, 5*time.Second).ClosedBy; closedBy != "driver" {
		t.Errorf("the refused connection was closed by %q; the driver must close it itself", closedBy)
	}

	client, err := hoarddb.Connect(ctx, fixture.uri(fixture.server.Password))
	if err != nil {
		t.Fatalf("the next attempt, with the right password: %v", err)
	}
	defer func() { _ = client.Close() }()

	seen := fixture.proxy.Connections()
	if len(seen) != 2 {
		t.Fatalf("want one connection per attempt: %s", servertest.Describe(seen))
	}
	requireAttempts(t, seen, 0, passwordAttempt(hoarddbwire.OpAuthFail, hoarddbwire.AuthFailCredentials))
	requireAttempts(t, seen, 1, passwordAttempt(hoarddbwire.OpAuthOK, 0))
	if most := servertest.PasswordAuthsPerConnection(seen); most != 1 {
		t.Fatalf("a connection carried %d password AUTHs: %s", most, servertest.Describe(seen))
	}
}

// The token minted by the password login authenticates the next connection the
// pool opens, and no password is sent on it.
func TestTokenFromOneConnectionAuthenticatesAnother(t *testing.T) {
	fixture := startAuthServer(t, nil)
	ctx := authContext(t)

	client, err := hoarddb.Connect(ctx, fixture.uri(fixture.server.Password))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if !client.ServerInfo().Has("auth_token") {
		t.Fatalf("the server does not advertise auth_token (%v); this test needs a server that mints tokens", client.ServerInfo().Capabilities)
	}

	fixture.proxy.DropAll() // the pooled connection is gone; the next call dials
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping after the connection was dropped: %v", err)
	}

	seen := fixture.proxy.Connections()
	if len(seen) != 2 {
		t.Fatalf("want the login connection and one more: %s", servertest.Describe(seen))
	}
	requireAttempts(t, seen, 0, passwordAttempt(hoarddbwire.OpAuthOK, 0))
	requireAttempts(t, seen, 1, tokenAttempt(hoarddbwire.OpAuthOK, 0))

	log, err := os.ReadFile(fixture.server.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(log), `"auth_method":"token"`) {
		t.Errorf("the server's log records no token login; the proxy saw one:\n%s", servertest.Describe(seen))
	}
}

// A token the server refuses as expired or invalid falls back to the password
// exactly once, on a new connection; a refused fallback is not tried again.
func TestExpiredTokenFallsBackOnce(t *testing.T) {
	t.Run("a token the program supplied, expired at the server", func(t *testing.T) {
		fixture := startAuthServer(t, shortTokens)
		ctx := authContext(t)
		token, expires := loginForToken(t, ctx, fixture.server)
		time.Sleep(time.Until(expires.Add(1100 * time.Millisecond)))

		client, err := hoarddb.Connect(ctx, fixture.uri(fixture.server.Password), hoarddb.WithToken(token))
		if err != nil {
			t.Fatalf("an expired token with the password beside it: %v", err)
		}
		defer func() { _ = client.Close() }()
		seen := fixture.proxy.Connections()
		if len(seen) != 2 {
			t.Fatalf("want the token attempt and one fallback: %s", servertest.Describe(seen))
		}
		requireAttempts(t, seen, 0, tokenAttempt(hoarddbwire.OpAuthFail, hoarddbwire.AuthFailTokenExpired))
		requireAttempts(t, seen, 1, passwordAttempt(hoarddbwire.OpAuthOK, 0))
		if closedBy := fixture.proxy.AwaitClosed(t, 0, 5*time.Second).ClosedBy; closedBy == "" {
			t.Fatal("the refused token connection was left open")
		}
	})

	t.Run("an invalid token", func(t *testing.T) {
		fixture := startAuthServer(t, shortTokens)
		ctx := authContext(t)
		client, err := hoarddb.Connect(ctx, fixture.uri(fixture.server.Password), hoarddb.WithToken("not-a-token"))
		if err != nil {
			t.Fatalf("an invalid token with the password beside it: %v", err)
		}
		defer func() { _ = client.Close() }()
		seen := fixture.proxy.Connections()
		if len(seen) != 2 {
			t.Fatalf("want the token attempt and one fallback: %s", servertest.Describe(seen))
		}
		requireAttempts(t, seen, 0, tokenAttempt(hoarddbwire.OpAuthFail, hoarddbwire.AuthFailTokenInvalid))
		requireAttempts(t, seen, 1, passwordAttempt(hoarddbwire.OpAuthOK, 0))
	})

	t.Run("the fallback is refused: no third attempt", func(t *testing.T) {
		fixture := startAuthServer(t, shortTokens)
		ctx := authContext(t)
		_, err := hoarddb.Connect(ctx, fixture.uri("not-the-password"), hoarddb.WithToken("not-a-token"))
		requireAuthFailure(t, err, hoarddb.AuthReasonCredentials, "invalid username or password")
		seen := fixture.proxy.Connections()
		if len(seen) != 2 {
			t.Fatalf("want the token attempt and one fallback, nothing more: %s", servertest.Describe(seen))
		}
		requireAttempts(t, seen, 0, tokenAttempt(hoarddbwire.OpAuthFail, hoarddbwire.AuthFailTokenInvalid))
		requireAttempts(t, seen, 1, passwordAttempt(hoarddbwire.OpAuthFail, hoarddbwire.AuthFailCredentials))
	})

	t.Run("the driver's own token, past its expiry, is not presented", func(t *testing.T) {
		fixture := startAuthServer(t, shortTokens)
		ctx := authContext(t)
		client, err := hoarddb.Connect(ctx, fixture.uri(fixture.server.Password))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		time.Sleep(2100 * time.Millisecond) // a one-second token, judged in whole seconds
		fixture.proxy.DropAll()
		if err := client.Ping(ctx); err != nil {
			t.Fatalf("Ping after the token expired and the connection dropped: %v", err)
		}
		seen := fixture.proxy.Connections()
		if len(seen) != 2 {
			t.Fatalf("want the login and one password login, no wasted token round trip: %s", servertest.Describe(seen))
		}
		requireAttempts(t, seen, 1, passwordAttempt(hoarddbwire.OpAuthOK, 0))
	})
}

// RATE_LIMITED is not a credential failure, and a driver neither retries it nor
// tries the next host with it (handbook/auth.md sections 1 and 4).
func TestRateLimitedIsNotRetried(t *testing.T) {
	const maxFails = 3
	fixture := startAuthServer(t, map[string]string{"HOARDDB_TCP_AUTH_MAX_FAILS": fmt.Sprint(maxFails)})
	second := servertest.StartProxy(t, fixture.server)
	ctx := authContext(t)

	for attempt := 1; attempt <= maxFails; attempt++ {
		_, err := hoarddb.Connect(ctx, fixture.uri("not-the-password"))
		requireAuthFailure(t, err, hoarddb.AuthReasonCredentials)
	}

	// Two hosts: the second is the same server through another proxy, so a
	// driver that tried the next host would show up there.
	twoHosts := fmt.Sprintf("hoarddb://%s:%s@%s,%s/?fingerprint=%s&fingerprint=%s", fixture.server.User, fixture.server.Password,
		fixture.proxy.Address, second.Address, fixture.proxy.Fingerprint, second.Fingerprint)
	_, err := hoarddb.Connect(ctx, twoHosts)
	requireAuthFailure(t, err, hoarddb.AuthReasonRateLimited, "RATE_LIMITED", "too many failed logins from this address")

	seen := fixture.proxy.Connections()
	if len(seen) != maxFails+1 {
		t.Fatalf("want %d refused logins and exactly one attempt after them: %s", maxFails, servertest.Describe(seen))
	}
	requireAttempts(t, seen, maxFails, passwordAttempt(hoarddbwire.OpAuthFail, hoarddbwire.AuthFailRateLimited))
	if other := second.Connections(); len(other) != 0 {
		t.Fatalf("RATE_LIMITED was retried on the next host: %s", servertest.Describe(other))
	}
}

// One refresh goes on the wire however many calls cross the half-life at once,
// and the connections the pool opens meanwhile present the token, not the
// password.
func TestRefreshIsSingleFlight(t *testing.T) {
	const poolSize, callers = 8, 100
	fixture := startAuthServer(t, map[string]string{"HOARDDB_TOKEN_TTL": "4", "HOARDDB_AUTH_SKEW": "0"})
	ctx := authContext(t)
	client, err := hoarddb.Connect(ctx, fixture.uri(fixture.server.Password), hoarddb.WithPoolSize(poolSize))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	refreshes := func(seen []servertest.Connection) int {
		count := 0
		for _, connection := range seen {
			for _, attempt := range connection.Auths {
				if !attempt.OnFreshConnection {
					count++
				}
			}
		}
		return count
	}

	// A four-second token, in whole seconds, is refreshed between 1.5 s and 2 s
	// after the login and next between 1.5 s and 2 s after that: stopping 300 ms
	// after the first refresh observes exactly one boundary.
	stop := make(chan struct{})
	var callersDone sync.WaitGroup
	failures := make(chan error, callers)
	for range callers {
		callersDone.Add(1)
		go func() {
			defer callersDone.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := client.Ping(ctx); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	deadline := time.After(3500 * time.Millisecond)
	waiting := true
	for waiting {
		select {
		case <-deadline:
			waiting = false
		case <-time.After(10 * time.Millisecond):
			if refreshes(fixture.proxy.Connections()) > 0 {
				time.Sleep(300 * time.Millisecond)
				waiting = false
			}
		}
	}
	close(stop)
	callersDone.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("a Ping across the refresh boundary failed: %v", err)
	}

	seen := fixture.proxy.Connections()
	if got := refreshes(seen); got != 1 {
		t.Fatalf("%d refreshes on the wire across one boundary, want 1: %s", got, servertest.Describe(seen))
	}
	passwords := 0
	for _, connection := range seen {
		for _, attempt := range connection.Auths {
			if attempt.Method == "password" {
				passwords++
			}
			if attempt.Answer != hoarddbwire.OpAuthOK {
				t.Errorf("an AUTH was refused: %s", servertest.Describe(seen))
			}
		}
	}
	if passwords != 1 || len(seen) < 2 {
		t.Fatalf("%d password logins over %d connections; want only the first connection to use the password: %s",
			passwords, len(seen), servertest.Describe(seen))
	}
}

// A password the server refused is not sent again by later calls on the same
// client: a pool that re-sent it on every call would be the failed-login loop
// the limiter locks an address out for (handbook/auth.md section 4). The token
// keeps the client working until it expires; then the wrong password is tried
// once, and every later call reports that refusal without dialling.
func TestRefusedPasswordIsNotResentByLaterCalls(t *testing.T) {
	// Three-second tokens: a one-second token could expire between minting it
	// and presenting it, and the test needs the token to work at first.
	fixture := startAuthServer(t, map[string]string{"HOARDDB_TOKEN_TTL": "3", "HOARDDB_AUTH_SKEW": "0"})
	ctx := authContext(t)
	token, _ := loginForToken(t, ctx, fixture.server)

	client, err := hoarddb.Connect(ctx, fixture.uri("not-the-password"), hoarddb.WithToken(token))
	if err != nil {
		t.Fatalf("a good token with a wrong password: %v", err)
	}
	defer func() { _ = client.Close() }()

	time.Sleep(4200 * time.Millisecond) // the token the login refreshed is now dead
	fixture.proxy.DropAll()
	err = client.Ping(ctx)
	requireAuthFailure(t, err, hoarddb.AuthReasonCredentials, "invalid username or password")

	before := len(fixture.proxy.Connections())
	for range 5 {
		// Past the pool's own dial back-off (100 ms doubling), so only the
		// session's refusal to resend the password keeps the call off the wire.
		time.Sleep(500 * time.Millisecond)
		err = client.Ping(ctx)
		requireAuthFailure(t, err, hoarddb.AuthReasonCredentials, "not sent again")
	}
	seen := fixture.proxy.Connections()
	if len(seen) != before {
		t.Fatalf("later calls dialled %d more connection(s) after the password was refused: %s", len(seen)-before, servertest.Describe(seen))
	}
	failed := 0
	for _, connection := range seen {
		for _, attempt := range connection.Auths {
			if attempt.Answer == hoarddbwire.OpAuthFail {
				failed++
			}
		}
	}
	if failed != 1 {
		t.Fatalf("%d refused logins; want exactly one: %s", failed, servertest.Describe(seen))
	}
}

// When a user is dropped the server closes the user's connections (handbook/wire.md
// section 8); the next call dials, is refused, and reports the refusal.
func TestDroppedUserLosesItsConnections(t *testing.T) {
	const poolSize = 2
	fixture := startAuthServer(t, nil)
	ctx := authContext(t)

	// The driver cannot derive the Argon2id credential USER_CREATE carries, so
	// the hoarddb CLI creates the second user and, below, drops it
	// (servertest.CreateUser). The test is the driver's behaviour afterwards.
	const name, password = "ada", "ada-password-9d2e41"
	fixture.server.CreateUser(t, name, password, "reader@app")
	uri := fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s", name, password, fixture.proxy.Address, fixture.proxy.Fingerprint)
	client, err := hoarddb.Connect(ctx, uri, hoarddb.WithPoolSize(poolSize))
	if err != nil {
		t.Fatalf("connecting as the created user: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Hold both pooled connections at once, so the drop has two to close.
	var group sync.WaitGroup
	var pings atomic.Int32
	for range 2 * poolSize {
		group.Add(1)
		go func() {
			defer group.Done()
			if client.Ping(ctx) == nil {
				pings.Add(1)
			}
		}()
	}
	group.Wait()
	if int(pings.Load()) != 2*poolSize {
		t.Fatalf("%d of %d pings by the created user succeeded", pings.Load(), 2*poolSize)
	}
	open := fixture.proxy.Connections()
	if len(open) == 0 {
		t.Fatal("the created user's client opened no connection")
	}

	fixture.server.DropUser(t, name)

	for index := range open {
		if closedBy := fixture.proxy.AwaitClosed(t, index, 10*time.Second).ClosedBy; closedBy != "server" {
			t.Errorf("connection %d was closed by %q; the server must close a dropped user's connections", index+1, closedBy)
		}
	}

	err = client.Ping(ctx)
	requireAuthFailure(t, err, hoarddb.AuthReasonCredentials, "invalid username or password")
	seen := fixture.proxy.Connections()
	if len(seen) != len(open)+2 {
		t.Fatalf("the next call must dial the token and then the password once: %s", servertest.Describe(seen))
	}
	requireAttempts(t, seen, len(open), tokenAttempt(hoarddbwire.OpAuthFail, hoarddbwire.AuthFailTokenInvalid))
	requireAttempts(t, seen, len(open)+1, passwordAttempt(hoarddbwire.OpAuthFail, hoarddbwire.AuthFailCredentials))

	// The refusal is recorded: later calls report it without another password.
	time.Sleep(600 * time.Millisecond)
	err = client.Ping(ctx)
	if !errors.Is(err, hoarddb.ErrAuthFailed) || !strings.Contains(err.Error(), "not sent again") {
		t.Fatalf("a later call: err = %v; want the recorded refusal", err)
	}
	if got := len(fixture.proxy.Connections()); got != len(seen) {
		t.Fatalf("a later call opened %d more connection(s); a refused password is never sent again", got-len(seen))
	}
}

// When the token is dead and every pooled connection is gone at once, a pool
// opening eight connections for 64 callers sends one password login, not eight:
// the others wait for it and present the token it mints (handbook/auth.md
// section 3, items 2 and 4). With a wrong password, the one refusal ends the
// client's logins, so the address sees one failed login, not eight (section 4).
func TestOnePasswordLoginWhenThePoolRedials(t *testing.T) {
	const poolSize, callers, pingsEach = 8, 64, 5
	stampede := func(t *testing.T, ctx context.Context, client *hoarddb.Client) {
		t.Helper()
		var group sync.WaitGroup
		for range callers {
			group.Add(1)
			go func() {
				defer group.Done()
				for range pingsEach {
					_ = client.Ping(ctx) // the counts on the wire are the assertion
				}
			}()
		}
		group.Wait()
	}
	passwordLogins := func(seen []servertest.Connection, answer hoarddbwire.Opcode) int {
		count := 0
		for _, connection := range seen {
			for _, attempt := range connection.Auths {
				if attempt.Method == "password" && attempt.Answer == answer {
					count++
				}
			}
		}
		return count
	}

	t.Run("the right password", func(t *testing.T) {
		// Three-second tokens, so the token the stampede's one password login
		// mints outlives the stampede: with one-second tokens a refresh after
		// expiry rightly needs a second password login.
		fixture := startAuthServer(t, map[string]string{"HOARDDB_TOKEN_TTL": "3", "HOARDDB_AUTH_SKEW": "0"})
		ctx := authContext(t)
		client, err := hoarddb.Connect(ctx, fixture.uri(fixture.server.Password), hoarddb.WithPoolSize(poolSize))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = client.Close() }()
		time.Sleep(4200 * time.Millisecond) // the three-second token is dead
		fixture.proxy.DropAll()
		before := len(fixture.proxy.Connections())
		stampede(t, ctx, client)

		after := fixture.proxy.Connections()[before:]
		if got := passwordLogins(after, hoarddbwire.OpAuthOK); got != 1 {
			t.Fatalf("%d password logins after the drop, want exactly 1 (the rest present its token): %s", got, servertest.Describe(after))
		}
		for _, connection := range after {
			for _, attempt := range connection.Auths {
				if attempt.Answer != hoarddbwire.OpAuthOK {
					t.Fatalf("a login was refused: %s", servertest.Describe(after))
				}
			}
		}
		if err := client.Ping(ctx); err != nil {
			t.Fatalf("the client after the stampede: %v", err)
		}
	})

	t.Run("a wrong password", func(t *testing.T) {
		fixture := startAuthServer(t, map[string]string{"HOARDDB_TOKEN_TTL": "3", "HOARDDB_AUTH_SKEW": "0"})
		ctx := authContext(t)
		token, _ := loginForToken(t, ctx, fixture.server)
		client, err := hoarddb.Connect(ctx, fixture.uri("not-the-password"), hoarddb.WithToken(token), hoarddb.WithPoolSize(poolSize))
		if err != nil {
			t.Fatalf("a good token with a wrong password: %v", err)
		}
		defer func() { _ = client.Close() }()
		time.Sleep(4200 * time.Millisecond) // the token the login refreshed is now dead
		fixture.proxy.DropAll()
		stampede(t, ctx, client)

		seen := fixture.proxy.Connections()
		if got := passwordLogins(seen, hoarddbwire.OpAuthFail); got != 1 {
			t.Fatalf("%d refused password logins, want exactly 1: %s", got, servertest.Describe(seen))
		}
		if got := passwordLogins(seen, hoarddbwire.OpAuthOK); got != 0 {
			t.Fatalf("a wrong password logged in %d time(s)", got)
		}
	})
}
