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

// Package auth holds a client's credentials and runs the AUTH exchange that
// must be the first frame on every connection.
//
// A password AUTH is an Argon2id verification on the server, and the server
// accepts exactly one per connection, spent even when it fails. So a [Session]
// keeps the password in one place, mints a resumption token with the first
// password AUTH, and presents the token (never the password) on every later
// connection. When the token is rejected it falls back to the password once, on
// a fresh connection. It never retries a rejected login: a failed login in a
// loop is the credential-stuffing pattern the server's limiter exists to stop.
package auth

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// protocolVersion is the ALPN major sent in AuthRequest.
const protocolVersion = 1

// maxFallbackJitter spreads the password fallback of a pool whose connections
// all lose their token at once, so they do not arrive as one burst.
const maxFallbackJitter = 250 * time.Millisecond

// refreshRetryDelay is how long a refresh the server refused for a reason other
// than the token (rate limiting) waits before the next attempt.
const refreshRetryDelay = 30 * time.Second

// Errors a [Session] returns. Match them with errors.Is or errors.As.
var (
	// ErrNoCredentials is a Session with neither a password nor a token.
	ErrNoCredentials = errors.New("no password and no token to authenticate with")
)

// Rejection is a server's refusal of an AUTH.
type Rejection struct {
	// Username the attempt was made as.
	Username string
	// Method is "password" or "token".
	Method string
	// Reason is the server's reason code. Zero means the server answered
	// AUTH_OK with success false.
	Reason hoarddbwire.AuthFailReason
	// Message is the server's sentence, verbatim.
	Message string
}

func (rejection *Rejection) Error() string {
	reason := rejection.Reason.String()
	if rejection.Reason == 0 {
		reason = "not successful"
	}
	return fmt.Sprintf("the server refused the %s login for %q (%s): %s", rejection.Method, rejection.Username, reason, rejection.Message)
}

// rateLimitedHold is how long a session sends no AUTH after the server answered
// RATE_LIMITED. The refusal checked no credential, so the session is not ended;
// but the address is refused for up to a minute after repeated failures, and a
// login every time a pool wants a connection is the loop handbook/auth.md
// section 4 forbids.
const rateLimitedHold = 5 * time.Second

// EndedError is an Open after the server refused this session's credentials
// (CREDENTIALS) or protocol (PROTOCOL_MISMATCH). The session sends neither
// again: the same password would be refused again, and each refusal counts
// toward locking the address out. Refused is the original refusal.
type EndedError struct {
	Refused *Rejection
}

func (err *EndedError) Error() string {
	return fmt.Sprintf("the %s login for %q was refused earlier and is not sent again: %v", err.Refused.Method, err.Refused.Username, err.Refused)
}

// Unwrap returns the original refusal.
func (err *EndedError) Unwrap() error { return err.Refused }

// HoldingOffError is an Open within rateLimitedHold of a RATE_LIMITED refusal.
// No AUTH is sent until Until.
type HoldingOffError struct {
	Until   time.Time
	Refused *Rejection
}

func (err *HoldingOffError) Error() string {
	return fmt.Sprintf("holding off logins until %s after: %v", err.Until.Format(time.RFC3339), err.Refused)
}

// Unwrap returns the RATE_LIMITED refusal.
func (err *HoldingOffError) Unwrap() error { return err.Refused }

// UnexpectedReplyError is an answer to AUTH that is neither AUTH_OK nor
// AUTH_FAIL.
type UnexpectedReplyError struct {
	Op hoarddbwire.Opcode
	// Response is the decoded error, set when Op is OpError.
	Response hoarddbwire.ErrorResponse
}

func (err *UnexpectedReplyError) Error() string {
	if err.Op == hoarddbwire.OpError {
		return fmt.Sprintf("the server answered AUTH with an error (%s): %s", err.Response.Code, err.Response.Message)
	}
	return fmt.Sprintf("the server answered AUTH with an unexpected %s frame", err.Op)
}

// Config creates a [Session].
type Config struct {
	Username string
	Password string
	// Token is a resumption token from an earlier session, if the caller has one.
	Token string
	// ClientName is sent as AuthRequest.Client, for the server's logs.
	ClientName string

	// Now, Sleep and Jitter exist so tests can control time; nil means the real
	// ones.
	Now    func() time.Time
	Sleep  func(ctx context.Context, wait time.Duration) error
	Jitter func(limit time.Duration) time.Duration
}

// Info is what the server told a connection at AUTH.
type Info struct {
	ServerVersion   string
	ProtocolVersion uint32
	Capabilities    []string
}

// Session is a client's credentials and the token state derived from them. It
// is safe for concurrent use.
type Session struct {
	username   string
	password   string
	clientName string
	now        func() time.Time
	sleep      func(ctx context.Context, wait time.Duration) error
	jitter     func(limit time.Duration) time.Duration

	refreshing sync.Mutex // held for the length of one refresh: single flight

	// passwordLogin is a one-slot semaphore held for the length of one password
	// AUTH, dial included, so a pool that redials eight connections at once sends
	// one password: the others wait, then present the token it minted or report
	// its refusal. It is a channel so a waiter can give up with its context.
	passwordLogin chan struct{}

	mu               sync.Mutex
	token            string
	expiresAt        time.Time
	refreshAt        time.Time
	deadline         time.Time
	tokenUnsupported bool
	ended            *Rejection // CREDENTIALS or PROTOCOL_MISMATCH: no more logins
	held             *Rejection // the last RATE_LIMITED, holding logins off until holdUntil
	holdUntil        time.Time
}

// NewSession returns a Session. A Session with neither password nor token
// reports [ErrNoCredentials] from every Open.
func NewSession(config Config) *Session {
	session := &Session{
		username:   config.Username,
		password:   config.Password,
		clientName: config.ClientName,
		token:      config.Token,
		now:        config.Now,
		sleep:      config.Sleep,
		jitter:     config.Jitter,

		passwordLogin: make(chan struct{}, 1),
	}
	if session.now == nil {
		session.now = time.Now
	}
	if session.sleep == nil {
		session.sleep = sleepContext
	}
	if session.jitter == nil {
		session.jitter = func(limit time.Duration) time.Duration { return rand.N(limit) }
	}
	return session
}

// String omits the credentials, so a Session is safe to log.
func (session *Session) String() string {
	return fmt.Sprintf("auth.Session{user=%q, password=%s}", session.username, redact(session.password))
}

// GoString is String, so %#v does not print the fields.
func (session *Session) GoString() string { return session.String() }

// Format is String under every verb: without it %d, %x and %+v of a nested
// value would print the fields, password included. A short write to the
// fmt.State has no one to report to, so its error is not checked.
func (session *Session) Format(state fmt.State, _ rune) {
	_, _ = fmt.Fprint(state, session.String())
}

// String omits the credentials, so a Config is safe to log.
func (config Config) String() string {
	return fmt.Sprintf("auth.Config{user=%q, password=%s, token=%s}", config.Username, redact(config.Password), redact(config.Token))
}

// GoString is String, so %#v does not print the fields.
func (config Config) GoString() string { return config.String() }

// Format is String under every verb, see [Session.Format].
func (config Config) Format(state fmt.State, _ rune) { _, _ = fmt.Fprint(state, config.String()) }

func redact(secret string) string {
	if secret == "" {
		return "unset"
	}
	return "***"
}

// Username is the user the session logs in as.
func (session *Session) Username() string { return session.username }

// Open dials a connection with dial and authenticates it, returning the
// connection ready for requests. It tries the token first when one is held and
// believed alive, and falls back to the password once, on a new connection. dial
// is called once per attempt; Open closes any connection it does not return.
//
// One password AUTH is in flight per Session at a time; concurrent Opens that
// need the password wait for it and then use what it left: the token it minted,
// or its refusal.
//
// After a refusal of the credentials or the protocol, Open returns
// [EndedError] without dialling; within a while of RATE_LIMITED, it returns
// [HoldingOffError] without dialling.
func (session *Session) Open(ctx context.Context, dial func(context.Context) (*hoarddbwire.Conn, error)) (*hoarddbwire.Conn, Info, error) {
	fellBack := false
	for {
		chosen, release, err := session.beginLogin(ctx)
		if err != nil {
			return nil, Info{}, err
		}
		conn, greeting, err := session.attempt(ctx, dial, chosen)
		release()
		if err == nil {
			return conn, greeting, nil
		}
		if chosen.method != "token" || fellBack || !session.fallBackToPassword(err, chosen.secret) {
			return nil, Info{}, err
		}
		fellBack = true
		if err := session.sleep(ctx, session.jitter(maxFallbackJitter)); err != nil {
			return nil, Info{}, err
		}
	}
}

// attempt dials one connection and sends one AUTH with chosen on it, closing
// the connection when the AUTH fails.
func (session *Session) attempt(ctx context.Context, dial func(context.Context) (*hoarddbwire.Conn, error), chosen credential) (*hoarddbwire.Conn, Info, error) {
	conn, err := dial(ctx)
	if err != nil {
		return nil, Info{}, err
	}
	greeting, err := session.authenticate(ctx, conn, chosen)
	if err != nil {
		_ = conn.Close()
		session.remember(err)
		return nil, Info{}, err
	}
	return conn, greeting, nil
}

// beginLogin picks the credential for the next connection, copied out of the
// session so the frame sends exactly what was chosen even if another goroutine
// changes the session meanwhile. For the password it takes the session's one
// password slot and chooses again once it holds it, because the login it
// waited for may have minted a token or been refused. release frees the slot,
// and is a no-op for a token login.
func (session *Session) beginLogin(ctx context.Context) (chosen credential, release func(), err error) {
	noop := func() {}
	if err := session.refusedBefore(); err != nil {
		return credential{}, noop, err
	}
	chosen = session.chooseCredential()
	switch chosen.method {
	case "":
		return credential{}, noop, ErrNoCredentials
	case "token":
		return chosen, noop, nil
	}
	select {
	case session.passwordLogin <- struct{}{}:
	case <-ctx.Done():
		return credential{}, noop, ctx.Err()
	}
	release = func() { <-session.passwordLogin }
	if err := session.refusedBefore(); err != nil {
		release()
		return credential{}, noop, err
	}
	chosen = session.chooseCredential()
	switch chosen.method {
	case "password":
		return chosen, release, nil
	case "":
		release()
		return credential{}, noop, ErrNoCredentials
	default:
		release()
		return chosen, noop, nil
	}
}

// refusedBefore returns the error for a login the session will not attempt.
func (session *Session) refusedBefore() error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.ended != nil {
		return &EndedError{Refused: session.ended}
	}
	if session.held != nil && session.now().Before(session.holdUntil) {
		return &HoldingOffError{Until: session.holdUntil, Refused: session.held}
	}
	return nil
}

// refusedForGood reports whether a refusal has ended the session's logins.
func (session *Session) refusedForGood() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.ended != nil
}

// remember records a refusal that changes what later logins may do.
func (session *Session) remember(err error) {
	var rejection *Rejection
	if !errors.As(err, &rejection) {
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	switch rejection.Reason {
	case hoarddbwire.AuthFailCredentials, hoarddbwire.AuthFailProtocolMismatch:
		// The session will never send the password again, so it stops keeping
		// it (handbook/auth.md section 3, item 1).
		session.ended, session.password = rejection, ""
	case hoarddbwire.AuthFailRateLimited:
		session.held, session.holdUntil = rejection, session.now().Add(rateLimitedHold)
	}
}

// credential is what one AUTH frame sends, copied out of the session when the
// login is chosen. method is "password" or "token"; secret is never empty.
type credential struct {
	method string
	secret string
}

// chooseCredential picks the credential for the next connection: the token
// while it is believed alive, otherwise the password. The zero credential
// means there is nothing to send.
func (session *Session) chooseCredential() credential {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.token != "" && !session.tokenUnsupported && !session.tokenKnownDead() {
		return credential{method: "token", secret: session.token}
	}
	if session.password != "" {
		return credential{method: "password", secret: session.password}
	}
	if session.token != "" && !session.tokenUnsupported {
		// No password to fall back to: let the server say what is wrong.
		return credential{method: "token", secret: session.token}
	}
	return credential{}
}

// tokenKnownDead reports whether the expiry times, when known, have passed.
// Caller holds mu. A token handed in by the caller has no known expiry; the
// server decides.
func (session *Session) tokenKnownDead() bool {
	now := session.now()
	return (!session.expiresAt.IsZero() && !now.Before(session.expiresAt)) ||
		(!session.deadline.IsZero() && !now.Before(session.deadline))
}

// fallBackToPassword inspects a failed AUTH that presented rejected, a token.
// It forgets that token when the server rejected it, and reports whether there
// is something left to log in with: the password, or a newer token another
// connection minted meanwhile.
func (session *Session) fallBackToPassword(err error, rejected string) bool {
	var rejection *Rejection
	if !errors.As(err, &rejection) {
		return false
	}
	switch rejection.Reason {
	case hoarddbwire.AuthFailTokenUnsupported:
		session.dropToken(rejected, true)
	case hoarddbwire.AuthFailTokenExpired, hoarddbwire.AuthFailTokenInvalid:
		session.dropToken(rejected, false)
	default:
		return false
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.password != "" || (session.token != "" && session.token != rejected)
}

// dropToken forgets the token rejected, but not a newer one a concurrent login
// or refresh has adopted since. unsupported records that the server mints none.
func (session *Session) dropToken(rejected string, unsupported bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if unsupported {
		session.tokenUnsupported = true
	}
	if session.token != rejected {
		return
	}
	session.token = ""
	session.expiresAt, session.refreshAt, session.deadline = time.Time{}, time.Time{}, time.Time{}
}

// authenticate sends the AUTH frame for chosen on conn and interprets the
// reply. It sends chosen as it was copied out of the session and never reads
// the session's credentials again: another goroutine may have dropped them
// since, and a frame with neither a token nor a password is a failed login.
func (session *Session) authenticate(ctx context.Context, conn *hoarddbwire.Conn, chosen credential) (Info, error) {
	method := chosen.method
	request := hoarddbwire.AuthRequest{
		Username:        session.username,
		ProtocolVersion: protocolVersion,
		Capabilities:    []string{},
		Client:          session.clientName,
	}
	if method == "token" {
		// The token alone: a non-empty token makes the server ignore the password.
		request.Token = chosen.secret
	} else {
		request.Password = chosen.secret
	}

	var response hoarddbwire.AuthResponse
	reply, err := conn.Do(ctx, hoarddbwire.OpAuth, request, &response)
	if err != nil {
		return Info{}, err
	}
	switch reply.Op {
	case hoarddbwire.OpAuthOK:
		if !response.Success {
			return Info{}, &Rejection{Username: session.username, Method: method, Message: response.Message}
		}
		session.adopt(response)
		return Info{
			ServerVersion:   response.ServerVersion,
			ProtocolVersion: response.ProtocolVersion,
			Capabilities:    response.Capabilities,
		}, nil
	case hoarddbwire.OpAuthFail:
		failure := hoarddbwire.DecodeAuthFail(reply.Payload)
		return Info{}, &Rejection{Username: session.username, Method: method, Reason: failure.Reason, Message: failure.Message}
	case hoarddbwire.OpError:
		return Info{}, &UnexpectedReplyError{Op: reply.Op, Response: hoarddbwire.DecodeError(reply.Payload)}
	default:
		return Info{}, &UnexpectedReplyError{Op: reply.Op}
	}
}

// adopt records the token a successful AUTH minted. A server without the
// auth_token capability is not trusted to have minted one.
func (session *Session) adopt(response hoarddbwire.AuthResponse) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if !slices.Contains(response.Capabilities, hoarddbwire.CapabilityAuthToken) || response.Token == "" {
		session.token = ""
		session.tokenUnsupported = !slices.Contains(response.Capabilities, hoarddbwire.CapabilityAuthToken)
		return
	}
	now := session.now()
	session.token = response.Token
	session.expiresAt = time.Unix(response.TokenExpiresAt, 0)
	session.deadline = time.Unix(response.TokenDeadline, 0)
	session.refreshAt = now.Add(session.expiresAt.Sub(now) / 2)
}

// NeedsRefresh reports whether the token has passed half of its life and the
// refresh chain has not ended.
func (session *Session) NeedsRefresh() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.token == "" || session.refreshAt.IsZero() {
		return false
	}
	now := session.now()
	return !now.Before(session.refreshAt) && now.Before(session.expiresAt) && now.Before(session.deadline)
}

// Reauthenticate sends one AUTH on conn, a connection the server has answered
// NOT_AUTHENTICATED on, and so still takes an AUTH (handbook/wire.md section 8).
// The credential is chosen as for a new connection: the token while it is
// believed alive, else the password, one password login per session at a time.
// It sends exactly one frame and never falls back: a refusal is recorded as in
// Open (a rejected token is forgotten, so the next connection uses the
// password) and returned, and the caller discards conn.
func (session *Session) Reauthenticate(ctx context.Context, conn *hoarddbwire.Conn) (Info, error) {
	chosen, release, err := session.beginLogin(ctx)
	if err != nil {
		return Info{}, err
	}
	defer release()
	greeting, err := session.authenticate(ctx, conn, chosen)
	if err != nil {
		session.remember(err)
		if chosen.method == "token" {
			session.fallBackToPassword(err, chosen.secret)
		}
		return Info{}, err
	}
	return greeting, nil
}

// Refresh presents the token on conn, an already authenticated connection that
// no request is using, and records the token the server returns. Only one
// refresh runs at a time: a caller that finds one in flight returns nil at once
// and lets that one finish. When the server rejects the token, Refresh forgets
// it, so the next connection falls back to the password, and returns the
// rejection; the caller should discard conn. CREDENTIALS and PROTOCOL_MISMATCH
// end the session's logins, as in Open, and are returned. RATE_LIMITED holds
// new logins off, as in Open, but leaves the token and the connection alone:
// Refresh postpones the next attempt and returns nil.
func (session *Session) Refresh(ctx context.Context, conn *hoarddbwire.Conn) error {
	if !session.refreshing.TryLock() {
		return nil
	}
	defer session.refreshing.Unlock()
	if !session.NeedsRefresh() {
		return nil
	}
	session.mu.Lock()
	presented := credential{method: "token", secret: session.token}
	session.mu.Unlock()
	if presented.secret == "" {
		return nil
	}
	_, err := session.authenticate(ctx, conn, presented)
	if err != nil {
		var rejection *Rejection
		if !errors.As(err, &rejection) {
			return err
		}
		// The same bookkeeping as a refusal in Open: CREDENTIALS and
		// PROTOCOL_MISMATCH end the session's logins, RATE_LIMITED holds new
		// ones off.
		session.remember(err)
		if session.fallBackToPassword(err, presented.secret) || session.tokenRejected(rejection.Reason) || session.refusedForGood() {
			return err
		}
		// Refused for a reason that is not about the token (rate limiting): the
		// token is still good and so is the connection. Try again later instead
		// of on every call.
		session.postponeRefresh()
	}
	return nil
}

func (session *Session) tokenRejected(reason hoarddbwire.AuthFailReason) bool {
	switch reason {
	case hoarddbwire.AuthFailTokenExpired, hoarddbwire.AuthFailTokenInvalid, hoarddbwire.AuthFailTokenUnsupported:
		return true
	default:
		return false
	}
}

func (session *Session) postponeRefresh() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.refreshAt = session.now().Add(refreshRetryDelay)
}

func sleepContext(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
