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

package hoarddb

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/auth"
)

// Backoff for a host that failed: skipped, while another host is available, for
// a while that doubles from hostBackoffBase to hostBackoffMax.
const (
	hostBackoffBase = 500 * time.Millisecond
	hostBackoffMax  = 30 * time.Second
)

// hostEntry is one address from the connection string.
type hostEntry struct {
	address   string
	pin       string
	failures  int
	skipUntil time.Time
}

// dialer opens authenticated connections, trying the hosts in the order they
// were written. The first that completes TLS and AUTH wins; the others are a
// fallback for an unreachable node, not a load-balancing policy.
type dialer struct {
	settings    *config
	session     *auth.Session
	known       *trust.KnownServers
	dialTimeout time.Duration
	now         func() time.Time

	mu    sync.Mutex
	hosts []*hostEntry
}

func (d *dialer) logger() *slog.Logger { return d.settings.logger }

// open dials and authenticates one connection, with the per-host deadline
// covering dial, handshake and AUTH.
func (d *dialer) open(ctx context.Context) (*hoarddbwire.Conn, auth.Info, error) {
	var failures []error
	for _, index := range d.order() {
		conn, greeting, err := d.openHost(ctx, index)
		if err == nil {
			d.markUp(index)
			return conn, greeting, nil
		}
		if ctx.Err() != nil {
			return nil, auth.Info{}, &Error{Code: CodeUnreachable, Op: "connect", Err: ctx.Err(),
				Message: fmt.Sprintf("stopped before %s answered: %v", d.address(index), ctx.Err())}
		}
		if isFinal(err) {
			return nil, auth.Info{}, err
		}
		d.markDown(index)
		failures = append(failures, err)
	}
	return nil, auth.Info{}, allHostsFailed(failures)
}

func (d *dialer) openHost(ctx context.Context, index int) (*hoarddbwire.Conn, auth.Info, error) {
	address := d.address(index)
	hostContext, cancel := context.WithTimeout(ctx, d.dialTimeout)
	defer cancel()
	conn, greeting, err := d.session.Open(hostContext, func(dialContext context.Context) (*hoarddbwire.Conn, error) {
		return d.dialHost(dialContext, index)
	})
	if err != nil {
		return nil, auth.Info{}, d.classify(address, err, hostContext.Err() != nil && ctx.Err() == nil)
	}
	d.logger().Debug("connected", "address", address, "server_version", greeting.ServerVersion)
	return conn, greeting, nil
}

func (d *dialer) dialHost(ctx context.Context, index int) (*hoarddbwire.Conn, error) {
	address := d.address(index)
	tlsConfig, err := d.tlsConfig(index)
	if err != nil {
		return nil, err
	}
	return hoarddbwire.Dial(ctx, address, tlsConfig, hoarddbwire.DialOptions{})
}

// tlsConfig builds the trust policy for one written host.
func (d *dialer) tlsConfig(index int) (*tls.Config, error) {
	return d.buildTLS(d.address(index), d.hostPolicy(index))
}

// hostPolicy is the trust policy of one written host: a pin if there is one,
// else the named opt-out if the caller gave a reason, else the known-servers
// file with the caller's prompt, if any.
func (d *dialer) hostPolicy(index int) trust.Policy {
	var policy trust.Policy
	switch {
	case d.pin(index) != "":
		policy.Pin = d.pin(index)
	case d.settings.insecureReason != "":
		policy.InsecureReason = d.settings.insecureReason
		policy.OnInsecure = d.warnInsecure
	default:
		policy.Known = d.known
		if prompt := d.settings.trustPrompt; prompt != nil {
			policy.Prompt = func(address, fingerprint string) bool {
				accepted := prompt(address, fingerprint)
				if accepted {
					d.logger().Warn("trusted a server the first time it was seen, and recorded its key",
						"address", address, "fingerprint", fingerprint, "known_servers", d.known.Path())
				}
				return accepted
			}
		}
	}
	return policy
}

func (d *dialer) warnInsecure(address, fingerprint string) {
	d.logger().Warn("server authentication is switched off: this connection is not protected against impersonation",
		"address", address, "presented", fingerprint, "reason", d.settings.insecureReason)
}

func (d *dialer) buildTLS(address string, policy trust.Policy) (*tls.Config, error) {
	config, err := trust.NewTLSConfig(address, policy)
	if err != nil {
		return nil, fmt.Errorf("building the TLS trust policy for %s: %w", address, err)
	}
	return config, nil
}

// order lists host indexes: those not backing off first, in written order, then
// the others, so a downed host is retried only when nothing else is left.
func (d *dialer) order() []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	var ready, waiting []int
	for index, host := range d.hosts {
		if now.Before(host.skipUntil) {
			waiting = append(waiting, index)
		} else {
			ready = append(ready, index)
		}
	}
	return append(ready, waiting...)
}

func (d *dialer) markUp(index int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hosts[index].failures, d.hosts[index].skipUntil = 0, time.Time{}
}

func (d *dialer) markDown(index int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	host := d.hosts[index]
	host.failures++
	wait := hostBackoffBase
	for step := 1; step < host.failures && wait < hostBackoffMax; step++ {
		wait *= 2
	}
	host.skipUntil = d.now().Add(min(wait, hostBackoffMax))
}

func (d *dialer) address(index int) string { return d.hosts[index].address }
func (d *dialer) pin(index int) string     { return d.hosts[index].pin }

// classify turns whatever dialling or authenticating returned into an *Error
// whose message says what to do next. timedOut is true when the per-host
// deadline, not the caller's context, ended the attempt.
func (d *dialer) classify(address string, err error, timedOut bool) error {
	var (
		unknown     *trust.UnknownServerError
		changed     *trust.KeyChangedError
		mismatch    *trust.MismatchError
		unsupported *trust.UnsupportedKeyError
		unsafeFile  *trust.UnsafeFileError
		conflicting *trust.ConflictingEntriesError
		ended       *auth.EndedError
		holding     *auth.HoldingOffError
		rejection   *auth.Rejection
		unexpected  *auth.UnexpectedReplyError
		refused     *hoarddbwire.RefusedError
	)
	switch {
	case errors.As(err, &unknown):
		return d.untrusted(address, unknown)
	case errors.As(err, &changed):
		return &Error{Code: CodeServerKeyChanged, Op: "connect", Target: address, Err: err, Message: fmt.Sprintf(
			"the server at %s presented %s but %s is recorded in %s. If you rotated the server's keys, delete the line for %s from that file; "+
				"if you did not, someone may be intercepting this connection. The driver will not retry.",
			address, changed.Presented, changed.Recorded, changed.Path, address)}
	case errors.As(err, &mismatch):
		return &Error{Code: CodeFingerprintMismatch, Op: "connect", Target: address, Err: err, Message: fmt.Sprintf(
			"the server at %s presented %s but %s is pinned. If you replaced the server's keys, take the new fingerprint from its startup log "+
				"(the line that says \"TCP driver listener listening\") and pin that; if you did not, someone may be intercepting this connection. "+
				"The driver will not retry.", address, mismatch.Presented, mismatch.Pinned)}
	case errors.As(err, &unsafeFile):
		return &Error{Code: CodeUntrustedServer, Op: "connect", Target: address, Err: err, Message: fmt.Sprintf(
			"%v. The driver will not trust a server through a file other users can modify, and will not retry.", unsafeFile)}
	case errors.As(err, &conflicting):
		return &Error{Code: CodeUntrustedServer, Op: "connect", Target: address, Err: err, Message: fmt.Sprintf(
			"%v. The driver will not choose between them, so it trusts no server through that file until one of the two lines is deleted: "+
				"keep the line whose key matches the fingerprint the server at %s prints at startup. The driver will not retry.",
			conflicting, conflicting.Address)}
	case errors.Is(err, trust.ErrLockTimeout):
		return &Error{Code: CodeUnavailable, Op: "connect", Target: address, Err: err, Message: fmt.Sprintf(
			"could not record the server's key in %s: %v. Another process is holding the file; try again in a moment.", d.known.Path(), err)}
	case errors.As(err, &unsupported):
		return &Error{Code: CodeUntrustedServer, Op: "connect", Target: address, Err: err, Message: fmt.Sprintf(
			"%s. Check that the address is the hoardDB driver port (7433 by default).", unsupported)}
	case errors.As(err, &ended):
		failure := d.authFailed(address, ended.Refused)
		failure.Err = err
		failure.Message = "this client's login was refused earlier, so it is not sent again: the same credentials would be refused again, " +
			"and every refusal counts toward locking this address out. Create a new client once the credentials are fixed. " +
			"The refusal: " + failure.Message
		return failure
	case errors.As(err, &holding):
		failure := d.authFailed(address, holding.Refused)
		failure.Err = err
		failure.Message = fmt.Sprintf("the driver is holding off logins until %s, because the server answered RATE_LIMITED a moment ago. %s",
			holding.Until.Format(time.RFC3339), failure.Message)
		return failure
	case errors.As(err, &rejection):
		return d.authFailed(address, rejection)
	case errors.As(err, &refused):
		return wireError("connect", address, refused.Response)
	case errors.As(err, &unexpected):
		return &Error{Code: CodeInternal, Op: "connect", Target: address, Err: err, Message: fmt.Sprintf(
			"%v. This may not be a hoardDB driver port, or the server is a different version; check the address.", unexpected)}
	case timedOut:
		return &Error{Code: CodeUnreachable, Op: "connect", Target: address, Err: context.DeadlineExceeded, Message: fmt.Sprintf(
			"no answer from %s within %v. Check that the host is reachable and the server is running; "+
				"raise the limit with ?dialTimeoutMs=<milliseconds> in the connection string.", address, d.dialTimeout)}
	default:
		return &Error{Code: CodeUnreachable, Op: "connect", Target: address, Err: err, Message: unreachableText(address, err)}
	}
}

func unreachableText(address string, err error) string {
	var header tls.RecordHeaderError
	if errors.As(err, &header) || strings.Contains(err.Error(), "tls:") {
		return fmt.Sprintf("the port at %s answered but did not complete a TLS 1.3 handshake (%v). "+
			"This may not be a hoardDB driver port: the driver port is 7433, and 4433 is the internode port.", address, err)
	}
	if isLoopback(address) {
		return fmt.Sprintf("could not reach a hoardDB server at %s (%v). Check that the server is running (start one with 'hoardDB-server') "+
			"and that the port is right (the driver port is 7433). The server binary listens on loopback, 127.0.0.1:7433, by default, "+
			"so a server on this machine answers at localhost:7433; a server in a container listens on every interface inside it, "+
			"so check that the container publishes its port to this machine (for example -p 7433:7433).", address, err)
	}
	return fmt.Sprintf("could not reach a hoardDB server at %s (%v). Check that the server is running, that the address is right "+
		"(the driver port is 7433), and that nothing blocks the connection. The server binary listens on loopback, 127.0.0.1:7433, by default, "+
		"so another machine cannot reach it until it is configured to listen on a network interface; the container image listens on "+
		"every interface, and the ports it publishes decide what is reachable.", address, err)
}

// isLoopback reports whether address names this machine: "localhost" or a
// loopback IP. The unreachable message differs, because the server's default
// is to listen on loopback only.
func isLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// allHostsFailed reports a connect that no host completed. One failure is
// returned as it is, keeping its class (a refusal for overload stays
// ErrOverloaded); several are joined under ErrUnreachable and each keeps its
// class for errors.Is.
func allHostsFailed(failures []error) error {
	if len(failures) == 1 {
		return failures[0]
	}
	messages := make([]string, len(failures))
	for index, failure := range failures {
		messages[index] = failure.Error()
	}
	return &Error{Code: CodeUnreachable, Op: "connect", Err: errors.Join(failures...),
		Message: fmt.Sprintf("none of the %d hosts answered: %s", len(failures), strings.Join(messages, "; "))}
}

func (d *dialer) untrusted(address string, unknown *trust.UnknownServerError) *Error {
	known := d.known.Path()
	if unknown.Declined {
		return &Error{Code: CodeUntrustedServer, Op: "connect", Target: address, Err: unknown, Message: fmt.Sprintf(
			"the server at %s presented %s and the trust prompt declined it. Pin it with ?fingerprint=%s if it is the server you meant to reach.",
			address, unknown.Fingerprint, unknown.Fingerprint)}
	}
	return &Error{Code: CodeUntrustedServer, Op: "connect", Target: address, Err: unknown, Message: fmt.Sprintf(
		"the server at %s is not trusted yet: it presented %s and no fingerprint is pinned for it. "+
			"Pin it with hoarddb://...?fingerprint=%s (the server prints this line at startup), "+
			"or run the hoardDB command line tool once against it and answer y (this driver reads the same %s), "+
			"or, for local development only, add hoarddb.WithTrustPrompt(hoarddb.AcceptFirstSight).",
		address, unknown.Fingerprint, unknown.Fingerprint, known)}
}

func (d *dialer) authFailed(address string, rejection *auth.Rejection) *Error {
	user := rejection.Username
	failure := &Error{Code: CodeAuthFailed, Op: "connect", Target: address, Reason: AuthReason(rejection.Reason), Err: rejection}
	verbatim := ""
	if rejection.Message != "" {
		verbatim = fmt.Sprintf(" The server said: %q.", rejection.Message)
	}
	switch rejection.Reason {
	case hoarddbwire.AuthFailRateLimited:
		// The wire gives one reason for two causes, so say both and let the
		// server's own sentence (appended) say which applies.
		failure.Message = "authentication was refused as RATE_LIMITED, which is not a credential failure and means no credential was checked. " +
			"The server sends it either because of too many failed logins from this address, or because it is busy verifying other passwords. " +
			"Wait a few seconds (about a minute after repeated failed logins) and try again; the driver has stopped retrying." + verbatim
	case hoarddbwire.AuthFailProtocolMismatch:
		failure.Message = fmt.Sprintf("the server and this driver speak different protocol versions.%s Upgrade whichever is older.", verbatim)
	case hoarddbwire.AuthFailInternal:
		failure.Message = fmt.Sprintf("the node at %s cannot authenticate anyone yet; it has no user configuration (it may be starting up or cut off from the cluster)."+
			" The next host is tried, or retry shortly.%s", address, verbatim)
	case hoarddbwire.AuthFailTokenExpired, hoarddbwire.AuthFailTokenInvalid, hoarddbwire.AuthFailTokenUnsupported:
		failure.Message = fmt.Sprintf("authentication as %q failed: the server rejected the token (%s) and there is no password to fall back to. "+
			"Connect with a password (WithCredentials, or user:password@ in the connection string).%s", user, rejection.Reason, verbatim)
	default:
		failure.Message = fmt.Sprintf("authentication as %q failed: the server rejected the credentials. This driver does not retry a failed login. "+
			"The root password is whatever %s was when the server first started, or the contents of <data dir>/root.password if the server generated one.%s",
			user, envRootPassword, verbatim)
	}
	return failure
}
