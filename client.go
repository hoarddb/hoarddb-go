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
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/auth"
	"github.com/hoarddb/hoarddb-go/internal/pool"
	"github.com/hoarddb/hoarddb-go/internal/uri"
)

// Version is this driver's version, sent to the server in AUTH for its logs.
const Version = "0.1.0"

// ServerInfo is what the server told the driver when it authenticated.
type ServerInfo struct {
	// Version is the server's build version.
	Version string
	// ProtocolVersion is the wire protocol major the server allocated.
	ProtocolVersion uint32
	// Capabilities are the features the server advertises.
	Capabilities []string
}

// Has reports whether the server advertises the capability.
func (server ServerInfo) Has(capability string) bool {
	return slices.Contains(server.Capabilities, capability)
}

// PoolStats are aggregate connection counters. They are never per bucket.
type PoolStats struct {
	// Open connections, idle or in use.
	Open int
	// InUse connections have a call running on them.
	InUse int
	// Waiting calls are blocked for a free connection.
	Waiting int
	// Dials, DialErrors and Discarded count connections opened, failed dials,
	// and connections dropped as dead, idle or abandoned.
	Dials, DialErrors, Discarded uint64
	// Retries counts requests the driver sent again, and dials it made again
	// after a refusal for load, under the retry table of handbook/errors.md
	// section 6. A retry is otherwise invisible to the caller.
	Retries uint64
}

// Client is a connection pool to a hoardDB server. It is safe for concurrent
// use by many goroutines. Create one with [Connect], share it, and [Client.Close]
// it when the program is done.
type Client struct {
	settings *config
	target   *uri.URI
	session  *auth.Session
	dialer   *dialer
	// point and bulk are the two lanes (handbook/api-go.md section 8): each its
	// own pool, so a call waiting for a bulk connection never delays a point one.
	point *pool.Pool[*hoarddbwire.Conn]
	bulk  *pool.Pool[*hoarddbwire.Conn]

	serverMu sync.RWMutex
	server   ServerInfo

	retries atomic.Uint64
}

// String names the client by its user and hosts; it holds no credential that
// could be printed.
func (c *Client) String() string {
	return fmt.Sprintf("hoarddb.Client{user=%q, target=%s}", c.session.Username(), c.target.Redacted())
}

// GoString is String, so %#v does not print the fields of the client.
func (c *Client) GoString() string { return c.String() }

// Format is String under every verb, so no verb walks the client's fields
// (the session holds the password). A short write to the fmt.State has no one
// to report to, so its error is not checked.
func (c *Client) Format(state fmt.State, _ rune) { _, _ = fmt.Fprint(state, c.String()) }

// Connect dials the first reachable host in connectionString, verifies the server's identity,
// authenticates, and returns a Client whose pool is lazy: only the one
// connection that proved the credentials is open when Connect returns.
//
// connectionString is hoarddb://[user[:password]@]host[:port][,host...][/database[/bucket]][?params].
// An empty connectionString is read from HOARDDB_URI. A password the URI omits comes from
// HOARDDB_ROOT_PASSWORD and a user from HOARDDB_ROOT_USER (default "admin").
// The server must be identified by a pinned fingerprint (?fingerprint= or
// [WithFingerprint]) or by an entry in the known-servers file; see
// [WithTrustPrompt].
//
// ctx bounds the whole call; ?dialTimeoutMs= bounds each host.
func Connect(ctx context.Context, connectionString string, opts ...Option) (*Client, error) {
	client, err := newClient(connectionString, opts)
	if err != nil {
		return nil, err
	}
	lease, err := client.point.Acquire(ctx)
	if err != nil {
		_ = client.Close()
		return nil, client.connectError(err)
	}
	lease.Release()
	return client, nil
}

func newClient(connectionString string, opts []Option) (*Client, error) {
	if connectionString == "" {
		connectionString = os.Getenv(envURI)
	}
	if connectionString == "" {
		return nil, &Error{Code: CodeInvalidURI, Op: "connect", Message: "no connection string given: pass one to Connect, or set " +
			envURI + ", for example hoarddb://localhost:7433"}
	}
	target, err := uri.Parse(connectionString)
	if err != nil {
		return nil, &Error{Code: CodeInvalidURI, Op: "connect", Message: err.Error(), Err: err}
	}
	settings := &config{
		poolSize:       defaultPoolSize,
		bulkPoolSize:   defaultBulkPoolSize,
		requestTimeout: defaultRequestTimeout,
		logger:         discardLogger(),
	}
	for _, option := range opts {
		if err := option(settings); err != nil {
			return nil, err
		}
	}
	if err := resolveCredentials(settings, target); err != nil {
		return nil, err
	}
	pins, err := resolvePins(settings, target)
	if err != nil {
		return nil, err
	}
	known, err := resolveKnownServers(settings)
	if err != nil {
		return nil, err
	}
	if settings.insecureReason != "" && slices.ContainsFunc(pins, func(pin string) bool { return pin != "" }) {
		return nil, optionError("both a fingerprint and WithInsecureSkipVerify were given; they contradict each other. " +
			"Drop WithInsecureSkipVerify: the fingerprint is the safer choice and is enough")
	}
	if settings.poolSize > warnPoolSize {
		settings.logger.Warn("pool size is above 16; the server allows 64 connections per source address by default, shared by every process on this host",
			"pool_size", settings.poolSize)
	}

	client := &Client{settings: settings, target: target}
	client.session = auth.NewSession(auth.Config{
		Username:   settings.username,
		Password:   settings.password,
		Token:      settings.token,
		ClientName: clientIdentity(settings.clientName, target.ClientName),
	})
	// From here the session is the only holder of the credentials
	// (handbook/auth.md section 3, item 1): it drops them when it can no longer
	// use them, which a copy left here would outlive.
	settings.password, settings.token = "", ""
	target.Password, target.AuthToken = "", ""
	client.dialer = &dialer{
		settings:    settings,
		session:     client.session,
		known:       known,
		dialTimeout: time.Duration(target.DialTimeoutMs) * time.Millisecond,
		now:         time.Now,
	}
	for index, host := range target.Hosts {
		client.dialer.hosts = append(client.dialer.hosts, &hostEntry{address: host.Address(), pin: pins[index]})
	}
	client.point = pool.New(pool.Config[*hoarddbwire.Conn]{Dial: client.dialAuthenticated, MaxConns: settings.poolSize})
	client.bulk = pool.New(pool.Config[*hoarddbwire.Conn]{Dial: client.dialAuthenticated, MaxConns: settings.bulkPoolSize})
	return client, nil
}

// dialAuthenticated is the pool's dialler: a connection that has passed trust
// and AUTH. It remembers what the server said.
func (c *Client) dialAuthenticated(ctx context.Context) (*hoarddbwire.Conn, error) {
	conn, greeting, err := c.dialer.open(ctx)
	if err != nil {
		return nil, err
	}
	c.serverMu.Lock()
	c.server = ServerInfo{Version: greeting.ServerVersion, ProtocolVersion: greeting.ProtocolVersion, Capabilities: greeting.Capabilities}
	c.serverMu.Unlock()
	return conn, nil
}

// connectError gives the error Connect returns. Dial and trust failures are
// already *Error; the pool adds only its own.
func (c *Client) connectError(err error) error {
	var failure *Error
	switch {
	case errors.As(err, &failure):
		return err
	case errors.Is(err, pool.ErrClosed):
		return &Error{Code: CodeClosed, Op: "connect", Message: "the client was closed"}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("connect: %w", err)
	default:
		return &Error{Code: CodeUnreachable, Op: "connect", Err: err}
	}
}

// resolveCredentials settles the user and password: options first, then the
// connection string, then the environment.
func resolveCredentials(settings *config, target *uri.URI) error {
	if settings.username == "" {
		switch {
		case target.HasUsername:
			settings.username = target.Username
		case os.Getenv(envRootUser) != "":
			settings.username = os.Getenv(envRootUser)
		default:
			settings.username = defaultRootUser
		}
	}
	if settings.password == "" && target.HasPassword {
		settings.password = target.Password
	}
	if settings.password == "" {
		settings.password = os.Getenv(envRootPassword)
	}
	if settings.token == "" {
		settings.token = target.AuthToken
	}
	if settings.password == "" && settings.token == "" {
		return &Error{Code: CodeAuthFailed, Op: "connect", Message: fmt.Sprintf(
			"no password to log in as %q with. Put it in the connection string (hoarddb://%s:<password>@host), pass hoarddb.WithCredentials, "+
				"or export %s. If you started the server without choosing a password, it generated one in <data dir>/root.password.",
			settings.username, settings.username, envRootPassword)}
	}
	return nil
}

// resolvePins gives each host its pinned fingerprint, or "". Options replace the
// connection string's pins.
func resolvePins(settings *config, target *uri.URI) ([]string, error) {
	fingerprints := target.Fingerprints
	if len(settings.fingerprints) > 0 {
		fingerprints = settings.fingerprints
	}
	hosts := len(target.Hosts)
	if len(fingerprints) > 1 && len(fingerprints) != hosts {
		return nil, optionError("%d fingerprints were given for %d hosts; give one fingerprint for all hosts, or exactly one per host in the order the hosts are written",
			len(fingerprints), hosts)
	}
	pins := make([]string, hosts)
	for index := range pins {
		switch len(fingerprints) {
		case 0:
		case 1:
			pins[index] = fingerprints[0]
		default:
			pins[index] = fingerprints[index]
		}
	}
	return pins, nil
}

func resolveKnownServers(settings *config) (*trust.KnownServers, error) {
	path := settings.knownServersPath
	if path == "" {
		defaultPath, err := trust.DefaultKnownServersPath()
		if err != nil {
			return nil, optionError("cannot find the default known-servers file (%v); pass WithKnownServers, or pin the server with WithFingerprint", err)
		}
		path = defaultPath
	}
	store := trust.NewKnownServers(path)
	store.OnLockUnsupported = func(path string) {
		settings.logger.Warn("this platform or file system has no file locks; two processes recording a first-seen server at the same moment are not serialised (clients in one process are)",
			"known_servers", path)
	}
	return store, nil
}

func clientIdentity(optionName, uriName string) string {
	identity := "hoarddb-go/" + Version
	name := optionName
	if name == "" {
		name = uriName
	}
	if name != "" {
		identity += " (" + name + ")"
	}
	return identity
}

// Close closes every connection. It is safe to call more than once and from any
// goroutine. Calls running on the client fail with [ErrClosed].
func (c *Client) Close() error {
	return errors.Join(c.point.Close(), c.bulk.Close())
}

// ServerInfo is what the server reported at the last login. It costs no round
// trip.
func (c *Client) ServerInfo() ServerInfo {
	c.serverMu.RLock()
	defer c.serverMu.RUnlock()
	return c.server
}

// PoolStats returns aggregate connection counters, summed over the point and
// the bulk lane.
func (c *Client) PoolStats() PoolStats {
	point, bulk := c.point.Stats(), c.bulk.Stats()
	return PoolStats{
		Open: point.Open + bulk.Open, InUse: point.InUse + bulk.InUse,
		Waiting: point.Waiting + bulk.Waiting,
		Dials:   point.Dials + bulk.Dials, DialErrors: point.DialErrors + bulk.DialErrors,
		Discarded: point.Discarded + bulk.Discarded,
		Retries:   c.retries.Load(),
	}
}

// Ping checks that the server answers. It is retried once on a fresh connection
// when the connection it was sent on turns out to be dead, which is invisible
// except in [Client.PoolStats].
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := c.requestContext(ctx)
	defer cancel()
	const attempts = 2
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		lease, err := c.point.Acquire(ctx)
		if err != nil {
			return c.callError("ping", "", err)
		}
		retry, err := c.pingOnce(ctx, lease)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retry {
			break
		}
	}
	return lastErr
}

// pingOnce sends one PING on the leased connection and settles the lease. It
// reports whether the failure is one a fresh connection might not repeat.
func (c *Client) pingOnce(ctx context.Context, lease *pool.Lease[*hoarddbwire.Conn]) (retry bool, err error) {
	conn := lease.Conn()
	if c.session.NeedsRefresh() {
		if err := c.session.Refresh(ctx, conn); err != nil {
			lease.Discard()
			return true, c.callError("ping", "", err)
		}
	}
	reply, err := conn.Do(ctx, hoarddbwire.OpPing, nil, nil)
	if err != nil {
		lease.Discard()
		var refused *hoarddbwire.RefusedError
		return !errors.As(err, &refused), c.callError("ping", "", err)
	}
	switch reply.Op {
	case hoarddbwire.OpPong:
		lease.Release()
		return false, nil
	case hoarddbwire.OpError:
		// A server may close right after an unexpected error frame, so the
		// connection is not reused.
		lease.Discard()
		return false, wireError("ping", conn.RemoteAddr().String(), hoarddbwire.DecodeError(reply.Payload))
	default:
		lease.Discard()
		return false, &Error{Code: CodeInternal, Op: "ping", Target: conn.RemoteAddr().String(), Message: fmt.Sprintf(
			"the server answered PING with %s instead of PONG; is this a hoardDB driver port?", reply.Op)}
	}
}

// requestContext applies the default timeout when the caller set no deadline.
func (c *Client) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.settings.requestTimeout)
}

// callError folds what a call's plumbing returned into an *Error. The caller's
// own cancellation and deadline stay matchable as the standard context errors,
// through [Error.Unwrap].
// target is what the call was aimed at, or "" for none.
func (c *Client) callError(op, target string, err error) error {
	var failure *Error
	switch {
	case errors.As(err, &failure):
		return err
	case errors.Is(err, pool.ErrClosed), errors.Is(err, hoarddbwire.ErrConnClosed):
		return &Error{Code: CodeClosed, Op: op, Target: target, Message: "the client is closed; create a new one with hoarddb.Connect"}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return interrupted(op, target, err)
	default:
		return &Error{Code: CodeUnreachable, Op: op, Target: target, Err: err, Message: strings.TrimSpace(fmt.Sprintf(
			"the connection to the server failed (%v). Check that the server is still running; the next call dials a new connection.", err))}
	}
}
