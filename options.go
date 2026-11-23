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
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
)

// Environment variables the driver reads when the connection string omits what
// they supply. They are the ones the server and the command line tool use.
const (
	envURI          = "HOARDDB_URI"
	envRootUser     = "HOARDDB_ROOT_USER"
	envRootPassword = "HOARDDB_ROOT_PASSWORD"

	// defaultRootUser is the user the server creates on first start.
	defaultRootUser = "admin"
)

const (
	defaultPoolSize       = 4
	defaultBulkPoolSize   = 2
	warnPoolSize          = 16
	defaultRequestTimeout = 30 * time.Second
)

// config is the immutable result of the options. Its String and LogValue
// methods redact the secrets, so logging it is safe.
type config struct {
	username string
	password string
	token    string

	fingerprints     []string
	knownServersPath string
	trustPrompt      func(host, fingerprint string) bool
	insecureReason   string

	poolSize       int
	bulkPoolSize   int
	requestTimeout time.Duration
	clientName     string
	logger         *slog.Logger
	// writeConcern is WithDefaultWriteConcern's level, empty for the server's
	// default.
	writeConcern WriteConcern
}

// Option configures [Connect]. Options override the connection string, which
// overrides the environment.
type Option func(*config) error

func optionError(format string, args ...any) error {
	return &Error{Code: CodeInvalidURI, Op: "connect", Message: fmt.Sprintf(format, args...)}
}

// WithCredentials logs in as username with password.
func WithCredentials(username, password string) Option {
	return func(settings *config) error {
		if username == "" {
			return optionError("WithCredentials needs a user name; the server's default is %q", defaultRootUser)
		}
		settings.username, settings.password = username, password
		return nil
	}
}

// CredentialsFromEnv reads the user from HOARDDB_ROOT_USER and the password
// from HOARDDB_ROOT_PASSWORD when the program is started, overriding the
// connection string. A connection string that omits its password already does
// this; use the option for a program that wants the environment to win.
func CredentialsFromEnv() Option {
	return func(settings *config) error {
		if user := os.Getenv(envRootUser); user != "" {
			settings.username = user
		}
		password := os.Getenv(envRootPassword)
		if password == "" {
			return optionError("CredentialsFromEnv: %s is not set; export it, or pass the password to WithCredentials", envRootPassword)
		}
		settings.password = password
		return nil
	}
}

// WithToken offers a resumption token from an earlier session instead of a
// password. A token is a credential, not a password: when the server rejects it
// and a password is also given, the driver falls back to the password once.
func WithToken(token string) Option {
	return func(settings *config) error {
		if token == "" {
			return optionError("WithToken needs a non-empty token")
		}
		settings.token = token
		return nil
	}
}

// WithFingerprint pins the server's key. Each value is "SHA256:" and 64 hex
// characters, the line the server prints at startup. Give one to pin every
// host, or one per host in the order the hosts are written. A mismatch is a
// hard failure, never a prompt. This is the production way to trust a server.
func WithFingerprint(fingerprints ...string) Option {
	return func(settings *config) error {
		canonical := make([]string, 0, len(fingerprints))
		for _, fingerprint := range fingerprints {
			parsed, err := trust.ParseFingerprint(fingerprint)
			if err != nil {
				return optionError("WithFingerprint: %v", err)
			}
			canonical = append(canonical, parsed)
		}
		settings.fingerprints = canonical
		return nil
	}
}

// WithKnownServers sets the file of already trusted servers. The default is
// ~/.hoarddb/known_servers, the file the hoardDB command line tool writes, so a
// server trusted there is trusted here.
func WithKnownServers(path string) Option {
	return func(settings *config) error {
		if path == "" {
			return optionError("WithKnownServers needs a file path")
		}
		settings.knownServersPath = path
		return nil
	}
}

// WithTrustPrompt is asked about a server that is not pinned and not recorded
// in the known-servers file, with its address and the fingerprint it presented.
// Returning true trusts the server and records it. A library has no terminal,
// so without this option such a server is refused with the steps to trust it.
func WithTrustPrompt(accept func(host, fingerprint string) bool) Option {
	return func(settings *config) error {
		if accept == nil {
			return optionError("WithTrustPrompt needs a function")
		}
		settings.trustPrompt = accept
		return nil
	}
}

// AcceptFirstSight is a ready-made trust prompt that accepts every server it is
// asked about and so records the first key a host presents. It is trust on
// first use, as with SSH: the first connection is unauthenticated and every
// later one is pinned. The driver logs a warning each time it is used. For local
// development only: use WithFingerprint anywhere an attacker could be on the
// network.
//
//	hoarddb.Connect(ctx, uri, hoarddb.WithTrustPrompt(hoarddb.AcceptFirstSight))
func AcceptFirstSight(host, fingerprint string) bool { return true }

// WithInsecureSkipVerify turns server authentication off. The reason is
// required and is logged at WARN on every dial. It cannot be set from the
// connection string. Prefer WithFingerprint: with this option anyone who can
// reach the network path can impersonate the server and read the password.
func WithInsecureSkipVerify(reason string) Option {
	return func(settings *config) error {
		if strings.TrimSpace(reason) == "" {
			return optionError("WithInsecureSkipVerify needs a reason, for example \"local development server, no fingerprint to hand\"; it is written to the log on every connection")
		}
		settings.insecureReason = reason
		return nil
	}
}

// WithPoolSize sets the most connections the client opens. The default is 4.
// The server allows 64 per source address by default, shared by every process
// on this host.
func WithPoolSize(connections int) Option {
	return func(settings *config) error {
		if connections < 1 {
			return optionError("WithPoolSize(%d): the pool needs at least one connection", connections)
		}
		settings.poolSize = connections
		return nil
	}
}

// WithBulkPoolSize sets the most connections the bulk lane opens. The default
// is 2. The bulk lane carries blobs, query pages and batches, so that a long
// scan cannot hold up the point reads and writes on the other lane (handbook/
// wire.md section 7). A client may hold up to WithPoolSize plus this many
// connections.
func WithBulkPoolSize(connections int) Option {
	return func(settings *config) error {
		if connections < 1 {
			return optionError("WithBulkPoolSize(%d): the bulk lane needs at least one connection", connections)
		}
		settings.bulkPoolSize = connections
		return nil
	}
}

// WithRequestTimeout sets the time a call may take when its context has no
// deadline of its own. The default is 30 seconds.
func WithRequestTimeout(timeout time.Duration) Option {
	return func(settings *config) error {
		if timeout <= 0 {
			return optionError("WithRequestTimeout(%v): the timeout must be positive", timeout)
		}
		settings.requestTimeout = timeout
		return nil
	}
}

// WithClientName names this program in the server's logs.
func WithClientName(name string) Option {
	return func(settings *config) error {
		settings.clientName = name
		return nil
	}
}

// WithLogger sends the driver's log lines to logger. The default discards them:
// a library does not write to stderr on its own.
func WithLogger(logger *slog.Logger) Option {
	return func(settings *config) error {
		if logger == nil {
			return optionError("WithLogger needs a logger; omit the option to discard log lines")
		}
		settings.logger = logger
		return nil
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// String redacts the credentials.
func (settings config) String() string {
	return fmt.Sprintf("config{user=%q password=%s token=%s pool=%d}",
		settings.username, secretState(settings.password), secretState(settings.token), settings.poolSize)
}

// GoString redacts the credentials for %#v.
func (settings config) GoString() string { return settings.String() }

// Format is String under every verb: without it %d or %x would print the
// fields, password included. A short write to the fmt.State has no one to
// report to, so its error is not checked.
func (settings config) Format(state fmt.State, _ rune) { _, _ = fmt.Fprint(state, settings.String()) }

// LogValue redacts the credentials when a config is logged with slog.
func (settings config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("user", settings.username),
		slog.String("password", secretState(settings.password)),
		slog.String("token", secretState(settings.token)),
		slog.Int("pool_size", settings.poolSize),
	)
}

func secretState(secret string) string {
	if secret == "" {
		return "unset"
	}
	return "***"
}
