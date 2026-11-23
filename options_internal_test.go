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
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/uri"
)

// resolved applies opts and the connection string's credentials the way
// newClient does, and returns the settings before newClient hands the
// credentials to the session and clears its own copies.
func resolved(t *testing.T, connectionString string, opts ...Option) *config {
	t.Helper()
	target, err := uri.Parse(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	settings := &config{}
	for _, option := range opts {
		if err := option(settings); err != nil {
			t.Fatal(err)
		}
	}
	if err := resolveCredentials(settings, target); err != nil {
		t.Fatal(err)
	}
	return settings
}

func TestConfigNeverPrintsItsSecrets(t *testing.T) {
	settings := config{username: "root", password: "hunter2-very-secret", token: "token-very-secret", poolSize: 4}
	renderings := []string{
		settings.String(),
		settings.GoString(),
		fmt.Sprintf("%v", settings),
		fmt.Sprintf("%+v", settings),
		fmt.Sprintf("%#v", settings),
		fmt.Sprintf("%v", &settings),
	}
	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("config", "config", settings)
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("config", "config", settings)
	renderings = append(renderings, logs.String())
	for _, rendering := range renderings {
		if strings.Contains(rendering, "hunter2") || strings.Contains(rendering, "token-very") {
			t.Fatalf("a secret leaked: %s", rendering)
		}
		if !strings.Contains(rendering, "root") {
			t.Fatalf("the user name should still be visible for debugging: %s", rendering)
		}
	}
	if !strings.Contains(settings.String(), "password=***") {
		t.Fatalf("a set password should render as ***: %s", settings.String())
	}
	if !strings.Contains(config{username: "u"}.String(), "password=unset") {
		t.Fatal("an unset password should say so, so a missing credential is visible")
	}
}

// The public constants are literals, so the godoc shows no wire-package type.
// This pins them to the wire package's values instead.
func TestPublicCodesEqualTheWireValues(t *testing.T) {
	wire := map[Code]hoarddbwire.ErrorCode{
		CodeNotFound: hoarddbwire.CodeNotFound, CodeAlreadyExists: hoarddbwire.CodeAlreadyExists,
		CodePermissionDenied: hoarddbwire.CodePermissionDenied, CodeInvalidArgument: hoarddbwire.CodeInvalidArgument,
		CodeInternal: hoarddbwire.CodeInternal, CodeUnavailable: hoarddbwire.CodeUnavailable,
		CodeTimeout: hoarddbwire.CodeTimeout, CodeConflict: hoarddbwire.CodeConflict,
		CodeQuotaExceeded: hoarddbwire.CodeQuotaExceeded, CodeUnsupportedVersion: hoarddbwire.CodeUnsupportedVersion,
		CodeNotAuthenticated: hoarddbwire.CodeNotAuthenticated, CodeOverloaded: hoarddbwire.CodeOverloaded,
		CodeResultTooLarge: hoarddbwire.CodeResultTooLarge, CodeDuplicateKey: hoarddbwire.CodeDuplicateKey,
		CodeFieldType: hoarddbwire.CodeFieldType,
	}
	for public, internal := range wire {
		if uint16(public) != uint16(internal) {
			t.Errorf("%v = %#x, the wire says %#x", public, uint16(public), uint8(internal))
		}
	}
	reasons := map[AuthReason]hoarddbwire.AuthFailReason{
		AuthReasonCredentials: hoarddbwire.AuthFailCredentials, AuthReasonTokenExpired: hoarddbwire.AuthFailTokenExpired,
		AuthReasonTokenInvalid: hoarddbwire.AuthFailTokenInvalid, AuthReasonTokenUnsupported: hoarddbwire.AuthFailTokenUnsupported,
		AuthReasonProtocolMismatch: hoarddbwire.AuthFailProtocolMismatch, AuthReasonRateLimited: hoarddbwire.AuthFailRateLimited,
		AuthReasonAlreadyAuthed: hoarddbwire.AuthFailAlreadyAuthed, AuthReasonInternal: hoarddbwire.AuthFailInternal,
	}
	for public, internal := range reasons {
		if uint8(public) != uint8(internal) {
			t.Errorf("%v = %d, the wire says %d", public, uint8(public), uint8(internal))
		}
	}
}

func TestCredentialResolutionWithoutNetwork(t *testing.T) {
	t.Setenv(envRootUser, "")
	t.Setenv(envRootPassword, "")
	if settings := resolved(t, "hoarddb://127.0.0.1:1/", WithCredentials("a", "b")); settings.username != "a" || settings.password != "b" {
		t.Fatalf("settings = %v", settings)
	}
	client, err := newClient("hoarddb://127.0.0.1:1/", []Option{WithCredentials("a", "b")})
	if err != nil {
		t.Fatal(err)
	}
	if client.settings.poolSize != defaultPoolSize || client.settings.requestTimeout != defaultRequestTimeout {
		t.Fatalf("defaults drifted: pool %d, timeout %v (the scope says 4 and 30s)", client.settings.poolSize, client.settings.requestTimeout)
	}
	// A token alone is a credential.
	if _, err := newClient("hoarddb://127.0.0.1:1/?authToken=abc", nil); err != nil {
		t.Fatalf("authToken in the URI should stand in for a password: %v", err)
	}
}

func TestCredentialsFromEnvOption(t *testing.T) {
	t.Setenv(envRootUser, "ops")
	t.Setenv(envRootPassword, "from-env")
	if settings := resolved(t, "hoarddb://uri-user:uri-pass@127.0.0.1:1/", CredentialsFromEnv()); settings.username != "ops" || settings.password != "from-env" {
		t.Fatalf("the explicit option should let the environment win: %v", settings)
	}
	t.Setenv(envRootPassword, "")
	if _, err := newClient("hoarddb://127.0.0.1:1/", []Option{CredentialsFromEnv()}); err == nil || !strings.Contains(err.Error(), envRootPassword) {
		t.Fatalf("err = %v; an unset variable should be named", err)
	}
}

func TestInsecureConfigurationIsNeverReachableFromTheURI(t *testing.T) {
	for _, parameter := range []string{"insecure=true", "tls=false", "sslVerify=false", "INSECURE=1"} {
		_, err := newClient("hoarddb://a:b@127.0.0.1:1/?"+parameter, nil)
		if err == nil {
			t.Errorf("?%s was accepted; trust bypass must be an option, never URI text", parameter)
		}
	}
}
