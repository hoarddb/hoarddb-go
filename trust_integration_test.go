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
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

// otherKey is a key no test server holds, and its fingerprint as the server
// would print it: SHA-256 over the raw 32 key bytes (handbook/trust.md
// section 2), computed here without the driver's own function.
func otherKey(t *testing.T) (ed25519.PublicKey, string) {
	t.Helper()
	key := ed25519.NewKeyFromSeed([]byte(strings.Repeat("k", ed25519.SeedSize))).Public().(ed25519.PublicKey)
	digest := sha256.Sum256(key)
	return key, "SHA256:" + hex.EncodeToString(digest[:])
}

func requireAll(t *testing.T, err error, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message must contain %q:\n%v", want, err)
		}
	}
}

// TestConnectRefusesAnUnknownServerNamingThreeWays: a real server, an empty
// known-servers file, no pin and no prompt. The refusal names the host, the key
// it presented and the three ways to trust it (handbook/errors.md section 7).
func TestConnectRefusesAnUnknownServerNamingThreeWays(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	known := knownServersOf(server)

	_, err := hoarddb.Connect(ctx, uriFor(server, server.Password), hoarddb.WithKnownServers(known))
	if !errors.Is(err, hoarddb.ErrUntrustedServer) {
		t.Fatalf("err = %v, want ErrUntrustedServer", err)
	}
	requireAll(t, err,
		server.Address, server.Fingerprint,
		"?fingerprint="+server.Fingerprint, // one: pin it
		"command line tool", known,         // two: trust it once with the CLI, which writes the file the driver reads
		"WithTrustPrompt(hoarddb.AcceptFirstSight)", "local development", // three: first sight, for local development
	)
	if _, statErr := os.Stat(known); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a refused server must not be recorded: stat %s = %v", known, statErr)
	}
}

// TestConnectRefusesAChangedKeyNamingBoth: a real server whose address the
// store records with another key. The refusal names both fingerprints and the
// line to delete, and no prompt can override it.
func TestConnectRefusesAChangedKeyNamingBoth(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	known := knownServersOf(server)
	recordedKey, recordedFingerprint := otherKey(t)
	if err := os.WriteFile(known, []byte(server.Address+" "+hex.EncodeToString(recordedKey)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	prompted := false
	_, err := hoarddb.Connect(ctx, uriFor(server, server.Password), hoarddb.WithKnownServers(known),
		hoarddb.WithTrustPrompt(func(string, string) bool { prompted = true; return true }))
	if !errors.Is(err, hoarddb.ErrServerKeyChanged) {
		t.Fatalf("err = %v, want ErrServerKeyChanged", err)
	}
	if prompted {
		t.Fatal("a changed key was put to the prompt; it must be a hard failure")
	}
	requireAll(t, err, recordedFingerprint, server.Fingerprint, known,
		"delete the line for "+server.Address, "intercepting", "will not retry")
	contents, readErr := os.ReadFile(known)
	if readErr != nil || strings.Count(string(contents), "\n") != 1 {
		t.Fatalf("the store changed after a refused key: %q, %v", contents, readErr)
	}
}

// TestPinMismatchNamesBothFingerprints: a real server pinned to another key.
// The refusal names the pinned and the presented fingerprint, and comes before
// AUTH (the password is wrong too, so a credential failure would mean AUTH ran).
func TestPinMismatchNamesBothFingerprints(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	_, pinned := otherKey(t)

	_, err := hoarddb.Connect(ctx, uriFor(server, "not-the-password")+"?fingerprint="+pinned)
	if !errors.Is(err, hoarddb.ErrFingerprintMismatch) {
		t.Fatalf("err = %v, want ErrFingerprintMismatch", err)
	}
	if errors.Is(err, hoarddb.ErrAuthFailed) {
		t.Fatal("AUTH ran against a server that failed the pin")
	}
	requireAll(t, err, server.Address, pinned, server.Fingerprint, "will not retry")
}

// TestUnreachableLocalhostMentionsLoopback: nothing listens at localhost on
// the port. The message names the address and the network error, and says the
// server binary listens on loopback by default while the container image
// listens on every interface, so a container must publish its port
// (handbook/wire.md section 1, errors.md section 7).
func TestUnreachableLocalhostMentionsLoopback(t *testing.T) {
	ctx, cancel := background()
	defer cancel()
	address := fmt.Sprintf("localhost:%d", freePort(t))
	_, err := hoarddb.Connect(ctx, "hoarddb://admin:pw@"+address+"/?fingerprint=SHA256:"+strings.Repeat("cd", 32))
	if !errors.Is(err, hoarddb.ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable", err)
	}
	requireAll(t, err, address, "connection refused", "loopback", "127.0.0.1:7433", "answers at localhost:7433",
		"container", "every interface", "-p 7433:7433")
	if strings.Contains(err.Error(), "another machine") {
		t.Errorf("localhost is this machine; the advice for a remote host does not apply:\n%v", err)
	}
}

// TestConflictingKnownServersFileIsNamedNotUnreachable: two keys for another
// host refuse the whole file (handbook/trust.md section 4). The refusal is a
// trust failure that names the file and the host, never "unreachable".
func TestConflictingKnownServersFileIsNamedNotUnreachable(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	known := knownServersOf(server)
	first, _ := otherKey(t)
	second := ed25519.NewKeyFromSeed([]byte(strings.Repeat("j", ed25519.SeedSize))).Public().(ed25519.PublicKey)
	lines := "db.internal:7433 " + hex.EncodeToString(first) + "\n" + "db.internal:7433 " + hex.EncodeToString(second) + "\n"
	if err := os.WriteFile(known, []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := hoarddb.Connect(ctx, uriFor(server, server.Password), hoarddb.WithKnownServers(known))
	if !errors.Is(err, hoarddb.ErrUntrustedServer) || errors.Is(err, hoarddb.ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUntrustedServer", err)
	}
	requireAll(t, err, known, "records two different keys for db.internal:7433", "deleted")
}
