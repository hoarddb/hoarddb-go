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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
)

// TestUnreachableTextSaysWhereTheServerListens: every unreachable message
// says the binary listens on loopback by default (handbook/wire.md section 1);
// for this machine it says how a container's port reaches it, for another
// machine that the default cannot be reached from there.
func TestUnreachableTextSaysWhereTheServerListens(t *testing.T) {
	refused := errors.New("connect: connection refused")
	for _, address := range []string{"localhost:7433", "LOCALHOST:1", "127.0.0.1:7433", "127.0.0.2:9", "[::1]:7433"} {
		message := unreachableText(address, refused)
		for _, want := range []string{address, "connection refused", "127.0.0.1:7433", "every interface", "-p 7433:7433"} {
			if !strings.Contains(message, want) {
				t.Errorf("%s: the message must contain %q:\n%s", address, want, message)
			}
		}
	}
	for _, address := range []string{"db.internal:7433", "192.0.2.5:7433", "[2001:db8::1]:7433"} {
		message := unreachableText(address, refused)
		for _, want := range []string{address, "connection refused", "loopback", "another machine cannot reach it", "every interface"} {
			if !strings.Contains(message, want) {
				t.Errorf("%s: the message must contain %q:\n%s", address, want, message)
			}
		}
	}
}

// TestEveryTrustOptionCombinationReachesTheKeyCheck carries the trust
// package's option matrix up to the options a program passes: fingerprint,
// known-servers file (explicit or the default under HOME), prompt (none,
// declining, AcceptFirstSight) and the insecure reason. Every combination
// either is refused by Connect or dials with a key check that refuses a key
// other than the pinned or recorded one, except the named opt-out, which
// accepts it and logs its reason at WARN.
func TestEveryTrustOptionCombinationReachesTheKeyCheck(t *testing.T) {
	const address = "127.0.0.1:7433"
	expected, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	presented := selfSignedEd25519(t)
	record := func(path string) {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(address+" "+hex.EncodeToString(expected)+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	home := filepath.Join(t.TempDir(), "home")
	t.Setenv("HOME", home)
	defaultPath, err := trust.DefaultKnownServersPath()
	if err != nil {
		t.Fatal(err)
	}
	record(defaultPath)

	prompts := map[string]Option{
		"no prompt":        nil,
		"declining":        WithTrustPrompt(func(string, string) bool { return false }),
		"AcceptFirstSight": WithTrustPrompt(AcceptFirstSight),
	}
	for _, pinned := range []bool{false, true} {
		for _, explicitStore := range []bool{false, true} {
			for promptName, prompt := range prompts {
				for _, insecure := range []bool{false, true} {
					name := fmt.Sprintf("pin=%v explicit-store=%v prompt=%s insecure=%v", pinned, explicitStore, promptName, insecure)
					t.Run(name, func(t *testing.T) {
						var logs bytes.Buffer
						options := []Option{WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))}
						if pinned {
							options = append(options, WithFingerprint(trust.Fingerprint(expected)))
						}
						if explicitStore {
							path := filepath.Join(t.TempDir(), "private", "known_servers")
							record(path)
							options = append(options, WithKnownServers(path))
						}
						if prompt != nil {
							options = append(options, prompt)
						}
						if insecure {
							options = append(options, WithInsecureSkipVerify("matrix test reason"))
						}
						client, err := newClient("hoarddb://admin:pw@"+address+"/", options)
						if pinned && insecure {
							if !errors.Is(err, ErrInvalidURI) {
								t.Fatalf("a fingerprint and the opt-out together: err = %v, want a refusal", err)
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						config, err := client.dialer.tlsConfig(0)
						if err != nil {
							t.Fatal(err)
						}
						if !config.InsecureSkipVerify || config.VerifyPeerCertificate == nil {
							t.Fatalf("InsecureSkipVerify=%v with check=%v; the platform check is replaced, never dropped",
								config.InsecureSkipVerify, config.VerifyPeerCertificate != nil)
						}
						verdict := config.VerifyPeerCertificate([][]byte{presented}, nil)
						var mismatch *trust.MismatchError
						var changed *trust.KeyChangedError
						switch {
						case pinned:
							if !errors.As(verdict, &mismatch) {
								t.Fatalf("pinned, another key presented: %v, want *trust.MismatchError", verdict)
							}
						case insecure:
							if verdict != nil {
								t.Fatalf("the named opt-out refused: %v", verdict)
							}
							if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "matrix test reason") {
								t.Fatalf("the opt-out must log its reason at WARN on every dial; log:\n%s", logs.String())
							}
						default:
							if !errors.As(verdict, &changed) {
								t.Fatalf("recorded, another key presented: %v, want *trust.KeyChangedError", verdict)
							}
						}
					})
				}
			}
		}
	}
}

func selfSignedEd25519(t *testing.T) []byte {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "hoarddb-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	return der
}
