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

package trust

import (
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
)

// TestEveryOptionCombinationInstallsTheKeyCheck is the guard for silent
// failure 7 (handbook/build-go.md section 2) and handbook/trust.md section 3:
// every combination of pin, store, prompt (none, declining, first sight) and
// insecure reason either refuses to build or builds a configuration whose own
// key check is installed and bites. The check is proven by presenting a key
// other than the pinned or recorded one: only the insecure policy may accept
// it, and it must report the dial when asked to.
func TestEveryOptionCombinationInstallsTheKeyCheck(t *testing.T) {
	const address = "127.0.0.1:7433"
	expectedPublic, _ := newEd25519Key(t)
	presentedPublic, presentedPrivate := newEd25519Key(t)
	presented := selfSigned(t, presentedPublic, presentedPrivate).Certificate[0]

	prompts := map[string]func(string, string) bool{
		"no prompt":   nil,
		"declining":   func(string, string) bool { return false },
		"first sight": func(string, string) bool { return true },
	}
	built := 0
	for _, pinned := range []bool{false, true} {
		for _, withStore := range []bool{false, true} {
			for promptName, prompt := range prompts {
				for _, reason := range []string{"", "   ", "unit test"} {
					for _, reports := range []bool{false, true} {
						name := fmt.Sprintf("pin=%v store=%v prompt=%s reason=%q reports=%v", pinned, withStore, promptName, reason, reports)
						t.Run(name, func(t *testing.T) {
							var policy Policy
							if pinned {
								policy.Pin = Fingerprint(expectedPublic)
							}
							if withStore {
								policy.Known = NewKnownServers(knownPath(t))
								if err := policy.Known.Add(address, expectedPublic); err != nil {
									t.Fatal(err)
								}
							}
							policy.Prompt = prompt
							policy.InsecureReason = reason
							reported := 0
							if reports {
								policy.OnInsecure = func(string, string) { reported++ }
							}

							config, err := NewTLSConfig(address, policy)
							insecure := reason == "unit test"
							choices := 0
							for _, chosen := range []bool{pinned, withStore, insecure} {
								if chosen {
									choices++
								}
							}
							if choices != 1 {
								if !errors.Is(err, ErrNoPolicy) {
									t.Fatalf("%d ways to trust chosen: err = %v, want ErrNoPolicy", choices, err)
								}
								return
							}
							if err != nil {
								t.Fatalf("one way to trust chosen: %v", err)
							}
							built++
							if config.VerifyPeerCertificate == nil {
								t.Fatal("InsecureSkipVerify without the key check")
							}
							if config.MinVersion != tls.VersionTLS13 || len(config.NextProtos) != 1 || config.NextProtos[0] != ALPN {
								t.Fatalf("MinVersion %x NextProtos %v; want TLS 1.3 and [%s]", config.MinVersion, config.NextProtos, ALPN)
							}

							verdict := config.VerifyPeerCertificate([][]byte{presented}, nil)
							var mismatch *MismatchError
							var changed *KeyChangedError
							switch {
							case pinned && !errors.As(verdict, &mismatch):
								t.Fatalf("a pin, another key presented: %v, want *MismatchError", verdict)
							case withStore && !errors.As(verdict, &changed):
								t.Fatalf("a recorded key, another presented: %v, want *KeyChangedError", verdict)
							case insecure && verdict != nil:
								t.Fatalf("the named opt-out refused: %v", verdict)
							}
							if insecure && reports && reported != 1 {
								t.Fatalf("OnInsecure called %d times for one dial, want 1", reported)
							}
						})
					}
				}
			}
		}
	}
	// A pin or a store builds with either blank reason (2), the opt-out with the
	// real one (1); each with every prompt (3) and report setting (2).
	if want := (2 + 2 + 1) * len(prompts) * 2; built != want {
		t.Fatalf("%d combinations built a configuration, want %d", built, want)
	}
}
