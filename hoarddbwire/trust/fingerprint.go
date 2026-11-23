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

// Package trust decides whether a hoardDB server is the server the caller
// meant to reach.
//
// A server mints a self-signed ed25519 certificate, so there is no CA and no
// host name to check. Identity is the server's public key, written as a
// fingerprint: "SHA256:" followed by the lowercase hex of the SHA-256 of the
// raw 32-byte key (not of the certificate, and not of the SPKI structure that
// wraps the key). The server prints it at startup.
//
// A [Policy] names exactly one way to trust a server: a pinned fingerprint, a
// known-servers file with an optional prompt, or an explicit, reasoned opt-out.
// There is no zero-value policy that trusts everything.
package trust

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// FingerprintPrefix starts every fingerprint.
const FingerprintPrefix = "SHA256:"

// fingerprintHexLen is the hex length of a SHA-256 digest.
const fingerprintHexLen = 64

// Fingerprint returns the canonical fingerprint of an ed25519 public key.
func Fingerprint(key ed25519.PublicKey) string {
	digest := sha256.Sum256(key)
	return FingerprintPrefix + hex.EncodeToString(digest[:])
}

// ParseFingerprint validates text and returns its canonical form: the prefix
// in upper case and the hex in lower case. The prefix and the hex are accepted
// in either case; nothing else is. One canonicalisation path serves the URI
// parser, the options and the comparison, so two spellings of one key can never
// disagree.
func ParseFingerprint(text string) (string, error) {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < len(FingerprintPrefix) || !strings.EqualFold(trimmed[:len(FingerprintPrefix)], FingerprintPrefix) {
		return "", fmt.Errorf("the fingerprint is missing the %q prefix; the server prints it at startup as %s<64 hex characters>",
			FingerprintPrefix, FingerprintPrefix)
	}
	digits := trimmed[len(FingerprintPrefix):]
	if len(digits) != fingerprintHexLen {
		return "", fmt.Errorf("the fingerprint has %d hex characters after %q; it must have exactly %d",
			len(digits), FingerprintPrefix, fingerprintHexLen)
	}
	if _, err := hex.DecodeString(digits); err != nil {
		return "", fmt.Errorf("the fingerprint is not valid hex after %q; it must be %d characters of 0-9 and a-f",
			FingerprintPrefix, fingerprintHexLen)
	}
	return FingerprintPrefix + strings.ToLower(digits), nil
}
