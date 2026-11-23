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
	"crypto/ed25519"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
)

// ALPN is the application protocol offered in the handshake. A server refuses
// a connection that negotiates anything else.
const ALPN = "hoarddb/1"

// Policy states how to decide that a server is the intended one. Exactly one
// of Pin, Known or InsecureReason must be set; [NewTLSConfig] refuses anything
// else, so "no policy" cannot be expressed by forgetting a field.
type Policy struct {
	// Pin is the only fingerprint accepted for the server being dialled. The
	// comparison is constant-time, made before any credential is sent, and a
	// mismatch is final: never a prompt, never a re-trust. Set it to a value
	// [ParseFingerprint] returned.
	Pin string

	// Known is a store of servers already trusted. A server that is recorded
	// must present the recorded key. A server that is not recorded is passed to
	// Prompt; with no Prompt it is refused.
	Known *KnownServers
	// Prompt is asked about a server that Known has never seen, with the address
	// and the fingerprint it presented. Returning true records the key. A
	// library has no terminal, so there is no default prompt.
	Prompt func(address, fingerprint string) bool

	// InsecureReason, when non-empty, disables the check. It must say why, and
	// OnInsecure (if set) is called on every dial so the caller can log it.
	InsecureReason string
	// OnInsecure is called with the address and presented fingerprint of every
	// server accepted without a check.
	OnInsecure func(address, fingerprint string)
}

// MismatchError is a server whose key is not the pinned one.
type MismatchError struct {
	Address   string
	Pinned    string
	Presented string
}

func (err *MismatchError) Error() string {
	return fmt.Sprintf("the server at %s presented %s but %s is pinned", err.Address, err.Presented, err.Pinned)
}

// UnknownServerError is a server that no policy vouches for: it is not
// recorded and no prompt accepted it.
type UnknownServerError struct {
	Address     string
	Fingerprint string
	// Declined is true when a prompt was asked and said no.
	Declined bool
}

func (err *UnknownServerError) Error() string {
	if err.Declined {
		return fmt.Sprintf("the server at %s presented %s and was not trusted", err.Address, err.Fingerprint)
	}
	return fmt.Sprintf("the server at %s is not trusted yet: it presented %s and none is pinned or recorded", err.Address, err.Fingerprint)
}

// KeyChangedError is a server recorded with one key that now presents another.
type KeyChangedError struct {
	Address   string
	Recorded  string
	Presented string
	// Path is the known-servers file holding the old entry.
	Path string
}

func (err *KeyChangedError) Error() string {
	return fmt.Sprintf("the server at %s presented %s but %s is recorded in %s", err.Address, err.Presented, err.Recorded, err.Path)
}

// UnsupportedKeyError is a server certificate that is not ed25519.
type UnsupportedKeyError struct {
	Address string
	Type    string
}

func (err *UnsupportedKeyError) Error() string {
	return fmt.Sprintf("the server at %s presented a %s key; hoardDB servers use ed25519, so this may not be a hoardDB server", err.Address, err.Type)
}

// ErrNoPolicy is returned for a [Policy] that names no way to trust a server,
// or more than one.
var ErrNoPolicy = errors.New("trust policy must set exactly one of Pin, Known, InsecureReason")

// NewTLSConfig returns the client TLS configuration for one dial to address
// ("host:port"). TLS 1.3 is unconditional.
//
// The configuration sets InsecureSkipVerify only to switch off Go's
// certificate-chain check, which has no CA to check against; the policy's own
// verifier replaces it and is always installed.
func NewTLSConfig(address string, policy Policy) (*tls.Config, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	config := &tls.Config{
		MinVersion:            tls.VersionTLS13,
		NextProtos:            []string{ALPN},
		InsecureSkipVerify:    true, //nolint:gosec // replaced by VerifyPeerCertificate below; there is no CA or host name to verify
		VerifyPeerCertificate: policy.verifier(address),
	}
	if host, _, err := net.SplitHostPort(address); err == nil && net.ParseIP(host) == nil {
		config.ServerName = host
	}
	return config, nil
}

func (policy Policy) validate() error {
	chosen := 0
	if policy.Pin != "" {
		chosen++
		if canonical, err := ParseFingerprint(policy.Pin); err != nil || canonical != policy.Pin {
			return fmt.Errorf("trust policy Pin %q is not a canonical fingerprint (use ParseFingerprint)", policy.Pin)
		}
	}
	if policy.Known != nil {
		chosen++
	}
	if strings.TrimSpace(policy.InsecureReason) != "" {
		chosen++
	}
	if chosen != 1 {
		return ErrNoPolicy
	}
	return nil
}

func (policy Policy) verifier(address string) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCertificates [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCertificates) == 0 {
			return fmt.Errorf("the server at %s presented no certificate", address)
		}
		leaf, err := x509.ParseCertificate(rawCertificates[0])
		if err != nil {
			return fmt.Errorf("the server at %s presented a certificate that cannot be parsed: %w", address, err)
		}
		key, isEd25519 := leaf.PublicKey.(ed25519.PublicKey)
		if !isEd25519 {
			return &UnsupportedKeyError{Address: address, Type: fmt.Sprintf("%T", leaf.PublicKey)}
		}
		presented := Fingerprint(key)
		switch {
		case policy.Pin != "":
			return policy.checkPin(address, presented)
		case policy.Known != nil:
			return policy.checkKnown(address, key, presented)
		default:
			if policy.OnInsecure != nil {
				policy.OnInsecure(address, presented)
			}
			return nil
		}
	}
}

func (policy Policy) checkPin(address, presented string) error {
	if !fingerprintsEqual(policy.Pin, presented) {
		return &MismatchError{Address: address, Pinned: policy.Pin, Presented: presented}
	}
	return nil
}

func (policy Policy) checkKnown(address string, key ed25519.PublicKey, presented string) error {
	recorded, found, err := policy.Known.Lookup(address)
	var conflicting *ConflictingEntriesError
	if errors.As(err, &conflicting) && conflicting.Address == address {
		// Two keys recorded for this host: whichever this server shows, one entry
		// disagrees with it, so it is a changed key, not a choice to make quietly.
		// Two keys for another host refuse the file as a whole, below.
		return &KeyChangedError{Address: address, Recorded: conflicting.First, Presented: presented, Path: conflicting.Path}
	}
	if err != nil {
		return err
	}
	if found {
		if !fingerprintsEqual(recorded, presented) {
			return &KeyChangedError{Address: address, Recorded: recorded, Presented: presented, Path: policy.Known.Path()}
		}
		return nil
	}
	if policy.Prompt == nil {
		return &UnknownServerError{Address: address, Fingerprint: presented}
	}
	if !policy.Prompt(address, presented) {
		return &UnknownServerError{Address: address, Fingerprint: presented, Declined: true}
	}
	if err := policy.Known.Add(address, key); err != nil {
		return fmt.Errorf("trusting %s: %w", address, err)
	}
	return nil
}

// fingerprintsEqual compares two canonical fingerprints in constant time.
func fingerprintsEqual(left, right string) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}
