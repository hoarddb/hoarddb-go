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

package conformance_test

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// TestLeg2Trust is conformance leg 2 (handbook/testing.md section 2): every
// trust vector, then the same rules against a real server: its logged
// fingerprint is the one the driver computes, an unknown key is refused, a
// changed key is refused naming both, and verification is off only when asked
// for with a reason.
func TestLeg2Trust(t *testing.T) {
	file := loadVectors(t)
	report := conformance.LegReport{Leg: 2, Name: "trust"}
	for _, vector := range file.Vectors {
		if vector.Kind != "trust" {
			continue
		}
		report.Vectors++
		if vector.Provisional() {
			report.Provisional++
		}
		t.Run(vector.Name, func(t *testing.T) {
			var err error
			switch {
			case vector.KnownServers != nil:
				err = checkKnownServers(t, *vector.KnownServers)
			case vector.PublicKeyHex != "":
				err = checkFingerprint(vector)
			default:
				err = errors.New("a trust vector with neither a key nor a known_servers case: this harness does not know it")
			}
			switch {
			case err == nil && !vector.Provisional():
				report.Passed++
			case err == nil:
				t.Logf("provisional vector (%s) passes", vector.Status)
			case vector.Provisional():
				t.Logf("provisional vector (%s) does not pass, and that is not a failure: %v", vector.Status, err)
			default:
				t.Error(err)
			}
		})
	}
	if report.Vectors == 0 {
		t.Fatal("no trust vectors ran")
	}

	t.Run("real server", func(t *testing.T) { checkTrustAgainstARealServer(t, file) })
	t.Log(report)
}

// checkFingerprint computes the fingerprint from the raw key and from the
// certificate, the second through the verifier a connection installs, so the
// path a real handshake takes is the one checked.
func checkFingerprint(vector conformance.Vector) error {
	key, err := hex.DecodeString(vector.PublicKeyHex)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("public_key_hex is not a 32-byte key: %v", err)
	}
	if got := trust.Fingerprint(key); got != vector.Fingerprint {
		return fmt.Errorf("fingerprint from the key = %s, want %s", got, vector.Fingerprint)
	}
	block, _ := pem.Decode([]byte(vector.CertificatePEM))
	if block == nil || block.Type != "CERTIFICATE" {
		return errors.New("certificate_pem holds no CERTIFICATE block")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parsing certificate_pem: %w", err)
	}
	certificateKey, isEd25519 := certificate.PublicKey.(ed25519.PublicKey)
	if !isEd25519 {
		return fmt.Errorf("certificate_pem carries a %T key, want ed25519", certificate.PublicKey)
	}
	if got := trust.Fingerprint(certificateKey); got != vector.Fingerprint {
		return fmt.Errorf("fingerprint from the certificate = %s, want %s", got, vector.Fingerprint)
	}

	const address = "127.0.0.1:7433"
	accept, err := trust.NewTLSConfig(address, trust.Policy{Pin: vector.Fingerprint})
	if err != nil {
		return fmt.Errorf("a pin of the vector's fingerprint: %w", err)
	}
	if err := accept.VerifyPeerCertificate([][]byte{block.Bytes}, nil); err != nil {
		return fmt.Errorf("the verifier refused the certificate its fingerprint pins: %w", err)
	}
	otherPin := trust.Fingerprint(make(ed25519.PublicKey, ed25519.PublicKeySize))
	refuse, err := trust.NewTLSConfig(address, trust.Policy{Pin: otherPin})
	if err != nil {
		return fmt.Errorf("a pin of another key: %w", err)
	}
	var mismatch *trust.MismatchError
	if err := refuse.VerifyPeerCertificate([][]byte{block.Bytes}, nil); !errors.As(err, &mismatch) || mismatch.Presented != vector.Fingerprint {
		return fmt.Errorf("the verifier with another pin: err = %v, want a mismatch presenting %s", err, vector.Fingerprint)
	}
	return nil
}

// checkKnownServers writes the vector's file where a user's would be (mode
// 0600 in a 0700 directory) and loads it.
func checkKnownServers(t *testing.T, known conformance.KnownServersCase) error {
	path := privateKnownServers(t)
	if err := os.WriteFile(path, []byte(known.FileText), 0o600); err != nil {
		return err
	}
	entries, err := trust.NewKnownServers(path).Load()
	if known.ErrorContains != "" {
		if err == nil || !strings.Contains(err.Error(), known.ErrorContains) {
			return fmt.Errorf("loading %q: err = %v, want a refusal containing %q", known.FileText, err, known.ErrorContains)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("loading %q: %w", known.FileText, err)
	}
	got := map[string]string{}
	for address, key := range entries {
		got[address] = hex.EncodeToString(key)
	}
	if !maps.Equal(got, known.Entries) {
		return fmt.Errorf("loading %q gave %v, want exactly %v", known.FileText, got, known.Entries)
	}
	// Every entry is also what a connection's lookup finds.
	for _, address := range slices.Sorted(maps.Keys(known.Entries)) {
		recorded, found, err := trust.NewKnownServers(path).Lookup(address)
		want := trust.Fingerprint(entries[address])
		if err != nil || !found || recorded != want {
			return fmt.Errorf("Lookup(%s) = %q, %v, %v; want %s", address, recorded, found, err, want)
		}
	}
	return nil
}

func checkTrustAgainstARealServer(t *testing.T, file *conformance.File) {
	server := servertest.Start(t, servertest.Options{})
	presented := presentedFingerprint(t, server.Address)
	onDisk := certificateFingerprint(t, filepath.Join(server.DataDir, "keys", "server.crt"))
	if presented != server.Fingerprint || onDisk != server.Fingerprint {
		t.Fatalf("the server logged %s, its certificate file has %s, and it presented %s in the handshake; "+
			"handbook/trust.md section 1 says the log line is the pin, so all three must agree", server.Fingerprint, onDisk, presented)
	}

	uri := fmt.Sprintf("hoarddb://%s:%s@%s/", server.User, server.Password, server.Address)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("pinned by the logged fingerprint connects", func(t *testing.T) {
		client, err := hoarddb.Connect(ctx, uri+"?fingerprint="+server.Fingerprint)
		if err != nil {
			t.Fatalf("Connect pinned to the logged fingerprint: %v", err)
		}
		_ = client.Close()
	})

	t.Run("an unknown key is refused", func(t *testing.T) {
		known := privateKnownServers(t)
		_, err := hoarddb.Connect(ctx, uri, hoarddb.WithKnownServers(known))
		if !errors.Is(err, hoarddb.ErrUntrustedServer) || !strings.Contains(err.Error(), server.Fingerprint) {
			t.Fatalf("an empty store: err = %v, want ErrUntrustedServer naming %s", err, server.Fingerprint)
		}
	})

	t.Run("a changed key is refused naming both", func(t *testing.T) {
		recordedKey := firstVectorKey(t, file)
		known := privateKnownServers(t)
		line := server.Address + " " + hex.EncodeToString(recordedKey) + "\n"
		if err := os.WriteFile(known, []byte(line), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := hoarddb.Connect(ctx, uri, hoarddb.WithKnownServers(known), hoarddb.WithTrustPrompt(hoarddb.AcceptFirstSight))
		if !errors.Is(err, hoarddb.ErrServerKeyChanged) {
			t.Fatalf("err = %v, want ErrServerKeyChanged", err)
		}
		for _, want := range []string{trust.Fingerprint(recordedKey), server.Fingerprint} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal must name %s:\n%v", want, err)
			}
		}
	})

	t.Run("verification is off only when asked for with a reason", func(t *testing.T) {
		known := privateKnownServers(t)
		if _, err := hoarddb.Connect(ctx, uri, hoarddb.WithKnownServers(known), hoarddb.WithInsecureSkipVerify("")); !errors.Is(err, hoarddb.ErrInvalidURI) {
			t.Fatalf("an empty reason: err = %v, want a refusal before dialling", err)
		}
		client, err := hoarddb.Connect(ctx, uri, hoarddb.WithKnownServers(known), hoarddb.WithInsecureSkipVerify("conformance leg 2"))
		if err != nil {
			t.Fatalf("with a reason: %v", err)
		}
		_ = client.Close()
	})
}

// presentedFingerprint completes a handshake with the check switched off and
// reports the key the server showed.
func presentedFingerprint(t *testing.T, address string) string {
	t.Helper()
	var presented string
	config, err := trust.NewTLSConfig(address, trust.Policy{
		InsecureReason: "reading the presented key for comparison",
		OnInsecure:     func(_, fingerprint string) { presented = fingerprint },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := (&tls.Dialer{Config: config}).DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatalf("handshake with %s: %v", address, err)
	}
	_ = conn.Close()
	return presented
}

func certificateFingerprint(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the server's certificate (handbook/trust.md section 1): %v", err)
	}
	block, _ := pem.Decode(contents)
	if block == nil {
		t.Fatalf("%s holds no PEM block", path)
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	key, isEd25519 := certificate.PublicKey.(ed25519.PublicKey)
	if !isEd25519 {
		t.Fatalf("%s carries a %T key, want ed25519", path, certificate.PublicKey)
	}
	return trust.Fingerprint(key)
}

func firstVectorKey(t *testing.T, file *conformance.File) ed25519.PublicKey {
	t.Helper()
	for _, vector := range file.Vectors {
		if vector.Kind == "trust" && vector.PublicKeyHex != "" {
			key, err := hex.DecodeString(vector.PublicKeyHex)
			if err != nil {
				t.Fatal(err)
			}
			return key
		}
	}
	t.Fatal("no trust vector carries a key")
	return nil
}

// privateKnownServers is a known_servers path in a fresh 0700 directory, as
// ~/.hoarddb is: the store refuses a directory other users can write, and
// t.TempDir follows the umask.
func privateKnownServers(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), ".hoarddb")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, "known_servers")
}
