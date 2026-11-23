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
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testServer is a TLS listener standing in for a hoardDB server: ed25519
// certificate, TLS 1.3, ALPN hoarddb/1.
type testServer struct {
	address string
	key     ed25519.PublicKey
}

func newEd25519Key(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return public, private
}

func selfSigned(t *testing.T, public, private any) tls.Certificate {
	t.Helper()
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
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}
}

func startServer(t *testing.T, certificate tls.Certificate, serverConfig func(*tls.Config)) string {
	t.Helper()
	config := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{ALPN},
	}
	if serverConfig != nil {
		serverConfig(config)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.WaitGroup
	connections.Add(1)
	go func() {
		defer connections.Done()
		for {
			accepted, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer func() { _ = accepted.Close() }()
				_ = accepted.SetDeadline(time.Now().Add(2 * time.Second))
				buffer := make([]byte, 1)
				_, _ = accepted.Read(buffer) // drives the handshake, returns when the client closes
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		connections.Wait()
	})
	return listener.Addr().String()
}

func newServer(t *testing.T) testServer {
	t.Helper()
	public, private := newEd25519Key(t)
	return testServer{address: startServer(t, selfSigned(t, public, private), nil), key: public}
}

func dial(t *testing.T, address string, policy Policy) (*tls.ConnectionState, error) {
	t.Helper()
	config, err := NewTLSConfig(address, policy)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := (&tls.Dialer{Config: config}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	state := conn.(*tls.Conn).ConnectionState()
	return &state, nil
}

func TestFingerprintKnownAnswer(t *testing.T) {
	// RFC 8032 test 1 is a different key; this one is the key of the all-zero
	// seed. The expected value was computed independently with sha256sum over
	// the raw 32 key bytes, which is what the server prints.
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if got := hex.EncodeToString(key); got != "3b6a27bcceb6a42d62a3a8d02a6f0d73653215771de243a63ac048a18b59da29" {
		t.Fatalf("test key is %s; the known answer below was computed for another key", got)
	}
	const want = "SHA256:139e3940e64b5491722088d9a0d741628fc826e09475d341a780acde3c4b8070"
	if got := Fingerprint(key); got != want {
		t.Fatalf("Fingerprint = %s, want %s (sha256 of the raw key, not of the certificate or SPKI)", got, want)
	}
}

func TestParseFingerprint(t *testing.T) {
	canonical := "SHA256:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68"
	valid := []struct{ name, input string }{
		{"canonical", canonical},
		{"lower-case prefix", "sha256:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68"},
		{"mixed-case prefix", "Sha256:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68"},
		{"upper-case hex", "SHA256:3B7C1F0A9D2E4C6B8A0F5D3E7C1B9A4F6D2E8C0B5A7F9D1E3C5B7A9F0D2E4C68"},
		{"surrounding space", "  " + canonical + "\n"},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseFingerprint(test.input)
			if err != nil || got != canonical {
				t.Fatalf("ParseFingerprint(%q) = %q, %v; want %q", test.input, got, err, canonical)
			}
		})
	}
	invalid := []struct{ name, input, wantInMessage string }{
		{"no prefix", "3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68", "SHA256:"},
		{"empty", "", "SHA256:"},
		{"too short", "SHA256:3b7c1f0a", "64"},
		{"too long", canonical + "00", "64"},
		{"not hex", "SHA256:zzzz1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4czz", "hex"},
		{"other digest name", "SHA1:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68", "SHA256:"},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseFingerprint(test.input)
			if err == nil || !strings.Contains(err.Error(), test.wantInMessage) {
				t.Fatalf("ParseFingerprint(%q) error = %v; want one mentioning %q", test.input, err, test.wantInMessage)
			}
		})
	}
}

func TestPinnedFingerprint(t *testing.T) {
	server := newServer(t)
	good := Fingerprint(server.key)
	state, err := dial(t, server.address, Policy{Pin: good})
	if err != nil {
		t.Fatalf("a correct pin must connect: %v", err)
	}
	if state.Version != tls.VersionTLS13 || state.NegotiatedProtocol != ALPN {
		t.Fatalf("negotiated version %x protocol %q; want TLS 1.3 and %q", state.Version, state.NegotiatedProtocol, ALPN)
	}

	otherPublic, _ := newEd25519Key(t)
	wrong := Fingerprint(otherPublic)
	_, err = dial(t, server.address, Policy{Pin: wrong})
	var mismatch *MismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("a wrong pin: err = %v, want *MismatchError", err)
	}
	if mismatch.Pinned != wrong || mismatch.Presented != good {
		t.Fatalf("mismatch names pinned=%s presented=%s; want pinned=%s presented=%s", mismatch.Pinned, mismatch.Presented, wrong, good)
	}
	if !strings.Contains(err.Error(), wrong) || !strings.Contains(err.Error(), good) {
		t.Fatalf("the message must name both fingerprints: %v", err)
	}
}

func TestPinMustBeCanonical(t *testing.T) {
	_, err := NewTLSConfig("127.0.0.1:7433", Policy{Pin: "sha256:3B7C1F0A9D2E4C6B8A0F5D3E7C1B9A4F6D2E8C0B5A7F9D1E3C5B7A9F0D2E4C68"})
	if err == nil || !strings.Contains(err.Error(), "ParseFingerprint") {
		t.Fatalf("a non-canonical Pin would never compare equal and must be refused up front; err = %v", err)
	}
}

func TestPolicyMustChooseExactlyOne(t *testing.T) {
	known := NewKnownServers(knownPath(t))
	pin := "SHA256:" + strings.Repeat("ab", 32)
	tests := []struct {
		name   string
		policy Policy
	}{
		{"empty policy", Policy{}},
		{"blank insecure reason", Policy{InsecureReason: "   "}},
		{"pin and known", Policy{Pin: pin, Known: known}},
		{"pin and insecure", Policy{Pin: pin, InsecureReason: "local test"}},
		{"known and insecure", Policy{Known: known, InsecureReason: "local test"}},
		{"prompt alone", Policy{Prompt: func(string, string) bool { return true }}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewTLSConfig("127.0.0.1:7433", test.policy); !errors.Is(err, ErrNoPolicy) {
				t.Fatalf("err = %v, want ErrNoPolicy", err)
			}
		})
	}
}

// TestInsecureSkipVerifyNeverStandsAlone is the guard from handbook/trust.md
// section 3: the config may set InsecureSkipVerify only because a verifier
// replaces the chain check, and every policy that builds must carry one.
func TestInsecureSkipVerifyNeverStandsAlone(t *testing.T) {
	known := NewKnownServers(knownPath(t))
	policies := []Policy{
		{Pin: "SHA256:" + strings.Repeat("0f", 32)},
		{Known: known},
		{Known: known, Prompt: func(string, string) bool { return false }},
		{InsecureReason: "unit test"},
		{InsecureReason: "unit test", OnInsecure: func(string, string) {}},
	}
	for _, address := range []string{"127.0.0.1:7433", "db.internal:7433", "[::1]:7433", "no-port"} {
		for index, policy := range policies {
			config, err := NewTLSConfig(address, policy)
			if err != nil {
				t.Fatalf("policy %d, address %s: %v", index, address, err)
			}
			if config.InsecureSkipVerify && config.VerifyPeerCertificate == nil {
				t.Fatalf("policy %d, address %s: InsecureSkipVerify set with no verifier", index, address)
			}
			if config.MinVersion != tls.VersionTLS13 {
				t.Fatalf("policy %d: MinVersion %x, want TLS 1.3", index, config.MinVersion)
			}
			if len(config.NextProtos) != 1 || config.NextProtos[0] != "hoarddb/1" {
				t.Fatalf("policy %d: NextProtos %v, want [hoarddb/1]", index, config.NextProtos)
			}
		}
	}
}

func TestKnownServersFirstSightPromptRecordsThenTrustsWithoutPrompt(t *testing.T) {
	server := newServer(t)
	store := NewKnownServers(filepath.Join(t.TempDir(), "sub", "known_servers"))

	// No prompt: an unknown server is refused, not trusted.
	_, err := dial(t, server.address, Policy{Known: store})
	var unknown *UnknownServerError
	if !errors.As(err, &unknown) || unknown.Declined || unknown.Fingerprint != Fingerprint(server.key) {
		t.Fatalf("no prompt: err = %v, want *UnknownServerError naming the fingerprint", err)
	}
	if _, found, _ := store.Lookup(server.address); found {
		t.Fatal("a refused server must not be recorded")
	}

	// A declining prompt refuses and records nothing.
	_, err = dial(t, server.address, Policy{Known: store, Prompt: func(string, string) bool { return false }})
	if !errors.As(err, &unknown) || !unknown.Declined {
		t.Fatalf("declined: err = %v, want *UnknownServerError with Declined", err)
	}

	// An accepting prompt connects and records.
	var asked []string
	_, err = dial(t, server.address, Policy{Known: store, Prompt: func(address, fingerprint string) bool {
		asked = append(asked, address+" "+fingerprint)
		return true
	}})
	if err != nil {
		t.Fatalf("accepted first sight: %v", err)
	}
	if len(asked) != 1 || asked[0] != server.address+" "+Fingerprint(server.key) {
		t.Fatalf("prompt asked %v", asked)
	}

	// The second connection needs no prompt at all.
	if _, err := dial(t, server.address, Policy{Known: store, Prompt: func(string, string) bool {
		t.Error("a recorded server must not be prompted about")
		return false
	}}); err != nil {
		t.Fatalf("recorded server: %v", err)
	}
}

func TestKnownServersChangedKeyIsHardFailureNeverPrompted(t *testing.T) {
	original := newServer(t)
	store := NewKnownServers(knownPath(t))
	if err := store.Add(original.address, original.key); err != nil {
		t.Fatal(err)
	}
	// A different server now answers on the recorded address.
	impostorPublic, impostorPrivate := newEd25519Key(t)
	impostor := startServer(t, selfSigned(t, impostorPublic, impostorPrivate), nil)
	if err := os.WriteFile(store.Path(), []byte(impostor+" "+hex.EncodeToString(original.key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := dial(t, impostor, Policy{Known: store, Prompt: func(string, string) bool {
		t.Error("a changed key must never be re-prompted")
		return true
	}})
	var changed *KeyChangedError
	if !errors.As(err, &changed) {
		t.Fatalf("err = %v, want *KeyChangedError", err)
	}
	if changed.Recorded != Fingerprint(original.key) || changed.Presented != Fingerprint(impostorPublic) || changed.Path != store.Path() {
		t.Fatalf("KeyChangedError = %+v", changed)
	}
}

func TestInsecurePolicyAcceptsAndReportsEveryDial(t *testing.T) {
	server := newServer(t)
	var seen []string
	policy := Policy{InsecureReason: "unit test", OnInsecure: func(address, fingerprint string) { seen = append(seen, fingerprint) }}
	for range 2 {
		if _, err := dial(t, server.address, policy); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 2 || seen[0] != Fingerprint(server.key) {
		t.Fatalf("OnInsecure calls = %v; want one per dial with the presented fingerprint", seen)
	}
}

func TestNonEd25519ServerIsRefusedByEveryPolicy(t *testing.T) {
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	address := startServer(t, selfSigned(t, &ecdsaKey.PublicKey, ecdsaKey), nil)
	policies := map[string]Policy{
		"pin":      {Pin: "SHA256:" + strings.Repeat("ab", 32)},
		"known":    {Known: NewKnownServers(knownPath(t))},
		"insecure": {InsecureReason: "unit test"},
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			_, err := dial(t, address, policy)
			var unsupported *UnsupportedKeyError
			if !errors.As(err, &unsupported) {
				t.Fatalf("err = %v, want *UnsupportedKeyError", err)
			}
		})
	}
}

func TestHandshakeRefusesWhatTheServerMustRefuse(t *testing.T) {
	public, private := newEd25519Key(t)
	pin := Policy{Pin: Fingerprint(public)}

	t.Run("a server limited to TLS 1.2", func(t *testing.T) {
		address := startServer(t, selfSigned(t, public, private), func(config *tls.Config) {
			config.MinVersion = tls.VersionTLS12
			config.MaxVersion = tls.VersionTLS12
		})
		if _, err := dial(t, address, pin); err == nil {
			t.Fatal("TLS 1.3 is unconditional; a TLS 1.2 server must not connect")
		}
	})

	t.Run("a server that speaks another application protocol", func(t *testing.T) {
		address := startServer(t, selfSigned(t, public, private), func(config *tls.Config) {
			config.NextProtos = []string{"other/1"}
		})
		// Go's TLS server fails the handshake when no offered protocol matches
		// its list, which is the behaviour hoardDB's own ALPN check enforces.
		if _, err := dial(t, address, pin); err == nil {
			t.Fatal("a server that does not serve hoarddb/1 must not connect")
		}
	})
}

func TestNothingListeningIsADialErrorNotATrustError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	_, err = dial(t, address, Policy{InsecureReason: "unit test"})
	var mismatch *MismatchError
	var unknown *UnknownServerError
	if err == nil || errors.As(err, &mismatch) || errors.As(err, &unknown) {
		t.Fatalf("err = %v; a closed port is a network error", err)
	}
}

func TestKnownServersFileFormat(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(directory, "known_servers")
	store := NewKnownServers(path)
	first, _ := newEd25519Key(t)
	second, _ := newEd25519Key(t)

	if _, found, err := store.Lookup("a:1"); found || err != nil {
		t.Fatalf("missing file: found=%v err=%v; want an empty store", found, err)
	}
	if err := store.Add("a:1", first); err != nil {
		t.Fatal(err)
	}
	directoryInfo, _ := os.Stat(directory)
	fileInfo, _ := os.Stat(path)
	if directoryInfo.Mode().Perm() != 0o700 || fileInfo.Mode().Perm() != 0o600 {
		t.Fatalf("modes: directory %v file %v; want 0700 and 0600", directoryInfo.Mode().Perm(), fileInfo.Mode().Perm())
	}
	contents, _ := os.ReadFile(path)
	if want := "a:1 " + hex.EncodeToString(first) + "\n"; string(contents) != want {
		t.Fatalf("file = %q, want %q (host:port, a space, lowercase hex of the 32-byte key)", contents, want)
	}

	// Lines the store did not write survive, even without a final newline.
	if err := os.WriteFile(path, []byte("# my servers\n\nb:2 "+hex.EncodeToString(second)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Add("c:3", first); err != nil {
		t.Fatal(err)
	}
	contents, _ = os.ReadFile(path)
	if !strings.HasPrefix(string(contents), "# my servers\n\nb:2 ") || !strings.HasSuffix(string(contents), "\nc:3 "+hex.EncodeToString(first)+"\n") {
		t.Fatalf("file after append = %q", contents)
	}
	for address, key := range map[string]ed25519.PublicKey{"b:2": second, "c:3": first} {
		got, found, err := store.Lookup(address)
		if err != nil || !found || got != Fingerprint(key) {
			t.Fatalf("Lookup(%s) = %q, %v, %v", address, got, found, err)
		}
	}
	if _, found, _ := store.Lookup("d:4"); found {
		t.Fatal("an unrecorded server was found")
	}
}

// TestKnownServersSkipsACorruptLine follows the vector "known_servers:
// malformed lines are skipped": a line the reader cannot parse is not an entry,
// and does not hide the lines around it.
func TestKnownServersSkipsACorruptLine(t *testing.T) {
	path := knownPath(t)
	if err := os.WriteFile(path, []byte("bad:1 nothex\ngood:2 "+strings.Repeat("ab", 32)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewKnownServers(path)
	if _, found, err := store.Lookup("bad:1"); found || err != nil {
		t.Fatalf("a corrupt line: found=%v err=%v; want no entry and no error", found, err)
	}
	if _, found, err := store.Lookup("good:2"); !found || err != nil {
		t.Fatalf("an unrelated corrupt line must not hide a good entry: found=%v err=%v", found, err)
	}
}

// TestConflictForAnyHostRefusesTheFile: handbook/trust.md section 4 makes two
// keys for one host an error when the file is loaded, whichever host is dialled.
func TestConflictForAnyHostRefusesTheFile(t *testing.T) {
	server := newServer(t)
	first, _ := newEd25519Key(t)
	second, _ := newEd25519Key(t)
	store := NewKnownServers(knownPath(t))
	lines := server.address + " " + hex.EncodeToString(server.key) + "\n" +
		"other:7433 " + hex.EncodeToString(first) + "\n" +
		"other:7433 " + hex.EncodeToString(second) + "\n"
	if err := os.WriteFile(store.Path(), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := dial(t, server.address, Policy{Known: store})
	var conflicting *ConflictingEntriesError
	if !errors.As(err, &conflicting) || conflicting.Address != "other:7433" {
		t.Fatalf("err = %v, want *ConflictingEntriesError naming other:7433", err)
	}
	if err := store.Add("third:7433", first); !errors.As(err, &conflicting) {
		t.Fatalf("Add to a conflicting file: err = %v, want *ConflictingEntriesError", err)
	}
}

func TestKnownServersConcurrentAdds(t *testing.T) {
	store := NewKnownServers(knownPath(t))
	key, _ := newEd25519Key(t)
	var group sync.WaitGroup
	for index := range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := store.Add("host:"+string(rune('a'+index)), key); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	contents, _ := os.ReadFile(store.Path())
	if lines := strings.Count(string(contents), "\n"); lines != 20 {
		t.Fatalf("%d lines after 20 concurrent adds, want 20:\n%s", lines, contents)
	}
}
