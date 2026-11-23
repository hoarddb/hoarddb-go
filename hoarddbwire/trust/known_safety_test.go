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
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// knownPath is a known_servers path in a private (0700) directory: t.TempDir's
// own directory follows the umask and is group-writable under 002.
func knownPath(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "known_servers")
}

func TestAddRefusesADifferentKeyForARecordedHost(t *testing.T) {
	store := NewKnownServers(knownPath(t))
	first, _ := newEd25519Key(t)
	second, _ := newEd25519Key(t)
	if err := store.Add("h:1", first); err != nil {
		t.Fatal(err)
	}
	var changed *KeyChangedError
	if err := store.Add("h:1", second); !errors.As(err, &changed) {
		t.Fatalf("recording a second key for one host: err = %v, want *KeyChangedError", err)
	}
	if changed.Recorded != Fingerprint(first) || changed.Presented != Fingerprint(second) {
		t.Fatalf("KeyChangedError = %+v", changed)
	}
	if err := store.Add("h:1", first); err != nil {
		t.Fatalf("re-recording the same key is a no-op: %v", err)
	}
	contents, _ := os.ReadFile(store.Path())
	if strings.Count(string(contents), "\n") != 1 {
		t.Fatalf("file = %q; want exactly one entry", contents)
	}
}

// TestConcurrentFirstUseRecordsAtMostOneKey is the race behind first-use
// trust: Lookup, the prompt and Add are separate steps, so two connections to a
// host that shows a different key each time can both pass the lookup.
func TestConcurrentFirstUseRecordsAtMostOneKey(t *testing.T) {
	store := NewKnownServers(knownPath(t))
	const contenders = 24
	var (
		group     sync.WaitGroup
		successes int
		mu        sync.Mutex
		start     = make(chan struct{})
	)
	for range contenders {
		key, _ := newEd25519Key(t)
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			if err := store.Add("h:1", key); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			} else if !errors.As(err, new(*KeyChangedError)) {
				t.Errorf("loser err = %v, want *KeyChangedError", err)
			}
		}()
	}
	close(start)
	group.Wait()
	contents, _ := os.ReadFile(store.Path())
	if successes != 1 || strings.Count(string(contents), "\n") != 1 {
		t.Fatalf("%d successes, file %q; want exactly one key recorded", successes, contents)
	}
}

func TestConflictingEntriesForOneHostAreRefused(t *testing.T) {
	server := newServer(t)
	other, _ := newEd25519Key(t)
	store := NewKnownServers(knownPath(t))
	lines := server.address + " " + hex.EncodeToString(server.key) + "\n" + server.address + " " + hex.EncodeToString(other) + "\n"
	if err := os.WriteFile(store.Path(), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	// The first entry is the right key, so silently picking it (or the last)
	// would connect; two different keys for one host must refuse instead.
	_, err := dial(t, server.address, Policy{Known: store, Prompt: func(string, string) bool { return true }})
	var changed *KeyChangedError
	if !errors.As(err, &changed) {
		t.Fatalf("err = %v, want *KeyChangedError", err)
	}
	// The same key twice is harmless.
	same := server.address + " " + hex.EncodeToString(server.key) + "\n"
	if err := os.WriteFile(store.Path(), []byte(same+same), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := dial(t, server.address, Policy{Known: store}); err != nil {
		t.Fatalf("a repeated identical entry: %v", err)
	}
}

func TestKnownServersRefusesAFileOrDirectoryOthersCanWrite(t *testing.T) {
	key, _ := newEd25519Key(t)
	for name, mutate := range map[string]func(dir, file string){
		"file":      func(_, file string) { _ = os.Chmod(file, 0o666) },
		"directory": func(dir, _ string) { _ = os.Chmod(dir, 0o777) },
		"group":     func(_, file string) { _ = os.Chmod(file, 0o620) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "state")
			store := NewKnownServers(filepath.Join(dir, "known_servers"))
			if err := store.Add("h:1", key); err != nil {
				t.Fatal(err)
			}
			mutate(dir, store.Path())
			_, _, lookupErr := store.Lookup("h:1")
			addErr := store.Add("h:2", key)
			for _, err := range []error{lookupErr, addErr} {
				if err == nil || !strings.Contains(err.Error(), "chmod") {
					t.Fatalf("err = %v; want a refusal that says how to chmod it", err)
				}
			}
		})
	}
}

func TestStickySharedDirectoryIsNotToldToChmod(t *testing.T) {
	shared := &UnsafeFileError{Path: "/tmp", Mode: os.ModeDir | os.ModeSticky | 0o777, Directory: true}
	message := shared.Error()
	if strings.Contains(message, "chmod 700 /tmp") || !strings.Contains(message, "private directory") {
		t.Fatalf("advice for a shared sticky directory: %s", message)
	}
	own := &UnsafeFileError{Path: "/var/empty/u/.hoarddb", Mode: os.ModeDir | 0o777, Directory: true}
	if !strings.Contains(own.Error(), "chmod 700 /var/empty/u/.hoarddb") {
		t.Fatalf("advice for one's own directory: %s", own)
	}
}

var _ = ed25519.PublicKeySize

// TestLookupCanonicalisesTheAddressTheCommandLineToolDoes: the hoardDB command line tool writes
// a host name lowercased with one trailing dot removed, so the same server spelled differently
// in Options.Addr must find that line. The lookup never resolves a name and never equates a name
// with an IP address: a line for one name must not vouch for another.
func TestLookupCanonicalisesTheAddressTheCommandLineToolDoes(t *testing.T) {
	key, _ := newEd25519Key(t)
	store := NewKnownServers(knownPath(t))
	if err := store.Add("DB.Internal.:7433", key); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	want := "db.internal:7433 " + hex.EncodeToString(key) + "\n"
	if string(contents) != want {
		t.Fatalf("the file holds %q, want the canonical line %q", contents, want)
	}
	for _, spelling := range []string{"db.internal:7433", "DB.INTERNAL:7433", "Db.Internal.:7433"} {
		fingerprint, found, err := store.Lookup(spelling)
		if err != nil || !found || fingerprint != Fingerprint(key) {
			t.Fatalf("Lookup(%q) = %q, %v, %v; want the recorded key", spelling, fingerprint, found, err)
		}
	}
	for _, stranger := range []string{"192.0.2.7:7433", "other.internal:7433", "db.internal:7434"} {
		fingerprint, found, err := store.Lookup(stranger)
		if err != nil || found || fingerprint != "" {
			t.Fatalf("Lookup(%q) = %q, %v, %v; want no entry: a name is not an IP address, and a line vouches for one name and port",
				stranger, fingerprint, found, err)
		}
	}
}

// TestAFileWrittenByHandInAnotherSpellingIsFound: a line an older tool or a person wrote in
// another case or with a trailing dot is the same server, so the canonical lookup finds it.
func TestAFileWrittenByHandInAnotherSpellingIsFound(t *testing.T) {
	key, _ := newEd25519Key(t)
	path := knownPath(t)
	if err := os.WriteFile(path, []byte("DB.Internal.:7433 "+hex.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fingerprint, found, err := NewKnownServers(path).Lookup("db.internal:7433")
	if err != nil || !found || fingerprint != Fingerprint(key) {
		t.Fatalf("Lookup = %q, %v, %v; want the key of the hand-written line", fingerprint, found, err)
	}
}
