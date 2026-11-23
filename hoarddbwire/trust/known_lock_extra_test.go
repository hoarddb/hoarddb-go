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

//go:build unix && !solaris && !aix

package trust

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestAddGivesUpWaitingForTheLockInsteadOfHangingTheHandshake(t *testing.T) {
	store := NewKnownServers(knownPath(t))
	key, _ := newEd25519Key(t)
	if err := store.Add("seed:1", key); err != nil {
		t.Fatal(err)
	}
	holder, err := os.Open(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	previous := lockTimeout
	lockTimeout = 150 * time.Millisecond
	defer func() { lockTimeout = previous }()

	started := time.Now()
	err = store.Add("h:1", key)
	if !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("err = %v, want ErrLockTimeout", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("the wait was not bounded: %v", elapsed)
	}
}

func TestAddFallsBackWithoutFileLocksAndSaysSoOnce(t *testing.T) {
	previous := flock
	flock = func(int, int) error { return syscall.ENOLCK } // a mount with no lock support
	defer func() { flock = previous }()

	store := NewKnownServers(knownPath(t))
	var notices atomic.Int32
	store.OnLockUnsupported = func(path string) {
		if path != store.Path() {
			t.Errorf("notice named %q", path)
		}
		notices.Add(1)
	}
	first, _ := newEd25519Key(t)
	second, _ := newEd25519Key(t)
	if err := store.Add("a:1", first); err != nil {
		t.Fatalf("Add must not fail closed when locks are unsupported: %v", err)
	}
	if err := store.Add("b:2", first); err != nil {
		t.Fatal(err)
	}
	// The re-read still protects this process.
	if err := store.Add("a:1", second); !errors.As(err, new(*KeyChangedError)) {
		t.Fatalf("err = %v, want *KeyChangedError", err)
	}
	if notices.Load() != 1 {
		t.Fatalf("%d notices, want exactly one", notices.Load())
	}
	contents, _ := os.ReadFile(store.Path())
	if strings.Count(string(contents), "\n") != 2 {
		t.Fatalf("file = %q", contents)
	}
}
