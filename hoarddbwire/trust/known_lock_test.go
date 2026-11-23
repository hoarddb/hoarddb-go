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
	"os"
	"syscall"
	"testing"
	"time"
)

// TestAddWaitsForAnotherProcessHoldingTheFileLock stands in for a second
// process: an advisory lock held on another descriptor must block Add.
func TestAddWaitsForAnotherProcessHoldingTheFileLock(t *testing.T) {
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
	finished := make(chan error, 1)
	go func() { finished <- store.Add("h:1", key) }()
	select {
	case err := <-finished:
		t.Fatalf("Add returned (%v) while another holder had the lock", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}
