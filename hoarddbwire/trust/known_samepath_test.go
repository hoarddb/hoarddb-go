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
	"strings"
	"sync"
	"syscall"
	"testing"
)

// TestTwoInstancesOnOnePathRecordOneFirstSeenKey: without file locks (and
// always on windows, solaris and aix) the per-instance mutex does not cover
// two Clients in one process that share a known_servers path.
func TestTwoInstancesOnOnePathRecordOneFirstSeenKey(t *testing.T) {
	previous := flock
	flock = func(int, int) error { return syscall.ENOLCK }
	defer func() { flock = previous }()

	const rounds, writers = 200, 8
	for round := 0; round < rounds; round++ {
		path := knownPath(t)
		stores := []*KnownServers{NewKnownServers(path), NewKnownServers(path)}
		start := make(chan struct{})
		var finished sync.WaitGroup
		for writer := 0; writer < writers; writer++ {
			key, _ := newEd25519Key(t)
			store := stores[writer%2]
			finished.Add(1)
			go func() {
				defer finished.Done()
				<-start
				_ = store.Add("h:1", key) // KeyChangedError is the expected loser
			}()
		}
		close(start)
		finished.Wait()
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if lines := strings.Count(string(contents), "h:1 "); lines != 1 {
			t.Fatalf("round %d recorded %d keys for one host, want 1:\n%s", round, lines, contents)
		}
	}
}
