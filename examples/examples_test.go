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

package examples_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// What each example must print, in part. Every example is a main package that
// reads HOARDDB_URI, so the test is the way a reader runs it.
var expectedOutput = map[string][]string{
	"hash":      {"stored ada", "read back: Ada, 36", "deleted ada: true", "gone: not found"},
	"placement": {"events placement: spread", "fifo refused: true"},
	"upsert":    {"put refused: ada is taken", "ada is now: Ada Lovelace"},
	"btree":     {"smallest key: apple", "largest key: pear", "a to c: apple, banana, cherry"},
	"fifo":      {"pushed 3 orders", "shipping order 1", "shipping order 3", "queue drained"},
	"lifo":      {"pushed 3 pages", "back to page 3", "back to page 1", "stack drained"},
	"consume":   {"claimed 2 jobs", "acknowledged 2", "consumed 2 jobs; stopped: true"},
	"heap":      {"pushed 3 jobs", "next job: backup (priority 9)", "next job: report (priority 5)", "next job: cleanup (priority 1)", "heap drained"},
	"blob":      {"stored 11 bytes", "read back: hello world", "type: text/plain"},
	"fields":    {"ada's purchases: 1", "ada bought: tea for ada", "bob bought: tea for bob", "bob's friend: Ada"},
	"errors":    {"not found:", "Run '", "duplicate key: ada@example.com is taken"},
	"trust":     {"connected: hoardDB"},
	// One node has no second node to copy to, so the example prints the server's
	// refusal; TestAlterExampleOnACluster runs it where the alter is accepted.
	"alter": {"refused:", "standalone"},
}

// storeTypeExamples is the six store types, each of which must have an example
// of its own.
var storeTypeExamples = []string{"hash", "btree", "fifo", "lifo", "heap", "blob"}

func TestEveryStoreTypeHasAnExample(t *testing.T) {
	for _, name := range storeTypeExamples {
		if _, err := os.Stat(filepath.Join(name, "main.go")); err != nil {
			t.Errorf("there is no example for the %s store type: %v", name, err)
		}
	}
}

const licenceHeaderLines = 13

// The build doc's size rule for an example: short enough to read at a glance.
func TestEveryExampleIsUnderSixtyLines(t *testing.T) {
	for name := range expectedOutput {
		source, err := os.ReadFile(filepath.Join(name, "main.go"))
		if err != nil {
			t.Errorf("example %s: %v", name, err)
			continue
		}
		// The 13-line licence header is boilerplate, not part of what a reader
		// reads. The trust example predates this rule (step 4) and is exempt.
		if lines := strings.Count(string(source), "\n") - licenceHeaderLines; name != "trust" && lines >= 60 {
			t.Errorf("example %s is %d lines after its licence header; keep it under 60", name, lines)
		}
	}
}

func TestEveryExampleRunsAgainstAServer(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if entry.Name() == "gin" {
			t.Log("skipping gin: it is a separate module with its own go.mod; CI runs 'cd examples/gin && go test ./...'")
			continue
		}
		found = append(found, entry.Name())
	}
	for name := range expectedOutput {
		if _, err := os.Stat(filepath.Join(name, "main.go")); err != nil {
			t.Errorf("example %s is missing: %v", name, err)
		}
	}
	server := servertest.Start(t, servertest.Options{})
	binaries := t.TempDir()
	for _, name := range found {
		t.Run(name, func(t *testing.T) {
			wanted, listed := expectedOutput[name]
			if !listed {
				t.Fatalf("example %s has no entry in expectedOutput: say what it must print", name)
			}
			program := filepath.Join(binaries, name)
			if built, err := exec.Command("go", "build", "-o", program, "./"+name).CombinedOutput(); err != nil {
				t.Fatalf("building example %s: %v\n%s", name, err, built)
			}
			command := exec.Command(program)
			command.Env = append(os.Environ(), fmt.Sprintf("HOARDDB_URI=hoarddb://%s:%s@%s/examples?fingerprint=%s",
				server.User, server.Password, server.Address, server.Fingerprint))
			timer := time.AfterFunc(60*time.Second, func() { _ = command.Process.Kill() })
			defer timer.Stop()
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("running example %s: %v\n%s", name, err, output)
			}
			t.Logf("example %s printed:\n%s", name, output)
			for _, want := range wanted {
				if !strings.Contains(string(output), want) {
					t.Errorf("example %s should print %q; it printed:\n%s", name, want, output)
				}
			}
			// A second run must work too: an example is run twice by anyone who
			// copies it, and the first run's buckets are still there.
			again := exec.Command(program)
			again.Env = command.Env
			if output, err := again.CombinedOutput(); err != nil {
				t.Errorf("running example %s a second time: %v\n%s", name, err, output)
			}
		})
	}
}

// TestAlterExampleOnACluster runs the alter example where the alter is
// accepted: three nodes, a bucket created with the node's default factor.
func TestAlterExampleOnACluster(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	node := cluster.Node(0)
	program := filepath.Join(t.TempDir(), "alter")
	if built, err := exec.Command("go", "build", "-o", program, "./alter").CombinedOutput(); err != nil {
		t.Fatalf("building the example: %v\n%s", err, built)
	}
	command := exec.Command(program)
	command.Env = append(os.Environ(), fmt.Sprintf("HOARDDB_URI=hoarddb://%s:%s@%s/examples?fingerprint=%s",
		node.User, node.Password, node.Address, node.Fingerprint))
	timer := time.AfterFunc(60*time.Second, func() { _ = command.Process.Kill() })
	defer timer.Stop()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("running the example: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "invoices keeps 3 copies") {
		t.Fatalf("the example should print that invoices keeps 3 copies; it printed:\n%s", output)
	}
}
