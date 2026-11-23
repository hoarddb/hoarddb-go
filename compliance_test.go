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

package hoarddb_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runBannedNameCheck runs the script's banned-name step on a scratch tree
// holding one README with the given text, and returns its exit code and output.
func runBannedNameCheck(t *testing.T, readme string) (int, string) {
	t.Helper()
	script, err := filepath.Abs("scripts/compliance-check.sh")
	if err != nil {
		t.Fatal(err)
	}
	tree := t.TempDir()
	if err := os.WriteFile(filepath.Join(tree, "README.md"), []byte(readme), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", script, "--only", "names", tree)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(output)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(output)
	default:
		t.Fatalf("running %s: %v\n%s", script, err, output)
		return -1, ""
	}
}

// TestComplianceCheckFailsOnABannedName plants another database's name in a
// scratch README: the check must exit 1 and name the file. A clean README is
// the control, so a script that always fails would not pass this test.
func TestComplianceCheckFailsOnABannedName(t *testing.T) {
	// Built in pieces so this file is not itself a hit for the check.
	planted := "Migrating from " + "Mon" + "goDB is one command.\n"

	if code, output := runBannedNameCheck(t, "# hoardDB driver\n\nA clean README.\n"); code != 0 {
		t.Fatalf("a clean tree should pass; exit %d:\n%s", code, output)
	}
	// Whole words only: a word that merely contains a banned name (the licence
	// text says "Redistribution") is not a hit.
	if code, output := runBannedNameCheck(t, "Redistribution of copies is permitted.\n"); code != 0 {
		t.Fatalf("a word that only contains a banned name should pass; exit %d:\n%s", code, output)
	}
	code, output := runBannedNameCheck(t, "# hoardDB driver\n\n"+planted)
	if code != 1 {
		t.Fatalf("a planted name should exit 1; exit %d:\n%s", code, output)
	}
	if !strings.Contains(output, "README.md") {
		t.Errorf("the failure should name the file; it printed:\n%s", output)
	}
}
