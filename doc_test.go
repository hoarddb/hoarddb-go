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
	"os/exec"
	"strings"
	"testing"
)

// TestGodocNamesEveryStoreType reads the package documentation as a developer
// sees it (go doc) and checks that its verb table has a row for each of the
// six store types, naming that type's verbs.
func TestGodocNamesEveryStoreType(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to go doc")
	}
	output, err := exec.Command("go", "doc", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go doc: %v\n%s", err, output)
	}
	rows := map[string][]string{
		"fifo":  {"Push", "Pop", "Peek", "Find"},
		"lifo":  {"Push", "Pop", "Peek", "Find"},
		"heap":  {"Push", "WithPriority", "Pop", "Peek", "Find"},
		"hash":  {"Put", "Insert", "Get", "Delete", "Update", "Search", "Find"},
		"btree": {"Put", "Get", "Range", "First", "Last", "Find"},
		"blob":  {"PutBlob", "GetBlob", "Delete"},
	}
	for storeType, verbs := range rows {
		var row string
		for _, line := range strings.Split(string(output), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), storeType+" ") {
				row = line
			}
		}
		if row == "" {
			t.Errorf("the package documentation has no table row for the %s store type", storeType)
			continue
		}
		for _, verb := range verbs {
			if !strings.Contains(row, verb) {
				t.Errorf("the %s row should name %s; it reads %q", storeType, verb, strings.TrimSpace(row))
			}
		}
	}
}
