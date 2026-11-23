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

package codec_test

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// TestCodecImportsOnlyHoarddbBson fails while any package the driver builds,
// tests included, comes from the third-party BSON module the codec used before
// hoarddb-bson, or from the server module. The codec is the only place
// the library is imported, and the library is Apache-2.0.
func TestCodecImportsOnlyHoarddbBson(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to go list")
	}
	out, err := exec.Command("go", "list", "-deps", "-test",
		"-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "../../...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	serverModule := regexp.MustCompile(`^hoardDB(/|$)`)
	for _, importPath := range strings.Fields(string(out)) {
		if strings.HasPrefix(importPath, "go.mongodb.org/") {
			t.Errorf("package %q is the previous BSON module; the codec must use github.com/hoarddb/hoarddb-bson", importPath)
		}
		if serverModule.MatchString(importPath) {
			t.Errorf("package %q is part of the server module", importPath)
		}
	}
}
