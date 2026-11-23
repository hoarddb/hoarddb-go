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

package ring_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestNoErrorTextNamesTheOldPackage fails while a string literal of this
// package still begins "ringclient: ", the in-tree package it was copied from:
// a caller reads "ring: " instead. The callers that strip the prefix match it
// exactly, so a stale one would show up in their output.
func TestNoErrorTextNamesTheOldPackage(t *testing.T) {
	const (
		stale   = "ringclient: "
		current = "ring: "
	)
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing the package files: %v", err)
	}
	files := token.NewFileSet()
	literals, staleCount, currentCount := 0, 0, 0
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatalf("unquoting %s:%d: %v", path, files.Position(literal.Pos()).Line, err)
			}
			literals++
			switch {
			case strings.HasPrefix(text, stale):
				staleCount++
				t.Errorf("%s:%d still begins %q: %s", path, files.Position(literal.Pos()).Line, stale, strings.SplitN(text, "\n", 2)[0])
			case strings.HasPrefix(text, current):
				currentCount++
			}
			return true
		})
	}
	if literals == 0 {
		t.Fatalf("found no string literal in %v; the test read nothing", paths)
	}
	if currentCount == 0 {
		t.Errorf("no error text begins %q; a caller that strips the prefix has nothing to strip", current)
	}
	t.Logf("read %d string literals: %d begin %q, %d begin %q", literals, currentCount, current, staleCount, stale)
}

// TestNoConsumerExpectsTheOldPackage fails while a test outside the package
// still expects an error text beginning "ringclient: ". Those assertions read
// this driver's texts, so they went stale when the driver's text changed, and
// the scan above never sees them: it reads this package alone. A test may
// expect the old prefix only while code in its own directory writes it.
func TestNoConsumerExpectsTheOldPackage(t *testing.T) {
	const stale = "ringclient: "
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("locating the checkout: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "hoardDB", "go.mod")); err != nil {
		t.Skipf("%s is not a checkout of both modules: the driver is used on its own", root)
	}
	here, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("locating this package: %v", err)
	}
	writes := make(map[string]bool)
	testFiles := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			name := entry.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "docs" || name == "testdata" || name == "node_modules" || name == "vendor") {
				return fs.SkipDir
			}
			return nil
		}
		dir := filepath.Dir(path)
		if !strings.HasSuffix(path, "_test.go") || dir == here {
			return nil
		}
		testFiles++
		line := staleLine(t, path, stale)
		if line == 0 {
			return nil
		}
		known, cached := writes[dir]
		if !cached {
			known = dirWrites(t, dir, stale)
			writes[dir] = known
		}
		if !known {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			t.Errorf("%s:%d expects an error text beginning %q and no code in %s writes one, so it asserts the texts of another package", rel, line, stale, dir)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if testFiles == 0 {
		t.Fatalf("read no test file under %s; the test read nothing", root)
	}
	t.Logf("read %d test files under %s outside this package", testFiles, root)
}

// staleLine returns the line of the first string literal of the Go file at path
// that begins with prefix, or 0 when none does.
func staleLine(t *testing.T, path, prefix string) int {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	line := 0
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING || line != 0 {
			return true
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatalf("unquoting %s:%d: %v", path, files.Position(literal.Pos()).Line, err)
		}
		if strings.HasPrefix(text, prefix) {
			line = files.Position(literal.Pos()).Line
		}
		return true
	})
	return line
}

// dirWrites reports whether non-test code in dir writes a string literal that
// begins with prefix.
func dirWrites(t *testing.T, dir, prefix string) bool {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		if staleLine(t, path, prefix) != 0 {
			return true
		}
	}
	return false
}
