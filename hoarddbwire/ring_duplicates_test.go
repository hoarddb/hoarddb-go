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

package hoarddbwire_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// hoarddbwireImportPath is this package, as the root package imports it.
const hoarddbwireImportPath = "github.com/hoarddb/hoarddb-go/hoarddbwire"

// keptRingFiles are the ring files this package keeps: the string id, the slot
// of an entity, and the topology request and reply with the members and the
// table the reply carries. Every other ring request and reply is read and
// written by the hoarddb-wire module, which the server uses too, so a second
// codec here could only drift from it.
var keptRingFiles = []string{"ring_id.go", "ring_members.go", "ring_slot.go", "ring_table.go", "ring_topology.go"}

// removedRootFiles are the files of the root package's unexported ring router
// and its tests. Package ring is the ring client; nothing public called the
// router.
var removedRootFiles = []string{
	"ring_call.go", "ring_retry.go", "ring_topology.go", "ring_outcome.go", "ring_pools.go",
	"ring_call_test.go", "ring_call_internal_test.go", "ring_retry_internal_test.go",
	"ring_topology_internal_test.go", "ring_outcome_internal_test.go",
	"ring_pools_internal_test.go", "ring_pools_test.go",
}

// removedRootNames are router declarations whose names do not say ring.
var removedRootNames = []string{"memberPools", "newMemberPools"}

// namesRing reports whether an identifier is about the ring.
func namesRing(name string) bool {
	return strings.Contains(name, "Ring") || strings.HasPrefix(name, "ring")
}

// declaredNames parses the non-test Go files of one directory and returns each
// top-level name (function, method, type, constant, variable) with its file.
func declaredNames(t *testing.T, dir string) (files *token.FileSet, parsed map[string]*ast.File, declared map[string]string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	files = token.NewFileSet()
	parsed, declared = map[string]*ast.File{}, map[string]string{}
	for _, path := range paths {
		base := filepath.Base(path)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		parsed[base] = file
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				declared[declaration.Name.Name] = base
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						declared[spec.Name.Name] = base
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							declared[name.Name] = base
						}
					}
				}
			}
		}
	}
	if len(parsed) == 0 {
		t.Fatalf("found no Go file in %s; the test read nothing", dir)
	}
	return files, parsed, declared
}

// TestThisPackageKeepsOnlyTheTopologyRingCodec fails while this package still
// holds a ring codec that hoarddb-wire provides: a ring file besides the kept
// five, or a ring declaration in any other file (the opcode numbers excepted).
func TestThisPackageKeepsOnlyTheTopologyRingCodec(t *testing.T) {
	_, parsed, declared := declaredNames(t, ".")
	for base := range parsed {
		if strings.HasPrefix(base, "ring_") && !slices.Contains(keptRingFiles, base) {
			t.Errorf("%s is still here: this package keeps only %v, and hoarddb-wire holds every other ring request and reply", base, keptRingFiles)
		}
	}
	for _, kept := range keptRingFiles {
		if parsed[kept] == nil {
			t.Errorf("%s is missing: package ring reads the topology with it", kept)
		}
	}
	for name, base := range declared {
		if !namesRing(name) || slices.Contains(keptRingFiles, base) {
			continue
		}
		if base == "opcodes.go" && strings.HasPrefix(name, "OpRing") {
			continue
		}
		t.Errorf("%s declares %s: a ring declaration lives in one of %v, or in hoarddb-wire", base, name, keptRingFiles)
	}
}

// TestTheRootPackageHasNoRingRouter fails while the root package still holds
// its unexported ring router: one of the router's files, a ring declaration,
// or a use of a ring name of this package.
func TestTheRootPackageHasNoRingRouter(t *testing.T) {
	for _, base := range removedRootFiles {
		if _, err := os.Stat(filepath.Join("..", base)); err == nil {
			t.Errorf("../%s is still here: the ring client is package ring, and the root package routes nothing", base)
		}
	}
	_, parsed, declared := declaredNames(t, "..")
	for name, base := range declared {
		if namesRing(name) || slices.Contains(removedRootNames, name) {
			t.Errorf("../%s declares %s: the root package holds no ring router", base, name)
		}
	}
	for base, file := range parsed {
		qualifier := ""
		for _, spec := range file.Imports {
			if strings.Trim(spec.Path.Value, `"`) != hoarddbwireImportPath {
				continue
			}
			qualifier = "hoarddbwire"
			if spec.Name != nil {
				qualifier = spec.Name.Name
			}
		}
		if qualifier == "" {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if on, ok := selector.X.(*ast.Ident); ok && on.Name == qualifier && namesRing(selector.Sel.Name) {
				t.Errorf("../%s uses %s.%s: the root package sends no ring request", base, qualifier, selector.Sel.Name)
			}
			return true
		})
	}
}
