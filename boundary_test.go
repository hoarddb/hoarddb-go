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
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The server tree is BSL 1.1; this module is Apache-2.0 (the drivers are
// Apache-2.0; the server is not). These tests make the boundary a failing
// build instead of a review comment.

// TestNoServerModuleInDependencyGraph fails when any package this module
// builds, tests included, comes from the server module.
func TestNoServerModuleInDependencyGraph(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to go list")
	}
	out, err := exec.Command("go", "list", "-deps", "-test",
		"-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	serverModule := regexp.MustCompile(`^hoardDB(/|$)`)
	for _, importPath := range strings.Fields(string(out)) {
		if serverModule.MatchString(importPath) {
			t.Errorf("package %q is part of the BSL server module; this module must not depend on it "+
				"(move the code here and relicense it instead)", importPath)
		}
	}
}

// TestPublicAPIExposesNoUnstableOrThirdPartyTypes fails when an exported
// signature, field or doc comment of the public package mentions the unstable
// wire package or the BSON library behind internal/codec. A caller must be able
// to upgrade the driver without either showing through.
func TestPublicAPIExposesNoUnstableOrThirdPartyTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to go doc")
	}
	out, err := exec.Command("go", "doc", "-all", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go doc: %v\n%s", err, out)
	}
	for _, forbidden := range []string{"hoarddbwire", "mongo-driver", "primitive.", "bson."} {
		if strings.Contains(string(out), forbidden) {
			t.Errorf("the public package's documented API mentions %q:\n%s", forbidden, out)
		}
	}
}

// ringPackage is the import path of the ring client, the root of the packages
// TestRingPublicAPIUsesOnlyAllowedModules reads.
const ringPackage = "github.com/hoarddb/hoarddb-go/ring"

// ringAPIModules are the modules whose types the exported API of ./ring/...
// may name, besides the standard library and the ring packages themselves.
// The document types come from the BSON codec and the codes and details of a
// refusal from the ring wire codec, and a caller already holds both. Nothing
// else is listed: the wire package of this module is unstable, so a type of it
// in a ring signature would tie every caller to it.
var ringAPIModules = []string{
	"github.com/hoarddb/hoarddb-bson",
	"github.com/hoarddb/hoarddb-wire",
}

// TestRingPublicAPIUsesOnlyAllowedModules fails when an exported function,
// method, type, field, variable or constant under ./ring/... names a type of a
// package outside the standard library, the ring packages and ringAPIModules.
// It reads the source, so it runs without the go command and under -short.
func TestRingPublicAPIUsesOnlyAllowedModules(t *testing.T) {
	files := token.NewFileSet()
	declarations := 0
	err := filepath.WalkDir("ring", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(files, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		imports, err := importsByName(file)
		if err != nil {
			return err
		}
		for _, exposed := range exportedAPI(file) {
			declarations++
			ast.Inspect(exposed.expr, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				qualifier, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				importPath, imported := imports[qualifier.Name]
				if imported && !ringAPIAllows(importPath) {
					t.Errorf("%s: exported %s names %s.%s of package %q, which is outside the standard library, "+
						"the ring packages and the allowed modules %v; give the ring package its own type instead",
						files.Position(selector.Pos()), exposed.name, qualifier.Name, selector.Sel.Name, importPath, ringAPIModules)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if declarations == 0 {
		t.Fatal("found no exported declaration under ./ring; the test read nothing")
	}
}

// ringAPIAllows reports whether a type of the package at importPath may appear
// in the exported API of the ring packages.
func ringAPIAllows(importPath string) bool {
	firstElement, _, _ := strings.Cut(importPath, "/")
	if !strings.Contains(firstElement, ".") {
		return true // the standard library has no dot in its first element
	}
	for _, allowed := range append([]string{ringPackage}, ringAPIModules...) {
		if importPath == allowed || strings.HasPrefix(importPath, allowed+"/") {
			return true
		}
	}
	return false
}

// importsByName maps the name each import has in file to its path. An import
// with no name of its own is known by the last element of its path, which is
// the package name for every package the ring client imports.
func importsByName(file *ast.File) (map[string]string, error) {
	imports := map[string]string{}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, err
		}
		name := importPath[strings.LastIndex(importPath, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = importPath
	}
	return imports, nil
}

// exposedExpr is one expression of the exported API and the declaration it
// belongs to.
type exposedExpr struct {
	name string
	expr ast.Expr
}

// exportedAPI lists the type expressions a caller of file's package can see:
// the signature of every exported function and of every exported method of an
// exported type, the definition of every exported type less its unexported
// struct fields, and the type of every exported variable and constant (its
// value when it declares no type).
func exportedAPI(file *ast.File) []exposedExpr {
	var exposed []exposedExpr
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.FuncDecl:
			name := typed.Name.Name
			if typed.Recv != nil && len(typed.Recv.List) == 1 {
				receiver := receiverName(typed.Recv.List[0].Type)
				if !ast.IsExported(receiver) {
					continue
				}
				name = receiver + "." + name
			}
			if typed.Name.IsExported() {
				exposed = append(exposed, exposedExpr{name, typed.Type})
			}
		case *ast.GenDecl:
			for _, spec := range typed.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					if spec.Name.IsExported() {
						for _, expr := range visibleTypeExprs(spec.Type) {
							exposed = append(exposed, exposedExpr{spec.Name.Name, expr})
						}
					}
				case *ast.ValueSpec:
					for index, name := range spec.Names {
						switch {
						case !name.IsExported():
						case spec.Type != nil:
							exposed = append(exposed, exposedExpr{name.Name, spec.Type})
						case index < len(spec.Values):
							exposed = append(exposed, exposedExpr{name.Name, spec.Values[index]})
						}
					}
				}
			}
		}
	}
	return exposed
}

// receiverName is the type name of a method receiver, without its pointer and
// its type parameters.
func receiverName(receiver ast.Expr) string {
	for {
		switch typed := receiver.(type) {
		case *ast.StarExpr:
			receiver = typed.X
		case *ast.IndexExpr:
			receiver = typed.X
		case *ast.IndexListExpr:
			receiver = typed.X
		case *ast.Ident:
			return typed.Name
		default:
			return ""
		}
	}
}

// visibleTypeExprs is the definition of an exported type as a caller sees it:
// a struct shows its exported and embedded fields only, any other type the
// whole expression.
func visibleTypeExprs(definition ast.Expr) []ast.Expr {
	structure, ok := definition.(*ast.StructType)
	if !ok {
		return []ast.Expr{definition}
	}
	var visible []ast.Expr
	for _, field := range structure.Fields.List {
		shown := len(field.Names) == 0
		for _, name := range field.Names {
			shown = shown || name.IsExported()
		}
		if shown {
			visible = append(visible, visibleTypeExprs(field.Type)...)
		}
	}
	return visible
}

// TestBuildsOnEveryPlatform cross-compiles the module. A file lock that only
// some unixes have once broke solaris and aix; this catches the next one.
func TestBuildsOnEveryPlatform(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-compiles six targets")
	}
	targets := []struct{ goos, goarch string }{
		{"linux", "amd64"}, {"darwin", "arm64"}, {"windows", "amd64"},
		{"freebsd", "amd64"}, {"solaris", "amd64"}, {"aix", "ppc64"},
	}
	for _, target := range targets {
		t.Run(target.goos+"_"+target.goarch, func(t *testing.T) {
			build := exec.Command("go", "build", "./...")
			build.Env = append(os.Environ(), "GOOS="+target.goos, "GOARCH="+target.goarch, "CGO_ENABLED=0")
			if output, err := build.CombinedOutput(); err != nil {
				t.Fatalf("GOOS=%s GOARCH=%s go build ./...: %v\n%s", target.goos, target.goarch, err, output)
			}
		})
	}
}

// TestEveryFileIsApacheLicensed fails on a Go file without the Apache-2.0
// header, and on any file in the module that mentions the server's licence.
func TestEveryFileIsApacheLicensed(t *testing.T) {
	serverLicence := []string{"B" + "SL", "Business " + "Source License"}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// .cache holds the fetched test server, a binary whose machine code can
			// contain any three bytes; git ignores it and it is not the module.
			if path == ".cache" || path == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(contents)
		if strings.HasSuffix(path, ".go") && !strings.Contains(firstLines(text, 12), "Apache License, Version 2.0") {
			t.Errorf("%s: missing the Apache License 2.0 header in its first lines", path)
		}
		// The module's go.sum lists hashes only; every other file is prose or code.
		for _, marker := range serverLicence {
			if path != "boundary_test.go" && strings.Contains(text, marker) {
				t.Errorf("%s: mentions %q; nothing under clients/ may carry the server's licence", path, marker)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func firstLines(text string, count int) string {
	lines := strings.SplitN(text, "\n", count+1)
	if len(lines) > count {
		lines = lines[:count]
	}
	return strings.Join(lines, "\n")
}
