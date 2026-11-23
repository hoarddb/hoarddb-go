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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// goBlocks returns the body of every fenced Go block of a Markdown file, with
// the line the block starts on, so a failure points a reader at the README.
func goBlocks(markdown string) (blocks []string, startLines []int) {
	var current []string
	inGo := false
	for number, line := range strings.Split(markdown, "\n") {
		switch {
		case !inGo && strings.HasPrefix(line, "```go"):
			inGo, current = true, nil
			startLines = append(startLines, number+2)
		case inGo && strings.HasPrefix(line, "```"):
			inGo = false
			blocks = append(blocks, strings.Join(current, "\n"))
		case inGo:
			current = append(current, line)
		}
	}
	return blocks, startLines
}

// fragmentFile wraps a README block that is not a whole program in a function
// whose parameters (ctx, client, uri) are the names the README introduces before its first fragment.
// The blank assignments keep the shared imports from being reported unused.
func fragmentFile(body string) string {
	header, _, _ := strings.Cut(readFile("doc.go"), "package hoarddb")
	return header + `package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

var (
	_ = context.Background
	_ = errors.Is
	_ = fmt.Println
	_ = log.Fatal
	_ = time.Second
	_ = hoarddb.Connect
)

func fragment(ctx context.Context, client *hoarddb.Client, uri string) {
` + body + `
}

func main() {}
`
}

func readFile(path string) string {
	contents, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(contents)
}

// compileReadmeBlocks type-checks every Go block of the Markdown file at path
// against this module and returns one message per block that does not compile.
// A block that starts with "package" is a whole program; any other is a
// fragment wrapped by fragmentFile.
func compileReadmeBlocks(t *testing.T, path string) []string {
	t.Helper()
	markdown, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	blocks, startLines := goBlocks(string(markdown))
	if len(blocks) == 0 {
		return []string{path + " has no Go block to compile"}
	}
	// A directory that starts with an underscore is skipped by ./... and so by
	// every other test and tool, yet is part of this module for go vet.
	scratch, err := os.MkdirTemp(".", "_readmecheck")
	if err != nil {
		t.Fatalf("creating the scratch directory: %v", err)
	}
	defer func() { _ = os.RemoveAll(scratch) }()

	var failures []string
	for index, block := range blocks {
		source := block + "\n"
		if !strings.HasPrefix(strings.TrimSpace(block), "package ") {
			source = fragmentFile(block)
		}
		dir := filepath.Join(scratch, fmt.Sprintf("block%d", index))
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("creating %s: %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(source), 0o644); err != nil {
			t.Fatalf("writing %s: %v", dir, err)
		}
		if output, err := exec.Command("go", "vet", "./"+dir).CombinedOutput(); err != nil {
			failures = append(failures, fmt.Sprintf("%s: the Go block at line %d does not compile:\n%s", path, startLines[index], output))
		}
	}
	return failures
}

// TestReadmeSnippetsCompile is the promise that a developer who pastes the
// README into a file gets code that builds against the driver as it is.
func TestReadmeSnippetsCompile(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to go vet")
	}
	for _, failure := range compileReadmeBlocks(t, "README.md") {
		t.Error(failure)
	}
}

// TestReadmeCheckRejectsABrokenSnippet proves the check above can fail: a
// README with one block that calls a method the driver does not have.
func TestReadmeCheckRejectsABrokenSnippet(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to go vet")
	}
	scratch := filepath.Join(t.TempDir(), "README.md")
	broken := "# scratch\n\n```go\nclient.NoSuchMethod(ctx)\n```\n"
	if err := os.WriteFile(scratch, []byte(broken), 0o644); err != nil {
		t.Fatal(err)
	}
	if failures := compileReadmeBlocks(t, scratch); len(failures) != 1 {
		t.Errorf("a snippet calling a method that does not exist should be one failure; got %d: %v", len(failures), failures)
	}
}

// TestReadmeStatesWhatADeveloperMustKnow keeps the README's load-bearing
// statements from being edited away: what a handle costs, that ListBuckets is
// an operator call, the open issues, and the examples a web developer looks for.
func TestReadmeStatesWhatADeveloperMustKnow(t *testing.T) {
	readme := readFile("README.md")
	for _, want := range []string{
		"a million",                            // the per-handle cost, in words
		"operator call",                        // ListBuckets
		"examples/gin",                         // the web-framework example
		"GetAs",                                // typed reads
		"Known issues",                         // the open driver issues
		"Support",                              // the support table
		"Not in v1",                            // what is deliberately missing
		"Apache License 2.0",                   // the licence
		"HOARDDB_URI",                          // how the examples are pointed at a server
		"OutcomeUnknown",                       // the write outcome a caller must read
		"hoarddb://",                           // the connection string
		"examples/fields",                      // store fields
		"AcceptFirstSight",                     // the trust order, local development
		"WithWriteConcern",                     // write concern
		"go get github.com/hoarddb/hoarddb-go", // the install line
	} {
		if !strings.Contains(readme, want) {
			t.Errorf("README.md should contain %q", want)
		}
	}
}
