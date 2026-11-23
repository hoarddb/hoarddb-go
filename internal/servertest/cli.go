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

package servertest

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const cliTimeout = 60 * time.Second

// CLIBinary returns the path of the hoarddb command-line client named by
// HOARDDB_CLI_BIN. Without it the test fails, naming the fetch script, for the
// reason [Binary] does. Only -short skips.
func CLIBinary(t testing.TB) string {
	t.Helper()
	if path := os.Getenv("HOARDDB_CLI_BIN"); path != "" {
		return path
	}
	if testing.Short() {
		t.Skip("tests that need the hoarddb CLI are skipped with -short")
	}
	t.Fatal("HOARDDB_CLI_BIN is not set: run scripts/fetch-server.sh --cli and export HOARDDB_CLI_BIN=$(scripts/fetch-server.sh --cli)")
	return ""
}

// RunCLI runs one statement through the hoarddb CLI against the server as the
// root user and returns what it printed. A statement that fails the test is
// reported with the CLI's own output.
//
// The driver cannot create a user itself: USER_CREATE carries a credential the
// client derives with Argon2id, which is outside the driver's go.mod. The CLI
// already does that, so tests that need a second user ask it to. The password
// of a created user is read from the line after the statement, as a piped CLI
// script does (--password-stdin); the CLI trusts the server's self-signed
// certificate with -insecure, which is safe on loopback to a process the test
// started.
func (server *Server) RunCLI(t testing.TB, statement string, passwordLines ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cliTimeout)
	defer cancel()
	arguments := []string{"-address", server.Address, "-insecure", "-no-pager"}
	input := statement + "\n"
	if len(passwordLines) > 0 {
		arguments = append(arguments, "--password-stdin")
		input += strings.Join(passwordLines, "\n") + "\n"
	}
	command := exec.CommandContext(ctx, CLIBinary(t), arguments...)
	command.Dir = t.TempDir()
	// A killed CLI can leave a grandchild holding the output pipe; without a
	// delay Run would wait on it past the context's deadline.
	command.WaitDelay = 5 * time.Second
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + filepath.Join(server.directory, "home"),
		"HOARDDB_ROOT_USER=" + server.User,
		"HOARDDB_ROOT_PASSWORD=" + server.Password,
	}
	command.Stdin = strings.NewReader(input)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		t.Fatalf("hoarddb CLI %q failed (%v):\n%s", statement, err, output.String())
	}
	return output.String()
}

// CreateUser creates a user that can log in, with the given password and
// roles ("reader@app"), by running the CLI.
func (server *Server) CreateUser(t testing.TB, name, password string, roles ...string) {
	t.Helper()
	statement := "create user " + name
	if len(roles) > 0 {
		statement += " roles [" + strings.Join(roles, ", ") + "]"
	}
	server.RunCLI(t, statement, password)
}

// DropUser drops a user by running the CLI. The server closes the user's live
// connections (handbook/wire.md section 8).
func (server *Server) DropUser(t testing.TB, name string) {
	t.Helper()
	server.RunCLI(t, "drop user "+name)
}
