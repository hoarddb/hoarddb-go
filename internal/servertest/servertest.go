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

// Package servertest runs real hoardDB servers for the driver's tests: one
// node with [Start], several with [StartCluster].
//
// It executes the server binary named by HOARDDB_SERVER_BIN, which
// scripts/fetch-server.sh downloads and checks against testing/server.json. It
// never builds a server and imports no server package: the module boundary is
// the point of this module (see boundary_test.go in the root package).
//
// Every server gets fresh directories, its own HOME, its own free loopback
// ports (metrics included) and is stopped when the test ends.
package servertest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
)

const (
	rootUser     = "admin"
	rootPassword = "integration-password-4c1f"

	startTimeout = 60 * time.Second
	stopTimeout  = 15 * time.Second
)

// Binary returns the path of the server executable named by
// HOARDDB_SERVER_BIN. Without it the test fails, naming the fetch script: a
// green run that tested nothing is the failure this guards against. Only
// -short skips, for a quick local loop.
func Binary(t testing.TB) string {
	t.Helper()
	if path := os.Getenv("HOARDDB_SERVER_BIN"); path != "" {
		return path
	}
	if testing.Short() {
		t.Skip("real-server tests are skipped with -short")
	}
	t.Fatal("HOARDDB_SERVER_BIN is not set: run scripts/fetch-server.sh and export HOARDDB_SERVER_BIN=$(scripts/fetch-server.sh)")
	return ""
}

// Options tunes a started server.
type Options struct {
	// Env is added to the server's environment after the defaults, for the
	// operator limits a test needs (handbook/testing.md section 3.1).
	Env map[string]string
	// Password is the root password; empty means the default. The replay of the
	// shared vectors sets the one their requests carry.
	Password string
}

// Server is a running server on loopback.
type Server struct {
	// Address is the driver port, host:port.
	Address string
	// RingAddress is the node's cluster address, host:port; set only by
	// StartNewcomer, the one node whose own sentences name it.
	RingAddress string
	// Fingerprint is the server's key, as it logs it at startup.
	Fingerprint string
	// User and Password are the root credentials the server was started with.
	User, Password string
	// Home is a HOME directory private to this server, for a known-servers file.
	Home string
	// LogPath is the server's log.
	LogPath string
	// DataDir is the server's data directory; its key and certificate are in
	// keys/ (handbook/trust.md section 1).
	DataDir string

	executable string
	directory  string
	env        []string
	logFile    *os.File
	command    *exec.Cmd
	exited     chan error
}

var fingerprintLine = regexp.MustCompile(`"fingerprint":"(SHA256:[0-9a-f]{64})"`)

// FreePort returns a loopback port nothing is listening on.
func FreePort(t testing.TB) int {
	t.Helper()
	ports := reservePorts(t, 1)
	return ports[0]
}

// reservePorts returns n distinct free loopback ports. The listeners stay open
// until all n are chosen, so the ports cannot repeat.
func reservePorts(t testing.TB, n int) []int {
	t.Helper()
	listeners := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	for range n {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}
	return ports
}

// reserveRingPort returns a port whose successor is also free: a node's
// internode listener is always its ring port plus one.
func reserveRingPort(t testing.TB, taken map[int]bool) int {
	t.Helper()
	for range 100 {
		port := FreePort(t)
		if port >= 65535 || taken[port] || taken[port+1] {
			continue
		}
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if err != nil {
			continue
		}
		_ = listener.Close()
		taken[port], taken[port+1] = true, true
		return port
	}
	t.Fatal("no free pair of consecutive loopback ports")
	return 0
}

// Start runs a fresh single node with a self-signed certificate minted at
// first start, and stops it when the test ends.
func Start(t testing.TB, opts Options) *Server {
	t.Helper()
	directory := t.TempDir()
	ports := reservePorts(t, 4)
	server := &Server{
		Address:  net.JoinHostPort("127.0.0.1", fmt.Sprint(ports[0])),
		User:     rootUser,
		Password: rootPassword,
	}
	server.env = []string{
		"HOARDDB_TCP_LISTEN=" + server.Address,
		fmt.Sprintf("HOARDDB_LISTEN=127.0.0.1:%d", ports[1]),
		fmt.Sprintf("HOARDDB_TCP_INTERNODE_LISTEN=127.0.0.1:%d", ports[2]),
		fmt.Sprintf("HOARDDB_METRICS_LISTEN=127.0.0.1:%d", ports[3]),
	}
	if opts.Password != "" {
		server.Password = opts.Password
	}
	server.prepare(t, directory, opts)
	server.run(t)
	return server
}

// prepare creates the server's directories and its environment from the
// per-server part already in server.env.
func (server *Server) prepare(t testing.TB, directory string, opts Options) {
	t.Helper()
	server.executable = Binary(t)
	server.directory = directory
	server.Home = filepath.Join(directory, "home")
	server.LogPath = filepath.Join(directory, "server.log")
	server.DataDir = filepath.Join(directory, "data")
	if err := os.MkdirAll(server.Home, 0o700); err != nil {
		t.Fatal(err)
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + server.Home,
		"HOARDDB_DATA_DIR=" + server.DataDir,
		"HOARDDB_ROOT_USER=" + server.User,
		"HOARDDB_ROOT_PASSWORD=" + server.Password,
	}
	env = append(env, server.env...)
	for name, value := range opts.Env {
		env = append(env, name+"="+value)
	}
	server.env = env
	t.Cleanup(func() { server.Stop() })
}

// run starts the process and waits for the log line that carries its
// fingerprint, which is "ready" and is the pin (handbook/trust.md section 1).
func (server *Server) run(t testing.TB) {
	t.Helper()
	logFile, err := os.OpenFile(server.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := logFile.Seek(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(server.executable)
	command.Dir = server.directory
	command.Env = server.env
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("starting the server: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	server.logFile, server.command, server.exited = logFile, command, exited

	deadline := time.Now().Add(startTimeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-exited:
			server.command = nil
			log, _ := os.ReadFile(server.LogPath)
			t.Fatalf("the server exited during start (%v):\n%s", err, log[offset:])
		default:
		}
		log, _ := os.ReadFile(server.LogPath)
		if match := fingerprintLine.FindSubmatch(log[offset:]); match != nil {
			// A restart keeps its data directory and so its key. The field is
			// written only on the first start: a proxy in front of a restarting
			// server reads it from another goroutine.
			switch logged := string(match[1]); {
			case server.Fingerprint == "":
				server.Fingerprint = logged
			case server.Fingerprint != logged:
				t.Fatalf("the server started with key %s, not %s", logged, server.Fingerprint)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	log, _ := os.ReadFile(server.LogPath)
	t.Fatalf("the server did not print its fingerprint within %v:\n%s", startTimeout, log[offset:])
}

// Stop sends SIGTERM, then SIGKILL after 15 seconds, and waits for the process
// to be gone. It does nothing when the server is not running.
func (server *Server) Stop() {
	if server.command == nil {
		return
	}
	_ = server.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-server.exited:
	case <-time.After(stopTimeout):
		_ = server.command.Process.Kill()
		<-server.exited
	}
	_ = server.logFile.Close()
	server.command = nil
}

// Kill stops the server with SIGKILL, as a crash or a power cut would: it gets
// no chance to answer what is in flight or to close its connections cleanly.
// It does nothing when the server is not running.
func (server *Server) Kill() {
	if server.command == nil {
		return
	}
	_ = server.command.Process.Kill()
	<-server.exited
	_ = server.logFile.Close()
	server.command = nil
}

// Restart starts the server again with the same data directory, ports and
// variables, and waits until it prints its fingerprint. A server still running
// is stopped first.
func (server *Server) Restart(t testing.TB) {
	t.Helper()
	server.Stop()
	server.run(t)
}

// DialUnauthenticated opens a connection pinned to the server's fingerprint and
// sends nothing on it.
func (server *Server) DialUnauthenticated(ctx context.Context) (*hoarddbwire.Conn, error) {
	config, err := trust.NewTLSConfig(server.Address, trust.Policy{Pin: server.Fingerprint})
	if err != nil {
		return nil, fmt.Errorf("trust policy for %s: %w", server.Address, err)
	}
	return hoarddbwire.Dial(ctx, server.Address, config, hoarddbwire.DialOptions{})
}

// Dial opens a connection pinned to the server's fingerprint and authenticates
// it as the root user.
func (server *Server) Dial(ctx context.Context) (*hoarddbwire.Conn, error) {
	conn, err := server.DialUnauthenticated(ctx)
	if err != nil {
		return nil, err
	}
	var answer hoarddbwire.AuthResponse
	reply, err := conn.Do(ctx, hoarddbwire.OpAuth, hoarddbwire.AuthRequest{
		Username: server.User, Password: server.Password, ProtocolVersion: 1,
		Capabilities: []string{}, Client: "hoarddb-go/servertest",
	}, &answer)
	if err != nil || reply.Op != hoarddbwire.OpAuthOK || !answer.Success {
		_ = conn.Close()
		return nil, fmt.Errorf("authenticating to %s: op %v, message %q, error %v", server.Address, reply.Op, answer.Message, err)
	}
	return conn, nil
}
