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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// These tests run a real hoardDB server, executed by
// internal/servertest. They never import a server package.

func startServer(t *testing.T) *servertest.Server { return servertest.Start(t, servertest.Options{}) }

func freePort(t *testing.T) int { return servertest.FreePort(t) }

func uriFor(server *servertest.Server, password string) string {
	return fmt.Sprintf("hoarddb://%s:%s@%s/", server.User, password, server.Address)
}

func pinnedFor(server *servertest.Server, password string) string {
	return uriFor(server, password) + "?fingerprint=" + server.Fingerprint
}

func knownServersOf(server *servertest.Server) string {
	return filepath.Join(server.Home, "known_servers")
}

func background() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 20*time.Second)
}

func TestIntegrationConnectWithFingerprintAuthenticatesAndPings(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()

	client, err := hoarddb.Connect(ctx, pinnedFor(server, server.Password))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	info := client.ServerInfo()
	if info.Version == "" || info.ProtocolVersion != 1 {
		t.Fatalf("ServerInfo = %+v; AUTH should have reported a version and protocol 1", info)
	}
	if !info.Has("auth_token") {
		t.Fatalf("the server advertised %v; the driver relies on auth_token", info.Capabilities)
	}
	if stats := client.PoolStats(); stats.Open != 1 || stats.Dials != 1 || stats.InUse != 0 {
		t.Fatalf("PoolStats = %+v; Connect should leave exactly the one connection that proved the credentials", stats)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(ctx); !errors.Is(err, hoarddb.ErrClosed) {
		t.Fatalf("Ping after Close: %v, want ErrClosed", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestIntegrationPasswordFromTheEnvironmentAndFingerprintOption(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	t.Setenv("HOARDDB_ROOT_PASSWORD", server.Password)
	t.Setenv("HOARDDB_ROOT_USER", server.User)

	// The URI names no credentials at all and no fingerprint: both come from
	// outside it.
	client, err := hoarddb.Connect(ctx, "hoarddb://"+server.Address, hoarddb.WithFingerprint(server.Fingerprint))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationWrongFingerprintFailsBeforeAnyCredentialIsSent(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	wrong := "SHA256:" + strings.Repeat("ab", 32)

	// The password is also wrong. If AUTH ran first the error would be a
	// credential failure; a mismatch proves the pin was checked first.
	_, err := hoarddb.Connect(ctx, uriFor(server, "definitely-not-the-password")+"?fingerprint="+wrong)
	if !errors.Is(err, hoarddb.ErrFingerprintMismatch) {
		t.Fatalf("err = %v, want ErrFingerprintMismatch", err)
	}
	if errors.Is(err, hoarddb.ErrAuthFailed) {
		t.Fatal("AUTH ran against a server that failed the pin")
	}
	for _, want := range []string{wrong, server.Fingerprint, "someone may be intercepting"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message must contain %q:\n%v", want, err)
		}
	}
	if strings.Contains(err.Error(), "definitely-not-the-password") {
		t.Fatal("the password leaked into an error")
	}
}

func TestIntegrationWrongPassword(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	_, err := hoarddb.Connect(ctx, pinnedFor(server, "a-wrong-password-9d2e"))
	if !errors.Is(err, hoarddb.ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed", err)
	}
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Reason != hoarddb.AuthReasonCredentials {
		t.Fatalf("Reason = %v, want CREDENTIALS (err: %#v)", failure, err)
	}
	for _, want := range []string{`"admin"`, "does not retry", "root.password", "HOARDDB_ROOT_PASSWORD"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message should contain %q:\n%v", want, err)
		}
	}
	if strings.Contains(err.Error(), "a-wrong-password-9d2e") {
		t.Fatal("the password leaked into an error")
	}
}

func TestIntegrationServerDown(t *testing.T) {
	ctx, cancel := background()
	defer cancel()
	dead := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	started := time.Now()
	_, err := hoarddb.Connect(ctx, "hoarddb://admin:pw@"+dead+"/?fingerprint=SHA256:"+strings.Repeat("cd", 32))
	if !errors.Is(err, hoarddb.ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("a refused connection took %v to report", elapsed)
	}
	for _, want := range []string{dead, "is running", "7433"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message should contain %q:\n%v", want, err)
		}
	}
}

func TestIntegrationFailoverSkipsADeadHost(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	dead := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	// One fingerprint is the live server's, and it is pinned to both hosts: the
	// dead one never presents a key, so the pin is never tested against it.
	connectionString := fmt.Sprintf("hoarddb://%s:%s@%s,%s/?fingerprint=%s", server.User, server.Password, dead, server.Address, server.Fingerprint)
	client, err := hoarddb.Connect(ctx, connectionString)
	if err != nil {
		t.Fatalf("Connect with the first host down: %v", err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationUnknownServerIsRefusedWithTheWayToTrustIt(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	known := knownServersOf(server)
	_, err := hoarddb.Connect(ctx, uriFor(server, server.Password), hoarddb.WithKnownServers(known))
	if !errors.Is(err, hoarddb.ErrUntrustedServer) {
		t.Fatalf("err = %v, want ErrUntrustedServer", err)
	}
	for _, want := range []string{server.Fingerprint, "?fingerprint=", "AcceptFirstSight", known} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message should contain %q:\n%v", want, err)
		}
	}
	if _, statErr := os.Stat(known); statErr == nil {
		t.Fatal("a refused server was recorded in the known-servers file")
	}
}

func TestIntegrationTrustOnFirstUseThenPinnedForever(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	known := knownServersOf(server)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	first, err := hoarddb.Connect(ctx, uriFor(server, server.Password),
		hoarddb.WithKnownServers(known), hoarddb.WithTrustPrompt(hoarddb.AcceptFirstSight), hoarddb.WithLogger(logger))
	if err != nil {
		t.Fatalf("first sight: %v", err)
	}
	_ = first.Close()
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), server.Fingerprint) {
		t.Fatalf("trusting on first sight must warn, naming the fingerprint; log:\n%s", logs.String())
	}
	contents, err := os.ReadFile(known)
	if err != nil || !strings.HasPrefix(string(contents), server.Address+" ") {
		t.Fatalf("known_servers = %q, %v; want a line for %s", contents, err, server.Address)
	}
	if info, _ := os.Stat(known); info.Mode().Perm() != 0o600 {
		t.Fatalf("known_servers mode %v, want 0600", info.Mode().Perm())
	}

	// No prompt this time: the recorded key is enough.
	second, err := hoarddb.Connect(ctx, uriFor(server, server.Password), hoarddb.WithKnownServers(known))
	if err != nil {
		t.Fatalf("recorded server: %v", err)
	}
	_ = second.Close()

	// The recorded key changes: a hard failure that no prompt can override.
	impostor := server.Address + " " + strings.Repeat("11", 32) + "\n"
	if err := os.WriteFile(known, []byte(impostor), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = hoarddb.Connect(ctx, uriFor(server, server.Password),
		hoarddb.WithKnownServers(known), hoarddb.WithTrustPrompt(hoarddb.AcceptFirstSight))
	if !errors.Is(err, hoarddb.ErrServerKeyChanged) {
		t.Fatalf("err = %v, want ErrServerKeyChanged", err)
	}
	for _, want := range []string{server.Fingerprint, known, "intercepting", "will not retry"} {
		if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(want)) {
			t.Errorf("the message should contain %q:\n%v", want, err)
		}
	}
}

func TestIntegrationInsecureOptionRequiresAReasonAndWarnsEveryDial(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	if _, err := hoarddb.Connect(ctx, uriFor(server, server.Password), hoarddb.WithInsecureSkipVerify("")); !errors.Is(err, hoarddb.ErrInvalidURI) {
		t.Fatalf("an empty reason: err = %v, want a refusal", err)
	}
	var logs bytes.Buffer
	client, err := hoarddb.Connect(ctx, uriFor(server, server.Password),
		hoarddb.WithInsecureSkipVerify("integration test against a throwaway server"),
		hoarddb.WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "integration test against a throwaway server") {
		t.Fatalf("the opt-out must log its reason at WARN; log:\n%s", logs.String())
	}
}

func TestIntegrationPoolUnderContention(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	client, err := hoarddb.Connect(ctx, pinnedFor(server, server.Password), hoarddb.WithPoolSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	const workers, pingsEach = 24, 15
	var group sync.WaitGroup
	failures := make(chan error, workers)
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for range pingsEach {
				if err := client.Ping(ctx); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("Ping under contention: %v", err)
	}
	stats := client.PoolStats()
	if stats.Open > 2 || stats.Dials > 2 || stats.InUse != 0 || stats.Waiting != 0 {
		t.Fatalf("PoolStats = %+v; 360 pings on a pool of 2 must reuse the same two connections", stats)
	}
}

func TestIntegrationCancelledPingRecovers(t *testing.T) {
	server := startServer(t)
	ctx, cancel := background()
	defer cancel()
	client, err := hoarddb.Connect(ctx, pinnedFor(server, server.Password))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	cancelled, stop := context.WithCancel(ctx)
	stop()
	if err := client.Ping(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ping with a cancelled context: %v, want context.Canceled", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("the client must still work after a cancelled call: %v", err)
	}

	expired, release := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer release()
	if err := client.Ping(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ping past its deadline: %v, want context.DeadlineExceeded", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestIntegrationTrustExampleFollowsTheREADME runs examples/trust as the README
// quick-start tells a developer to, against the real server.
func TestIntegrationTrustExampleFollowsTheREADME(t *testing.T) {
	server := startServer(t)
	example := filepath.Join(t.TempDir(), "trust")
	if output, err := exec.Command("go", "build", "-o", example, "./examples/trust").CombinedOutput(); err != nil {
		t.Fatalf("building the example: %v\n%s", err, output)
	}
	run := func(extraEnv []string, args ...string) (string, error) {
		command := exec.Command(example, args...)
		command.Env = append([]string{
			"PATH=" + os.Getenv("PATH"), "HOME=" + server.Home,
			"HOARDDB_URI=hoarddb://" + server.Address,
			"HOARDDB_ROOT_USER=" + server.User, "HOARDDB_ROOT_PASSWORD=" + server.Password,
		}, extraEnv...)
		output, err := command.CombinedOutput()
		return string(output), err
	}

	// 1. With the fingerprint the server printed: the production path.
	output, err := run([]string{"HOARDDB_FINGERPRINT=" + server.Fingerprint})
	if err != nil || !strings.Contains(output, "connected: hoardDB") {
		t.Fatalf("pinned run: %v\n%s", err, output)
	}
	// 2. With nothing to vouch for the server: refused, with the way out.
	output, err = run(nil)
	if err == nil || !strings.Contains(output, "not trusted yet") || !strings.Contains(output, server.Fingerprint) {
		t.Fatalf("unpinned run should fail naming the fingerprint (err %v):\n%s", err, output)
	}
	// 3. First sight, typed on purpose, then remembered.
	output, err = run(nil, "-first-sight")
	if err != nil || !strings.Contains(output, "connected: hoardDB") || !strings.Contains(output, "level=WARN") {
		t.Fatalf("first-sight run: %v\n%s", err, output)
	}
	output, err = run(nil)
	if err != nil || !strings.Contains(output, "connected: hoardDB") {
		t.Fatalf("a remembered server should need no flag: %v\n%s", err, output)
	}
}

func TestIntegrationConnectHonoursTheCallersDeadline(t *testing.T) {
	// A listener that accepts and never speaks: the handshake cannot finish.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	go func() {
		for {
			accepted, err := listener.Accept()
			if err != nil {
				return
			}
			defer func() { _ = accepted.Close() }()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = hoarddb.Connect(ctx, "hoarddb://admin:pw@"+listener.Addr().String()+"/?fingerprint=SHA256:"+strings.Repeat("ee", 32))
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want one wrapping context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("Connect ignored the deadline: %v", elapsed)
	}
}
