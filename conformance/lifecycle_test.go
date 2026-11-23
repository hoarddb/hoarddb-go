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

package conformance_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// TestLeg7Lifecycle is conformance leg 7 (handbook/testing.md section 2): what
// the driver does when the server goes away under it, or closes a connection
// it is not using.
func TestLeg7Lifecycle(t *testing.T) {
	checks := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"the server killed mid-request", checkKilledMidRequest},
		{"an idle connection the server closed is replaced, not used", checkIdleConnectionReplaced},
	}
	passed := 0
	for _, check := range checks {
		if t.Run(check.name, check.run) {
			passed++
		}
	}
	t.Logf("leg 7 lifecycle: %d checks, %d passed", len(checks), passed)
}

// The server is killed while a read is in flight: the driver reports a local
// UNAVAILABLE-class error well before the call's deadline, and the next request
// after a restart is answered.
func checkKilledMidRequest(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	proxy := servertest.StartProxy(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := hoarddb.Connect(ctx, fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s",
		server.User, server.Password, proxy.Address, proxy.Fingerprint))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	bucket, err := client.Database("lifecycle").CreateBucket(ctx, "users", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}

	killed := make(chan struct{})
	armed := true
	proxy.AfterEachRequest(func(frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpGet && armed {
			armed = false
			server.Kill()
			close(killed)
		}
	})
	const deadline = 30 * time.Second
	call, cancelCall := context.WithTimeout(ctx, deadline)
	defer cancelCall()
	started := time.Now()
	_, err = bucket.Get(call, "ada")
	elapsed := time.Since(started)
	<-killed
	if !errors.Is(err, hoarddb.ErrUnavailable) {
		t.Fatalf("Get while the server is killed = %v; want an UNAVAILABLE-class error", err)
	}
	if errors.Is(err, context.DeadlineExceeded) || elapsed > 10*time.Second {
		t.Fatalf("Get returned after %v (%v); it must not hang until its %v deadline", elapsed, err, deadline)
	}
	t.Logf("in flight during the kill, after %v: %v", elapsed.Round(time.Millisecond), err)

	server.Restart(t)
	proxy.AfterEachRequest(nil)
	document, err := bucket.Get(ctx, "ada")
	if err != nil {
		t.Fatalf("the first request after the restart: %v", err)
	}
	if name, _ := document.Lookup("Name"); name.String() != `"Ada"` {
		t.Fatalf("read back %s after the restart", document)
	}
}

// The server closes a connection that has been idle past its idle timeout. The
// pool notices before the next call and dials a fresh one: the call goes out
// once, on the new connection, and succeeds.
func checkIdleConnectionReplaced(t *testing.T) {
	server := servertest.Start(t, servertest.Options{Env: map[string]string{"HOARDDB_TCP_IDLE_TIMEOUT": "1"}})
	proxy := servertest.StartProxy(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := hoarddb.Connect(ctx, fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s",
		server.User, server.Password, proxy.Address, proxy.Fingerprint))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	bucket, err := client.Database("lifecycle").CreateBucket(ctx, "users", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	closed := proxy.AwaitClosed(t, 0, 10*time.Second)
	if closed.ClosedBy != "server" {
		t.Fatalf("the idle connection was closed by %q; want the server's idle timeout", closed.ClosedBy)
	}
	// A write, because a write is never resent: had the driver written into the
	// dead connection, the call would fail with its outcome unknown.
	if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
		t.Fatalf("the first call after the idle close: %v", err)
	}
	seen := proxy.Connections()
	if len(seen) != 2 || seen[0].Frames != 2 {
		t.Fatalf("want the write on a second connection and nothing more on the closed one: %s", servertest.Describe(seen))
	}
	if exchanges := proxy.Exchanges(); len(exchanges) != 2 || exchanges[1].Request.Op != hoarddbwire.OpPut {
		t.Fatalf("%d requests crossed the wire; want the create and the put", len(exchanges))
	}
	if stats := client.PoolStats(); stats.Discarded < 1 || stats.Dials != 2 {
		t.Fatalf("PoolStats = %+v; want the dead connection discarded and one new dial", stats)
	}
}
