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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// Real-server proofs of the retry policy (handbook/build-go.md step 12). A
// servertest.Proxy between the driver and the server counts what the driver
// sent; the answers are the server's own.

func proxiedClient(t *testing.T, server *servertest.Server, proxy *servertest.Proxy, opts ...hoarddb.Option) *hoarddb.Client {
	t.Helper()
	return mustConnect(t, fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s", server.User, server.Password, proxy.Address, proxy.Fingerprint), opts...)
}

func sentOf(proxy *servertest.Proxy, op hoarddbwire.Opcode) int {
	sent := 0
	for _, exchange := range proxy.Exchanges() {
		if exchange.Request.Op == op {
			sent++
		}
	}
	return sent
}

// Failure 4 (handbook/build-go.md section 2): the server is killed while a push
// is in flight. The driver reports the outcome unknown and does not resend the
// push; after a restart the queue holds at most the one element.
func TestKillMidWriteIsOutcomeUnknownAndNotResent(t *testing.T) {
	server := startServer(t)
	proxy := servertest.StartProxy(t, server)
	client := proxiedClient(t, server, proxy)
	ctx := testContext(t)
	queue, err := client.Database("app").CreateBucket(ctx, "jobs", hoarddb.FIFO)
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	killed := make(chan struct{})
	var pushes atomic.Int32
	proxy.AfterEachRequest(func(frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpFIFOPush && pushes.Add(1) == 1 {
			server.Kill() // the push has been forwarded; its answer never comes back
			close(killed)
		}
	})
	_, err = queue.Push(ctx, hoarddb.Map{"Job": "send the invoice"})
	<-killed
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || !errors.Is(err, hoarddb.ErrUnavailable) {
		t.Fatalf("Push during a kill = %v; want ErrUnavailable", err)
	}
	if failure.Outcome != hoarddb.OutcomeUnknown || !strings.Contains(failure.Message, "never resend a push or an increment") {
		t.Fatalf("error = %+v; want OutcomeUnknown and the do-not-resend sentence", failure)
	}
	t.Logf("Push during the kill: %v", err)

	server.Restart(t)
	proxy.AfterEachRequest(nil)
	stored, err := queue.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count after the restart: %v", err)
	}
	if stored > 1 {
		t.Fatalf("the queue holds %d elements after one push: it was resent", stored)
	}
	if sent := sentOf(proxy, hoarddbwire.OpFIFOPush); sent != 1 {
		t.Fatalf("FIFO_PUSH crossed the wire %d times, want exactly 1", sent)
	}
	t.Logf("after the restart the queue holds %d element(s); FIFO_PUSH was sent once", stored)
}

// A node that has not been added to its cluster refuses every request that
// names a bucket with UNAVAILABLE and nothing applied. The driver retries with
// backoff until the caller's deadline, then returns the server's sentence.
func TestNotAppliedUnavailableIsRetriedWithinTheDeadline(t *testing.T) {
	server := servertest.StartUnjoined(t, servertest.Options{})

	// The server's own sentence, from a raw connection, to compare verbatim.
	raw, err := server.Dial(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	reply, err := raw.Do(testContext(t), hoarddbwire.OpPut, hoarddbwire.PutRequest{Database: "app", Bucket: "users", Key: "ada", Data: adaDocument(t)}, nil)
	_ = raw.Close()
	if err != nil || reply.Op != hoarddbwire.OpError {
		t.Fatalf("the unjoined node answered %s, %v; want an ERROR", reply.Op, err)
	}
	refusal := hoarddbwire.DecodeError(reply.Payload)
	if refusal.Code != hoarddbwire.CodeUnavailable || refusal.WriteOutcome != hoarddbwire.WriteNotApplied {
		t.Fatalf("the unjoined node answered %+v; want UNAVAILABLE, not_applied", refusal)
	}
	t.Logf("the server's sentence: %s", refusal.Message)

	proxy := servertest.StartProxy(t, server)
	client := proxiedClient(t, server, proxy)
	bucket := client.Database("app").Bucket("users")
	for _, tc := range []struct {
		name    string
		op      hoarddbwire.Opcode
		call    func(ctx context.Context) error
		outcome hoarddb.WriteOutcome
	}{
		{"a write", hoarddbwire.OpPut, func(ctx context.Context) error {
			_, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"})
			return err
		}, hoarddb.OutcomeNotApplied},
		{"a read", hoarddbwire.OpGet, func(ctx context.Context) error {
			_, err := bucket.Get(ctx, "ada")
			return err
		}, hoarddb.OutcomeNotReported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const deadline = 2 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), deadline)
			defer cancel()
			before := sentOf(proxy, tc.op)
			started := time.Now()
			err := tc.call(ctx)
			elapsed := time.Since(started)

			var failure *hoarddb.Error
			if !errors.As(err, &failure) || !errors.Is(err, hoarddb.ErrUnavailable) {
				t.Fatalf("err = %v; want ErrUnavailable", err)
			}
			if failure.Message != refusal.Message {
				t.Errorf("message = %q; want the server's sentence verbatim: %q", failure.Message, refusal.Message)
			}
			if failure.Outcome != tc.outcome {
				t.Errorf("Outcome = %v, want %v", failure.Outcome, tc.outcome)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("err = %v; the caller's deadline ended the retries and must be its cause", err)
			}
			if elapsed < deadline-300*time.Millisecond || elapsed > deadline+time.Second {
				t.Errorf("returned after %v; want close to the %v deadline: retried until it, and no longer", elapsed, deadline)
			}
			attempts := sentOf(proxy, tc.op) - before
			if attempts < 3 {
				t.Errorf("%s sent %d times in %v; want it retried with backoff", tc.op, attempts, elapsed)
			}
			t.Logf("%s: %d attempts in %v, then %v", tc.op, attempts, elapsed.Round(time.Millisecond), err)
		})
	}
}

// On a cluster: node1 is killed while a request is in flight on it. A read is
// sent once more on a fresh connection, which the driver dials to the next
// host, and is answered there; a push is reported unknown and not resent, and
// the cluster holds it at most once.
func TestClusterKillResendsAReadElsewhereAndNeverAWrite(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	first := cluster.Node(0)
	proxy := servertest.StartProxy(t, first)
	hosts := []string{proxy.Address, cluster.Node(1).Address, cluster.Node(2).Address}
	client := mustConnect(t, fmt.Sprintf("hoarddb://%s:%s@%s/", first.User, first.Password, strings.Join(hosts, ",")),
		hoarddb.WithFingerprint(proxy.Fingerprint, cluster.Node(1).Fingerprint, cluster.Node(2).Fingerprint))
	ctx := testContext(t)
	users, err := client.Database("app").CreateBucket(ctx, "users", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Database("app").CreateBucket(ctx, "jobs", hoarddb.FIFO); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	// Wait until the third node holds Ada, so the resent read can find her there.
	waitUntil(t, func() bool {
		conn, err := cluster.Node(1).Dial(ctx)
		if err != nil {
			return false
		}
		defer func() { _ = conn.Close() }()
		var answer hoarddbwire.GetResponse
		reply, err := conn.Do(ctx, hoarddbwire.OpGet, hoarddbwire.KeyRequest{Database: "app", Bucket: "users", Key: "ada"}, &answer)
		return err == nil && reply.Op == hoarddbwire.OpGetResponse && answer.Found
	})

	killed := make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	proxy.AfterEachRequest(func(frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpGet && armed.CompareAndSwap(true, false) {
			first.Kill()
			close(killed)
		}
	})
	document, err := users.Get(ctx, "ada")
	<-killed
	if err != nil {
		t.Fatalf("Get with node1 killed under it: %v; want it answered by another node", err)
	}
	if name, _ := document.Lookup("Name"); name.String() != `"Ada"` {
		t.Fatalf("Get = %s", document)
	}
	if sent := sentOf(proxy, hoarddbwire.OpGet); sent != 1 {
		t.Fatalf("GET reached node1 %d times, want 1: the resend goes to a fresh connection", sent)
	}

	cluster.Restart(t, 0) // and wait until node1 is a confirmed member again
	stopped := make(chan struct{})
	proxy.AfterEachRequest(func(frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpFIFOPush && armed.CompareAndSwap(false, true) {
			first.Kill()
			close(stopped)
		}
	})
	// The pool holds connections to node2 now; drop them so the push is sent
	// through the proxy to node1, the host written first.
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	client = mustConnect(t, fmt.Sprintf("hoarddb://%s:%s@%s/", first.User, first.Password, strings.Join(hosts, ",")),
		hoarddb.WithFingerprint(proxy.Fingerprint, cluster.Node(1).Fingerprint, cluster.Node(2).Fingerprint))
	jobs := client.Database("app").Bucket("jobs")
	_, err = jobs.Push(ctx, hoarddb.Map{"Job": "send the invoice"})
	<-stopped
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Outcome != hoarddb.OutcomeUnknown {
		t.Fatalf("Push with node1 killed under it = %v; want OutcomeUnknown", err)
	}
	if sent := sentOf(proxy, hoarddbwire.OpFIFOPush); sent != 1 {
		t.Fatalf("FIFO_PUSH reached node1 %d times, want 1", sent)
	}
	held, err := jobs.Count(ctx, nil)
	if err != nil || held > 1 {
		t.Fatalf("the queue holds %d after one push (%v); want at most 1", held, err)
	}
	t.Logf("read answered by another node; push unknown and not resent, the cluster holds %d", held)
}
