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
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Component tests of the retry policy (handbook/errors.md section 6) against
// the in-process fake server, for the answers a real server cannot be made to
// give on demand: a connection lost at a chosen frame, an unknown write
// outcome, a late answer. The real-server proofs are in retry_integration_test.go
// and the conformance legs 5, 6 and 7.

// frameLog counts the frames a fake server received, by opcode.
type frameLog struct {
	mu     sync.Mutex
	counts map[hoarddbwire.Opcode]int
}

func (log *frameLog) add(op hoarddbwire.Opcode) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.counts == nil {
		log.counts = map[hoarddbwire.Opcode]int{}
	}
	log.counts[op]++
	return log.counts[op]
}

func (log *frameLog) count(op hoarddbwire.Opcode) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.counts[op]
}

func adaDocument(t *testing.T) hoarddbwire.Document {
	t.Helper()
	encoded, err := marshal(map[string]any{"_id_": "ada", "Name": "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// Failure 4 (handbook/build-go.md section 2), on the fake server: a write whose
// connection is lost, or whose answer says the outcome is unknown, is sent
// exactly once and reported as OutcomeUnknown with the do-not-resend sentence.
func TestWriteIsNeverRetriedOnUnknown(t *testing.T) {
	cases := []struct {
		name   string
		answer func(c *fakeConn, request hoarddbwire.Frame) bool
		write  func(ctx context.Context, bucket *hoarddb.Bucket) error
		writes hoarddbwire.Opcode
		// class is the error class; driverSentence is whether the message is the
		// driver's (which carries the do-not-resend advice) or the server's.
		class          *hoarddb.Error
		driverSentence bool
		// advice replaces the do-not-resend sentence for a verb with its own, a pop.
		advice string
	}{
		{
			name: "put answered unavailable with an unknown outcome",
			answer: func(c *fakeConn, request hoarddbwire.Frame) bool {
				c.errorWith(request, map[string]any{"code": int32(6), "write_outcome": "unknown",
					"message": "the bucket's owner changed while the write was in flight: read it back before resending"})
				return true
			},
			write: func(ctx context.Context, bucket *hoarddb.Bucket) error {
				_, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"})
				return err
			},
			writes: hoarddbwire.OpPut, class: hoarddb.ErrUnavailable,
		},
		{
			name: "push whose connection is lost before the answer",
			answer: func(*fakeConn, hoarddbwire.Frame) bool {
				return false // close without answering
			},
			write: func(ctx context.Context, bucket *hoarddb.Bucket) error {
				_, err := bucket.Push(ctx, hoarddb.Map{"Item": 1})
				return err
			},
			writes: hoarddbwire.OpFIFOPush, class: hoarddb.ErrUnavailable, driverSentence: true,
		},
		{
			name: "pop whose connection is lost before the answer",
			answer: func(*fakeConn, hoarddbwire.Frame) bool {
				return false
			},
			write: func(ctx context.Context, bucket *hoarddb.Bucket) error {
				_, err := bucket.Pop(ctx)
				return err
			},
			writes: hoarddbwire.OpFIFOPop, class: hoarddb.ErrUnavailable, driverSentence: true,
			advice: "outcome unknown: the pop may have removed up to 1 entries; run peek to see what is left",
		},
		{
			name: "update unanswered past the caller's deadline",
			answer: func(*fakeConn, hoarddbwire.Frame) bool {
				return true // never answer
			},
			write: func(ctx context.Context, bucket *hoarddb.Bucket) error {
				short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()
				_, err := bucket.Update(short, "ada", hoarddb.Update{Inc: map[string]int64{"Visits": 1}})
				if !errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("want context.DeadlineExceeded through the error, got %w", err)
				}
				return err
			},
			writes: hoarddbwire.OpUpdate, driverSentence: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var log frameLog
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				log.add(request.Op)
				return tc.answer(c, request)
			}))
			client := mustConnect(t, server.uri())
			err := tc.write(testContext(t), client.Database("app").Bucket("q"))

			var failure *hoarddb.Error
			if !errors.As(err, &failure) {
				t.Fatalf("err = %v (%T), want *hoarddb.Error", err, err)
			}
			if tc.class != nil && !errors.Is(err, tc.class) {
				t.Errorf("err = %v, want class %v", err, tc.class)
			}
			if failure.Outcome != hoarddb.OutcomeUnknown {
				t.Errorf("Outcome = %v, want unknown: %v", failure.Outcome, err)
			}
			advice := cmp.Or(tc.advice, "never resend a push or an increment")
			if tc.driverSentence && !strings.Contains(failure.Message, advice) {
				t.Errorf("the message must say %q: %q", advice, failure.Message)
			}
			// Give a wrongly scheduled resend time to arrive before counting.
			time.Sleep(300 * time.Millisecond)
			if sent := log.count(tc.writes); sent != 1 {
				t.Fatalf("%s was sent %d times, want exactly 1: a write with an unknown outcome is never resent", tc.writes, sent)
			}
		})
	}
}

// TIMEOUT means the write is stored on the owner: never resend it, and say how
// many copies hold it.
func TestTimeoutIsAppliedAndNeverRetried(t *testing.T) {
	var log frameLog
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		log.add(request.Op)
		c.errorWith(request, map[string]any{"code": int32(7), "write_outcome": "applied", "copies_confirmed": int32(1),
			"replication_factor": int32(3), "message": "write concern all not met in time: 1 of 3 copies hold the write; do not resend a push or an increment"})
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("q").Push(testContext(t), hoarddb.Map{"Item": 1})
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrTimeout) || !errors.As(err, &failure) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
	if failure.Outcome != hoarddb.OutcomeApplied || failure.CopiesConfirmed != 1 || failure.ReplicationFactor != 3 {
		t.Errorf("error = %+v; want applied, 1 of 3 copies", failure)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Error("the server's TIMEOUT is not the caller's deadline")
	}
	time.Sleep(300 * time.Millisecond)
	if sent := log.count(hoarddbwire.OpFIFOPush); sent != 1 {
		t.Fatalf("FIFO_PUSH sent %d times, want 1", sent)
	}
}

// A read whose connection fails before the answer is sent once more on a fresh
// connection, and only once.
func TestReadIsRetriedOnceAfterAConnectionLoss(t *testing.T) {
	t.Run("the resend succeeds", func(t *testing.T) {
		var log frameLog
		server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
			if log.add(request.Op) == 1 {
				return false // the first GET loses its connection
			}
			c.send(request, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: adaDocument(t)})
			return true
		}))
		client := mustConnect(t, server.uri())
		document, err := client.Database("app").Bucket("users").Get(testContext(t), "ada")
		if err != nil {
			t.Fatalf("Get: %v; a read is resent once on a fresh connection", err)
		}
		if name, _ := document.Lookup("Name"); name.String() != `"Ada"` {
			t.Errorf("document = %s", document)
		}
		if sent := log.count(hoarddbwire.OpGet); sent != 2 {
			t.Errorf("GET sent %d times, want 2", sent)
		}
		if connections := server.connections.Load(); connections != 2 {
			t.Errorf("%d connections, want 2: the resend goes on a fresh one", connections)
		}
	})
	t.Run("the resend fails too", func(t *testing.T) {
		var log frameLog
		server := startFake(t, serveFrames(func(_ *fakeConn, request hoarddbwire.Frame, _ int) bool {
			log.add(request.Op)
			return false // every GET loses its connection
		}))
		client := mustConnect(t, server.uri())
		_, err := client.Database("app").Bucket("users").Get(testContext(t), "ada")
		var failure *hoarddb.Error
		if !errors.Is(err, hoarddb.ErrUnavailable) || !errors.As(err, &failure) {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}
		if failure.Outcome != hoarddb.OutcomeNotReported {
			t.Errorf("a read reports no write outcome, got %v", failure.Outcome)
		}
		if !strings.Contains(failure.Message, "sent once more on a fresh connection") {
			t.Errorf("the message must say the read was resent once: %q", failure.Message)
		}
		time.Sleep(300 * time.Millisecond)
		if sent := log.count(hoarddbwire.OpGet); sent != 2 {
			t.Fatalf("GET sent %d times, want exactly 2: once, then once more", sent)
		}
	})
}

// UNAVAILABLE on a read, and UNAVAILABLE with not_applied on a write, are
// retried with backoff until the server answers.
func TestUnavailableIsRetriedUntilItClears(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome string
		call    func(ctx context.Context, bucket *hoarddb.Bucket) error
		op      hoarddbwire.Opcode
		answer  func(c *fakeConn, request hoarddbwire.Frame)
	}{
		{
			name: "a read", op: hoarddbwire.OpGet,
			call: func(ctx context.Context, bucket *hoarddb.Bucket) error {
				_, err := bucket.Get(ctx, "ada")
				return err
			},
			answer: func(c *fakeConn, request hoarddbwire.Frame) {
				c.send(request, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: adaDocument(t)})
			},
		},
		{
			name: "a write the server did not apply", outcome: "not_applied", op: hoarddbwire.OpPut,
			call: func(ctx context.Context, bucket *hoarddb.Bucket) error {
				_, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"})
				return err
			},
			answer: func(c *fakeConn, request hoarddbwire.Frame) {
				c.send(request, hoarddbwire.OpPut, hoarddbwire.InsertResponse{ID: "ada", Success: true})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var log frameLog
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				if log.add(request.Op) <= 2 {
					members := map[string]any{"code": int32(6), "message": "bucket \"users\" in database \"app\" is copying it from the bucket's other holders now. Retry in a moment"}
					if tc.outcome != "" {
						members["write_outcome"] = tc.outcome
					}
					c.errorWith(request, members)
					return true
				}
				tc.answer(c, request)
				return true
			}))
			client := mustConnect(t, server.uri())
			if err := tc.call(testContext(t), client.Database("app").Bucket("users")); err != nil {
				t.Fatalf("err = %v; UNAVAILABLE that clears is retried with backoff", err)
			}
			if sent := log.count(tc.op); sent != 3 {
				t.Fatalf("%s sent %d times, want 3", tc.op, sent)
			}
			if retries := client.PoolStats().Retries; retries != 2 {
				t.Fatalf("PoolStats().Retries = %d, want 2: a retry shows in the aggregate statistics", retries)
			}
		})
	}
}

// Failure 5: a call cancelled while its answer is outstanding closes the
// connection it used, so the late answer can never be read as the next call's.
func TestCancelledCallDoesNotPoisonThePool(t *testing.T) {
	var mu sync.Mutex
	asks := map[string]int{}
	// owed holds, per connection, the answer to "slow" that the server sends
	// late: just before it answers the next request on that connection, as a
	// server working through its requests in order would.
	owed := map[*fakeConn]hoarddbwire.Frame{}
	answer := func(c *fakeConn, request hoarddbwire.Frame, key string) {
		encoded, err := marshal(map[string]any{"_id_": key})
		if err != nil {
			t.Error(err)
			return
		}
		c.send(request, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: encoded})
	}
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var asked hoarddbwire.KeyRequest
		if err := unmarshal(request.Payload, &asked); err != nil {
			t.Errorf("GET payload: %v", err)
			return false
		}
		mu.Lock()
		asks[asked.Key]++
		late, isOwed := owed[c]
		delete(owed, c)
		if asked.Key == "slow" {
			owed[c] = request
		}
		mu.Unlock()
		if isOwed {
			answer(c, late, "slow")
		}
		if asked.Key != "slow" {
			answer(c, request, asked.Key)
		}
		return true
	}))
	client := mustConnect(t, server.uri(), hoarddb.WithPoolSize(1))
	bucket := client.Database("app").Bucket("users")

	cancelled, cancel := context.WithCancel(testContext(t))
	done := make(chan error, 1)
	go func() {
		_, err := bucket.Get(cancelled, "slow")
		done <- err
	}()
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return asks["slow"] == 1
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Get = %v, want context.Canceled", err)
	}

	for _, key := range []string{"next", "after"} {
		document, err := bucket.Get(testContext(t), key)
		if err != nil {
			t.Fatalf("Get(%q) after a cancelled call: %v", key, err)
		}
		if got, _ := document.Lookup("_id_"); got.String() != fmt.Sprintf("%q", key) {
			t.Fatalf("Get(%q) returned the document %s: a late answer landed on the next call", key, document)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	// Had the cancelled call's connection been reused, the late answer would
	// have arrived ahead of the next call's: at best it tears the connection
	// down under that call, which then needs a resend.
	if asks["next"] != 1 || asks["after"] != 1 {
		t.Fatalf("GETs per key %v; want each later call sent once, on a connection with nothing late on it", asks)
	}
	if stats := client.PoolStats(); stats.Discarded < 1 || stats.Dials != 2 {
		t.Errorf("PoolStats = %+v; the cancelled call's connection must be discarded and one new one dialled", stats)
	}
}

// NOT_AUTHENTICATED: log that connection in again, once, and resend the request
// once if it is a read; a write is not resent.
func TestNotAuthenticatedReauthenticatesOnce(t *testing.T) {
	script := func(log *frameLog, refusals int) func(*fakeConn) {
		return func(c *fakeConn) {
			first, _, ok := c.authRequest()
			if !ok {
				return
			}
			c.authOK(first, "token-1", time.Hour)
			refused := 0
			for {
				frame, err := c.read()
				if err != nil {
					return
				}
				log.add(frame.Op)
				switch {
				case frame.Op == hoarddbwire.OpAuth:
					c.authOK(frame, "token-2", time.Hour)
				case refused < refusals:
					refused++
					c.errorFrame(frame, 12, "connection not authenticated; send AUTH first")
				case frame.Op == hoarddbwire.OpGet:
					c.send(frame, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: adaDocument(t)})
				default:
					c.send(frame, frame.Op, hoarddbwire.InsertResponse{ID: "ada", Success: true})
				}
			}
		}
	}
	t.Run("a read is resent on the same connection", func(t *testing.T) {
		var log frameLog
		server := startFake(t, script(&log, 1))
		client := mustConnect(t, server.uri())
		if _, err := client.Database("app").Bucket("users").Get(testContext(t), "ada"); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if log.count(hoarddbwire.OpAuth) != 1 || log.count(hoarddbwire.OpGet) != 2 || server.connections.Load() != 1 {
			t.Fatalf("AUTH %d, GET %d, connections %d; want 1 AUTH and 2 GETs on the one connection",
				log.count(hoarddbwire.OpAuth), log.count(hoarddbwire.OpGet), server.connections.Load())
		}
		if retries := client.PoolStats().Retries; retries != 1 {
			t.Fatalf("PoolStats().Retries = %d, want 1", retries)
		}
	})
	t.Run("a write is not resent", func(t *testing.T) {
		var log frameLog
		server := startFake(t, script(&log, 1))
		client := mustConnect(t, server.uri())
		_, err := client.Database("app").Bucket("users").Put(testContext(t), "ada", hoarddb.Map{"Name": "Ada"})
		if !errors.Is(err, hoarddb.ErrNotAuthenticated) || !strings.Contains(err.Error(), "send AUTH first") {
			t.Fatalf("err = %v, want ErrNotAuthenticated with the server's sentence", err)
		}
		if log.count(hoarddbwire.OpAuth) != 1 || log.count(hoarddbwire.OpPut) != 1 {
			t.Fatalf("AUTH %d, PUT %d; want the connection logged in again and the write not resent",
				log.count(hoarddbwire.OpAuth), log.count(hoarddbwire.OpPut))
		}
		// The connection was logged in again, so the next call uses it.
		if _, err := client.Database("app").Bucket("users").Get(testContext(t), "ada"); err != nil || server.connections.Load() != 1 {
			t.Fatalf("Get after re-authentication = %v on %d connections; want success on the same connection", err, server.connections.Load())
		}
	})
	t.Run("only once per call", func(t *testing.T) {
		var log frameLog
		server := startFake(t, script(&log, 2))
		client := mustConnect(t, server.uri())
		_, err := client.Database("app").Bucket("users").Get(testContext(t), "ada")
		if !errors.Is(err, hoarddb.ErrNotAuthenticated) {
			t.Fatalf("err = %v, want ErrNotAuthenticated after one re-authentication", err)
		}
		if log.count(hoarddbwire.OpAuth) != 1 || log.count(hoarddbwire.OpGet) != 2 {
			t.Fatalf("AUTH %d, GET %d; want exactly 1 and 2", log.count(hoarddbwire.OpAuth), log.count(hoarddbwire.OpGet))
		}
	})
}

// OVERLOADED is a refusal of one dial, before anything was sent: back off that
// dial and try again; the call succeeds once the server has room.
func TestOverloadedDialIsRetriedWithBackoff(t *testing.T) {
	var log frameLog
	server := startFake(t, func(c *fakeConn) {
		if c.index == 2 || c.index == 3 {
			c.errorFrame(hoarddbwire.Frame{}, 13, "too many connections from 127.0.0.1 (limit 1)")
			return
		}
		serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
			log.add(request.Op)
			c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Success: true, IDs: []string{"ada"}, Count: 1})
			return true
		})(c)
	})
	client := mustConnect(t, server.uri())
	// The bulk lane dials a second connection: the server refuses it twice for
	// load, then lets the third dial through.
	receipt, err := client.Database("app").Bucket("users").InsertMany(testContext(t), []any{hoarddb.Map{"Name": "Ada"}})
	if err != nil || receipt.Count != 1 {
		t.Fatalf("InsertMany = %+v, %v; want it stored once the server has room", receipt, err)
	}
	if got := server.connections.Load(); got != 4 {
		t.Fatalf("%d dials, want 4: Connect, two refused for load, one that got through", got)
	}
	if log.count(hoarddbwire.OpBatchPut) != 1 {
		t.Fatalf("BATCH_PUT sent %d times, want 1", log.count(hoarddbwire.OpBatchPut))
	}
}

// A part of a split InsertMany that fails in flight may or may not be stored:
// the error says so, names its documents, and the part is never resent.
func TestInsertManyPartLostInFlightIsUnknownAndNotResent(t *testing.T) {
	for _, tc := range []struct {
		name string
		// closeConnection ends the third BATCH_PUT's connection without an
		// answer, instead of leaving it unanswered; deadline is the caller's.
		closeConnection bool
		deadline        time.Duration
		cause           error
	}{
		{name: "the connection is lost", closeConnection: true, deadline: 20 * time.Second},
		{name: "the caller's deadline passes", deadline: 1500 * time.Millisecond, cause: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var batches atomic.Int32
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				switch batches.Add(1) {
				case 1:
					c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Message: noneWritten})
				case 2:
					c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Success: true, IDs: []string{"a", "b"}, Count: 2})
				default:
					return !tc.closeConnection // no answer: close, or stay silent
				}
				return true
			}))
			client := mustConnect(t, server.uri())
			ctx, cancel := context.WithTimeout(context.Background(), tc.deadline)
			defer cancel()

			receipt, err := client.Database("app").Bucket("users").InsertMany(ctx, mapsOf(5))
			var failure *hoarddb.Error
			if !errors.As(err, &failure) {
				t.Fatalf("InsertMany = %+v, %v; want an *hoarddb.Error", receipt, err)
			}
			if failure.Outcome != hoarddb.OutcomeUnknown {
				t.Errorf("Outcome = %v, want unknown: %v", failure.Outcome, err)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Errorf("err = %v, want it to wrap %v", err, tc.cause)
			}
			if fmt.Sprint(receipt.IDs) != "[a b]" || receipt.Count != 2 {
				t.Errorf("receipt = %+v; want only the part known to be stored", receipt)
			}
			for _, want := range []string{"2 of 5 documents were stored", "Documents 3 to 5 of 5 were in the part in flight", "unknown", "has not resent"} {
				if !strings.Contains(failure.Message, want) {
					t.Errorf("the message must contain %q:\n%s", want, failure.Message)
				}
			}
			if strings.Contains(failure.Message, "before this refusal") {
				t.Errorf("a part lost in flight was not refused: %s", failure.Message)
			}
			time.Sleep(300 * time.Millisecond)
			if sent := batches.Load(); sent != 3 {
				t.Fatalf("%d BATCH_PUT requests, want 3: the part in flight is never resent", sent)
			}
		})
	}
}

// OVERLOADED is a connection-level refusal (handbook/wire.md section 8): one
// ERROR frame with an all-zero request ID, then a close, and the server serves
// nothing on that connection. A write that meets it was not applied, and the
// table says to back off and go again (handbook/errors.md section 6): a write
// is resent until the server has room. The same code under a non-zero request
// ID answers a specific request and proves nothing: see
// TestOverloadedWithARequestIDIsNeverResentForAWrite.
func TestOverloadedAfterARequestWasSentIsBackedOffAndResent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer func(c *fakeConn, request hoarddbwire.Frame) bool
	}{
		{"refused with a zero request ID, then closed", func(c *fakeConn, _ hoarddbwire.Frame) bool {
			c.errorFrame(hoarddbwire.Frame{}, 13, "too many connections from 127.0.0.1 (limit 1)")
			return false
		}},
	} {
		for _, call := range []struct {
			name string
			op   hoarddbwire.Opcode
			do   func(ctx context.Context, client *hoarddb.Client) error
			ok   func(c *fakeConn, request hoarddbwire.Frame)
		}{
			{"a data write", hoarddbwire.OpFIFOPush, func(ctx context.Context, client *hoarddb.Client) error {
				_, err := client.Database("app").Bucket("q").Push(ctx, hoarddb.Map{"Item": 1})
				return err
			}, func(c *fakeConn, request hoarddbwire.Frame) {
				c.send(request, hoarddbwire.OpFIFOPush, hoarddbwire.InsertResponse{ID: "x", Success: true})
			}},
			{"a DDL request", hoarddbwire.OpCreateDB, func(ctx context.Context, client *hoarddb.Client) error {
				return client.CreateDatabase(ctx, "app")
			}, func(c *fakeConn, request hoarddbwire.Frame) {
				c.send(request, hoarddbwire.OpCreateDB, hoarddbwire.AckResponse{Success: true})
			}},
		} {
			t.Run(tc.name+"/"+call.name+" goes again once the server has room", func(t *testing.T) {
				var log frameLog
				server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
					if log.add(request.Op) <= 2 {
						return tc.answer(c, request)
					}
					call.ok(c, request)
					return true
				}))
				client := mustConnect(t, server.uri())
				if err := call.do(testContext(t), client); err != nil {
					t.Fatalf("%s = %v; want it stored once the server has room", call.name, err)
				}
				if sent := log.count(call.op); sent != 3 {
					t.Fatalf("%s sent %d times, want 3: refused twice, then through", call.op, sent)
				}
				if retries := client.PoolStats().Retries; retries < 2 {
					t.Errorf("PoolStats().Retries = %d, want at least 2", retries)
				}
			})
		}
	}

	// When the server never has room before the caller's deadline, the error is
	// the server's OVERLOADED: a data write that was sent is then unknown, with
	// the advice, and a DDL request is not reported (it has no write outcome).
	const sentence = "too many connections from 127.0.0.1 (limit 1)"
	for _, tc := range []struct {
		name   string
		do     func(ctx context.Context, client *hoarddb.Client) error
		want   hoarddb.WriteOutcome
		advice bool
	}{
		{"a data write", func(ctx context.Context, client *hoarddb.Client) error {
			_, err := client.Database("app").Bucket("q").Push(ctx, hoarddb.Map{"Item": 1})
			return err
		}, hoarddb.OutcomeUnknown, true},
		{"a DDL request", func(ctx context.Context, client *hoarddb.Client) error {
			return client.CreateDatabase(ctx, "app")
		}, hoarddb.OutcomeNotReported, false},
	} {
		t.Run(tc.name+" never given room", func(t *testing.T) {
			server := startFake(t, serveFrames(func(c *fakeConn, _ hoarddbwire.Frame, _ int) bool {
				c.errorFrame(hoarddbwire.Frame{}, 13, sentence)
				return false
			}))
			client := mustConnect(t, server.uri())
			ctx, cancel := context.WithTimeout(testContext(t), 1500*time.Millisecond)
			defer cancel()
			var failure *hoarddb.Error
			err := tc.do(ctx, client)
			if !errors.As(err, &failure) || !errors.Is(err, hoarddb.ErrOverloaded) {
				t.Fatalf("err = %v; want ErrOverloaded", err)
			}
			if failure.Outcome != tc.want || strings.Contains(failure.Message, "Whether the") != tc.advice || !strings.HasPrefix(failure.Message, sentence) {
				t.Errorf("error = %+v; want outcome %v, the server's sentence first and advice %t", failure, tc.want, tc.advice)
			}
			if client.PoolStats().Retries == 0 {
				t.Error("no retry was made; OVERLOADED is backed off until the deadline")
			}
		})
	}
	t.Run("a read answered OVERLOADED is backed off and resent", func(t *testing.T) {
		var log frameLog
		server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
			if log.add(request.Op) <= 2 {
				c.errorFrame(request, 13, "too many connections from 127.0.0.1 (limit 1)")
				return true
			}
			c.send(request, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: adaDocument(t)})
			return true
		}))
		client := mustConnect(t, server.uri())
		if _, err := client.Database("app").Bucket("users").Get(testContext(t), "ada"); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if sent := log.count(hoarddbwire.OpGet); sent != 3 {
			t.Fatalf("GET sent %d times, want 3", sent)
		}
	})
}

// A DDL request whose connection is lost may or may not have been applied: the
// outcome is unknown and the driver does not resend it, but the advice is not
// a write's. A DDL request is not a push or an increment: it says to read the
// state back.
func TestDDLLostInFlightIsUnknownWithItsOwnAdvice(t *testing.T) {
	var log frameLog
	server := startFake(t, serveFrames(func(_ *fakeConn, request hoarddbwire.Frame, _ int) bool {
		log.add(request.Op)
		return false // the connection is lost before the answer
	}))
	client := mustConnect(t, server.uri())
	err := client.CreateDatabase(testContext(t), "app")
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Outcome != hoarddb.OutcomeUnknown {
		t.Fatalf("CreateDatabase = %v; want an error with OutcomeUnknown", err)
	}
	if strings.Contains(failure.Message, "push or an increment") || !strings.Contains(failure.Message, "read it back") ||
		!strings.Contains(failure.Message, "has not resent") {
		t.Errorf("message = %q; want a DDL advice: read it back, not resent, and no push or increment", failure.Message)
	}
	time.Sleep(300 * time.Millisecond)
	if sent := log.count(hoarddbwire.OpCreateDB); sent != 1 {
		t.Fatalf("CREATE_DB sent %d times, want exactly 1", sent)
	}
}

// A DDL request whose caller's deadline passes in flight is unknown with the
// same DDL advice.
func TestDDLInterruptedInFlightIsUnknown(t *testing.T) {
	server := startFake(t, serveFrames(func(_ *fakeConn, _ hoarddbwire.Frame, _ int) bool {
		time.Sleep(2 * time.Second) // no answer before the deadline
		return false
	}))
	client := mustConnect(t, server.uri())
	ctx, cancel := context.WithTimeout(testContext(t), 400*time.Millisecond)
	defer cancel()
	var failure *hoarddb.Error
	if err := client.CreateDatabase(ctx, "app"); !errors.As(err, &failure) || failure.Outcome != hoarddb.OutcomeUnknown ||
		strings.Contains(failure.Message, "push or an increment") || !strings.Contains(failure.Message, "read it back") {
		t.Fatalf("CreateDatabase = %v; want OutcomeUnknown with the DDL advice", err)
	}
}

// A zero-ID refusal that arrives after a write was sent is read as the write's
// answer, and it carries no write outcome: for a write that is unknown
// (handbook/errors.md section 4), and the error says not to resend it.
func TestZeroIDRefusalAfterAWriteIsUnknown(t *testing.T) {
	var log frameLog
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		log.add(request.Op)
		c.errorFrame(hoarddbwire.Frame{}, 6, "this node is shutting down")
		return false
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("q").Push(testContext(t), hoarddb.Map{"Item": 1})
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || !errors.Is(err, hoarddb.ErrUnavailable) {
		t.Fatalf("Push = %v; want ErrUnavailable", err)
	}
	if failure.Outcome != hoarddb.OutcomeUnknown || !strings.Contains(failure.Message, "never resend a push or an increment") {
		t.Errorf("error = %+v; want OutcomeUnknown and the do-not-resend sentence", failure)
	}
	time.Sleep(300 * time.Millisecond)
	if sent := log.count(hoarddbwire.OpFIFOPush); sent != 1 {
		t.Fatalf("FIFO_PUSH sent %d times, want exactly 1", sent)
	}
}

// An OVERLOADED answer that carries the request's own ID answers a specific
// request, which the server may have applied (the handbook describes only the
// all-zero connection refusal; no current server answers a request this way).
// A write is therefore sent exactly once and reported unknown, with the advice
// that fits it, unless the server stamped an outcome, which wins. A read is
// resent on any OVERLOADED.
func TestOverloadedWithARequestIDIsNeverResentForAWrite(t *testing.T) {
	const sentence = "too many connections from 127.0.0.1 (limit 1)"
	call := func(name string, op hoarddbwire.Opcode, do func(ctx context.Context, client *hoarddb.Client) error, advice string) struct {
		name   string
		op     hoarddbwire.Opcode
		do     func(ctx context.Context, client *hoarddb.Client) error
		advice string
	} {
		return struct {
			name   string
			op     hoarddbwire.Opcode
			do     func(ctx context.Context, client *hoarddb.Client) error
			advice string
		}{name, op, do, advice}
	}
	const dataAdvice, ddlAdvice = "never resend a push or an increment", "read it back"
	bucket := func(client *hoarddb.Client, name string) *hoarddb.Bucket { return client.Database("app").Bucket(name) }
	calls := []struct {
		name   string
		op     hoarddbwire.Opcode
		do     func(ctx context.Context, client *hoarddb.Client) error
		advice string
	}{
		call("put", hoarddbwire.OpPut, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "users").Put(ctx, "ada", hoarddb.Map{"Name": "Ada"})
			return err
		}, dataAdvice),
		call("insert many", hoarddbwire.OpBatchPut, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "users").InsertMany(ctx, []any{hoarddb.Map{"Name": "Ada"}})
			return err
		}, dataAdvice),
		call("push", hoarddbwire.OpFIFOPush, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "q").Push(ctx, hoarddb.Map{"Item": 1})
			return err
		}, dataAdvice),
		call("pop", hoarddbwire.OpFIFOPop, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "q").Pop(ctx, hoarddb.WithLimit(3))
			return err
		}, "outcome unknown: the pop may have removed up to 3 entries; run peek to see what is left"),
		call("claim", hoarddbwire.OpQueueClaim, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "q").Claim(ctx)
			return err
		}, dataAdvice),
		call("ack", hoarddbwire.OpQueueAck, func(ctx context.Context, client *hoarddb.Client) error {
			lease, _ := hoarddb.ParseLeaseID(aLeaseID)
			_, err := bucket(client, "q").Ack(ctx, lease)
			return err
		}, dataAdvice),
		call("release", hoarddbwire.OpQueueRelease, func(ctx context.Context, client *hoarddb.Client) error {
			lease, _ := hoarddb.ParseLeaseID(aLeaseID)
			_, err := bucket(client, "q").Release(ctx, lease)
			return err
		}, dataAdvice),
		call("extend", hoarddbwire.OpQueueExtend, func(ctx context.Context, client *hoarddb.Client) error {
			lease, _ := hoarddb.ParseLeaseID(aLeaseID)
			_, err := bucket(client, "q").Extend(ctx, lease, time.Minute)
			return err
		}, dataAdvice),
		call("requeue", hoarddbwire.OpDeadRequeue, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "q").Requeue(ctx, hoarddb.AllDead())
			return err
		}, dataAdvice),
		call("discard", hoarddbwire.OpDeadDiscard, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "q").Discard(ctx, hoarddb.AllDead())
			return err
		}, dataAdvice),
		call("delete", hoarddbwire.OpDelete, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "users").Delete(ctx, "ada")
			return err
		}, dataAdvice),
		call("update", hoarddbwire.OpUpdate, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "users").Update(ctx, "ada", hoarddb.Update{Set: hoarddb.Map{"Name": "Ada"}})
			return err
		}, dataAdvice),
		call("put blob", hoarddbwire.OpBlobWrite, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "files").PutBlob(ctx, "a.txt", []byte("a"))
			return err
		}, dataAdvice),
		call("store field push", hoarddbwire.OpFieldPush, func(ctx context.Context, client *hoarddb.Client) error {
			_, err := bucket(client, "User").Field("ada", "Purchases").Push(ctx, hoarddb.Map{"Item": "pen"})
			return err
		}, dataAdvice),
		call("create database", hoarddbwire.OpCreateDB, func(ctx context.Context, client *hoarddb.Client) error {
			return client.CreateDatabase(ctx, "app")
		}, ddlAdvice),
		call("drop bucket", hoarddbwire.OpDeleteBucket, func(ctx context.Context, client *hoarddb.Client) error {
			return client.Database("app").DropBucket(ctx, "users")
		}, ddlAdvice),
	}
	for _, tc := range calls {
		for _, stamp := range []struct {
			name string
			want hoarddb.WriteOutcome
			// advised: the driver's advice is added; a stamp is the server's word.
			advised bool
		}{{"", hoarddb.OutcomeUnknown, true}, {"applied", hoarddb.OutcomeApplied, false},
			{"unknown", hoarddb.OutcomeUnknown, false}} {
			t.Run(tc.name+"/stamp "+stamp.name, func(t *testing.T) {
				var log frameLog
				server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
					log.add(request.Op)
					members := map[string]any{"code": int32(13), "message": sentence}
					if stamp.name != "" {
						members["write_outcome"] = stamp.name
					}
					c.errorWith(request, members)
					return true
				}))
				client := mustConnect(t, server.uri())
				ctx, cancel := context.WithTimeout(testContext(t), 3*time.Second)
				defer cancel()
				err := tc.do(ctx, client)
				var failure *hoarddb.Error
				if !errors.As(err, &failure) || !errors.Is(err, hoarddb.ErrOverloaded) {
					t.Fatalf("%s = %v; want ErrOverloaded", tc.name, err)
				}
				if failure.Outcome != stamp.want {
					t.Errorf("Outcome = %v, want %v", failure.Outcome, stamp.want)
				}
				if got := strings.Contains(failure.Message, tc.advice); got != stamp.advised {
					t.Errorf("advice %q present = %t, want %t: %q", tc.advice, got, stamp.advised, failure.Message)
				}
				if !strings.Contains(failure.Message, sentence) {
					t.Errorf("message = %q; want the server's sentence in it", failure.Message)
				}
				time.Sleep(300 * time.Millisecond) // a wrong resend would have arrived
				if sent := log.count(tc.op); sent != 1 {
					t.Fatalf("%s sent %d times, want exactly 1", tc.op, sent)
				}
			})
		}
	}
	// The server's own not_applied stamp proves the request was refused before
	// anything was applied, so it is the one stamp that may be resent.
	t.Run("a not_applied stamp is the server's word and is resent", func(t *testing.T) {
		var log frameLog
		server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
			if log.add(request.Op) <= 2 {
				c.errorWith(request, map[string]any{"code": int32(13), "message": sentence, "write_outcome": "not_applied"})
				return true
			}
			c.send(request, hoarddbwire.OpPut, hoarddbwire.InsertResponse{ID: "ada", Success: true})
			return true
		}))
		client := mustConnect(t, server.uri())
		if _, err := bucket(client, "users").Put(testContext(t), "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if sent := log.count(hoarddbwire.OpPut); sent != 3 {
			t.Fatalf("PUT sent %d times, want 3", sent)
		}
	})
	t.Run("a read answered with a request ID is still resent", func(t *testing.T) {
		var log frameLog
		server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
			if log.add(request.Op) <= 2 {
				c.errorFrame(request, 13, sentence)
				return true
			}
			c.send(request, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: adaDocument(t)})
			return true
		}))
		client := mustConnect(t, server.uri())
		if _, err := bucket(client, "users").Get(testContext(t), "ada"); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if sent := log.count(hoarddbwire.OpGet); sent != 3 {
			t.Fatalf("GET sent %d times, want 3", sent)
		}
	})
}

// A write that is never given room (a zero-ID OVERLOADED, then a close, every
// time) is retried until the deadline in a bounded number of dials, not in a
// loop: the retry waits 100 ms doubling to 5 s with full jitter, and a host
// that refused a dial is skipped for 500 ms or more besides. The ceiling is
// generous. The dialer's own host backoff bounds the dials whatever the retry
// wait is, so this test cannot see a retry wait of zero; that is held directly
// by TestBackoffDoublesToFiveSecondsWithFullJitter.
func TestNeverGivenRoomDialsAreBoundedByTheBackoff(t *testing.T) {
	server := startFake(t, func(c *fakeConn) {
		if c.index == 1 {
			serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				c.errorFrame(hoarddbwire.Frame{}, 13, "too many connections")
				return false
			})(c)
			return
		}
		c.errorFrame(hoarddbwire.Frame{}, 13, "too many connections")
	})
	client := mustConnect(t, server.uri())
	ctx, cancel := context.WithTimeout(testContext(t), 3*time.Second)
	defer cancel()
	_, err := client.Database("app").Bucket("q").Push(ctx, hoarddb.Map{"Item": 1})
	if !errors.Is(err, hoarddb.ErrOverloaded) {
		t.Fatalf("Push = %v; want ErrOverloaded", err)
	}
	// The mean wait of attempt n is half of min(100 ms * 2^(n-1), 5 s); the sum
	// of the first 5 means is 1.55 s and of 6 is 3.1 s, so a 3 s deadline holds
	// about 6 attempts on average. The ceiling is generous (random waits can be
	// short) and a loop with no backoff reaches hundreds.
	if dials := server.connections.Load(); dials > 40 {
		t.Fatalf("%d dials in 3 s; want a bounded number from the backoff schedule", dials)
	}
}
