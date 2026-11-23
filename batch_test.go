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

// Component tests of InsertMany against the in-process fake server, for what a
// real server cannot be made to do on demand (handbook/build-go.md step 11).
// The real-server proof is in batch_integration_test.go.

const noneWritten = "hash batch put failed: This batch exceeds the largest batch the store commits atomically, so none of it was written."

func mapsOf(count int) []any {
	docs := make([]any, count)
	for position := range docs {
		docs[position] = hoarddb.Map{"Seq": int32(position)}
	}
	return docs
}

// batchServer answers BATCH_PUT: a batch of more than limit documents is
// refused in-band with the server's none-written sentence, any other is stored
// and answered with ids id-0, id-1 and so on across the whole run. sizes
// records the size of every batch that arrived, in order.
func batchServer(t *testing.T, limit int, sizes *[]int, mu *sync.Mutex) *fakeServer {
	t.Helper()
	var stored int
	return startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var batch hoarddbwire.BatchPutRequest
		if err := unmarshal(request.Payload, &batch); err != nil {
			t.Errorf("the request is not a BATCH_PUT payload: %v", err)
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		*sizes = append(*sizes, len(batch.Data))
		if len(batch.Data) > limit {
			c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Message: noneWritten})
			return true
		}
		ids := make([]string, len(batch.Data))
		for position := range ids {
			ids[position] = fmt.Sprintf("id-%d", stored)
			stored++
		}
		c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Success: true, IDs: ids, Count: int64(len(ids))})
		return true
	}))
}

// D-5: a refusal that says none of it was written is safe to answer by halving,
// and the receipt reads as one batch: every id, in the order the documents were
// given.
func TestInsertManySplitsInHalvesAndKeepsTheOrder(t *testing.T) {
	var sizes []int
	var mu sync.Mutex
	server := batchServer(t, 2, &sizes, &mu)
	client := mustConnect(t, server.uri())

	receipt, err := client.Database("app").Bucket("users").InsertMany(testContext(t), mapsOf(5))
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	want := []string{"id-0", "id-1", "id-2", "id-3", "id-4"}
	if fmt.Sprint(receipt.IDs) != fmt.Sprint(want) || receipt.Count != 5 {
		t.Fatalf("receipt = %+v; want the ids %v in order and Count 5", receipt, want)
	}
	// 5 is refused; the halves are [0:2] (stored) and [2:5], refused and halved
	// again into [2:3] and [3:5]. Every document was sent in a stored batch once.
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(sizes) != fmt.Sprint([]int{5, 2, 3, 1, 2}) {
		t.Fatalf("batch sizes sent = %v, want [5 2 3 1 2]: halves, left before right", sizes)
	}
}

// The current server refuses an oversize batch with an ERROR frame and code
// BATCH_TOO_LARGE. InsertMany splits on that code, not on the message.
func TestInsertManySplitsOnTheBatchTooLargeCode(t *testing.T) {
	var sizes []int
	var mu sync.Mutex
	var stored int
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var batch hoarddbwire.BatchPutRequest
		if err := unmarshal(request.Payload, &batch); err != nil {
			t.Errorf("BATCH_PUT payload: %v", err)
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		sizes = append(sizes, len(batch.Data))
		if len(batch.Data) > 2 {
			c.errorWith(request, map[string]any{"code": int32(0x11), "message": noneWritten, "write_outcome": "not_applied"})
			return true
		}
		ids := make([]string, len(batch.Data))
		for position := range ids {
			ids[position] = fmt.Sprintf("id-%d", stored)
			stored++
		}
		c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Success: true, IDs: ids, Count: int64(len(ids))})
		return true
	}))
	client := mustConnect(t, server.uri())

	receipt, err := client.Database("app").Bucket("users").InsertMany(testContext(t), mapsOf(5))
	if err != nil || receipt.Count != 5 {
		t.Fatalf("InsertMany = %+v, %v; want 5 documents stored", receipt, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(sizes) != fmt.Sprint([]int{5, 2, 3, 1, 2}) {
		t.Fatalf("batch sizes sent = %v, want [5 2 3 1 2]", sizes)
	}
}

// A cluster owner's refusal is unknown, and an unknown batch is never split
// automatically: a member with a larger memtable may have stored it.
func TestInsertManyDoesNotSplitAnUnknownBatchTooLarge(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.errorWith(request, map[string]any{"code": int32(0x11), "message": noneWritten, "write_outcome": "unknown"})
		return true
	}))
	client := mustConnect(t, server.uri())

	receipt, err := client.Database("app").Bucket("users").InsertMany(testContext(t), mapsOf(5))
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Code != hoarddb.CodeBatchTooLarge || failure.Outcome != hoarddb.OutcomeUnknown {
		t.Fatalf("InsertMany = %+v, %v; want BATCH_TOO_LARGE with an unknown outcome", receipt, err)
	}
	if frames.Load() != 1 {
		t.Fatalf("%d requests, want 1: an unknown batch is not split or resent", frames.Load())
	}
}

// A batch the server takes is one request: no probing, no extra frames.
func TestInsertManySendsAnAcceptedBatchOnce(t *testing.T) {
	var sizes []int
	var mu sync.Mutex
	client := mustConnect(t, batchServer(t, 10, &sizes, &mu).uri())

	receipt, err := client.Database("app").Bucket("users").InsertMany(testContext(t), mapsOf(4))
	if err != nil || receipt.Count != 4 || len(receipt.IDs) != 4 {
		t.Fatalf("InsertMany = %+v, %v; want 4 ids", receipt, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) != 1 {
		t.Fatalf("%d requests for an accepted batch, want 1", len(sizes))
	}
}

// D-5's floor: a batch cannot be split below one document, whose refusal is the
// answer, with the server's sentence.
func TestInsertManyOneDocumentTheServerRefusesIsReturned(t *testing.T) {
	var sizes []int
	var mu sync.Mutex
	client := mustConnect(t, batchServer(t, 0, &sizes, &mu).uri())

	receipt, err := client.Database("app").Bucket("users").InsertMany(testContext(t), mapsOf(3))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("InsertMany = %+v, %v; want ErrInvalidArgument", receipt, err)
	}
	if !strings.Contains(err.Error(), "none of it was written") {
		t.Errorf("the server's sentence must come through verbatim:\n%v", err)
	}
	if receipt.Count != 0 || len(receipt.IDs) != 0 {
		t.Errorf("receipt = %+v, but nothing was stored", receipt)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(sizes) != fmt.Sprint([]int{3, 1}) {
		t.Fatalf("batch sizes sent = %v, want [3 1]: halve once, then stop at the single document", sizes)
	}
}

// Documents stored before a later half fails are reported, not lost: the
// receipt names them and the error says how many.
func TestInsertManyLaterHalfFailsAfterTheFirstWasStored(t *testing.T) {
	var batches atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var batch hoarddbwire.BatchPutRequest
		if err := unmarshal(request.Payload, &batch); err != nil {
			t.Errorf("BATCH_PUT payload: %v", err)
			return false
		}
		switch batches.Add(1) {
		case 1:
			c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Message: noneWritten})
		case 2:
			c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Success: true, IDs: []string{"a", "b"}, Count: 2})
		default:
			c.errorFrame(request, 4, "Document 1 of the batch is too large")
		}
		return true
	}))
	client := mustConnect(t, server.uri())

	receipt, err := client.Database("app").Bucket("users").InsertMany(testContext(t), mapsOf(4))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("InsertMany = %+v, %v; want ErrInvalidArgument", receipt, err)
	}
	if fmt.Sprint(receipt.IDs) != "[a b]" || receipt.Count != 2 {
		t.Errorf("receipt = %+v; want the 2 documents that were stored", receipt)
	}
	for _, want := range []string{"2 of 4 documents were stored", "too large"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error must contain %q:\n%v", want, err)
		}
	}
}

// Nothing to insert is a mistake worth saying so, and costs no round trip.
func TestInsertManyWithNoDocumentsIsRefusedBeforeSending(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.send(request, hoarddbwire.OpPong, nil)
		return true
	}))
	client := mustConnect(t, server.uri())

	_, err := client.Database("app").Bucket("users").InsertMany(testContext(t), nil)
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "Nothing was sent") {
		t.Fatalf("InsertMany(nil) = %v; want ErrInvalidArgument saying nothing was sent", err)
	}
	if frames.Load() != 0 {
		t.Fatalf("%d frames reached the server, want 0", frames.Load())
	}
}

// One document that is not a document stops the whole batch before any byte
// goes out, and the message says which one.
func TestInsertManyNamesTheDocumentItCannotEncode(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.send(request, hoarddbwire.OpPong, nil)
		return true
	}))
	client := mustConnect(t, server.uri())

	_, err := client.Database("app").Bucket("users").InsertMany(testContext(t), []any{hoarddb.Map{"a": 1}, 42, hoarddb.Map{"c": 3}})
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "document 2") {
		t.Fatalf("InsertMany = %v; want ErrInvalidArgument naming document 2", err)
	}
	if frames.Load() != 0 {
		t.Fatalf("%d frames reached the server, want 0", frames.Load())
	}
}

// WithPriority belongs to Push on a heap: here it is refused, not dropped.
func TestInsertManyRefusesAPriority(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.send(request, hoarddbwire.OpPong, nil)
		return true
	}))
	client := mustConnect(t, server.uri())

	_, err := client.Database("app").Bucket("users").InsertMany(testContext(t), mapsOf(2), hoarddb.WithPriority(3))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || frames.Load() != 0 {
		t.Fatalf("InsertMany with a priority = %v after %d frames; want ErrInvalidArgument and no frame", err, frames.Load())
	}
}

// A batch is bulk traffic (handbook/wire.md section 7): a slow one holds a bulk
// connection and a point read is answered at once.
func TestInsertManyLeavesPointReadsAlone(t *testing.T) {
	release := make(chan struct{})
	var batchFrames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		switch request.Op {
		case hoarddbwire.OpBatchPut:
			batchFrames.Add(1)
			<-release
			c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Success: true, IDs: []string{"a"}, Count: 1})
		case hoarddbwire.OpGet:
			c.send(request, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: []byte{5, 0, 0, 0, 0}})
		default:
			c.send(request, hoarddbwire.OpPong, nil)
		}
		return true
	}))
	client := mustConnect(t, server.uri(), hoarddb.WithPoolSize(1), hoarddb.WithBulkPoolSize(1))
	users := client.Database("app").Bucket("users")
	var closeRelease sync.Once
	releaseBatch := func() { closeRelease.Do(func() { close(release) }) }
	t.Cleanup(releaseBatch)

	inserted := make(chan error, 1)
	go func() {
		_, err := users.InsertMany(testContext(t), mapsOf(1))
		inserted <- err
	}()
	waitUntil(t, func() bool { return batchFrames.Load() == 1 })

	started := time.Now()
	if _, err := users.Get(testContext(t), "a"); err != nil {
		t.Fatalf("a point read behind a batch: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("the point read took %v behind a batch", elapsed)
	}
	releaseBatch()
	if err := <-inserted; err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
}

// The frame is the one limit the driver enforces itself: a batch that cannot
// fit one frame is refused before any byte is sent, and costs no connection.
func TestInsertManyOverTheFrameIsRefusedBeforeSending(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.send(request, hoarddbwire.OpPong, nil)
		return true
	}))
	client := mustConnect(t, server.uri())

	docs := make([]any, 34)
	for position := range docs {
		docs[position] = hoarddb.Map{"Pad": strings.Repeat("x", 1<<20)}
	}
	_, err := client.Database("app").Bucket("users").InsertMany(testContext(t), docs)
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "Nothing was sent") {
		t.Fatalf("InsertMany of 34 MiB = %v; want ErrInvalidArgument saying nothing was sent", err)
	}
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatalf("the call after the refusal failed: %v", err)
	}
	if frames.Load() != 1 || client.PoolStats().Dials != 1 {
		t.Fatalf("%d frames over %d dials; the oversize batch must not reach the server and must not cost the connection",
			frames.Load(), client.PoolStats().Dials)
	}
}

// Only the none-written refusal is answered by halving. Any other in-band
// refusal is returned as it came, because splitting a batch the server refused
// for another reason could store part of it.
func TestInsertManyOtherRefusalsAreNotSplit(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Message: "no documents provided"})
		return true
	}))
	client := mustConnect(t, server.uri())

	_, err := client.Database("app").Bucket("users").InsertMany(testContext(t), mapsOf(3))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "no documents provided") {
		t.Fatalf("InsertMany = %v; want the server's refusal verbatim", err)
	}
	if frames.Load() != 1 {
		t.Fatalf("%d requests; a refusal that does not say none of it was written is not split", frames.Load())
	}
}
