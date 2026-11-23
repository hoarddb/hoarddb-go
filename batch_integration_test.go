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
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// Real-server tests of InsertMany (handbook/build-go.md step 11). Each starts
// its own server and needs HOARDDB_SERVER_BIN.

type person struct {
	Name string `bson:"Name"`
	Born int32  `bson:"Born"`
}

func peopleBucket(t *testing.T, client *hoarddb.Client) *hoarddb.Bucket {
	t.Helper()
	people, err := client.Database("app").CreateBucket(testContext(t), "people", hoarddb.Hash)
	if err != nil {
		t.Fatalf("CreateBucket people: %v", err)
	}
	return people
}

// Every document of a batch comes back with its key, in the order given, and
// each key reads back its own document.
func TestInsertManyReturnsEveryID(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	people := peopleBucket(t, client)

	receipt, err := people.InsertMany(ctx, []any{person{"Ada", 1815}, hoarddb.Map{"Name": "Grace", "Born": int32(1906)}, person{"Alan", 1912}})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if receipt.Count != 3 || len(receipt.IDs) != 3 {
		t.Fatalf("receipt = %+v; want 3 documents and 3 ids", receipt)
	}
	seen := map[string]bool{}
	for _, id := range receipt.IDs {
		if id == "" || seen[id] {
			t.Fatalf("ids %v: every id must be non-empty and distinct", receipt.IDs)
		}
		seen[id] = true
	}
	for position, want := range []string{"Ada", "Grace", "Alan"} {
		stored, err := people.Get(ctx, receipt.IDs[position])
		if err != nil {
			t.Fatalf("Get %q: %v", receipt.IDs[position], err)
		}
		var got person
		if err := stored.Decode(&got); err != nil || got.Name != want {
			t.Fatalf("id %d reads back %+v, %v; want %s: the ids are in the order of the documents", position, got, err, want)
		}
	}
	if count, err := people.Count(ctx, nil); err != nil || count != 3 {
		t.Fatalf("Count = %d, %v; want 3", count, err)
	}
	if receipt.CopiesConfirmed != 0 || receipt.ReplicationFactor != 0 {
		t.Errorf("a single node reports no copy counts, receipt has %+v", receipt)
	}
}

// batchBoundOf reads the node's atomic batch bound from its own health answer
// (memtable.batch_bytes), as the handbook says to.
func batchBoundOf(t *testing.T, server *servertest.Server) int64 {
	t.Helper()
	conn, err := server.Dial(testContext(t))
	if err != nil {
		t.Fatalf("dialling for the health answer: %v", err)
	}
	defer func() { _ = conn.Close() }()
	var health hoarddbwire.HealthResponse
	if _, err := conn.Do(testContext(t), hoarddbwire.OpHealthCheck, nil, &health); err != nil {
		t.Fatalf("health: %v", err)
	}
	var memtable struct {
		BatchBytes int64 `bson:"batch_bytes"`
	}
	if err := codec.Unmarshal(health.Memtable, &memtable); err != nil || memtable.BatchBytes <= 0 {
		t.Fatalf("health memtable = %v (%v); want a batch_bytes figure", health.Memtable, err)
	}
	return memtable.BatchBytes
}

// A batch over the node's atomic bound is refused whole by the server, and the
// driver halves it until every part is taken: every document is stored once.
func TestInsertManySplitsAnOversizedBatch(t *testing.T) {
	server := startServer(t)
	proxy := servertest.StartProxy(t, server)
	client := mustConnect(t, fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s", server.User, server.Password, proxy.Address, proxy.Fingerprint))
	ctx := testContext(t)
	people := peopleBucket(t, client)

	// 64 KiB documents, a quarter more of them than the bound allows, still
	// under the 32 MiB frame.
	const documentBytes = 64 << 10
	count := int(batchBoundOf(t, server))*5/4/documentBytes + 1
	docs := make([]any, count)
	for position := range docs {
		docs[position] = hoarddb.Map{"Seq": int32(position), "Pad": strings.Repeat("x", documentBytes)}
	}

	receipt, err := people.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany of %d documents: %v", count, err)
	}
	if receipt.Count != count || len(receipt.IDs) != count {
		t.Fatalf("receipt has Count %d and %d ids, want %d", receipt.Count, len(receipt.IDs), count)
	}
	if stored, err := people.Count(ctx, nil); err != nil || stored != int64(count) {
		t.Fatalf("Count = %d, %v; want %d: every document stored once", stored, err, count)
	}
	// The ids are in input order: the Seq each reads back is its position.
	for _, position := range []int{0, count / 2, count - 1} {
		stored, err := people.Get(ctx, receipt.IDs[position])
		if err != nil {
			t.Fatalf("Get id %d: %v", position, err)
		}
		var got struct {
			Seq int32 `bson:"Seq"`
		}
		if err := stored.Decode(&got); err != nil || int(got.Seq) != position {
			t.Fatalf("id %d reads back Seq %d, %v; the ids must follow the input order", position, got.Seq, err)
		}
	}
	var batches int
	for _, exchange := range proxy.Exchanges() {
		if exchange.Request.Op == hoarddbwire.OpBatchPut {
			batches++
		}
	}
	if batches < 3 {
		t.Fatalf("%d BATCH_PUT requests; a batch over the bound needs the refused whole and at least two halves", batches)
	}
}

// One document over the server's per-document limit is refused with the
// server's sentence, and the documents beside it are not stored: the batch is
// atomic, so a driver that split it up would store the others.
func TestInsertManyOneTooLargeDocumentIsRefused(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	people := peopleBucket(t, client)

	_, err := people.InsertMany(ctx, []any{hoarddb.Map{"Name": "small"}, hoarddb.Map{"Pad": strings.Repeat("x", 25<<20)}})
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("InsertMany = %v; want ErrInvalidArgument", err)
	}
	for _, want := range []string{"Document 2 of the batch", "24 MiB"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the server's sentence must come through verbatim, missing %q:\n%v", want, err)
		}
	}
	if stored, err := people.Count(ctx, nil); err != nil || stored != 0 {
		t.Fatalf("Count = %d, %v; the refused batch must have stored nothing", stored, err)
	}
	if _, err := people.Insert(ctx, hoarddb.Map{"Name": "after"}); err != nil {
		t.Fatalf("the client after the refusal: %v", err)
	}
}

// InsertMany on a fifo or lifo answers the count and the ids it pushed, in
// order (handbook/operations.md section 1), and the
// queue then pops them in its own order.
func TestInsertManyOnAQueueReportsWhatItPushed(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO} {
		t.Run(string(kind), func(t *testing.T) {
			queue := bucketOf(t, client, kind)
			receipt, err := queue.InsertMany(ctx, []any{hoarddb.Map{"N": int32(1)}, hoarddb.Map{"N": int32(2)}, hoarddb.Map{"N": int32(3)}})
			if err != nil {
				t.Fatalf("InsertMany on a %s: %v", kind, err)
			}
			if receipt.Count != 3 || len(receipt.IDs) != 3 {
				t.Fatalf("receipt = %+v; want 3 documents and 3 ids", receipt)
			}
			first := int32(1)
			if kind == hoarddb.LIFO {
				first = 3
			}
			popped, err := onlyDocument(queue.Pop(ctx))
			if err != nil || intField(t, popped, "N") != int64(first) {
				t.Fatalf("Pop = %s, %v; want N=%d", popped, err, first)
			}
		})
	}
}
