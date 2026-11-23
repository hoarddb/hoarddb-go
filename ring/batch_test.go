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

package ring

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestBatchRetriesUnderTheSameRequestID: the first send is answered with one operation refused
// SCHEMA_NOT_CURRENT; the client describes the bucket again and sends the whole frame once more,
// under the id of the first, with the new declaration version in every operation.
func TestBatchRetriesUnderTheSameRequestID(t *testing.T) {
	var decl, schema atomic.Int64
	decl.Store(1)
	describe := describeHandler(t, &decl, &schema)
	var ids [][16]byte
	var declSeen [][]int64
	var lock sync.Mutex
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingBatch {
			describe(conn, frame)
			return
		}
		request, err := ringwire.DecodeRingBatch(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		versions := make([]int64, len(request.Ops))
		for i, op := range request.Ops {
			versions[i] = op.DeclVersion
		}
		lock.Lock()
		ids, declSeen = append(ids, frame.ID), append(declSeen, versions)
		first := len(ids) == 1
		lock.Unlock()
		applied := ringwire.RingReply{WriteOutcome: "applied"}
		answers := []ringwire.RingAnswer{{Reply: &applied}, {Reply: &applied}}
		outcome := "ok"
		if first {
			decl.Store(2) // the declaration moved on
			moved := ringwire.RingError{Code: ringwire.CodeSchemaNotCurrent, Message: "the bucket's schema moved"}
			answers[1] = ringwire.RingAnswer{Error: &moved}
			outcome = "partial"
		}
		payload, err := ringwire.EncodeRingReply(ringwire.RingReply{Outcome: outcome, Answers: answers})
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, payload)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	batch := client.NewBatch()
	bucket := client.Bucket("db", "User").In(batch)
	for _, id := range []string{"ada", "bob"} {
		if _, err := bucket.Insert(testContext(t), id, Doc{"Name": id}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := batch.Send(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "ok" || len(result.Answers) != 2 || !result.Answers[0].Write.Applied() || !result.Answers[1].Write.Applied() {
		t.Fatalf("result %+v", result)
	}
	lock.Lock()
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("the retry must reuse the request id: %x", ids)
	}
	if declSeen[0][0] != 1 || declSeen[0][1] != 1 || declSeen[1][0] != 2 || declSeen[1][1] != 2 {
		t.Fatalf("declaration versions sent %v, want [[1 1] [2 2]]", declSeen)
	}
	lock.Unlock()
	// A second Send on the same batch is the retry the doc promises: the same id again.
	if _, err := batch.Send(testContext(t)); err != nil {
		t.Fatal(err)
	}
	lock.Lock()
	defer lock.Unlock()
	if len(ids) != 3 || ids[2] != ids[0] {
		t.Fatalf("a Send after a Send must reuse the request id: %x", ids)
	}
}

func TestBatchOf256IsRefusedBeforeSending(t *testing.T) {
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	var sent atomic.Int64
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpRingBatch {
			sent.Add(1)
			return
		}
		describe(conn, frame)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	batch := client.NewBatch()
	bucket := client.Bucket("db", "User").In(batch)
	for i := range 255 {
		if _, err := bucket.Upsert(testContext(t), "u"+string(rune('a'+i%26))+string(rune('a'+i/26)), Doc{"Name": "x"}); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
	}
	_, err = bucket.Upsert(testContext(t), "one-too-many", Doc{"Name": "x"})
	if err == nil {
		t.Fatal("the 256th operation was queued")
	}
	for _, want := range []string{"256", "at most 255", "Split it into batches of 255 or fewer"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal %q does not say %q", err, want)
		}
	}
	if batch.Len() != 255 || sent.Load() != 0 {
		t.Fatalf("%d operations queued and %d frames sent, want 255 and 0", batch.Len(), sent.Load())
	}
}

func TestBatchAnswersAreTypedAndInOrder(t *testing.T) {
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	found := true
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingBatch {
			describe(conn, frame)
			return
		}
		applied := ringwire.RingReply{WriteOutcome: "applied", Position: &ringwire.Position{Epoch: 1, Seq: 5}}
		popped := ringwire.RingReply{WriteOutcome: "applied", Found: &found, Elements: [][]byte{{5, 0, 0, 0, 0}}}
		refused := ringwire.RingError{Code: ringwire.CodeFieldType, Message: "field Purchases is a store"}
		payload, err := ringwire.EncodeRingReply(ringwire.RingReply{Outcome: "partial",
			Answers: []ringwire.RingAnswer{{Reply: &applied}, {Error: &refused}, {Reply: &popped}}})
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, payload)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	batch := client.NewBatch()
	bucket := client.Bucket("db", "User").In(batch)
	ctx := testContext(t)
	if _, err := bucket.Upsert(ctx, "ada", Doc{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	if _, err := bucket.Delete(ctx, "bob"); err != nil {
		t.Fatal(err)
	}
	if element, err := bucket.Pop(ctx, "ada", "Purchases"); err != nil || element != nil {
		t.Fatalf("a queued Pop returns nothing and no error, got %v %v", element, err)
	}
	if _, err := bucket.Get(ctx, "ada"); err == nil || !strings.Contains(err.Error(), "writes only") {
		t.Fatalf("a read on a batch bucket must be refused, got %v", err)
	}
	result, err := batch.Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var refusal *Error
	if result.Outcome != "partial" || result.Answers[0].Write.Position != "1:5" ||
		!errors.As(result.Answers[1].Err, &refusal) || refusal.Code != ringwire.CodeFieldType ||
		!result.Answers[2].Found || len(result.Answers[2].Elements) != 1 {
		t.Fatalf("result %+v", result)
	}
}
