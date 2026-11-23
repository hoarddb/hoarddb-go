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
)

// Pop takes a count (handbook/api-go.md section 5): WithLimit asks for up to
// that many in one request and the answer is a list in pop order.

// popFake answers every FIFO_POP and FIELD_POP with the documents it is given
// and records the requests.
func popFake(t *testing.T, items ...hoarddbwire.Document) (*fakeServer, *requestRecorder) {
	t.Helper()
	recorder := &requestRecorder{}
	server := startFake(t, serveFramesWith([]string{"write_concern"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		recorder.add(t, request)
		switch request.Op {
		case hoarddbwire.OpFIFOPop:
			answer := hoarddbwire.PopResponse{Found: len(items) > 0, Items: items}
			if len(items) == 0 {
				answer.Message = "queue is empty"
			}
			c.send(request, hoarddbwire.OpFIFOPopResp, answer)
		case hoarddbwire.OpFieldPop:
			entries := make([]hoarddbwire.FieldEntry, len(items))
			for position, one := range items {
				entries[position] = hoarddbwire.FieldEntry{Element: one}
			}
			c.send(request, request.Op, hoarddbwire.FieldResponse{Success: true, Found: len(items) > 0, Items: entries})
		default:
			c.errorFrame(request, 5, "the fake server does not answer "+request.Op.String())
		}
		return true
	}))
	return server, recorder
}

func TestPopSendsTheLimitAndReturnsTheListInOrder(t *testing.T) {
	ada := adaDocument(t)
	server, recorder := popFake(t, ada, ada)
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	for name, pop := range map[string]func(opts ...hoarddb.PopOption) ([]hoarddb.Document, error){
		"bucket": func(opts ...hoarddb.PopOption) ([]hoarddb.Document, error) {
			return client.Database("app").Bucket("jobs").Pop(ctx, opts...)
		},
		"field": func(opts ...hoarddb.PopOption) ([]hoarddb.Document, error) {
			return client.Database("app").Bucket("User").Field("ada", "Purchases").Pop(ctx, opts...)
		},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(recorder.requests())
			got, err := pop(hoarddb.WithLimit(2), hoarddb.WithWriteConcern(hoarddb.Majority))
			if err != nil || len(got) != 2 {
				t.Fatalf("Pop(WithLimit(2)) = %d documents, %v; want 2", len(got), err)
			}
			sent := recorder.requests()[before:]
			if len(sent) != 1 {
				t.Fatalf("Pop sent %d requests, want 1", len(sent))
			}
			limit, concern := sent[0].members["limit"], sent[0].members["writeconcern"]
			if fmt.Sprint(limit) != "2" || concern != "majority" {
				t.Errorf("sent limit=%v writeconcern=%v; want 2 and majority", limit, concern)
			}
			if _, err := pop(); err != nil {
				t.Fatal(err)
			}
			if absent, present := recorder.requests()[before+1].members["limit"]; present {
				t.Errorf("a default Pop sent limit=%v; it leaves the member out so the server takes 1", absent)
			}
		})
	}
}

func TestPopRefusesALimitOutsideOneToAHundredBeforeSending(t *testing.T) {
	server, recorder := popFake(t, adaDocument(t))
	client := mustConnect(t, server.uri())
	for _, limit := range []int{0, -1, 101} {
		for name, pop := range map[string]func() error{
			"bucket": func() error {
				_, err := client.Database("app").Bucket("jobs").Pop(testContext(t), hoarddb.WithLimit(limit))
				return err
			},
			"field": func() error {
				_, err := client.Database("app").Bucket("User").Field("ada", "Purchases").Pop(testContext(t), hoarddb.WithLimit(limit))
				return err
			},
		} {
			err := pop()
			if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "1 to 100") || !strings.Contains(err.Error(), "nothing was sent") {
				t.Errorf("%s Pop(WithLimit(%d)) = %v; want ErrInvalidArgument naming 1 to 100 and that nothing was sent", name, limit, err)
			}
		}
	}
	if sent := len(recorder.requests()); sent != 0 {
		t.Fatalf("%d requests were sent; none should have been", sent)
	}
}

func TestPopOfNothingIsAnEmptyListAndNoError(t *testing.T) {
	server, _ := popFake(t)
	client := mustConnect(t, server.uri())
	got, err := client.Database("app").Bucket("jobs").Pop(testContext(t), hoarddb.WithLimit(5))
	if err != nil || len(got) != 0 {
		t.Fatalf("Pop of an empty store = %v, %v; want an empty list and a nil error", got, err)
	}
}

// A lost pop of N says it may have removed up to N entries, and a pop whose
// error frame says applied says its answer carried none.
func TestPopSaysHowManyEntriesItMayHaveRemoved(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.errorWith(request, map[string]any{"code": int32(7), "write_outcome": "applied", "message": "write concern all not met in time"})
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("q").Pop(testContext(t), hoarddb.WithLimit(7))
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Outcome != hoarddb.OutcomeApplied {
		t.Fatalf("Pop = %v; want an applied *Error", err)
	}
	for _, want := range []string{"write concern all not met in time", "its answer carried no entries", "up to 7 entries", "run peek to see what is left"} {
		if !strings.Contains(failure.Message, want) {
			t.Errorf("message %q lacks %q", failure.Message, want)
		}
	}
}

// Real server: one request removes a batch, in the store's pop order.
func TestPopManyAgainstTheServer(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, tc := range []struct {
		kind hoarddb.StoreType
		want []int64 // the order five pushes of 1 to 5 pop in
	}{
		{hoarddb.FIFO, []int64{1, 2, 3, 4, 5}},
		{hoarddb.LIFO, []int64{5, 4, 3, 2, 1}},
		{hoarddb.Heap, []int64{5, 4, 3, 2, 1}}, // priority n for the document n
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			bucket := bucketOf(t, client, tc.kind)
			for n := int64(1); n <= 5; n++ {
				if _, err := bucket.Push(ctx, hoarddb.Map{"n": n}, hoarddb.WithPriority(n)); err != nil {
					t.Fatal(err)
				}
			}
			batch, err := bucket.Pop(ctx, hoarddb.WithLimit(3))
			if err != nil || len(batch) != 3 {
				t.Fatalf("Pop(WithLimit(3)) = %d documents, %v; want 3", len(batch), err)
			}
			for position, one := range batch {
				if got := intField(t, one, "n"); got != tc.want[position] {
					t.Errorf("document %d is n=%d, want %d", position, got, tc.want[position])
				}
			}
			rest, err := bucket.Pop(ctx, hoarddb.WithLimit(100)) // more than are left: not an error
			if err != nil || len(rest) != 2 || intField(t, rest[0], "n") != tc.want[3] {
				t.Fatalf("Pop past the end = %v, %v; want the 2 left, in order", rest, err)
			}
			if drained, err := bucket.Pop(ctx, hoarddb.WithLimit(2)); err != nil || len(drained) != 0 {
				t.Fatalf("Pop of a drained store = %v, %v; want an empty list and a nil error", drained, err)
			}
		})
	}
}

func TestFieldPopManyAgainstTheServer(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	users, err := client.Database("app").CreateBucket(ctx, "User", hoarddb.Hash,
		hoarddb.WithStores(hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := users.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	queue := users.Field("ada", "Purchases")
	for _, item := range []string{"tea", "scones", "jam"} {
		if _, err := queue.Push(ctx, hoarddb.Map{"Item": item}); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := queue.Pop(ctx, hoarddb.WithLimit(2))
	if err != nil || len(batch) != 2 || stringField(t, batch[0], "Item") != "tea" || stringField(t, batch[1], "Item") != "scones" {
		t.Fatalf("field Pop(WithLimit(2)) = %v, %v; want tea then scones", batch, err)
	}
	if _, err := queue.Pop(ctx, hoarddb.WithLimit(101)); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("field Pop(WithLimit(101)) = %v; want ErrInvalidArgument", err)
	}
}

// A store-field pop of N that comes back applied or lost names N, not 1.
func TestFieldPopSaysHowManyEntriesItMayHaveRemoved(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.errorWith(request, map[string]any{"code": int32(7), "write_outcome": "applied", "message": "timed out"})
		return true
	}))
	field := mustConnect(t, server.uri()).Database("app").Bucket("User").Field("ada", "Purchases")
	_, err := field.Pop(testContext(t), hoarddb.WithLimit(7))
	if err == nil || !strings.Contains(err.Error(), "up to 7 entries") {
		t.Fatalf("applied field Pop(WithLimit(7)) = %v; want it to say it may have removed up to 7 entries", err)
	}
	lost := startFake(t, serveFrames(func(*fakeConn, hoarddbwire.Frame, int) bool { return false }))
	_, err = mustConnect(t, lost.uri()).Database("app").Bucket("User").Field("ada", "Purchases").Pop(testContext(t), hoarddb.WithLimit(7))
	if err == nil || !strings.Contains(err.Error(), "up to 7 entries") {
		t.Fatalf("lost field Pop(WithLimit(7)) = %v; want it to say it may have removed up to 7 entries", err)
	}
}
