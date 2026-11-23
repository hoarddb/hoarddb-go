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
	"slices"
	"strings"
	"sync"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

// Real-server tests of documents, per store type (handbook/build-go.md step 7).
// Each starts its own server. They need HOARDDB_SERVER_BIN.

// bucketOf creates a bucket of the given type named after it, in database app.
func bucketOf(t *testing.T, client *hoarddb.Client, kind hoarddb.StoreType) *hoarddb.Bucket {
	t.Helper()
	bucket, err := client.Database("app").CreateBucket(testContext(t), string(kind), kind)
	if err != nil {
		t.Fatalf("CreateBucket %s: %v", kind, err)
	}
	return bucket
}

func stringField(t *testing.T, doc hoarddb.Document, path ...string) string {
	t.Helper()
	value, found := doc.Lookup(path...)
	if !found {
		t.Fatalf("document %s has no field %v", doc, path)
	}
	text, isString := value.AsString()
	if !isString {
		t.Fatalf("field %v of %s is not a string", path, doc)
	}
	return text
}

func intField(t *testing.T, doc hoarddb.Document, path ...string) int64 {
	t.Helper()
	value, found := doc.Lookup(path...)
	if !found {
		t.Fatalf("document %s has no field %v", doc, path)
	}
	number, isInteger := value.Int64()
	if !isInteger {
		t.Fatalf("field %v of %s is not an integer", path, doc)
	}
	return number
}

// Failure 2 of handbook/build-go.md section 2: found:false is a successful
// frame, and decoded naively it is an empty document and a nil error.
func TestGetMissIsNotFound(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
				t.Fatal(err)
			}
			hit, err := bucket.Get(ctx, "ada")
			if err != nil || stringField(t, hit, "Name") != "Ada" {
				t.Fatalf("Get of a stored key = %s, %v", hit, err)
			}
			miss, err := bucket.Get(ctx, "nobody")
			if !errors.Is(err, hoarddb.ErrNotFound) {
				t.Fatalf("Get of a missing key: %s, %v; want ErrNotFound (a miss is never an empty document and a nil error)", miss, err)
			}
			if miss != nil {
				t.Fatalf("Get of a missing key = %s, %v; a missing key returns no document", miss, err)
			}
			var failure *hoarddb.Error
			if !errors.As(err, &failure) || failure.Op != "get" || failure.Target != "app/"+string(kind)+"/nobody" ||
				!strings.Contains(failure.Message, "document not found") {
				t.Fatalf("error = %#v; want Op get, the target and the server's sentence", failure)
			}
			if found, err := bucket.Exists(ctx, "nobody"); err != nil || found {
				t.Fatalf("Exists of a missing key = %v, %v", found, err)
			}
			if found, err := bucket.Exists(ctx, "ada"); err != nil || !found {
				t.Fatalf("Exists of a stored key = %v, %v", found, err)
			}
		})
	}
	// A bucket that does not exist is a different answer (an error frame), and
	// the message carries the server's fix.
	_, err := client.Database("app").Bucket("nope").Get(ctx, "x")
	if !errors.Is(err, hoarddb.ErrNotFound) || !strings.Contains(err.Error(), "Run '") {
		t.Fatalf("Get from a missing bucket: %v; want ErrNotFound with the server's fix", err)
	}
}

// Failure 2 again, for the stores that are read by taking from them.
func TestPopAndPeekOfAnEmptyStoreAreAnEmptyListAndNoError(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO, hoarddb.Heap} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			for name, call := range map[string]func() error{
				"Pop":  func() error { got, err := bucket.Pop(ctx); return emptyList(len(got), err) },
				"Peek": func() error { got, err := bucket.Peek(ctx); return emptyList(len(got), err) },
			} {
				if err := call(); err != nil {
					t.Fatalf("%s of an empty %s: %v; want an empty list and a nil error", name, kind, err)
				}
			}
			if _, err := bucket.Push(ctx, hoarddb.Map{"n": 1}, hoarddb.WithPriority(1)); err != nil {
				t.Fatal(err)
			}
			popped, err := onlyDocument(bucket.Pop(ctx))
			if err != nil || intField(t, popped, "n") != 1 {
				t.Fatalf("Pop = %s, %v", popped, err)
			}
			if drained, err := bucket.Pop(ctx); err != nil || len(drained) != 0 {
				t.Fatalf("Pop of a drained %s = %v, %v; want an empty list and a nil error", kind, drained, err)
			}
		})
	}

	btree := bucketOf(t, client, hoarddb.BTree)
	ends := map[string]func() (hoarddb.Entry, error){
		"First": func() (hoarddb.Entry, error) { return btree.First(ctx) },
		"Last":  func() (hoarddb.Entry, error) { return btree.Last(ctx) },
	}
	for name, call := range ends {
		entry, err := call()
		if !errors.Is(err, hoarddb.ErrNotFound) || entry.Key != "" || entry.Value != nil {
			t.Fatalf("%s of an empty btree = %+v, %v; want ErrNotFound", name, entry, err)
		}
	}
	// An empty range is not an error: it is a cursor with nothing in it.
	cursor, err := btree.Range(ctx, hoarddb.Bounds{})
	if err != nil {
		t.Fatalf("Range of an empty btree: %v", err)
	}
	if documents, err := cursor.All(ctx); err != nil || len(documents) != 0 {
		t.Fatalf("All of an empty range = %v, %v", documents, err)
	}
}

// Failure 10: a delete of a missing key reported as an error, or a delete
// error reported as "not there".
func TestDeleteMissingReturnsFalse(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			deleted, err := bucket.Delete(ctx, "nobody")
			if err != nil || deleted {
				t.Fatalf("Delete of a missing key = %v, %v; want false and no error", deleted, err)
			}
		})
	}
	// The other half of failure 10: a refusal is an error, never "not there".
	queue := bucketOf(t, client, hoarddb.FIFO)
	deleted, err := queue.Delete(ctx, "x")
	if err == nil || deleted {
		t.Fatalf("Delete on a fifo = %v, %v; the server refuses it, and a refusal is an error, not a missing key", deleted, err)
	}
	if !strings.Contains(err.Error(), "not supported for bucket type fifo") {
		t.Errorf("the server's sentence was lost: %v", err)
	}
	if _, err := client.Database("app").Bucket("nope").Delete(ctx, "x"); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("Delete from a missing bucket: %v; want ErrNotFound", err)
	}
}

func TestDeleteTwiceIsSafe(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
				t.Fatal(err)
			}
			for attempt, want := range []bool{true, false, false} {
				deleted, err := bucket.Delete(ctx, "ada")
				if err != nil || deleted != want {
					t.Fatalf("Delete attempt %d = %v, %v; want %v and no error", attempt+1, deleted, err, want)
				}
			}
			if _, err := bucket.Get(ctx, "ada"); !errors.Is(err, hoarddb.ErrNotFound) {
				t.Fatalf("Get after Delete: %v", err)
			}
		})
	}
}

func TestInsertAssignsAKey(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			first, err := bucket.Insert(ctx, hoarddb.Map{"Name": "Ada"})
			if err != nil || first.ID == "" {
				t.Fatalf("Insert = %+v, %v; want a server-assigned key", first, err)
			}
			second, err := bucket.Insert(ctx, hoarddb.Map{"Name": "Grace"})
			if err != nil || second.ID == "" || second.ID == first.ID {
				t.Fatalf("second Insert = %+v, %v; want a different key from %q", second, err, first.ID)
			}
			stored, err := bucket.Get(ctx, first.ID)
			if err != nil || stringField(t, stored, "Name") != "Ada" || stringField(t, stored, "_id_") != first.ID {
				t.Fatalf("Get(%q) = %s, %v", first.ID, stored, err)
			}
			if first.Seq == 0 {
				t.Errorf("Receipt.Seq = 0; the server numbers every write")
			}
			if first.CopiesConfirmed != 0 || first.ReplicationFactor != 0 {
				t.Errorf("a single node reports no copies, got %+v", first)
			}
		})
	}
}

func TestPutInsertsReportsTheKeyAndRefusesAHeldOne(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			receipt, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada", "Born": int32(1815)})
			if err != nil || receipt.ID != "ada" || receipt.Seq == 0 {
				t.Fatalf("Put = %+v, %v", receipt, err)
			}
			_, err = bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada Lovelace"})
			var failure *hoarddb.Error
			if !errors.Is(err, hoarddb.ErrDocumentExists) || !errors.As(err, &failure) || failure.Outcome != hoarddb.OutcomeNotApplied {
				t.Fatalf("a second Put = %v; want ErrDocumentExists and not applied", err)
			}
			if !strings.Contains(failure.Message, "upsert") {
				t.Errorf("message %q does not name upsert, the way to replace", failure.Message)
			}
			stored, err := bucket.Get(ctx, "ada")
			if err != nil || stringField(t, stored, "Name") != "Ada" {
				t.Fatalf("Get after the refused Put = %s, %v; want the first document untouched", stored, err)
			}
		})
	}
}

func TestUpsertReplacesCreatesAndReportsTheKey(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			receipt, err := bucket.Upsert(ctx, "ada", hoarddb.Map{"Name": "Ada", "Born": int32(1815)})
			if err != nil || receipt.ID != "ada" || receipt.Seq == 0 {
				t.Fatalf("Upsert of a new key = %+v, %v", receipt, err)
			}
			if _, err := bucket.Upsert(ctx, "ada", hoarddb.Map{"Name": "Ada Lovelace"}); err != nil {
				t.Fatalf("Upsert of a held key: %v", err)
			}
			stored, err := bucket.Get(ctx, "ada")
			if err != nil || stringField(t, stored, "Name") != "Ada Lovelace" {
				t.Fatalf("Get after the second Upsert = %s, %v", stored, err)
			}
			if _, found := stored.Lookup("Born"); found {
				t.Fatalf("Upsert replaces the document, but %s still has the old field", stored)
			}
		})
	}
}

func TestUpsertManyReplacesHeldIDsAndKeepsTheLastOfARepeat(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada", "Born": int32(1815)}); err != nil {
		t.Fatal(err)
	}
	receipt, err := bucket.UpsertMany(ctx, []any{
		hoarddb.Map{"_id_": "ada", "Name": "Ada Lovelace"},
		hoarddb.Map{"_id_": "grace", "Name": "Grace"},
		hoarddb.Map{"_id_": "grace", "Name": "Grace Hopper"},
	})
	if err != nil || len(receipt.IDs) != 3 || len(receipt.Results) != 3 {
		t.Fatalf("UpsertMany = %+v, %v; want one id and one result per document given", receipt, err)
	}
	if !strings.Contains(receipt.Results[1].Message, "replaced by the document at position 3") {
		t.Errorf("result 1 = %+v; the server says the first of a repeated id was replaced by the later one", receipt.Results[1])
	}
	for id, want := range map[string]string{"ada": "Ada Lovelace", "grace": "Grace Hopper"} {
		stored, err := bucket.Get(ctx, id)
		if err != nil || stringField(t, stored, "Name") != want {
			t.Errorf("Get(%q) = %s, %v; want Name %q", id, stored, err, want)
		}
	}
	if _, err := bucket.InsertMany(ctx, []any{hoarddb.Map{"_id_": "ada", "Name": "again"}}); !errors.Is(err, hoarddb.ErrDocumentExists) {
		t.Errorf("InsertMany of a held id = %v; want ErrDocumentExists", err)
	}
}

func TestUpsertOnABucketWithNoKeysIsRefusedWithTheVerbToUse(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO, hoarddb.Heap, hoarddb.Blob} {
		t.Run(string(kind), func(t *testing.T) {
			_, err := bucketOf(t, client, kind).Upsert(ctx, "k", hoarddb.Map{"Name": "Ada"})
			var failure *hoarddb.Error
			if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) || failure.Message == "" {
				t.Fatalf("Upsert on a %s bucket = %v; want the server's INVALID_ARGUMENT sentence", kind, err)
			}
		})
	}
}

func TestUpdateSetUnsetInc(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada", "Draft": true, "Visits": int64(1)}); err != nil {
				t.Fatal(err)
			}
			after, err := bucket.Update(ctx, "ada", hoarddb.Update{
				Set:   hoarddb.Map{"Address.City": "London"},
				Unset: []string{"Draft"},
				Inc:   map[string]int64{"Visits": 2},
			})
			if err != nil {
				t.Fatal(err)
			}
			if stringField(t, after, "Address", "City") != "London" || intField(t, after, "Visits") != 3 || stringField(t, after, "Name") != "Ada" {
				t.Fatalf("Update returned %s", after)
			}
			if _, found := after.Lookup("Draft"); found {
				t.Fatalf("Unset left the field in %s", after)
			}
			stored, err := bucket.Get(ctx, "ada")
			if err != nil || stringField(t, stored, "Address", "City") != "London" || intField(t, stored, "Visits") != 3 {
				t.Fatalf("Get after Update = %s, %v; the returned document must be the stored one", stored, err)
			}
			if _, found := stored.Lookup("Draft"); found {
				t.Fatalf("Unset did not persist: %s", stored)
			}
		})
	}
}

func TestUpdateMissingKeyIsNotFound(t *testing.T) {
	client := connectTo(t, startServer(t))
	bucket := bucketOf(t, client, hoarddb.Hash)
	after, err := bucket.Update(testContext(t), "nobody", hoarddb.Update{Set: hoarddb.Map{"Name": "Nobody"}})
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrNotFound) || after != nil || !errors.As(err, &failure) || failure.Op != "update" ||
		!strings.Contains(failure.Message, "insert it first") {
		t.Fatalf("Update of a missing key = %s, %v; want ErrNotFound carrying the server's sentence", after, err)
	}
	if found, err := bucket.Exists(testContext(t), "nobody"); err != nil || found {
		t.Fatalf("a refused Update created the document: %v, %v", found, err)
	}
}

func TestQueuesPopInTheOrderOfTheirType(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	order := map[hoarddb.StoreType][]int64{hoarddb.FIFO: {1, 2, 3}, hoarddb.LIFO: {3, 2, 1}}
	for kind, want := range order {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			for n := int64(1); n <= 3; n++ {
				receipt, err := bucket.Push(ctx, hoarddb.Map{"n": n})
				if err != nil || receipt.ID == "" {
					t.Fatalf("Push = %+v, %v", receipt, err)
				}
			}
			peeked, err := bucket.Peek(ctx, hoarddb.WithLimit(2))
			if err != nil || len(peeked) != 2 || intField(t, peeked[0], "n") != want[0] || intField(t, peeked[1], "n") != want[1] {
				t.Fatalf("Peek(2) = %v, %v; want %v first, in pop order", peeked, err, want[:2])
			}
			single, err := bucket.Peek(ctx)
			if err != nil || len(single) != 1 || intField(t, single[0], "n") != want[0] {
				t.Fatalf("Peek() = %v, %v; want one entry, the next to pop", single, err)
			}
			for _, expected := range want {
				popped, err := onlyDocument(bucket.Pop(ctx))
				if err != nil || intField(t, popped, "n") != expected {
					t.Fatalf("Pop = %s, %v; want n=%d", popped, err, expected)
				}
			}
		})
	}
}

// A heap pops its highest priority first. The handbook says a heap push needs
// a priority; this server accepts one without and files it at priority 0, and
// the driver, which never looks a store's type up, cannot refuse it. The test
// pins what the server does so a change shows up here (reported upstream in
// the pull request).
func TestHeapPushWithoutPriority(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	heap := bucketOf(t, client, hoarddb.Heap)
	for _, priority := range []int64{5, 1, 9} {
		if _, err := heap.Push(ctx, hoarddb.Map{"p": priority}, hoarddb.WithPriority(priority)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := heap.Push(ctx, hoarddb.Map{"p": int64(0)}); err != nil {
		t.Fatalf("a heap push without a priority: %v; the server accepts it", err)
	}
	for _, want := range []int64{9, 5, 1, 0} {
		popped, err := onlyDocument(heap.Pop(ctx))
		if err != nil || intField(t, popped, "p") != want {
			t.Fatalf("Pop = %s, %v; want p=%d (highest priority first; none given is 0)", popped, err, want)
		}
	}
}

// handbook/operations.md section 1: a PUT to a queue is accepted and pushes.
// The driver neither refuses it nor relies on it.
func TestPutToAQueueIsPassedThrough(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	queue := bucketOf(t, client, hoarddb.FIFO)
	if _, err := queue.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
		t.Fatalf("Put on a fifo: %v; the server accepts it", err)
	}
	popped, err := onlyDocument(queue.Pop(ctx))
	if err != nil || stringField(t, popped, "Name") != "Ada" {
		t.Fatalf("Pop = %s, %v", popped, err)
	}
}

// verbCell is one "-" cell of handbook/operations.md section 1: a verb on a
// store type that does not answer it, and what a real server does with it.
type verbCell struct {
	verb string
	kind hoarddb.StoreType
	// accepted: the server answers the verb as if the type supported it (S-4:
	// the driver neither refuses nor relies on it). Otherwise the server refuses
	// and the driver reports an error of class refused, carrying the server's
	// sentence.
	accepted bool
	refused  *hoarddb.Error
	sentence string
}

// TestVerbOnWrongStoreTypePassesTheRefusalThrough runs every "-" cell of
// handbook/operations.md section 1 for the verbs of this step against a real
// server and holds the driver to one rule: it neither refuses client-side nor
// swallows a refusal. A cell the server answers as success (S-4) is recorded
// as it is.
func TestVerbOnWrongStoreTypePassesTheRefusalThrough(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	doc := hoarddb.Map{"a": 1}
	call := map[string]func(*hoarddb.Bucket) error{
		"put":    func(b *hoarddb.Bucket) error { _, err := b.Put(ctx, "k", doc); return err },
		"insert": func(b *hoarddb.Bucket) error { _, err := b.Insert(ctx, doc); return err },
		"get":    func(b *hoarddb.Bucket) error { _, err := b.Get(ctx, "k"); return err },
		"exists": func(b *hoarddb.Bucket) error { _, err := b.Exists(ctx, "k"); return err },
		"delete": func(b *hoarddb.Bucket) error { _, err := b.Delete(ctx, "k"); return err },
		"update": func(b *hoarddb.Bucket) error {
			_, err := b.Update(ctx, "k", hoarddb.Update{Set: doc})
			return err
		},
		"push":  func(b *hoarddb.Bucket) error { _, err := b.Push(ctx, doc); return err },
		"pop":   func(b *hoarddb.Bucket) error { _, err := b.Pop(ctx); return err },
		"peek":  func(b *hoarddb.Bucket) error { _, err := b.Peek(ctx); return err },
		"range": func(b *hoarddb.Bucket) error { _, err := b.Range(ctx, hoarddb.Bounds{}); return err },
		"first": func(b *hoarddb.Bucket) error { _, err := b.First(ctx); return err },
		"last":  func(b *hoarddb.Bucket) error { _, err := b.Last(ctx); return err },
	}
	invalid := &hoarddb.Error{Code: hoarddb.CodeInvalidArgument}
	queues := []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO, hoarddb.Heap}
	notBTree := []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO, hoarddb.Hash, hoarddb.Heap, hoarddb.Blob}
	var cells []verbCell
	for _, kind := range queues {
		cells = append(cells,
			verbCell{verb: "put", kind: kind, accepted: true},
			verbCell{verb: "insert", kind: kind, accepted: true})
	}
	// A blob bucket no longer takes a document: the server names PutBlob's verb.
	cells = append(cells,
		verbCell{verb: "put", kind: hoarddb.Blob, refused: invalid, sentence: "is a blob bucket, and a blob bucket is written with put"},
		verbCell{verb: "insert", kind: hoarddb.Blob, refused: invalid, sentence: "is a blob bucket, and a blob bucket is written with put"})
	blobHint := " In Go that is Bucket.PutBlob."
	for _, kind := range append(slices.Clone(queues), hoarddb.Blob) {
		cells = append(cells, verbCell{verb: "update", kind: kind, refused: invalid, sentence: "this operation requires type hash or btree"})
	}
	for _, kind := range queues {
		cells = append(cells,
			verbCell{verb: "get", kind: kind, refused: invalid, sentence: "get is not supported for bucket type " + string(kind) + "."},
			verbCell{verb: "delete", kind: kind, refused: invalid, sentence: "delete is not supported for bucket type " + string(kind) + "."},
			// EXISTS on a type with no keys is refused, never answered false: an
			// exists:false always means the server looked.
			verbCell{verb: "exists", kind: kind, refused: invalid, sentence: "has no keys to test"})
	}
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		cells = append(cells,
			verbCell{verb: "push", kind: kind, accepted: true},
			verbCell{verb: "pop", kind: kind, refused: invalid, sentence: "pop is not defined for " + string(kind) + " buckets"},
			verbCell{verb: "peek", kind: kind, refused: invalid, sentence: "peek is not defined for " + string(kind) + " buckets"})
	}
	cells = append(cells,
		verbCell{verb: "push", kind: hoarddb.Blob, refused: invalid, sentence: "push is not supported for bucket type blob."},
		verbCell{verb: "pop", kind: hoarddb.Blob, refused: invalid, sentence: "pop is not defined for blob buckets"},
		verbCell{verb: "peek", kind: hoarddb.Blob, refused: invalid, sentence: "peek is not defined for blob buckets"})
	for _, kind := range notBTree {
		for _, verb := range []string{"range", "first", "last"} {
			cells = append(cells, verbCell{verb: verb, kind: kind, refused: invalid, sentence: "this operation requires type btree"})
		}
	}

	buckets := map[hoarddb.StoreType]*hoarddb.Bucket{}
	for _, kind := range []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO, hoarddb.Hash, hoarddb.BTree, hoarddb.Heap, hoarddb.Blob} {
		buckets[kind] = bucketOf(t, client, kind)
	}
	for _, cell := range cells {
		t.Run(cell.verb+"/"+string(cell.kind), func(t *testing.T) {
			err := call[cell.verb](buckets[cell.kind])
			if cell.accepted {
				if err != nil {
					t.Fatalf("the server accepts %s on a %s bucket (S-4), the driver reported %v", cell.verb, cell.kind, err)
				}
				return
			}
			var failure *hoarddb.Error
			if !errors.As(err, &failure) {
				t.Fatalf("%s on a %s bucket = %v; the server refuses it and the driver must pass the refusal through as an *Error", cell.verb, cell.kind, err)
			}
			if !errors.Is(err, cell.refused) {
				t.Errorf("class = %v, want %v: %v", failure.Code, cell.refused.Code, err)
			}
			if !strings.Contains(failure.Message, cell.sentence) {
				t.Errorf("the server's sentence %q was not passed through verbatim: %q", cell.sentence, failure.Message)
			}
			if wantsHint := cell.kind == hoarddb.Blob && (cell.verb == "put" || cell.verb == "insert"); wantsHint != strings.HasSuffix(failure.Message, blobHint) {
				t.Errorf("the Go hint %q: want present %v, message %q", blobHint, wantsHint, failure.Message)
			}
			if errors.Is(err, hoarddb.ErrNotFound) {
				t.Errorf("a refusal was reported as a miss: %v", err)
			}
		})
	}
}

func TestConcurrentWritersAndReadersOnOneClient(t *testing.T) {
	client := connectTo(t, startServer(t), hoarddb.WithPoolSize(4))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	var group sync.WaitGroup
	for worker := range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range 10 {
				key := fmt.Sprintf("w%d-k%d", worker, index)
				if _, err := bucket.Put(ctx, key, hoarddb.Map{"worker": worker, "index": index}); err != nil {
					t.Errorf("Put %s: %v", key, err)
					return
				}
				stored, err := bucket.Get(ctx, key)
				if err != nil || intField(t, stored, "worker") != int64(worker) || intField(t, stored, "index") != int64(index) {
					t.Errorf("Get %s = %s, %v", key, stored, err)
					return
				}
			}
		}()
	}
	group.Wait()
	stats, err := bucket.Stats(ctx)
	if err != nil || stats.Entries != 80 {
		t.Fatalf("Stats = %+v, %v; want 80 entries", stats, err)
	}
}

func TestRangeReadsABtreeInKeyOrderAcrossPages(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.BTree)
	keys := make([]string, 0, 25)
	for index := range 25 {
		key := fmt.Sprintf("k%02d", index)
		keys = append(keys, key)
		if _, err := bucket.Put(ctx, key, hoarddb.Map{"index": index}); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name   string
		bounds hoarddb.Bounds
		opts   []hoarddb.FindOption
		want   []string
	}{
		{"everything in one page", hoarddb.Bounds{}, nil, keys},
		{"everything across pages of 10", hoarddb.Bounds{}, []hoarddb.FindOption{hoarddb.PageSize(10)}, keys},
		{"pages of 1", hoarddb.Bounds{Gte: "k20"}, []hoarddb.FindOption{hoarddb.PageSize(1)}, keys[20:]},
		{"both bounds are inclusive", hoarddb.Bounds{Gte: "k05", Lte: "k09"}, []hoarddb.FindOption{hoarddb.PageSize(2)}, keys[5:10]},
		{"a limit across pages", hoarddb.Bounds{}, []hoarddb.FindOption{hoarddb.PageSize(4), hoarddb.Limit(11)}, keys[:11]},
		{"an open upper bound", hoarddb.Bounds{Gte: "k23"}, nil, keys[23:]},
		{"an open lower bound", hoarddb.Bounds{Lte: "k01"}, nil, keys[:2]},
		{"nothing in range", hoarddb.Bounds{Gte: "z"}, nil, nil},
		{"a page size equal to the count", hoarddb.Bounds{}, []hoarddb.FindOption{hoarddb.PageSize(25)}, keys},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cursor, err := bucket.Range(ctx, test.bounds, test.opts...)
			if err != nil {
				t.Fatal(err)
			}
			documents, err := cursor.All(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, 0, len(documents))
			for _, document := range documents {
				got = append(got, stringField(t, document, "_id_"))
			}
			if !slices.Equal(got, test.want) && (len(got) != 0 || len(test.want) != 0) {
				t.Fatalf("keys = %v, want %v", got, test.want)
			}
		})
	}

	first, err := bucket.First(ctx)
	if err != nil || first.Key != "k00" || intField(t, first.Value, "index") != 0 {
		t.Fatalf("First = %+v, %v", first, err)
	}
	last, err := bucket.Last(ctx)
	if err != nil || last.Key != "k24" || intField(t, last.Value, "index") != 24 {
		t.Fatalf("Last = %+v, %v", last, err)
	}
}

// The other operators of Update (handbook/operations.md section 2), each on a
// hash and a btree bucket against a real server.

// updated stores doc under "ada" in a bucket of every keyed type and returns
// the Update's answer and the document read back, so each test holds the two
// to the same expectation.
func updated(t *testing.T, doc hoarddb.Map, update hoarddb.Update, check func(t *testing.T, after hoarddb.Document)) {
	t.Helper()
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			if _, err := bucket.Put(ctx, "ada", doc); err != nil {
				t.Fatal(err)
			}
			after, err := bucket.Update(ctx, "ada", update)
			if err != nil {
				t.Fatal(err)
			}
			check(t, after)
			stored, err := bucket.Get(ctx, "ada")
			if err != nil {
				t.Fatal(err)
			}
			check(t, stored)
		})
	}
}

func stringsAt(t *testing.T, doc hoarddb.Document, path ...string) []string {
	t.Helper()
	value, found := doc.Lookup(path...)
	if !found {
		t.Fatalf("document %s has no field %v", doc, path)
	}
	var list []string
	if err := value.Decode(&list); err != nil {
		t.Fatalf("field %v of %s is not a list of strings: %v", path, doc, err)
	}
	return list
}

func TestUpdatePush(t *testing.T) {
	updated(t, hoarddb.Map{"Name": "Ada", "Tags": []string{"maths", "poetry"}},
		hoarddb.Update{Push: hoarddb.Map{"Langs": "en", "Tags": "poetry"}},
		func(t *testing.T, after hoarddb.Document) {
			if got := stringsAt(t, after, "Tags"); !slices.Equal(got, []string{"maths", "poetry", "poetry"}) {
				t.Fatalf("Push appends even a value already there: Tags = %v", got)
			}
			if got := stringsAt(t, after, "Langs"); !slices.Equal(got, []string{"en"}) {
				t.Fatalf("Push on an absent field creates the array: Langs = %v", got)
			}
		})
}

func TestUpdateAddToSet(t *testing.T) {
	updated(t, hoarddb.Map{"Name": "Ada", "Tags": []string{"maths", "poetry"}},
		hoarddb.Update{AddToSet: hoarddb.Map{"Tags": "poetry", "Langs": "en"}},
		func(t *testing.T, after hoarddb.Document) {
			if got := stringsAt(t, after, "Tags"); !slices.Equal(got, []string{"maths", "poetry"}) {
				t.Fatalf("AddToSet leaves a value already there: Tags = %v", got)
			}
			if got := stringsAt(t, after, "Langs"); !slices.Equal(got, []string{"en"}) {
				t.Fatalf("AddToSet on an absent field creates the array: Langs = %v", got)
			}
		})
}

func TestUpdateMul(t *testing.T) {
	updated(t, hoarddb.Map{"Name": "Ada", "Score": 10.0},
		hoarddb.Update{Mul: map[string]float64{"Score": 1.5, "Missing": 0.5}},
		func(t *testing.T, after hoarddb.Document) {
			value, found := after.Lookup("Score")
			if score, isFloat := value.Float64(); !found || !isFloat || score != 15 {
				t.Fatalf("Mul: Score = %s, want 15", value)
			}
			if _, found := after.Lookup("Missing"); found {
				t.Fatalf("Mul creates nothing: an absent field stays absent, got %s", after)
			}
		})
}

func TestUpdateMinAndMax(t *testing.T) {
	updated(t, hoarddb.Map{"Name": "Ada", "Age": int32(36), "Best": int32(50)},
		hoarddb.Update{Min: hoarddb.Map{"Age": int32(30), "Floor": int32(5)}, Max: hoarddb.Map{"Best": int32(99), "Age2": int32(1)}},
		func(t *testing.T, after hoarddb.Document) {
			if intField(t, after, "Age") != 30 || intField(t, after, "Best") != 99 || intField(t, after, "Floor") != 5 || intField(t, after, "Age2") != 1 {
				t.Fatalf("Min keeps the smaller, Max the larger, and each sets an absent field: %s", after)
			}
		})
	updated(t, hoarddb.Map{"Name": "Ada", "Age": int32(20), "Best": int32(120)},
		hoarddb.Update{Min: hoarddb.Map{"Age": int32(30)}, Max: hoarddb.Map{"Best": int32(99)}},
		func(t *testing.T, after hoarddb.Document) {
			if intField(t, after, "Age") != 20 || intField(t, after, "Best") != 120 {
				t.Fatalf("a Min above and a Max below the stored value change nothing: %s", after)
			}
		})
}

func TestUpdateRename(t *testing.T) {
	updated(t, hoarddb.Map{"Name": "Ada", "Nick": "gh", "Address": hoarddb.Map{"Zip": "1001"}},
		hoarddb.Update{Rename: map[string]string{"Address.Zip": "Address.PostCode", "Nick": "Handle"}},
		func(t *testing.T, after hoarddb.Document) {
			if stringField(t, after, "Handle") != "gh" || stringField(t, after, "Address", "PostCode") != "1001" {
				t.Fatalf("Rename moves a field, a path included: %s", after)
			}
			if _, found := after.Lookup("Nick"); found {
				t.Fatalf("Rename left the old name in %s", after)
			}
		})
	updated(t, hoarddb.Map{"Name": "Ada", "Nick": "gh", "Handle": "ada"},
		hoarddb.Update{Rename: map[string]string{"Nick": "Handle"}},
		func(t *testing.T, after hoarddb.Document) {
			if stringField(t, after, "Handle") != "ada" || stringField(t, after, "Nick") != "gh" {
				t.Fatalf("Rename onto a name that exists does nothing: %s", after)
			}
		})
}

// onlyDocument is the one document a default pop answers, so a test that pops
// one entry reads it as a document. A pop that removed anything else is an
// error here, because a default pop removes exactly one.
// emptyList is nil when a list verb answered nothing without an error.
func emptyList(length int, err error) error {
	if err != nil {
		return err
	}
	if length != 0 {
		return fmt.Errorf("answered %d documents, want none", length)
	}
	return nil
}

func onlyDocument(documents []hoarddb.Document, err error) (hoarddb.Document, error) {
	if err != nil {
		return nil, err
	}
	if len(documents) != 1 {
		return nil, fmt.Errorf("a default pop answered %d documents, want 1", len(documents))
	}
	return documents[0], nil
}
