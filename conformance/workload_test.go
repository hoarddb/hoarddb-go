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
	"math/rand"
	"sync"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/internal/codec"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// workloadSize is how many documents leg 4 writes to each store.
const workloadSize = 200

// workload counts what a leg 4 run read back. A driver that returns nothing and
// reports no error shows up as misses, or as a count short of the size, and
// fails here.
type workload struct {
	t      *testing.T
	hits   int
	misses int
}

// sample is the i-th document: every BSON type the vectors exercise, written as
// canonical Extended JSON so each integer width is explicit. It is both what is
// written (as a Document) and what a read must equal, member by member and type
// by type.
func sample(index int) (hoarddb.Document, map[string]any, error) {
	text := fmt.Sprintf(`{
		"Name": "user-%[1]d",
		"Index": {"$numberInt": "%[1]d"},
		"Big": {"$numberLong": "%[2]d"},
		"Score": {"$numberDouble": "%[1]d.5"},
		"Active": %[3]t,
		"At": {"$date": {"$numberLong": "%[4]d"}},
		"Tags": ["a", "b", {"$numberInt": "%[1]d"}],
		"Address": {"City": "London", "Zip": {"$numberInt": "%[5]d"}},
		"Payload": {"$binary": {"base64": "aGVsbG8sIGhvYXJkREI=", "subType": "00"}},
		"Owner": {"$oid": "507f1f77bcf86cd799439011"},
		"Price": {"$numberDecimal": "%[1]d.25"}
	}`, index, int64(index)*1_000_000_007, index%2 == 0, 1767225600000+int64(index), 1000+index)
	encoded, err := codec.FromExtendedJSON([]byte(text))
	if err != nil {
		return nil, nil, err
	}
	tree, err := conformance.ParseExtended([]byte(text))
	if err != nil {
		return nil, nil, err
	}
	return encoded, tree, nil
}

// expect reads one stored document back and checks it equals sample(index),
// apart from _id_, the key the server adds. A hit is a document that is there
// and equal; a document that is not there is a miss.
func (run *workload) expect(index int, got hoarddb.Document, err error) {
	run.t.Helper()
	if errors.Is(err, hoarddb.ErrNotFound) {
		run.misses++
		run.t.Errorf("document %d: %v", index, err)
		return
	}
	if err != nil {
		run.t.Errorf("document %d: %v", index, err)
		return
	}
	_, want, sampleErr := sample(index)
	if sampleErr != nil {
		run.t.Fatal(sampleErr)
	}
	tree, err := conformance.WireTree(got)
	if err != nil {
		run.t.Errorf("document %d is not BSON: %v", index, err)
		return
	}
	if _, hasKey := tree["_id_"]; !hasKey {
		run.t.Errorf("document %d has no _id_: %s", index, got)
		return
	}
	delete(tree, "_id_")
	if difference := conformance.Diff(want, tree); difference != "" {
		run.t.Errorf("document %d differs from what was written: %s", index, difference)
		return
	}
	run.hits++
}

// finish reports the line the leg prints and fails unless every document came
// back.
func (run *workload) finish(name string, want int) {
	run.t.Helper()
	run.t.Logf("leg 4 workload %s: hits=%d misses=%d", name, run.hits, run.misses)
	if run.hits != want || run.misses != 0 {
		run.t.Fatalf("leg 4 workload %s: hits=%d misses=%d, want hits=%d misses=0", name, run.hits, run.misses, want)
	}
}

// TestLeg4Workload is conformance leg 4 (handbook/testing.md section 2): per
// store type, write documents through the public API, read them back, and
// assert every value is equal.
func TestLeg4Workload(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := hoarddb.Connect(ctx, fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s",
		server.User, server.Password, server.Address, server.Fingerprint))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	database := client.Database("workload")
	bucketOf := func(t *testing.T, kind hoarddb.StoreType) *hoarddb.Bucket {
		t.Helper()
		bucket, err := database.CreateBucket(ctx, string(kind), kind)
		if err != nil {
			t.Fatal(err)
		}
		return bucket
	}

	// writeConcurrently stores every document with four writers sharing one
	// client, as an application does, and returns the receipts by index.
	writeConcurrently := func(t *testing.T, put func(index int, doc hoarddb.Document) (string, error)) []string {
		t.Helper()
		keys := make([]string, workloadSize)
		var group sync.WaitGroup
		for worker := range 4 {
			group.Add(1)
			go func() {
				defer group.Done()
				for index := worker; index < workloadSize; index += 4 {
					doc, _, err := sample(index)
					if err != nil {
						t.Error(err)
						return
					}
					if keys[index], err = put(index, doc); err != nil {
						t.Errorf("writing document %d: %v", index, err)
						return
					}
				}
			}()
		}
		group.Wait()
		return keys
	}

	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, kind)
			keys := writeConcurrently(t, func(index int, doc hoarddb.Document) (string, error) {
				if index%4 == 3 { // a quarter of the documents get a server-assigned key
					receipt, err := bucket.Insert(ctx, doc)
					return receipt.ID, err
				}
				key := fmt.Sprintf("key-%04d", index)
				_, err := bucket.Put(ctx, key, doc)
				return key, err
			})
			run := &workload{t: t}
			for index, key := range keys {
				got, err := bucket.Get(ctx, key)
				run.expect(index, got, err)
				if found, err := bucket.Exists(ctx, key); err != nil || !found {
					t.Errorf("Exists(%q) = %v, %v after a write", key, found, err)
				}
			}
			run.finish(string(kind), workloadSize)
			if stats, err := bucket.Stats(ctx); err != nil || stats.Entries != workloadSize {
				t.Errorf("Stats = %+v, %v; want %d entries", stats, err, workloadSize)
			}
			if kind == hoarddb.BTree {
				checkBTreeOrder(t, ctx, bucket, keys)
			}
		})
	}

	for _, kind := range []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO, hoarddb.Heap} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, kind)
			// A heap pops its highest priority first; push in a shuffled order so
			// the arrival order is not the answer.
			arrival := rand.New(rand.NewSource(7)).Perm(workloadSize)
			for _, index := range arrival {
				doc, _, err := sample(index)
				if err != nil {
					t.Fatal(err)
				}
				var options []hoarddb.WriteOption
				if kind == hoarddb.Heap {
					options = append(options, hoarddb.WithPriority(int64(index)))
				}
				if _, err := bucket.Push(ctx, doc, options...); err != nil {
					t.Fatalf("Push: %v", err)
				}
			}
			run := &workload{t: t}
			for position := range workloadSize {
				got, err := bucket.Pop(ctx)
				if err == nil && len(got) != 1 {
					t.Fatalf("Pop returned %d documents, want 1", len(got))
				}
				if err == nil {
					run.expect(popped(kind, arrival, position), got[0], err)
					continue
				}
				run.expect(popped(kind, arrival, position), nil, err)
			}
			run.finish(string(kind), workloadSize)
			if drained, err := bucket.Pop(ctx); err != nil || len(drained) != 0 {
				t.Errorf("Pop of a drained %s = %v, %v, want an empty list and a nil error", kind, drained, err)
			}
		})
	}

	// The same documents drained by claim and ack, in batches: the order is the
	// pop order, every value is equal, and what was acknowledged is gone.
	for _, kind := range []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO, hoarddb.Heap} {
		t.Run(string(kind)+"/claim", func(t *testing.T) {
			bucket, err := database.CreateBucket(ctx, string(kind)+"-claimed", kind)
			if err != nil {
				t.Fatal(err)
			}
			arrival := rand.New(rand.NewSource(11)).Perm(workloadSize)
			for _, index := range arrival {
				doc, _, err := sample(index)
				if err != nil {
					t.Fatal(err)
				}
				var options []hoarddb.WriteOption
				if kind == hoarddb.Heap {
					options = append(options, hoarddb.WithPriority(int64(index)))
				}
				if _, err := bucket.Push(ctx, doc, options...); err != nil {
					t.Fatalf("Push: %v", err)
				}
			}
			run := &workload{t: t}
			position := 0
			for {
				claimed, err := bucket.Claim(ctx, hoarddb.WithLimit(25))
				if err != nil {
					t.Fatalf("Claim: %v", err)
				}
				if len(claimed.Items) == 0 {
					break
				}
				for _, delivery := range claimed.Items {
					run.expect(popped(kind, arrival, position), delivery.Document, nil)
					position++
				}
				if acked, err := bucket.Ack(ctx, claimed.LeaseID); err != nil || acked.Acked != int64(len(claimed.Items)) {
					t.Fatalf("Ack = %+v, %v; want %d acknowledged", acked, err, len(claimed.Items))
				}
			}
			run.finish(string(kind)+"/claim", workloadSize)
			if drained, err := bucket.Pop(ctx); err != nil || len(drained) != 0 {
				t.Errorf("Pop after every claim was acknowledged = %v, %v, want an empty list and a nil error", drained, err)
			}
		})
	}
}

// popped is the document the position-th pop must return: a fifo returns the
// first pushed, a lifo the last pushed, a heap the highest priority.
func popped(kind hoarddb.StoreType, arrival []int, position int) int {
	switch kind {
	case hoarddb.FIFO:
		return arrival[position]
	case hoarddb.LIFO:
		return arrival[len(arrival)-1-position]
	default:
		return workloadSize - 1 - position
	}
}

// checkBTreeOrder reads the whole tree back by range, first and last.
func checkBTreeOrder(t *testing.T, ctx context.Context, bucket *hoarddb.Bucket, keys []string) {
	t.Helper()
	cursor, err := bucket.Range(ctx, hoarddb.Bounds{}, hoarddb.PageSize(37))
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(ctx)
	if err != nil || len(documents) != len(keys) {
		t.Fatalf("Range read %d documents, %v; want %d", len(documents), err, len(keys))
	}
	previous := ""
	for position, document := range documents {
		tree, err := conformance.WireTree(document)
		if err != nil {
			t.Fatal(err)
		}
		key, _ := tree["_id_"].(string)
		if position > 0 && key <= previous {
			t.Fatalf("Range returned %q after %q: a btree reads in key order", key, previous)
		}
		previous = key
	}
	first, err := bucket.First(ctx)
	if err != nil || first.Key != documentKey(t, documents[0]) {
		t.Errorf("First = %q, %v; want the range's first key", first.Key, err)
	}
	last, err := bucket.Last(ctx)
	if err != nil || last.Key != documentKey(t, documents[len(documents)-1]) {
		t.Errorf("Last = %q, %v; want the range's last key", last.Key, err)
	}
}

func documentKey(t *testing.T, document hoarddb.Document) string {
	t.Helper()
	value, found := document.Lookup("_id_")
	if !found {
		t.Fatalf("document %s has no _id_", document)
	}
	key, _ := value.AsString()
	return key
}
