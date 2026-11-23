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
	"context"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// Entity is a handle on one document of a bucket: the thing the data is about, a user, an order, a
// device. Its methods are the document methods of [Bucket] with the id already given, and
// [Entity.Store] reaches the stores the document carries:
//
//	ada := client.Bucket("app", "User").Entity("ada")
//	ada.Upsert(ctx, ring.Doc{"Name": "Ada"})
//	ada.Store("Purchases").Push(ctx, purchase)
//
// A handle holds the bucket and the id and sends nothing until a method is called. It is safe for
// concurrent use unless its bucket came from [Bucket.In], because a [Batch] is not. Each method
// sends the request the Bucket method of the same name sends and returns its answer and its errors
// unchanged. Count, List and GetMany read across documents and stay on [Bucket] and [Client].
type Entity struct {
	bucket *Bucket
	id     string
}

// Store is a handle on one store field of one document: that document's own queue, stack, map,
// ordered map, heap or blob, of the type the bucket declares for the field. Its methods are the store,
// lease and aggregate methods of [Bucket] with the id and the field already given. Five are named
// without the word Store: Get is [Bucket.StoreGet], List is [Bucket.StoreList], ListExpanded is
// [Bucket.StoreListExpanded], Count is [Bucket.StoreCount] and Describe is [Bucket.DescribeStore].
//
// A handle sends nothing until a method is called, and is as safe for concurrent use as its [Entity].
// A field the bucket does not declare, or one that is not a store, is refused by the first call,
// with the store fields the bucket has.
type Store struct {
	bucket    *Bucket
	id, field string
}

// Entity returns the handle for document id of the bucket. It sends nothing, and the document need
// not exist: [Entity.Insert] or [Entity.Upsert] creates it. A handle made from a bucket of
// [Bucket.In] queues its writes on that batch.
func (b *Bucket) Entity(id string) *Entity { return &Entity{bucket: b, id: id} }

// Field returns the handle for store field of document id; it is Entity(id).Store(field) in one call.
func (b *Bucket) Field(id, field string) *Store { return &Store{bucket: b, id: id, field: field} }

// Store returns the handle for the document's store field of that name. It sends nothing.
func (e *Entity) Store(field string) *Store {
	return &Store{bucket: e.bucket, id: e.id, field: field}
}

// Get reads the document as stored: its scalar fields, not its stores. A missing document is an
// error wrapping [ErrNotFound]. See [Bucket.Get].
func (e *Entity) Get(ctx context.Context) (bson.Raw, error) {
	return e.bucket.Get(ctx, e.id)
}

// GetFields is Get returning only _id_ and the named scalar fields. See [Bucket.GetFields].
func (e *Entity) GetFields(ctx context.Context, fields []string) (bson.Raw, error) {
	return e.bucket.GetFields(ctx, e.id, fields)
}

// GetExpanded reads the document with the documents its fields point at, in one call, and returns
// the document and the expansion document. See [Bucket.GetExpanded].
func (e *Entity) GetExpanded(ctx context.Context, fields []string, expand []ExpandRequest, local bool) (document, expanded bson.Raw, err error) {
	return e.bucket.GetExpanded(ctx, e.id, fields, expand, local)
}

// Insert creates the document with the given scalar fields. A document that exists is an error.
// See [Bucket.Insert].
func (e *Entity) Insert(ctx context.Context, doc Doc, options ...CallOption) (WriteResult, error) {
	return e.bucket.Insert(ctx, e.id, doc, options...)
}

// Upsert creates the document or replaces its scalar fields; its stores are kept. See
// [Bucket.Upsert].
func (e *Entity) Upsert(ctx context.Context, doc Doc, options ...CallOption) (WriteResult, error) {
	return e.bucket.Upsert(ctx, e.id, doc, options...)
}

// Update changes scalar fields of the document, which must exist. See [Bucket.Update].
func (e *Entity) Update(ctx context.Context, change Update, options ...CallOption) (WriteResult, error) {
	return e.bucket.Update(ctx, e.id, change, options...)
}

// Delete removes the document and the stores it carries. See [Bucket.Delete].
func (e *Entity) Delete(ctx context.Context, options ...CallOption) (WriteResult, error) {
	return e.bucket.Delete(ctx, e.id, options...)
}

// Push appends element, a BSON document, to a fifoStore or lifoStore. See [Bucket.Push].
func (s *Store) Push(ctx context.Context, element any, options ...CallOption) (WriteResult, error) {
	return s.bucket.Push(ctx, s.id, s.field, element, options...)
}

// Put stores element under key in a hashStore or btreeStore, replacing an element that has the
// key. See [Bucket.Put].
func (s *Store) Put(ctx context.Context, key string, element any, options ...CallOption) (WriteResult, error) {
	return s.bucket.Put(ctx, s.id, s.field, key, element, options...)
}

// Inc adds to the numbers of the element stored under key in a hashStore or btreeStore, creating
// the element when the key is absent. See [Bucket.Inc] for the paths, the number types and what a
// retry does.
func (s *Store) Inc(ctx context.Context, key string, increments map[string]any, options ...CallOption) (WriteResult, error) {
	return s.bucket.Inc(ctx, s.id, s.field, key, increments, options...)
}

// IncAfter is [Store.Inc] returning the element as it stands after the add. See [Bucket.IncAfter].
func (s *Store) IncAfter(ctx context.Context, key string, increments map[string]any, options ...CallOption) (bson.Raw, WriteResult, error) {
	return s.bucket.IncAfter(ctx, s.id, s.field, key, increments, options...)
}

// Pop removes and returns the next element of a fifoStore (the oldest) or lifoStore (the newest).
// An empty store is an error wrapping [ErrNotFound]. See [Bucket.Pop].
func (s *Store) Pop(ctx context.Context, options ...CallOption) (bson.Raw, error) {
	return s.bucket.Pop(ctx, s.id, s.field, options...)
}

// PopN removes and returns up to n elements (1 to 100) in one write. An empty store gives an empty
// slice and no error. See [Bucket.PopN].
func (s *Store) PopN(ctx context.Context, n int, options ...CallOption) ([]bson.Raw, error) {
	return s.bucket.PopN(ctx, s.id, s.field, n, options...)
}

// Take removes and returns the element stored under key in a hashStore or btreeStore. An absent
// key is an error wrapping [ErrNotFound]. See [Bucket.Take].
func (s *Store) Take(ctx context.Context, key string, options ...CallOption) (bson.Raw, error) {
	return s.bucket.Take(ctx, s.id, s.field, key, options...)
}

// Peek returns up to count elements (1 to 100) a pop would take, next first, without removing
// them. See [Bucket.Peek].
func (s *Store) Peek(ctx context.Context, count int) ([]bson.Raw, error) {
	return s.bucket.Peek(ctx, s.id, s.field, count)
}

// PeekKeyed is Peek that also returns the entry ids and priorities of a heapStore's elements. See
// [Bucket.PeekKeyed].
func (s *Store) PeekKeyed(ctx context.Context, count int) (ElementPage, error) {
	return s.bucket.PeekKeyed(ctx, s.id, s.field, count)
}

// Nearest returns the K elements of the store closest to query.Vector, best first, with their keys
// and scores. See [Bucket.Nearest].
func (s *Store) Nearest(ctx context.Context, query NearestQuery) (NearestResult, error) {
	return s.bucket.Nearest(ctx, s.id, s.field, query)
}

// Claim leases up to count messages from a fifoStore or lifoStore without removing them. Ack the
// lease when they are handled, Release it to put them back, or let it run out. See [Bucket.Claim].
func (s *Store) Claim(ctx context.Context, count int, options ...CallOption) (Lease, error) {
	return s.bucket.Claim(ctx, s.id, s.field, count, options...)
}

// Ack confirms a lease: its messages are removed for good. See [Bucket.Ack].
func (s *Store) Ack(ctx context.Context, lease [16]byte, options ...CallOption) (LeaseChange, error) {
	return s.bucket.Ack(ctx, s.id, s.field, lease, options...)
}

// Release gives a lease back: its messages return to the store. See [Bucket.Release].
func (s *Store) Release(ctx context.Context, lease [16]byte, options ...CallOption) (LeaseChange, error) {
	return s.bucket.Release(ctx, s.id, s.field, lease, options...)
}

// Extend moves a live lease's deadline to leaseMs milliseconds (1000 to 3600000) from now. See
// [Bucket.Extend].
func (s *Store) Extend(ctx context.Context, lease [16]byte, leaseMs int64, options ...CallOption) (LeaseChange, error) {
	return s.bucket.Extend(ctx, s.id, s.field, lease, leaseMs, options...)
}

// LeaseGet reads the messages a lease still holds, so a client that lost the reply of a claim can
// recover them. See [Bucket.LeaseGet].
func (s *Store) LeaseGet(ctx context.Context, lease [16]byte) (LeaseRead, error) {
	return s.bucket.LeaseGet(ctx, s.id, s.field, lease)
}

// Aggregate reads a maintained aggregate of the store; the aggregate must be declared on the field
// when the bucket is created. See [Bucket.Aggregate].
func (s *Store) Aggregate(ctx context.Context, query AggregateQuery) (AggregatePage, error) {
	return s.bucket.Aggregate(ctx, s.id, s.field, query)
}

// Sum returns the maintained sum of elementField over the store: 0 for an empty store. See
// [Bucket.Sum].
func (s *Store) Sum(ctx context.Context, elementField string) (AggregateNumber, error) {
	return s.bucket.Sum(ctx, s.id, s.field, elementField)
}

// SumBy returns one page of the sums of elementField per value of by, in by order. See
// [Bucket.SumBy].
func (s *Store) SumBy(ctx context.Context, elementField, by string, limit int, after []byte) (AggregatePage, error) {
	return s.bucket.SumBy(ctx, s.id, s.field, elementField, by, limit, after)
}

// CountBy returns one page of the element counts per value of by, in by order. See
// [Bucket.CountBy].
func (s *Store) CountBy(ctx context.Context, by string, limit int, after []byte) (AggregatePage, error) {
	return s.bucket.CountBy(ctx, s.id, s.field, by, limit, after)
}

// Get reads the element a hashStore or btreeStore holds under key, or a heapStore holds under entry
// id key, without removing it. An absent key is an error wrapping [ErrNotFound]. It is
// [Bucket.StoreGet].
func (s *Store) Get(ctx context.Context, key string) (bson.Raw, error) {
	return s.bucket.StoreGet(ctx, s.id, s.field, key)
}

// List reads one page of the store, limit elements at most (0 means 100, the ceiling is 1000).
// Pass the previous page's Cursor as after to continue; nil starts at the first element. It is
// [Bucket.StoreList].
func (s *Store) List(ctx context.Context, limit int, after []byte) (ElementPage, error) {
	return s.bucket.StoreList(ctx, s.id, s.field, limit, after)
}

// ListExpanded is List of a hashStore or btreeStore that also reads, by key, the document each
// element key names in the far bucket, in one round trip. It is [Bucket.StoreListExpanded].
func (s *Store) ListExpanded(ctx context.Context, limit int, after []byte, expand *ExpandRequest, local bool) (ElementPage, error) {
	return s.bucket.StoreListExpanded(ctx, s.id, s.field, limit, after, expand, local)
}

// Count returns how many elements the store holds. It is [Bucket.StoreCount].
func (s *Store) Count(ctx context.Context) (int64, error) {
	return s.bucket.StoreCount(ctx, s.id, s.field)
}

// Describe returns every aggregate the store keeps and its current value. It is
// [Bucket.DescribeStore].
func (s *Store) Describe(ctx context.Context) (bson.Raw, error) {
	return s.bucket.DescribeStore(ctx, s.id, s.field)
}
