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

// Package hoarddb is the Go driver for hoardDB.
//
// [Connect] dials a server, verifies the server's identity, authenticates the
// caller, and returns a [Client]: a pool of connections in two lanes, safe for
// many goroutines. From it [Client.Database] and [Database.Bucket] give handles
// that cost a name and a pointer and do no I/O; databases and buckets are
// created, listed and dropped with [Client.CreateDatabase],
// [Database.CreateBucket] and their companions.
//
// A bucket's verbs are the same on every store type that answers them. Each
// store type answers these, and the server refuses the rest in a sentence that
// names the bucket, its type and the type the verb needs. A verb the type does
// not answer is [ErrInvalidArgument], never a miss: Exists on a type with no
// keys (fifo, lifo, heap) is that refusal, not false. Search is an index lookup
// on a hash bucket and a full scan on every other type but blob:
//
//	store type  verbs
//	hash        Put, Insert, InsertMany, Upsert, UpsertMany, Get (Consistent), Exists, Delete, Update, Find, FindOne, Search, Count, Distinct, Stats, Info
//	btree       Put, Insert, InsertMany, Upsert, UpsertMany, Get (Consistent), Exists, Delete, Update, Range, First, Last, Find, FindOne, Search, Count, Distinct, Stats, Info
//	fifo        Push, Pop, Peek, Claim, Ack, Release, Extend, Consume, DeadList, Requeue, Discard, InsertMany, Find, FindOne, Search, Count, Distinct, Stats, Info
//	lifo        Push, Pop, Peek, Claim, Ack, Release, Extend, Consume, DeadList, Requeue, Discard, InsertMany, Find, FindOne, Search, Count, Distinct, Stats, Info
//	heap        Push (WithPriority optional, 0 when omitted), Pop, Peek, Claim, Ack, Release, Extend, Consume, DeadList, Requeue, Discard, InsertMany, Find, FindOne, Search, Count, Distinct, Stats, Info
//	blob        PutBlob, GetBlob, Get, Exists, Delete, Count, Stats, Info
//
// Put inserts and refuses a key that already holds a document with
// ErrDocumentExists, while Upsert replaces the document or creates it.
//
// Pop removes up to [WithLimit] entries in one request and answers a list.
// [Bucket.Claim] takes entries under a lease instead and leaves them in the
// store; [Bucket.Ack] removes the ones it names, [Bucket.Release] and
// [Bucket.Extend] give them back or hold them longer, and [Bucket.Consume] is
// the claim, handle, ack loop. An entry delivered 5 times without an ack is
// dead-lettered: [Bucket.DeadList] reads those, [Bucket.Requeue] and
// [Bucket.Discard] move them.
//
// A bucket may also declare store fields: [Bucket.Field] names one document's
// own store of any of the six types, and [FieldStore] has the verbs of that type
// (Push, Pop, Peek, Put, Get, Delete, PutBlob, GetBlob, Len, List). [GetAs]
// reads a document straight into a struct.
//
// A document goes in as a struct, a [Map] or a [Document] and comes back as a
// [Document]: raw BSON, decoded when the caller asks. A miss is [ErrNotFound],
// and a pop, peek or claim of a store with nothing to take is an empty list and a nil error.
//
// Every call returns an [*Error]: match its class with errors.Is against the
// Err sentinels, and read [Error.Outcome] to learn whether a refused or
// interrupted write happened. The driver retries only what is safe to send
// twice: a read whose connection failed, once, on a fresh connection; a read
// the cluster cannot answer yet, or a request the server says it did not
// apply, with backoff until the caller's deadline; a connection refused for
// load. A write whose outcome is unknown is never resent; its error says so,
// and the caller reads the data back before sending it again.
package hoarddb
