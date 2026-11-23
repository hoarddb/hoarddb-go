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

package hoarddbwire

import "time"

// Queues, heaps and btree ranges (handbook/operations.md sections 2 and 3).
// The server routes a push, pop or peek by the bucket's store type, not the
// opcode, so one request type serves the FIFO, LIFO and HEAP families.

// BucketRequest names a bucket and nothing else: BUCKET_STATS.
type BucketRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
}

// PopRequest is the pop of every queue type. A pop consumes, so it is a write:
// it carries a write concern and is never retried. Limit is how many entries to
// remove and return, in pop order; absent or 0 means 1, and a negative one or
// one above 100 is refused.
type PopRequest struct {
	Database     string `bson:"database"`
	Bucket       string `bson:"bucket"`
	Limit        int64  `bson:"limit,omitempty"`
	WriteConcern string `bson:"writeconcern,omitempty"`
}

// PushRequest is the push of every queue type. Priority is required by a heap
// and ignored by the others; nil leaves it off the wire.
type PushRequest struct {
	Database     string   `bson:"database"`
	Bucket       string   `bson:"bucket"`
	Data         Document `bson:"data"`
	Priority     *int64   `bson:"priority,omitempty"`
	WriteConcern string   `bson:"writeconcern,omitempty"`
}

// PeekRequest is the peek of a bucket. Limit is how many entries to return, in
// pop order; absent or 0 means 1, and a negative one or one above 1000 is
// refused.
type PeekRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Limit    int64  `bson:"limit,omitempty"`
}

// RangeRequest is BTREE_RANGE, BTREE_FIRST and BTREE_LAST. The bounds are
// strings and an empty side is open.
type RangeRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Low      string `bson:"low,omitempty"`
	High     string `bson:"high,omitempty"`
	Limit    int64  `bson:"limit,omitempty"`
}

// PopResponse answers every pop with the entries it removed, in pop order. An
// empty queue or heap is Found false and a message ("queue is empty", "heap is
// empty"): a successful answer. Pending and Dead count the entries held under a
// lease and on the dead list, which a pop does not take; both are absent at
// zero.
type PopResponse struct {
	Found       bool       `bson:"found"`
	Items       []Document `bson:"items,omitempty"`
	Message     string     `bson:"message,omitempty"`
	Pending     int64      `bson:"pending,omitempty"`
	Dead        int64      `bson:"dead,omitempty"`
	WriteReport `bson:",inline"`
}

// PeekResponse answers a bucket peek: the next documents in pop order, none
// removed. Found is true when Items is not empty. Pending, Dead and Expired are
// the counts of entries held under a lease, on the dead list, and under a lease
// that has run out; each is absent at zero.
type PeekResponse struct {
	Found   bool       `bson:"found"`
	Items   []Document `bson:"items,omitempty"`
	Message string     `bson:"message,omitempty"`
	Pending int64      `bson:"pending,omitempty"`
	Dead    int64      `bson:"dead,omitempty"`
	Expired int64      `bson:"expired,omitempty"`
}

// RangeEntry is one key of a btree and its document.
type RangeEntry struct {
	Key   string   `bson:"key"`
	Value Document `bson:"value,omitempty"`
}

// RangeResponse answers BTREE_RANGE, BTREE_FIRST and BTREE_LAST.
type RangeResponse struct {
	Entries []RangeEntry `bson:"entries,omitempty"`
	Message string       `bson:"message,omitempty"`
}

// ClaimRequest is QUEUE_CLAIM: take up to Limit entries in pop order under a
// lease of LeaseMS milliseconds, leaving them in the store. Absent or 0 mean 1
// entry and 30,000 ms; the server refuses a limit above 100 and a lease outside
// 1,000 to 3,600,000 ms (testdata/driver-vectors/consume-constants.json). It is
// a write: it carries a write concern and is never retried.
type ClaimRequest struct {
	Database     string `bson:"database"`
	Bucket       string `bson:"bucket"`
	Limit        int64  `bson:"limit,omitempty"`
	LeaseMS      int64  `bson:"lease_ms,omitempty"`
	WriteConcern string `bson:"writeconcern,omitempty"`
}

// ClaimResponse answers QUEUE_CLAIM. Items, ItemIDs and Deliveries are
// parallel: the i-th item is settled by the i-th item id, and Deliveries is
// absent when every value is 1. Found false with a message is nothing to take
// or a refusal; Returned counts expired leases the same request gave back, so
// Found false with Returned above 0 is a queue to claim again at once.
type ClaimResponse struct {
	Found         bool       `bson:"found"`
	Items         []Document `bson:"items,omitempty"`
	ItemIDs       []string   `bson:"item_ids,omitempty"`
	Deliveries    []int64    `bson:"deliveries,omitempty"`
	LeaseID       string     `bson:"lease_id,omitempty"`
	LeaseMS       int64      `bson:"lease_ms,omitempty"`
	LeaseDeadline time.Time  `bson:"lease_deadline,omitempty"`
	Pending       int64      `bson:"pending,omitempty"`
	Dead          int64      `bson:"dead,omitempty"`
	Returned      int64      `bson:"returned,omitempty"`
	Message       string     `bson:"message,omitempty"`
	WriteReport   `bson:",inline"`
}

// LeaseRequest is QUEUE_ACK, QUEUE_RELEASE and QUEUE_EXTEND, which share a
// shape: the lease, and ItemIDs on an ack (at most 100; absent for the whole
// lease) or LeaseMS on an extend (required, 1,000 to 3,600,000). Each is a
// write and is never retried.
type LeaseRequest struct {
	Database     string   `bson:"database"`
	Bucket       string   `bson:"bucket"`
	LeaseID      string   `bson:"lease_id"`
	ItemIDs      []string `bson:"item_ids,omitempty"`
	LeaseMS      int64    `bson:"lease_ms,omitempty"`
	WriteConcern string   `bson:"writeconcern,omitempty"`
}

// LeaseResponse answers QUEUE_ACK, QUEUE_RELEASE and QUEUE_EXTEND. LeaseState is
// acked, released, extended, expired or unknown; the last two are Success false,
// in band, with a message naming the fix.
type LeaseResponse struct {
	Success       bool      `bson:"success"`
	LeaseState    string    `bson:"lease_state,omitempty"`
	Acked         int64     `bson:"acked,omitempty"`
	Missing       int64     `bson:"missing,omitempty"`
	Remaining     int64     `bson:"remaining,omitempty"`
	Released      int64     `bson:"released,omitempty"`
	LeaseMS       int64     `bson:"lease_ms,omitempty"`
	LeaseDeadline time.Time `bson:"lease_deadline,omitempty"`
	Message       string    `bson:"message,omitempty"`
	WriteReport   `bson:",inline"`
}

// DeadListRequest is DEAD_LIST, a read: up to Limit dead entries (1 to 1,000,
// absent for 10), starting past the dead id After.
type DeadListRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Limit    int64  `bson:"limit,omitempty"`
	After    string `bson:"after,omitempty"`
}

// DeadEntry is one dead entry: the document, how many times it was delivered
// and when it died, and why (max_deliveries or collided).
type DeadEntry struct {
	DeadID     string    `bson:"dead_id"`
	Data       Document  `bson:"data,omitempty"`
	Deliveries int64     `bson:"deliveries,omitempty"`
	DeadAt     time.Time `bson:"dead_at,omitempty"`
	Reason     string    `bson:"reason,omitempty"`
}

// DeadListResponse answers DEAD_LIST. An empty list is Found false with no
// message; Found false with a message is a refusal. Next is the last dead id
// listed, when more remain.
type DeadListResponse struct {
	Found   bool        `bson:"found"`
	Items   []DeadEntry `bson:"items,omitempty"`
	Next    string      `bson:"next,omitempty"`
	Message string      `bson:"message,omitempty"`
}

// DeadMoveRequest is DEAD_REQUEUE and DEAD_DISCARD: one entry named by DeadID,
// or All of them, at most 100 considered per request; After starts an All
// request past a dead id and is refused with DeadID. Each is a write and is never
// retried.
type DeadMoveRequest struct {
	Database     string `bson:"database"`
	Bucket       string `bson:"bucket"`
	DeadID       string `bson:"dead_id,omitempty"`
	All          bool   `bson:"all,omitempty"`
	After        string `bson:"after,omitempty"`
	WriteConcern string `bson:"writeconcern,omitempty"`
}

// DeadRefusal is an entry a requeue or discard left where it was, and why.
type DeadRefusal struct {
	DeadID  string `bson:"dead_id"`
	Message string `bson:"message"`
}

// DeadMoveResponse answers DEAD_REQUEUE and DEAD_DISCARD: Done entries moved and
// Left still on the dead list. Success false with Refused names the entries
// that stayed, while those that moved stay moved.
type DeadMoveResponse struct {
	Success     bool          `bson:"success"`
	Done        int64         `bson:"done"`
	Left        int64         `bson:"left,omitempty"`
	Refused     []DeadRefusal `bson:"refused,omitempty"`
	Message     string        `bson:"message,omitempty"`
	WriteReport `bson:",inline"`
}
