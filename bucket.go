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

package hoarddb

import (
	"context"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Bucket is the unit of storage. A handle is a name and a pointer to its
// database: it does no I/O and holds no connection, goroutine, timer, cache
// entry or metric.
type Bucket struct {
	db   *Database
	name string
	// kind is the store type [Database.Open] or [Database.CreateBucket] learned;
	// empty on a handle from [Database.Bucket], which looks nothing up.
	kind StoreType
}

// Bucket returns a handle on the named bucket. It does no I/O, and it does not
// look the bucket's store type up: a verb the type does not answer is the
// server's refusal, passed through.
func (d *Database) Bucket(name string) *Bucket { return &Bucket{db: d, name: name} }

// Type is the bucket's store type when the handle came from [Database.Open] or
// [Database.CreateBucket], which know it, and empty when it came from
// [Database.Bucket], which does no I/O. It is read off the handle and never asks
// the server; use [Bucket.Info] for the current declaration.
func (b *Bucket) Type() StoreType { return b.kind }

// Drop drops the bucket and its data, at once. It is [Database.DropBucket] on
// the handle: a bucket that does not exist is [ErrNotFound]. The handle is of
// no further use afterwards.
func (b *Bucket) Drop(ctx context.Context) error { return b.db.DropBucket(ctx, b.name) }

// StoreStat is one store field of a bucket, as [BucketStats] reports it.
type StoreStat struct {
	// Name is the field and Type its store type.
	Name string
	Type StoreType
	// Parent is the field this one is nested inside, empty at the top level.
	Parent string
	// Elements is how many elements the field's stores hold in all.
	Elements int64
}

// BucketCopies is the bucket owner's view of its copies on a cluster, as
// [Database.ListBuckets], [Bucket.Info] and [Bucket.Stats] report it. It is
// embedded, so its members read as fields of the result. All are the zero
// value from a server that sends none.
type BucketCopies struct {
	// InSync is 1 plus the addressees whose stream from the owner is not
	// stalled; 0 when the server did not say.
	InSync int
	// State is "ok", "copying", "under-replicated", "owner-elsewhere" (the row
	// is a node that is not the owner and carries nothing else) or "standalone";
	// Stats adds "owner-unreachable" and "taking-over". A word a newer server
	// adds arrives as it was sent. Empty when the server did not say.
	State string
	// NotInSync names the addressees whose stream is stalled.
	NotInSync []string
	// PendingCopies are the members still to receive their copy.
	PendingCopies []PendingCopy
}

// PendingCopy is a member still to receive its copy of a bucket.
type PendingCopy struct {
	// Node is the member's id.
	Node string
	// Since is when the alter that placed the member applied, in Unix seconds
	// on the owner's clock.
	Since int64
}

func bucketCopiesOf(wireInSync int64, state string, notInSync []string, pending []hoarddbwire.PendingCopy) BucketCopies {
	copies := BucketCopies{InSync: int(wireInSync), State: state, NotInSync: notInSync}
	for _, member := range pending {
		copies.PendingCopies = append(copies.PendingCopies, PendingCopy{Node: member.Node, Since: member.Since})
	}
	return copies
}

// FieldInfo is one declared field of a bucket.
type FieldInfo struct {
	Name     string
	Type     string
	Required bool
}

// BucketStats is what the server reports about one bucket.
type BucketStats struct {
	Database, Bucket string
	Type             StoreType
	// Entries is how many documents the bucket holds and LogicalBytes their
	// size, when the server reports it.
	Entries, LogicalBytes int64
	// Indexes describes the secondary indexes in the server's words: a name,
	// with " (unique)" after a unique one.
	Indexes []string
	// Stores are the declared store fields and Fields the declared field
	// types; FieldTypes says how they are enforced, empty when none are
	// declared.
	Stores     []StoreStat
	Fields     []FieldInfo
	FieldTypes TypeMode
	// Encrypted is the server's word for whether the bucket is encrypted: "no"
	// when it is not.
	Encrypted string
	// ReplicationFactor is how many copies the bucket keeps; 0 means the server
	// sent none. Inherited says the factor comes from the bucket's database
	// rather than from the bucket; false when absent.
	ReplicationFactor int
	Inherited         bool
	// Placement is "whole" or "spread"; empty when the server sent none.
	Placement Placement
	// BucketCopies is the owner's view of the copies; Stats is always answered
	// by the owner.
	BucketCopies
}

// Stats reports the bucket's size, indexes, store fields and declarations. A
// bucket that does not exist is [ErrNotFound].
func (b *Bucket) Stats(ctx context.Context) (*BucketStats, error) {
	target := b.db.name + "/" + b.name
	var answer hoarddbwire.BucketStatsResponse
	err := b.db.client.do(ctx, call{op: "stats", target: target, opcode: hoarddbwire.OpBucketStats, answer: hoarddbwire.OpBucketStats,
		request: hoarddbwire.BucketRequest{Database: b.db.name, Bucket: b.name}, response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Found {
		return nil, &Error{Code: CodeNotFound, Op: "stats", Target: target, Message: answer.Message}
	}
	stats := &BucketStats{
		Database: answer.Database, Bucket: answer.Bucket, Type: StoreType(answer.Type),
		Entries: answer.Entries, LogicalBytes: answer.LogicalBytes, Indexes: answer.Indexes,
		FieldTypes: TypeMode(answer.FieldTypes), Encrypted: answer.Encrypted,
		ReplicationFactor: int(answer.ReplicationFactor), Inherited: answer.Inherited, Placement: Placement(answer.Placement),
		BucketCopies: bucketCopiesOf(answer.InSync, answer.State, answer.NotInSync, answer.PendingCopies),
	}
	for _, store := range answer.Stores {
		stats.Stores = append(stats.Stores, StoreStat{Name: store.Name, Type: StoreType(store.Engine), Parent: store.Parent, Elements: store.Elements})
	}
	for _, field := range answer.Fields {
		stats.Fields = append(stats.Fields, FieldInfo{Name: field.Name, Type: field.Type, Required: field.Required})
	}
	return stats, nil
}

// BucketDeclaration is what a bucket is declared as, as [Bucket.Info] reports
// it. It carries no element counts of the bucket's store fields (Stats does).
type BucketDeclaration struct {
	Name string
	// Type is the bucket's own store type.
	Type StoreType
	// Entries is how many documents the bucket holds, not how many elements its
	// store fields hold.
	Entries int64
	// Stores are the declared top-level store field names, in declaration order.
	Stores []string
	// StoreFields are the same fields with their engines, in the same order;
	// empty when the bucket has none.
	StoreFields []StoreFieldInfo
	// ReplicationFactor is how many copies the bucket keeps; 0 means the server
	// sent none (not reported).
	ReplicationFactor int
	// Inherited says the replication factor comes from the bucket's database;
	// false when the server did not say.
	Inherited bool
	// BucketCopies is the owner's view of the copies.
	BucketCopies
}

// StoreFieldInfo is one declared store field of a bucket, in [BucketDeclaration].
type StoreFieldInfo struct {
	Name string
	// Engine is the server's catalog word as it sent it: [Hash], [BTree], [FIFO],
	// [LIFO], [Heap] or [Blob]. A word a newer server adds arrives as a StoreType
	// no constant names, and is never dropped: treat it as opaque.
	Engine StoreType
}

// Info returns the bucket's declaration: its store type, its entry count and
// its top-level store fields, in one round trip. It is read-only and safe to
// retry, and its cost does not depend on how many elements the store fields
// hold: it never walks them (use [Bucket.Stats] for element counts). A bucket or
// database that does not exist is [ErrNotFound]; a caller without read
// permission is [ErrPermissionDenied]. A store nested inside an element is not
// listed. Nothing is cached on the handle: the next Info asks the server again.
func (b *Bucket) Info(ctx context.Context) (*BucketDeclaration, error) {
	target := b.path()
	var answer hoarddbwire.BucketInfoResponse
	err := b.db.client.do(ctx, call{op: "info", target: target, opcode: hoarddbwire.OpBucketInfo, answer: hoarddbwire.OpBucketInfo,
		request: hoarddbwire.BucketInfoRequest{Database: b.db.name, Bucket: b.name}, response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Found {
		// The server answers a missing bucket with an ERROR frame; this is a
		// reply that says found false instead.
		return nil, &Error{Code: CodeNotFound, Op: "info", Target: target,
			Message: "the server says bucket " + target + " was not found. Run 'hoardDB bucket list " + b.db.name + "' to see what exists."}
	}
	declaration := &BucketDeclaration{
		Name: answer.Bucket.Name, Type: StoreType(answer.Bucket.Type), Entries: answer.Bucket.Entries,
		Stores: answer.Bucket.Stores, ReplicationFactor: int(answer.Bucket.ReplicationFactor), Inherited: answer.Bucket.Inherited,
		BucketCopies: bucketCopiesOf(answer.Bucket.InSync, answer.Bucket.State, answer.Bucket.NotInSync, answer.Bucket.PendingCopies),
	}
	for _, field := range answer.StoreFields {
		declaration.StoreFields = append(declaration.StoreFields, StoreFieldInfo{Name: field.Name, Engine: StoreType(field.Engine)})
	}
	return declaration, nil
}
