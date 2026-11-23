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

// The document family: keyed writes and reads on hash and btree buckets,
// blobs, and queries (handbook/operations.md sections 2 and 3).

// KeyRequest names one document: GET, EXISTS, BTREE_LOOKUP, CONSISTENT_GET
// and BLOB_READ.
type KeyRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Key      string `bson:"key"`
}

// PutRequest is PUT and BTREE_INSERT. An empty Key asks the server to assign
// one (an insert); it is then not sent.
type PutRequest struct {
	Database     string   `bson:"database"`
	Bucket       string   `bson:"bucket"`
	Key          string   `bson:"key,omitempty"`
	Data         Document `bson:"data"`
	WriteConcern string   `bson:"writeconcern,omitempty"`
}

// BatchPutRequest is BATCH_PUT: documents keyed by their _id_ member when they
// carry one, else by a key the server assigns; a held or repeated _id_ refuses
// the whole batch. Committed atomically up to the store's bound.
type BatchPutRequest struct {
	Database     string     `bson:"database"`
	Bucket       string     `bson:"bucket"`
	Data         []Document `bson:"data"`
	WriteConcern string     `bson:"writeconcern,omitempty"`
}

// UpsertRequest is UPSERT: the members of [PutRequest], the verb that replaces a
// document instead of refusing a held key. A key is required. It is a defined
// type, so a member added to PutRequest alone is not sent by UPSERT: add it to
// both on purpose.
type UpsertRequest PutRequest

// BatchUpsertRequest is BATCH_UPSERT: the members of [BatchPutRequest]. Each
// document carries its _id_; a repeated _id_ keeps the last document.
type BatchUpsertRequest BatchPutRequest

// ExpireCondition makes a delete conditional on a date field being older than
// a bound: DateBefore, or SecondsBefore as Unix seconds for a numeric field.
type ExpireCondition struct {
	Field         string     `bson:"field"`
	DateBefore    *time.Time `bson:"date_before,omitempty"`
	SecondsBefore *int64     `bson:"seconds_before,omitempty"`
}

// DeleteRequest is DELETE.
type DeleteRequest struct {
	Database     string           `bson:"database"`
	Bucket       string           `bson:"bucket"`
	Key          string           `bson:"key"`
	ExpireIf     *ExpireCondition `bson:"expire_if,omitempty"`
	WriteConcern string           `bson:"writeconcern,omitempty"`
}

// UpdateRequest is UPDATE: one document, named by its key, changed by
// operators. Each operator is a document of path-to-operand; Unset is a list
// of paths.
type UpdateRequest struct {
	Database     string   `bson:"database"`
	Bucket       string   `bson:"bucket"`
	Key          string   `bson:"key"`
	Set          Document `bson:"set,omitempty"`
	Unset        []string `bson:"unset,omitempty"`
	Inc          Document `bson:"inc,omitempty"`
	Push         Document `bson:"push,omitempty"`
	AddToSet     Document `bson:"addtoset,omitempty"`
	Mul          Document `bson:"mul,omitempty"`
	Min          Document `bson:"min,omitempty"`
	Max          Document `bson:"max,omitempty"`
	Rename       Document `bson:"rename,omitempty"`
	Filter       Document `bson:"filter,omitempty"`
	Multi        bool     `bson:"multi,omitempty"`
	WriteConcern string   `bson:"writeconcern,omitempty"`
}

// BlobWriteRequest is BLOB_WRITE; Data is the blob's bytes.
type BlobWriteRequest struct {
	Database     string   `bson:"database"`
	Bucket       string   `bson:"bucket"`
	Key          string   `bson:"key"`
	Data         []byte   `bson:"data"`
	Metadata     Document `bson:"metadata,omitempty"`
	WriteConcern string   `bson:"writeconcern,omitempty"`
}

// ScanRequest is SCAN, the find. Skip and Limit page it; the server has no
// cursor. ExpectBucketID is the previous page's BucketID, so a bucket dropped
// and re-created between two pages is not read as a continuation.
type ScanRequest struct {
	Database       string   `bson:"database"`
	Bucket         string   `bson:"bucket"`
	Filter         Document `bson:"filter,omitempty"`
	Skip           int64    `bson:"skip,omitempty"`
	Limit          int64    `bson:"limit,omitempty"`
	Sort           Document `bson:"sort,omitempty"`
	Projection     Document `bson:"projection,omitempty"`
	ExpectBucketID int64    `bson:"expect_bucket_id,omitempty"`
}

// CountRequest is COUNT.
type CountRequest struct {
	Database string   `bson:"database"`
	Bucket   string   `bson:"bucket"`
	Filter   Document `bson:"filter,omitempty"`
}

// DistinctRequest is DISTINCT.
type DistinctRequest struct {
	Database string   `bson:"database"`
	Bucket   string   `bson:"bucket"`
	Field    string   `bson:"field"`
	Filter   Document `bson:"filter,omitempty"`
}

// SearchRequest is SEARCH, the secondary-index query.
type SearchRequest struct {
	Database string   `bson:"database"`
	Bucket   string   `bson:"bucket"`
	Filter   Document `bson:"filter"`
	Limit    int64    `bson:"limit,omitempty"`
}

// InsertResponse answers PUT, BTREE_INSERT and the pushes. Seq is the
// store's sequence number for the write.
type InsertResponse struct {
	ID          string `bson:"id,omitempty"`
	Success     bool   `bson:"success"`
	Message     string `bson:"message,omitempty"`
	Seq         int64  `bson:"seq,omitempty"`
	WriteReport `bson:",inline"`
}

// BatchPutResponse answers BATCH_PUT. A batch over the atomic bound is refused
// with an ERROR frame, code [CodeBatchTooLarge]; an older server answers here
// with Success false and a message saying none of it was written, which is why
// splitting the batch into halves is safe.
type BatchPutResponse struct {
	IDs     []string `bson:"ids,omitempty"`
	Count   int64    `bson:"count,omitempty"`
	Success bool     `bson:"success"`
	Message string   `bson:"message,omitempty"`
	// Results is one entry per request position, absent from an older server.
	Results     []WriteResult `bson:"results,omitempty"`
	WriteReport `bson:",inline"`
}

// GetResponse answers GET and BTREE_LOOKUP. A miss is Found false with a
// message: a successful answer, not an error.
type GetResponse struct {
	Found   bool     `bson:"found"`
	Data    Document `bson:"data,omitempty"`
	Message string   `bson:"message,omitempty"`
}

// ExistsResponse answers EXISTS.
type ExistsResponse struct {
	Exists bool `bson:"exists"`
}

// ConsistentGetResponse answers CONSISTENT_GET: the newest copy, and whether
// reading it repaired a stale one.
type ConsistentGetResponse struct {
	Found    bool     `bson:"found"`
	Data     Document `bson:"data,omitempty"`
	Version  int64    `bson:"version,omitempty"`
	Repaired bool     `bson:"repaired,omitempty"`
	Message  string   `bson:"message,omitempty"`
}

// DeleteResponse answers DELETE. A key that is not there is Success false,
// "document not found", and a WriteNotApplied outcome: nothing was deleted,
// and repeating the delete is safe. Spared and Skipped say why a conditional
// delete (ExpireIf) deleted nothing.
type DeleteResponse struct {
	Success     bool   `bson:"success"`
	Message     string `bson:"message,omitempty"`
	Spared      bool   `bson:"spared,omitempty"`
	Skipped     bool   `bson:"skipped,omitempty"`
	WriteReport `bson:",inline"`
}

// UpdateResponse answers UPDATE; Doc is the document after the change.
type UpdateResponse struct {
	Success     bool     `bson:"success"`
	Doc         Document `bson:"doc,omitempty"`
	Message     string   `bson:"message,omitempty"`
	WriteReport `bson:",inline"`
}

// BlobWriteResponse answers BLOB_WRITE. The wire name of BlobID is one word.
type BlobWriteResponse struct {
	Success     bool   `bson:"success"`
	Key         string `bson:"key,omitempty"`
	BlobID      string `bson:"blobid,omitempty"`
	Size        int64  `bson:"size,omitempty"`
	Message     string `bson:"message,omitempty"`
	WriteReport `bson:",inline"`
}

// BlobReadResponse answers BLOB_READ.
type BlobReadResponse struct {
	Found    bool     `bson:"found"`
	BlobID   string   `bson:"blobid,omitempty"`
	Data     []byte   `bson:"data,omitempty"`
	Size     int64    `bson:"size,omitempty"`
	Metadata Document `bson:"metadata,omitempty"`
	Message  string   `bson:"message,omitempty"`
}

// ScanResponse answers SCAN: one page, the exact match Total, and whether more
// follows. BucketID identifies the bucket the page came from.
type ScanResponse struct {
	Found     bool       `bson:"found"`
	Database  string     `bson:"database,omitempty"`
	Bucket    string     `bson:"bucket,omitempty"`
	Documents []Document `bson:"documents,omitempty"`
	Total     int64      `bson:"total,omitempty"`
	HasMore   bool       `bson:"has_more,omitempty"`
	Message   string     `bson:"message,omitempty"`
	BucketID  int64      `bson:"bucket_id,omitempty"`
	IndexUsed string     `bson:"index_used,omitempty"`
}

// CountResponse answers COUNT. Pending, Dead and Expired are present on an
// unfiltered count of a queue or heap: the entries held under a lease, on the
// dead list, and under a lease that has run out. Each is absent at zero.
type CountResponse struct {
	Count   int64  `bson:"count"`
	Message string `bson:"message,omitempty"`
	Pending int64  `bson:"pending,omitempty"`
	Dead    int64  `bson:"dead,omitempty"`
	Expired int64  `bson:"expired,omitempty"`
}

// DistinctResponse answers DISTINCT. A value is whatever BSON type the field
// holds, so it is carried with its type.
type DistinctResponse struct {
	Values  []Value `bson:"values,omitempty"`
	Message string  `bson:"message,omitempty"`
}

// SearchResponse answers SEARCH. Total is the number of results returned, not
// the number that match; HasMore is true when a further match exists beyond
// the request's limit, and is always sent (false for a complete answer).
type SearchResponse struct {
	Results   []Document `bson:"results,omitempty"`
	Total     int64      `bson:"total,omitempty"`
	HasMore   bool       `bson:"has_more"`
	Message   string     `bson:"message,omitempty"`
	IndexUsed string     `bson:"index_used,omitempty"`
}
