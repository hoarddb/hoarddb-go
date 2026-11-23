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

// Request and response types, one per client-facing opcode, with the exact
// wire names of handbook/operations.md in an explicit bson tag on every field
// (a name guessed from a Go field name is silently ignored by the server:
// handbook/build-go.md section 2, failure 1). The vectors check every tag:
// conformance.TestLeg1Codec.
//
// Conventions, all from handbook/wire.md section 3:
//
//   - A stored document is a [codec.Raw]: nested BSON, handed over byte for
//     byte, never re-encoded.
//   - An integer is an int64 (or an *int64 where absent and zero differ). The
//     decoder takes int32, int64 and a double holding an integer into it.
//   - An optional request field is omitempty, so an empty one is not sent.
//   - A response type models every field of its vector's "fields"; a member a
//     newer server adds is ignored.

import (
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// The write outcomes a server reports (handbook/errors.md section 4).
const (
	// WriteApplied: the write is stored, though the request as a whole was
	// refused. Resending a push or an increment would apply it twice.
	WriteApplied WriteOutcome = "applied"
	// WriteNotApplied: nothing was written.
	WriteNotApplied WriteOutcome = "not_applied"
	// WriteUnknown: the server cannot tell; read the data back before resending.
	WriteUnknown WriteOutcome = "unknown"
)

// WriteReport is what a data mutation's answer says about the write. On a
// single node the copy counts are absent (nil); on a cluster they are present
// on success. It is embedded in every response marked † in
// handbook/operations.md.
type WriteReport struct {
	WriteOutcome      WriteOutcome `bson:"write_outcome,omitempty"`
	CopiesConfirmed   *int64       `bson:"copies_confirmed,omitempty"`
	ReplicationFactor *int64       `bson:"replication_factor,omitempty"`
}

// WriteResult is what the server says of one request position of a batch:
// the document's id, whether it was written, and for a refused position the
// code and the sentence. A batch answer and a refused batch's ERROR both carry
// one per position, in request order (handbook/operations.md, "Batches").
type WriteResult struct {
	ID           string       `bson:"id,omitempty"`
	WriteOutcome WriteOutcome `bson:"write_outcome,omitempty"`
	Code         int32        `bson:"code,omitempty"`
	Message      string       `bson:"message,omitempty"`
}

// EmptyRequest is the request of an opcode with no payload: PING,
// LIST_DATABASES, HEALTH_CHECK and LIST_NODES.
type EmptyRequest struct{}

// RequestFor returns a new, zero request value of the type op's payload
// carries, as a pointer to it, and false for an opcode a driver never sends: a
// node-to-node opcode, a reserved one, or a response opcode.
func RequestFor(op Opcode) (any, bool) {
	make, known := requestTypes[op]
	if !known {
		return nil, false
	}
	return make(), true
}

// ResponseFor returns a new, zero response value of the type that answers a
// request with opcode op, as a pointer to it. It is keyed by the request's
// opcode, not the answer's: HEALTH_CHECK, LIST_NODES and GET all answer with
// GET_RESPONSE, and only the request says which payload that is.
func ResponseFor(request Opcode) (any, bool) {
	make, known := responseTypes[request]
	if !known {
		return nil, false
	}
	return make(), true
}

func newValue[T any]() func() any { return func() any { return new(T) } }

var requestTypes = map[Opcode]func() any{
	OpAuth: newValue[AuthRequest](),
	OpPing: newValue[EmptyRequest](), OpListDBs: newValue[EmptyRequest](),
	OpHealthCheck: newValue[EmptyRequest](), OpListNodes: newValue[ListNodesRequest](),

	OpPut: newValue[PutRequest](), OpBTreeInsert: newValue[PutRequest](),
	OpBatchPut: newValue[BatchPutRequest](), OpUpsert: newValue[UpsertRequest](), OpBatchUpsert: newValue[BatchUpsertRequest](),
	OpGet: newValue[KeyRequest](), OpExists: newValue[KeyRequest](), OpBTreeLookup: newValue[KeyRequest](),
	OpConsistentGet: newValue[KeyRequest](), OpBlobRead: newValue[KeyRequest](),
	OpDelete: newValue[DeleteRequest](), OpUpdate: newValue[UpdateRequest](),
	OpBlobWrite: newValue[BlobWriteRequest](),

	OpFIFOPush: newValue[PushRequest](), OpLIFOPush: newValue[PushRequest](), OpHeapPush: newValue[PushRequest](),
	OpFIFOPop: newValue[PopRequest](), OpLIFOPop: newValue[PopRequest](), OpHeapPop: newValue[PopRequest](),
	OpFIFOPeek: newValue[PeekRequest](), OpLIFOPeek: newValue[PeekRequest](),
	OpQueueClaim: newValue[ClaimRequest](),
	OpQueueAck:   newValue[LeaseRequest](), OpQueueRelease: newValue[LeaseRequest](), OpQueueExtend: newValue[LeaseRequest](),
	OpDeadList: newValue[DeadListRequest](), OpDeadRequeue: newValue[DeadMoveRequest](), OpDeadDiscard: newValue[DeadMoveRequest](),
	OpBTreeRange: newValue[RangeRequest](), OpBTreeFirst: newValue[RangeRequest](), OpBTreeLast: newValue[RangeRequest](),

	OpScan: newValue[ScanRequest](), OpCount: newValue[CountRequest](),
	OpDistinct: newValue[DistinctRequest](), OpSearch: newValue[SearchRequest](),

	OpCreateDB: newValue[NameRequest](), OpDeleteDB: newValue[NameRequest](),
	OpCreateBucket: newValue[CreateBucketRequest](), OpDeleteBucket: newValue[DropBucketRequest](),
	OpListBuckets: newValue[DatabaseRequest](), OpBucketStats: newValue[BucketRequest](), OpBucketInfo: newValue[BucketInfoRequest](),
	OpDeclareStoreFields: newValue[DeclareStoreFieldsRequest](), OpAlterBucket: newValue[AlterBucketRequest](),

	OpUserCreate: newValue[UserCreateRequest](), OpUserAlter: newValue[UserAlterRequest](), OpUserDrop: newValue[UserDropRequest](),
	OpUserList: newValue[EmptyRequest](), OpUserGet: newValue[UserGetRequest](),
	OpUserGrant: newValue[UserGrantRequest](), OpUserRevoke: newValue[UserGrantRequest](),

	OpGetFast: newValue[GetFastRequest](),

	OpFieldPush: newValue[FieldRequest](), OpFieldPop: newValue[FieldPopRequest](), OpFieldPeek: newValue[FieldPeekRequest](),
	OpFieldPut: newValue[FieldRequest](), OpFieldGet: newValue[FieldRequest](), OpFieldDelete: newValue[FieldRequest](),
	OpFieldLen: newValue[FieldRequest](), OpFieldList: newValue[FieldListRequest](),

	// The topology is the one ring request this package encodes; the hoarddb-wire
	// module holds every other ring request and reply.
	OpRingTopology: newValue[RingTopologyRequest](),
}

var responseTypes = map[Opcode]func() any{
	OpAuth: newValue[AuthResponse](),
	OpPing: newValue[PongResponse](),

	OpPut: newValue[InsertResponse](), OpBTreeInsert: newValue[InsertResponse](),
	OpFIFOPush: newValue[InsertResponse](), OpLIFOPush: newValue[InsertResponse](), OpHeapPush: newValue[InsertResponse](),
	OpBatchPut: newValue[BatchPutResponse](), OpUpsert: newValue[InsertResponse](), OpBatchUpsert: newValue[BatchPutResponse](),
	OpGet: newValue[GetResponse](), OpBTreeLookup: newValue[GetResponse](),
	OpExists: newValue[ExistsResponse](), OpConsistentGet: newValue[ConsistentGetResponse](),
	OpDelete: newValue[DeleteResponse](), OpUpdate: newValue[UpdateResponse](),
	OpBlobWrite: newValue[BlobWriteResponse](), OpBlobRead: newValue[BlobReadResponse](),

	OpFIFOPop: newValue[PopResponse](), OpLIFOPop: newValue[PopResponse](), OpHeapPop: newValue[PopResponse](),
	OpFIFOPeek: newValue[PeekResponse](), OpLIFOPeek: newValue[PeekResponse](),
	OpQueueClaim: newValue[ClaimResponse](),
	OpQueueAck:   newValue[LeaseResponse](), OpQueueRelease: newValue[LeaseResponse](), OpQueueExtend: newValue[LeaseResponse](),
	OpDeadList: newValue[DeadListResponse](), OpDeadRequeue: newValue[DeadMoveResponse](), OpDeadDiscard: newValue[DeadMoveResponse](),
	OpBTreeRange: newValue[RangeResponse](), OpBTreeFirst: newValue[RangeResponse](), OpBTreeLast: newValue[RangeResponse](),

	OpScan: newValue[ScanResponse](), OpCount: newValue[CountResponse](),
	OpDistinct: newValue[DistinctResponse](), OpSearch: newValue[SearchResponse](),

	OpCreateDB: newValue[AckResponse](), OpDeleteDB: newValue[AckResponse](),
	OpCreateBucket: newValue[CreateBucketResponse](), OpDeleteBucket: newValue[AckResponse](),
	OpDeclareStoreFields: newValue[AckResponse](), OpAlterBucket: newValue[AlterBucketResponse](),
	OpListDBs: newValue[ListDatabasesResponse](), OpListBuckets: newValue[ListBucketsResponse](),
	OpBucketStats: newValue[BucketStatsResponse](), OpBucketInfo: newValue[BucketInfoResponse](),
	OpHealthCheck: newValue[HealthResponse](), OpListNodes: newValue[ListNodesResponse](),

	OpUserCreate: newValue[UserMutationResponse](), OpUserAlter: newValue[UserMutationResponse](), OpUserDrop: newValue[UserMutationResponse](),
	OpUserGrant: newValue[UserMutationResponse](), OpUserRevoke: newValue[UserMutationResponse](),
	OpUserList: newValue[UserListResponse](), OpUserGet: newValue[UserGetResponse](),

	OpGetFast: newValue[GetFastResponse](),

	OpRingTopology: newValue[RingTopologyReply](),

	OpFieldPush: newValue[FieldResponse](), OpFieldPop: newValue[FieldResponse](), OpFieldPeek: newValue[FieldResponse](),
	OpFieldPut: newValue[FieldResponse](), OpFieldGet: newValue[FieldResponse](), OpFieldDelete: newValue[FieldResponse](),
	OpFieldLen: newValue[FieldResponse](), OpFieldList: newValue[FieldListResponse](),
}

// Document is a BSON document carried as the bytes it arrived in: a stored
// document, a filter, a declaration. Written, it is an embedded document.
type Document = codec.Raw

// Value is one BSON value of any type, with its type, for the few fields whose
// type depends on the data.
type Value = codec.RawValue
