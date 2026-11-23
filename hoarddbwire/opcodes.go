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

import (
	"fmt"

	ringwire "github.com/hoarddb/hoarddb-wire"
)

// Opcode is the one-byte operation code of a frame.
//
// The response to a request is not always a distinct constant: most operations
// answer with the request's own opcode, a few use a dedicated response opcode,
// and HealthCheck and ListNodes answer GetResponse (handbook/wire.md section
// 4). Any failure is OpError in place of the success opcode.
type Opcode uint8

// Opcodes a driver sends or receives. The user management family (0xB5-0xBB)
// is outside the v1 application API: it is listed so the conformance harness
// can encode and decode its vectors, and no client method sends it.
const (
	OpAuth     Opcode = 0x01
	OpAuthOK   Opcode = 0x02
	OpAuthFail Opcode = 0x03

	OpPut            Opcode = 0x10
	OpGet            Opcode = 0x11
	OpGetResponse    Opcode = 0x12
	OpDelete         Opcode = 0x13
	OpExists         Opcode = 0x14
	OpExistsResponse Opcode = 0x15

	// UPSERT is PUT's replacing form: the document replaces the scalar fields of
	// the one stored under the key and keeps its store fields.
	OpUpsert   Opcode = 0x16
	OpUpsertOK Opcode = 0x17

	OpBatchPut   Opcode = 0x20
	OpBatchPutOK Opcode = 0x21

	// BATCH_UPSERT is BATCH_PUT's replacing form: each document replaces the
	// scalar fields of the one stored under its _id_.
	OpBatchUpsert   Opcode = 0x24
	OpBatchUpsertOK Opcode = 0x25

	OpFIFOPush    Opcode = 0x30
	OpFIFOPop     Opcode = 0x31
	OpFIFOPopResp Opcode = 0x32
	OpFIFOPeek    Opcode = 0x33

	// Leases and the dead list: one set for the fifo, lifo and heap families, the
	// server resolving the type from the bucket (handbook/wire.md section 4).
	// 0x3B, the restore's own, is never sent by a driver.
	OpQueueClaim   Opcode = 0x34
	OpQueueAck     Opcode = 0x35
	OpQueueRelease Opcode = 0x36
	OpQueueExtend  Opcode = 0x37
	OpDeadList     Opcode = 0x38
	OpDeadRequeue  Opcode = 0x39
	OpDeadDiscard  Opcode = 0x3A

	OpLIFOPush    Opcode = 0x40
	OpLIFOPop     Opcode = 0x41
	OpLIFOPopResp Opcode = 0x42
	OpLIFOPeek    Opcode = 0x43

	OpBTreeInsert   Opcode = 0x50
	OpBTreeLookup   Opcode = 0x51
	OpBTreeRange    Opcode = 0x52
	OpBTreeResponse Opcode = 0x53

	OpHeapPush    Opcode = 0x60
	OpHeapPop     Opcode = 0x61
	OpHeapPopResp Opcode = 0x62

	OpBlobWrite    Opcode = 0x70
	OpBlobRead     Opcode = 0x71
	OpBlobReadResp Opcode = 0x72

	OpCreateDB     Opcode = 0x80
	OpCreateBucket Opcode = 0x81
	OpDeleteDB     Opcode = 0x82
	OpDeleteBucket Opcode = 0x83
	OpListBuckets  Opcode = 0x84
	OpListDBs      Opcode = 0x85
	OpBucketStats  Opcode = 0x86
	OpCount        Opcode = 0x87
	OpUpdate       Opcode = 0x88
	OpBTreeFirst   Opcode = 0x89
	OpBTreeLast    Opcode = 0x8A
	OpSearch       Opcode = 0x8B
	OpScan         Opcode = 0x8C
	OpDistinct     Opcode = 0x8D
	OpAlterBucket  Opcode = 0x8E

	OpConsistentGet Opcode = 0x9C

	OpPing        Opcode = 0xA0
	OpPong        Opcode = 0xA1
	OpError       Opcode = 0xA2
	OpHealthCheck Opcode = 0xA3
	OpListNodes   Opcode = 0xA4

	OpUserCreate Opcode = 0xB5
	OpUserAlter  Opcode = 0xB6
	OpUserDrop   Opcode = 0xB7
	OpUserList   Opcode = 0xB8
	OpUserGet    Opcode = 0xB9
	OpUserGrant  Opcode = 0xBA
	OpUserRevoke Opcode = 0xBB

	OpGetFast         Opcode = 0xC1
	OpGetFastResponse Opcode = 0xC2

	// The field-store verbs (handbook/operations.md section 5).
	OpFieldPush          Opcode = 0xD0
	OpFieldPop           Opcode = 0xD1
	OpFieldPeek          Opcode = 0xD2
	OpFieldPut           Opcode = 0xD3
	OpFieldGet           Opcode = 0xD4
	OpFieldDelete        Opcode = 0xD5
	OpFieldLen           Opcode = 0xD6
	OpFieldList          Opcode = 0xD7
	OpDeclareStoreFields Opcode = 0xD8
	OpBucketInfo         Opcode = 0xD9

	// The ring opcodes 0xDA to 0xE1. Each number is declared once, in the wire
	// module the server reads it from too, so the two cannot disagree. Package
	// ring sends them.
	OpRingList     Opcode = ringwire.OpRingList
	OpRingCount    Opcode = ringwire.OpRingCount
	OpRingKeyed    Opcode = ringwire.OpRingKeyed
	OpRingAdmin    Opcode = ringwire.OpRingAdmin
	OpRingTopology Opcode = ringwire.OpRingTopology
	OpRingReply    Opcode = ringwire.OpRingReply
	OpRingBatch    Opcode = ringwire.OpRingBatch
	OpRingGetMany  Opcode = ringwire.OpRingGetMany
)

var opcodeNames = map[Opcode]string{
	OpAuth: "AUTH", OpAuthOK: "AUTH_OK", OpAuthFail: "AUTH_FAIL",
	OpPut: "PUT", OpGet: "GET", OpGetResponse: "GET_RESPONSE", OpDelete: "DELETE",
	OpExists: "EXISTS", OpExistsResponse: "EXISTS_RESPONSE",
	OpBatchPut: "BATCH_PUT", OpBatchPutOK: "BATCH_PUT_OK",
	OpUpsert: "UPSERT", OpUpsertOK: "UPSERT_OK", OpBatchUpsert: "BATCH_UPSERT", OpBatchUpsertOK: "BATCH_UPSERT_OK",
	OpFIFOPush: "FIFO_PUSH", OpFIFOPop: "FIFO_POP", OpFIFOPopResp: "FIFO_POP_RESPONSE", OpFIFOPeek: "FIFO_PEEK",
	OpQueueClaim: "QUEUE_CLAIM", OpQueueAck: "QUEUE_ACK", OpQueueRelease: "QUEUE_RELEASE", OpQueueExtend: "QUEUE_EXTEND",
	OpDeadList: "DEAD_LIST", OpDeadRequeue: "DEAD_REQUEUE", OpDeadDiscard: "DEAD_DISCARD",
	OpLIFOPush: "LIFO_PUSH", OpLIFOPop: "LIFO_POP", OpLIFOPopResp: "LIFO_POP_RESPONSE", OpLIFOPeek: "LIFO_PEEK",
	OpBTreeInsert: "BTREE_INSERT", OpBTreeLookup: "BTREE_LOOKUP", OpBTreeRange: "BTREE_RANGE",
	OpBTreeResponse: "BTREE_RESPONSE",
	OpHeapPush:      "HEAP_PUSH", OpHeapPop: "HEAP_POP", OpHeapPopResp: "HEAP_POP_RESPONSE",
	OpBlobWrite: "BLOB_WRITE", OpBlobRead: "BLOB_READ", OpBlobReadResp: "BLOB_READ_RESPONSE",
	OpCreateDB: "CREATE_DB", OpCreateBucket: "CREATE_BUCKET", OpDeleteDB: "DELETE_DB",
	OpDeleteBucket: "DELETE_BUCKET", OpListBuckets: "LIST_BUCKETS", OpListDBs: "LIST_DATABASES",
	OpBucketStats: "BUCKET_STATS", OpCount: "COUNT", OpUpdate: "UPDATE",
	OpBTreeFirst: "BTREE_FIRST", OpBTreeLast: "BTREE_LAST", OpSearch: "SEARCH", OpScan: "SCAN",
	OpDistinct: "DISTINCT", OpAlterBucket: "ALTER_BUCKET", OpConsistentGet: "CONSISTENT_GET",
	OpPing: "PING", OpPong: "PONG", OpError: "ERROR", OpHealthCheck: "HEALTH_CHECK",
	OpListNodes:  "LIST_NODES",
	OpUserCreate: "USER_CREATE", OpUserAlter: "USER_ALTER", OpUserDrop: "USER_DROP", OpUserList: "USER_LIST",
	OpUserGet: "USER_GET", OpUserGrant: "USER_GRANT", OpUserRevoke: "USER_REVOKE",
	OpGetFast: "GET_FAST", OpGetFastResponse: "GET_FAST_RESPONSE",
	OpFieldPush: "FIELD_PUSH", OpFieldPop: "FIELD_POP", OpFieldPeek: "FIELD_PEEK", OpFieldPut: "FIELD_PUT",
	OpFieldGet: "FIELD_GET", OpFieldDelete: "FIELD_DELETE", OpFieldLen: "FIELD_LEN", OpFieldList: "FIELD_LIST",
	OpDeclareStoreFields: "DECLARE_STORE_FIELDS", OpBucketInfo: "BUCKET_INFO",
	OpRingList: "RING_LIST", OpRingCount: "RING_COUNT", OpRingKeyed: "RING_KEYED", OpRingAdmin: "RING_ADMIN",
	OpRingTopology: "RING_TOPOLOGY", OpRingReply: "RING_REPLY", OpRingBatch: "RING_BATCH",
	OpRingGetMany: "RING_GET_MANY",
}

// String returns the spec's name for the opcode, or its hex value when the
// opcode is not in the table.
func (op Opcode) String() string {
	if name, known := opcodeNames[op]; known {
		return name
	}
	return fmt.Sprintf("0x%02X", uint8(op))
}
