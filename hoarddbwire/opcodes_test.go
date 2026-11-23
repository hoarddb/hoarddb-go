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

package hoarddbwire_test

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// wireOpcodes is handbook/wire.md section 4's table, request and response
// opcodes alike: the name and the value the server uses for each. The vectors
// carry the same names in opcode_name.
//
// This file is an external test package because it loads the vectors through
// conformance, which imports hoarddbwire: an internal test could not do that.
var wireOpcodes = []struct {
	op   hoarddbwire.Opcode
	name string
	hex  uint8
}{
	{hoarddbwire.OpAuth, "AUTH", 0x01}, {hoarddbwire.OpAuthOK, "AUTH_OK", 0x02}, {hoarddbwire.OpAuthFail, "AUTH_FAIL", 0x03},
	{hoarddbwire.OpPut, "PUT", 0x10}, {hoarddbwire.OpGet, "GET", 0x11}, {hoarddbwire.OpGetResponse, "GET_RESPONSE", 0x12},
	{hoarddbwire.OpDelete, "DELETE", 0x13}, {hoarddbwire.OpExists, "EXISTS", 0x14}, {hoarddbwire.OpExistsResponse, "EXISTS_RESPONSE", 0x15},
	{hoarddbwire.OpBatchPut, "BATCH_PUT", 0x20}, {hoarddbwire.OpBatchPutOK, "BATCH_PUT_OK", 0x21},
	{hoarddbwire.OpFIFOPush, "FIFO_PUSH", 0x30}, {hoarddbwire.OpFIFOPop, "FIFO_POP", 0x31},
	{hoarddbwire.OpFIFOPopResp, "FIFO_POP_RESPONSE", 0x32}, {hoarddbwire.OpFIFOPeek, "FIFO_PEEK", 0x33},
	{hoarddbwire.OpLIFOPush, "LIFO_PUSH", 0x40}, {hoarddbwire.OpLIFOPop, "LIFO_POP", 0x41},
	{hoarddbwire.OpLIFOPopResp, "LIFO_POP_RESPONSE", 0x42}, {hoarddbwire.OpLIFOPeek, "LIFO_PEEK", 0x43},
	{hoarddbwire.OpBTreeInsert, "BTREE_INSERT", 0x50}, {hoarddbwire.OpBTreeLookup, "BTREE_LOOKUP", 0x51},
	{hoarddbwire.OpBTreeRange, "BTREE_RANGE", 0x52}, {hoarddbwire.OpBTreeResponse, "BTREE_RESPONSE", 0x53},
	{hoarddbwire.OpHeapPush, "HEAP_PUSH", 0x60}, {hoarddbwire.OpHeapPop, "HEAP_POP", 0x61}, {hoarddbwire.OpHeapPopResp, "HEAP_POP_RESPONSE", 0x62},
	{hoarddbwire.OpBlobWrite, "BLOB_WRITE", 0x70}, {hoarddbwire.OpBlobRead, "BLOB_READ", 0x71}, {hoarddbwire.OpBlobReadResp, "BLOB_READ_RESPONSE", 0x72},
	{hoarddbwire.OpCreateDB, "CREATE_DB", 0x80}, {hoarddbwire.OpCreateBucket, "CREATE_BUCKET", 0x81},
	{hoarddbwire.OpDeleteDB, "DELETE_DB", 0x82}, {hoarddbwire.OpDeleteBucket, "DELETE_BUCKET", 0x83},
	{hoarddbwire.OpListBuckets, "LIST_BUCKETS", 0x84}, {hoarddbwire.OpListDBs, "LIST_DATABASES", 0x85},
	{hoarddbwire.OpBucketStats, "BUCKET_STATS", 0x86}, {hoarddbwire.OpCount, "COUNT", 0x87}, {hoarddbwire.OpUpdate, "UPDATE", 0x88},
	{hoarddbwire.OpBTreeFirst, "BTREE_FIRST", 0x89}, {hoarddbwire.OpBTreeLast, "BTREE_LAST", 0x8A}, {hoarddbwire.OpSearch, "SEARCH", 0x8B},
	{hoarddbwire.OpScan, "SCAN", 0x8C}, {hoarddbwire.OpDistinct, "DISTINCT", 0x8D}, {hoarddbwire.OpAlterBucket, "ALTER_BUCKET", 0x8E}, {hoarddbwire.OpConsistentGet, "CONSISTENT_GET", 0x9C},
	{hoarddbwire.OpPing, "PING", 0xA0}, {hoarddbwire.OpPong, "PONG", 0xA1}, {hoarddbwire.OpError, "ERROR", 0xA2},
	{hoarddbwire.OpHealthCheck, "HEALTH_CHECK", 0xA3}, {hoarddbwire.OpListNodes, "LIST_NODES", 0xA4},
	{hoarddbwire.OpGetFast, "GET_FAST", 0xC1}, {hoarddbwire.OpGetFastResponse, "GET_FAST_RESPONSE", 0xC2},
	{hoarddbwire.OpFieldPush, "FIELD_PUSH", 0xD0}, {hoarddbwire.OpFieldPop, "FIELD_POP", 0xD1}, {hoarddbwire.OpFieldPeek, "FIELD_PEEK", 0xD2},
	{hoarddbwire.OpFieldPut, "FIELD_PUT", 0xD3}, {hoarddbwire.OpFieldGet, "FIELD_GET", 0xD4}, {hoarddbwire.OpFieldDelete, "FIELD_DELETE", 0xD5},
	{hoarddbwire.OpFieldLen, "FIELD_LEN", 0xD6}, {hoarddbwire.OpFieldList, "FIELD_LIST", 0xD7},
	{hoarddbwire.OpDeclareStoreFields, "DECLARE_STORE_FIELDS", 0xD8},
}

func TestOpcodesCarryTheWireTablesValuesAndNames(t *testing.T) {
	for _, want := range wireOpcodes {
		if uint8(want.op) != want.hex {
			t.Errorf("%s is 0x%02X, handbook/wire.md section 4 says 0x%02X", want.name, uint8(want.op), want.hex)
		}
		if got := want.op.String(); got != want.name {
			t.Errorf("opcode 0x%02X is named %q, the table and the vectors say %q", want.hex, got, want.name)
		}
	}
}

// The ring opcodes 0xDA to 0xE0 (entry-ring-client-wire.md section 2): their
// values and names are the vectors', and every RING_ name the vectors carry, on
// a request or an answer, is the name the driver gives that value.
func TestRingOpcodesCarryTheVectorsValuesAndNames(t *testing.T) {
	ring := []struct {
		op   hoarddbwire.Opcode
		name string
		hex  uint8
	}{
		{hoarddbwire.OpRingList, "RING_LIST", 0xDA},
		{hoarddbwire.OpRingCount, "RING_COUNT", 0xDB},
		{hoarddbwire.OpRingKeyed, "RING_KEYED", 0xDC},
		{hoarddbwire.OpRingAdmin, "RING_ADMIN", 0xDD},
		{hoarddbwire.OpRingTopology, "RING_TOPOLOGY", 0xDE},
		{hoarddbwire.OpRingReply, "RING_REPLY", 0xDF},
		{hoarddbwire.OpRingBatch, "RING_BATCH", 0xE0},
	}
	for _, want := range ring {
		if uint8(want.op) != want.hex {
			t.Errorf("%s is 0x%02X, the ring opcode table says 0x%02X", want.name, uint8(want.op), want.hex)
		}
		if got := want.op.String(); got != want.name {
			t.Errorf("opcode 0x%02X is named %q, the vectors say %q", want.hex, got, want.name)
		}
	}

	file, err := conformance.Load(conformance.DriverVectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	check := func(vector conformance.Vector, half string, opcode int, name string) {
		if !strings.HasPrefix(name, "RING_") {
			return
		}
		checked++
		if got := hoarddbwire.Opcode(opcode).String(); got != name {
			t.Errorf("%s, %s: opcode 0x%02X is named %q, the vector says %q", vector.Name, half, opcode, got, name)
		}
	}
	for _, vector := range file.Vectors {
		if vector.Request != nil {
			check(vector, "request", vector.Request.Opcode, vector.Request.OpcodeName)
		}
		if vector.Response != nil {
			check(vector, "response", vector.Response.Opcode, vector.Response.OpcodeName)
		}
	}
	if checked == 0 {
		t.Fatal("the vectors carry no RING_ opcode name; the snapshot lost its ring rows")
	}
}

// 0xE1 is RING_GET_MANY, which package ring sends with the wire module's codec:
// it has its name and its number, and no request or response type here. 0xE2 is
// the value after it: it has no name and no driver type. A reply is never a
// request, so RING_REPLY has no request type either.
func TestAnOpcodeAfterRingGetManyIsStillUnnamed(t *testing.T) {
	if got := hoarddbwire.OpRingGetMany.String(); got != "RING_GET_MANY" || uint8(hoarddbwire.OpRingGetMany) != 0xE1 {
		t.Errorf("RING_GET_MANY is 0x%02X, named %q; the server sends it as 0xE1, RING_GET_MANY", uint8(hoarddbwire.OpRingGetMany), got)
	}
	const next hoarddbwire.Opcode = 0xE2
	if got := next.String(); got != "0xE2" {
		t.Errorf("0xE2 is named %q; no opcode after RING_GET_MANY has a name", got)
	}
	for _, op := range []hoarddbwire.Opcode{next, hoarddbwire.OpRingGetMany, hoarddbwire.OpRingReply} {
		if request, known := hoarddbwire.RequestFor(op); known {
			t.Errorf("%s has a request type %T; no driver sends it", op, request)
		}
		if response, known := hoarddbwire.ResponseFor(op); known {
			t.Errorf("%s has a response type %T; it is not a request opcode", op, response)
		}
	}
}
