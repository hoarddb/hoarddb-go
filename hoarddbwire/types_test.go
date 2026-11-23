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
	"bytes"
	"encoding"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// clientOpcodes is every opcode handbook/wire.md section 4 lists for a driver
// to send in the application API.
var clientOpcodes = []hoarddbwire.Opcode{
	hoarddbwire.OpAuth, hoarddbwire.OpPut, hoarddbwire.OpGet, hoarddbwire.OpDelete, hoarddbwire.OpExists,
	hoarddbwire.OpBatchPut,
	hoarddbwire.OpFIFOPush, hoarddbwire.OpFIFOPop, hoarddbwire.OpFIFOPeek,
	hoarddbwire.OpLIFOPush, hoarddbwire.OpLIFOPop, hoarddbwire.OpLIFOPeek,
	hoarddbwire.OpHeapPush, hoarddbwire.OpHeapPop,
	hoarddbwire.OpBTreeInsert, hoarddbwire.OpBTreeLookup, hoarddbwire.OpBTreeRange,
	hoarddbwire.OpBTreeFirst, hoarddbwire.OpBTreeLast,
	hoarddbwire.OpBlobWrite, hoarddbwire.OpBlobRead,
	hoarddbwire.OpCreateDB, hoarddbwire.OpDeleteDB, hoarddbwire.OpCreateBucket, hoarddbwire.OpDeleteBucket,
	hoarddbwire.OpListBuckets, hoarddbwire.OpListDBs, hoarddbwire.OpBucketStats,
	hoarddbwire.OpCount, hoarddbwire.OpUpdate, hoarddbwire.OpSearch, hoarddbwire.OpScan, hoarddbwire.OpDistinct,
	hoarddbwire.OpConsistentGet,
	hoarddbwire.OpPing, hoarddbwire.OpHealthCheck, hoarddbwire.OpListNodes, hoarddbwire.OpGetFast,
	hoarddbwire.OpFieldPush, hoarddbwire.OpFieldPop, hoarddbwire.OpFieldPeek, hoarddbwire.OpFieldPut,
	hoarddbwire.OpFieldGet, hoarddbwire.OpFieldDelete, hoarddbwire.OpFieldLen, hoarddbwire.OpFieldList,
	hoarddbwire.OpDeclareStoreFields, hoarddbwire.OpBucketInfo,
	hoarddbwire.OpRingTopology,
}

func TestEveryOpcodeHasTypes(t *testing.T) {
	for _, op := range clientOpcodes {
		request, known := hoarddbwire.RequestFor(op)
		if !known || reflect.TypeOf(request).Kind() != reflect.Pointer || reflect.TypeOf(request).Elem().Kind() != reflect.Struct {
			t.Errorf("%s: RequestFor is %T (known %t), want a pointer to a struct", op, request, known)
		}
		response, known := hoarddbwire.ResponseFor(op)
		if !known || reflect.TypeOf(response).Kind() != reflect.Pointer || reflect.TypeOf(response).Elem().Kind() != reflect.Struct {
			t.Errorf("%s: ResponseFor is %T (known %t), want a pointer to a struct", op, response, known)
		}
		again, _ := hoarddbwire.RequestFor(op)
		// A zero-size type has no state to share, and Go may give every new one the same address.
		if request != nil && request == again && reflect.TypeOf(request).Elem().Size() > 0 {
			t.Errorf("%s: RequestFor returned the same pointer twice; each call must be a fresh value", op)
		}
	}
}

// A driver never sends a node-to-node opcode or one the server does not
// declare, so it has no type for them: asking for one is a bug, not a request.
func TestNoTypesForOpcodesADriverNeverSends(t *testing.T) {
	never := []hoarddbwire.Opcode{0x04, 0x05, 0x90, 0x91, 0x92, 0x96, 0x9D, 0xB0, 0xC4, hoarddbwire.OpError, hoarddbwire.OpPong, hoarddbwire.OpRingReply}
	for _, op := range never {
		if request, known := hoarddbwire.RequestFor(op); known {
			t.Errorf("%s has a request type %T; a driver never sends it", op, request)
		}
		if response, known := hoarddbwire.ResponseFor(op); known {
			t.Errorf("%s has a response type %T; it is not a request opcode", op, response)
		}
	}
}

// Failure 8 of handbook/build-go.md section 2: a decoder that fails on an
// unknown member, or on another integer width, breaks on the next server
// release. Every answer in the vectors is decoded with an extra member in each
// document the driver models, and with every integer as int32, as int64 and as
// a double holding an integer.
func TestDecodersTolerateUnknownFieldsAndWidths(t *testing.T) {
	file, err := conformance.Load(conformance.DriverVectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	variants := map[string]func(conformance.Shape, map[string]any) map[string]any{
		"an extra member": conformance.Shape.WithExtraMembers,
		"integers as int32": func(shape conformance.Shape, tree map[string]any) map[string]any {
			return shape.WithIntegerWidth(tree, "int32")
		},
		"integers as int64": func(shape conformance.Shape, tree map[string]any) map[string]any {
			return shape.WithIntegerWidth(tree, "int64")
		},
		"integers as double": func(shape conformance.Shape, tree map[string]any) map[string]any {
			return shape.WithIntegerWidth(tree, "double")
		},
	}
	decoded := 0
	for _, vector := range file.Vectors {
		if vector.Kind != "exchange" {
			continue
		}
		if _, held := conformance.Deferred(vector); held {
			continue // a later step builds its types; conformance leg 1 holds the row
		}
		if name := vector.Request.OpcodeName; strings.HasPrefix(name, "RING_") && name != "RING_TOPOLOGY" {
			// The ring's answers are read with hoarddb-wire, whose decoders accept
			// only the canonical bytes, on purpose: conformance leg 1 holds those
			// rows. The topology reply is the one this package still decodes.
			continue
		}
		switch hoarddbwire.Opcode(vector.Response.Opcode) {
		case hoarddbwire.OpError, hoarddbwire.OpAuthFail:
			continue // their decoders are tested with DecodeError and DecodeAuthFail
		}
		want, err := conformance.ParseExtended(vector.Response.PayloadJSON)
		if err != nil {
			t.Fatal(err)
		}
		shape := conformance.ShapeOf(vector.Request.OpcodeName, true)
		for name, vary := range variants {
			typed, known := hoarddbwire.ResponseFor(hoarddbwire.Opcode(vector.Request.Opcode))
			if !known {
				t.Fatalf("%s: no response type for %s", vector.Name, vector.Request.OpcodeName)
			}
			if _, binary := typed.(encoding.BinaryUnmarshaler); binary {
				continue
			}
			text, err := conformance.ExtendedJSON(vary(shape, want))
			if err != nil {
				t.Fatal(err)
			}
			payload, err := codec.FromExtendedJSON(text)
			if err != nil {
				t.Fatalf("%s, %s: %v", vector.Name, name, err)
			}
			if name == "an extra member" && !bytes.Contains(payload, []byte(conformance.ExtraMember)) {
				t.Fatalf("%s: the variant carries no extra member, so it tests nothing", vector.Name)
			}
			if err := codec.Unmarshal(payload, typed); err != nil {
				t.Errorf("%s with %s: decoding failed: %v\npayload %s", vector.Name, name, err, hex.EncodeToString(payload))
				continue
			}
			reencoded, err := codec.Marshal(typed)
			if err != nil {
				t.Fatal(err)
			}
			got, err := conformance.WireTree(reencoded)
			if err != nil {
				t.Fatal(err)
			}
			if difference := conformance.Diff(shape.Normalise(want), shape.Normalise(got)); difference != "" {
				t.Errorf("%s with %s: the decoded value differs from the vector's: %s", vector.Name, name, difference)
			}
			decoded++
		}
	}
	if decoded < 4*100 {
		t.Fatalf("only %d decodings ran; the vectors hold more than 100 answers", decoded)
	}
}

// handbook/operations.md section 5: limit belongs to FIELD_PEEK and FIELD_POP,
// and FIELD_LIST pages with one. The server ignores a member it does not know,
// so a limit sent on another field verb, or a peek or pop that cannot carry
// one, would be silently dropped.
func TestOnlyPeekPopAndListCarryAFieldLimit(t *testing.T) {
	limit := func(opcode hoarddbwire.Opcode) bool {
		typed, known := hoarddbwire.RequestFor(opcode)
		if !known {
			t.Fatalf("no request type for %s", opcode)
		}
		field := reflect.ValueOf(typed).Elem().FieldByName("Limit")
		if !field.IsValid() {
			return false
		}
		field.SetInt(7)
		payload, err := codec.Marshal(typed)
		if err != nil {
			t.Fatal(err)
		}
		_, present := codec.Lookup(payload, "limit")
		return present
	}
	for _, opcode := range []hoarddbwire.Opcode{
		hoarddbwire.OpFieldPush, hoarddbwire.OpFieldPut,
		hoarddbwire.OpFieldGet, hoarddbwire.OpFieldDelete, hoarddbwire.OpFieldLen,
	} {
		if limit(opcode) {
			t.Errorf("%s carries a limit; only FIELD_PEEK, FIELD_POP and FIELD_LIST do", opcode)
		}
	}
	for _, opcode := range []hoarddbwire.Opcode{hoarddbwire.OpFieldPeek, hoarddbwire.OpFieldPop, hoarddbwire.OpFieldList} {
		if !limit(opcode) {
			t.Errorf("%s cannot carry a limit", opcode)
		}
	}
}

// The health vectors drop host from the comparison (it differs per machine),
// so no vector pins its decode: this does, with every member present and with
// cpus alone, the shape of a node that cannot read the rest.
func TestHealthHostDecodes(t *testing.T) {
	full, err := codec.FromExtendedJSON([]byte(`{"nodeid":"n","healthy":true,"host":{"cpus":{"$numberInt":"4"},` +
		`"host_cpus":{"$numberInt":"16"},"memory_total_bytes":{"$numberLong":"17179869184"},` +
		`"memory_used_bytes":{"$numberLong":"1073741824"},"memory_source":"cgroup",` +
		`"load":{"one_minute":0.5,"five_minutes":0.25,"fifteen_minutes":0}}}`))
	if err != nil {
		t.Fatal(err)
	}
	var health hoarddbwire.HealthResponse
	if err := codec.Unmarshal(full, &health); err != nil {
		t.Fatal(err)
	}
	want := hoarddbwire.HealthHost{CPUs: 4, HostCPUs: 16, MemoryTotalBytes: 17179869184, MemoryUsedBytes: 1073741824,
		MemorySource: "cgroup", Load: &hoarddbwire.HostLoad{OneMinute: 0.5, FiveMinutes: 0.25}}
	if host := health.HostFacts(); host == nil || !reflect.DeepEqual(*host, want) {
		t.Fatalf("host = %+v, want %+v", host, want)
	}

	alone, err := codec.FromExtendedJSON([]byte(`{"nodeid":"n","healthy":true,"host":{"cpus":{"$numberInt":"2"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	health = hoarddbwire.HealthResponse{}
	if err := codec.Unmarshal(alone, &health); err != nil {
		t.Fatal(err)
	}
	if host := health.HostFacts(); host == nil || host.CPUs != 2 || host.Load != nil || host.MemoryTotalBytes != 0 {
		t.Fatalf("host = %+v, want cpus 2 and nothing else", host)
	}
}

// A host that is not a document, or whose cpus cannot be read, decodes the
// health call without failing and reports no facts.
func TestHealthHostMalformedDecodesWithoutFailing(t *testing.T) {
	for name, text := range map[string]string{
		"a string":         `{"nodeid":"n","healthy":true,"host":"x"}`,
		"cpus as a string": `{"nodeid":"n","healthy":true,"host":{"cpus":"four"}}`,
		"cpus as a double": `{"nodeid":"n","healthy":true,"host":{"cpus":2.5}}`,
		"an array":         `{"nodeid":"n","healthy":true,"host":[1,2]}`,
		"no host at all":   `{"nodeid":"n","healthy":true}`,
	} {
		payload, err := codec.FromExtendedJSON([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		var health hoarddbwire.HealthResponse
		if err := codec.Unmarshal(payload, &health); err != nil {
			t.Errorf("%s: decoding the health answer failed: %v", name, err)
			continue
		}
		if host := health.HostFacts(); host != nil {
			t.Errorf("%s: host = %+v, want none", name, host)
		}
	}
}
