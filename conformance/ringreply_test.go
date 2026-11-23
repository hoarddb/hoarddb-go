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
	"encoding/hex"
	"reflect"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/conformance"
)

// TestTheReplyTypeCarriesTheServersAnswers holds hoarddb-wire's RingReply to
// the answers the vectors give a ring list, count or keyed request, one row
// for each shape of answer: the reply decodes, it writes back exactly the
// members the server sent, and the member the row is about is set.
func TestTheReplyTypeCarriesTheServersAnswers(t *testing.T) {
	file := loadVectors(t)
	rows := []struct {
		name string
		set  func(reply ringwire.RingReply) bool
	}{
		{"ring list: the first page of a bucket", func(reply ringwire.RingReply) bool { return len(reply.Entries) > 0 }},
		{"ring list: only the named fields", func(reply ringwire.RingReply) bool { return len(reply.Entries) > 0 }},
		{"ring count: documents in a bucket", func(reply ringwire.RingReply) bool { return reply.Count != nil }},
		{"ring keyed: get one document", func(reply ringwire.RingReply) bool {
			return reply.Found != nil && *reply.Found && len(reply.Elements) == 1
		}},
		{"ring keyed: insert one document", func(reply ringwire.RingReply) bool { return reply.WriteOutcome != "" && reply.Position != nil }},
		{"ring keyed: read one element by key (store_get)", func(reply ringwire.RingReply) bool { return len(reply.Elements) == 1 }},
		{"ring keyed: a hashStore page carries the keys", func(reply ringwire.RingReply) bool { return len(reply.Keys) > 0 }},
		{"ring keyed: a heapStore peek carries entry ids and priorities", func(reply ringwire.RingReply) bool { return len(reply.Priorities) > 0 }},
		{"ring keyed: pop up to three elements", func(reply ringwire.RingReply) bool { return len(reply.Elements) > 0 }},
		{"ring keyed: get only the named fields (get_fields)", func(reply ringwire.RingReply) bool { return len(reply.Elements) == 1 }},
		{"ring lease: claim", func(reply ringwire.RingReply) bool { return len(reply.Lease) > 0 }},
		{"ring store: store count", func(reply ringwire.RingReply) bool { return reply.Count != nil }},
		{"ring store: sum", func(reply ringwire.RingReply) bool { return len(reply.Aggregate) > 0 }},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			vector := vectorNamed(t, file, row.name)
			if err := checkVector(vector); err != nil {
				t.Fatal(err)
			}
			payload, err := hex.DecodeString(vector.Response.PayloadHex)
			if err != nil {
				t.Fatalf("payload_hex: %v", err)
			}
			reply, err := ringwire.DecodeRingReply(payload)
			if err != nil {
				t.Fatalf("DecodeRingReply refused the server's own answer: %v", err)
			}
			if !row.set(reply) {
				t.Errorf("the decoded reply %+v lacks the member this row is about", reply)
			}
			// The decoder accepts only bytes it would write, so the members
			// written back are the members sent; this states it on the trees.
			again, err := ringwire.EncodeRingReply(reply)
			if err != nil {
				t.Fatalf("EncodeRingReply: %v", err)
			}
			written, err := conformance.WireTree(again)
			if err != nil {
				t.Fatal(err)
			}
			sent, err := conformance.WireTree(payload)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := conformance.TopLevelNames(written), conformance.TopLevelNames(sent); !reflect.DeepEqual(got, want) {
				t.Errorf("the reply writes the members %v, the server sent %v", got, want)
			}
			if tag, held := conformance.Deferred(vector); held {
				t.Errorf("the row is still deferred as %s", tag)
			}
		})
	}
}
