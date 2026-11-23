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
	"testing"

	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestTheTopologyExchangesDecodeAsTheVectorsSay: each of the four topology
// exchanges that need the reply type passes checkExchange and is no longer
// deferred. Three of them have a null payload_json, so the typed answer is
// held to the payload's own tree.
func TestTheTopologyExchangesDecodeAsTheVectorsSay(t *testing.T) {
	file := loadVectors(t)
	for _, name := range []string{
		"ring topology: the first call after AUTH",
		"ring topology: a single node",
		"ring topology: the view held is current",
		"ring topology: a witness is listed apart from the members",
	} {
		vector := vectorNamed(t, file, name)
		if err := checkExchange(vector); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if tag, held := conformance.Deferred(vector); held {
			t.Errorf("%s is still deferred as %s", name, tag)
		}
	}
}

// TestTheTenRefusedTopologyRepliesAreRefused: the driver's own decoder refuses
// the ten reply rows it builds and none is deferred. The eleventh, a widened
// integer, is the one the driver reads on purpose.
func TestTheTenRefusedTopologyRepliesAreRefused(t *testing.T) {
	file := loadVectors(t)
	for _, name := range []string{
		"ring topology reply refused: members out of order",
		"ring topology reply refused: an extra member",
		"ring topology reply refused: a reserved flag bit",
		"ring topology reply refused: a witness that is also ranked",
		"ring topology reply refused: an empty witnesses blob",
		"ring topology reply refused: a non-canonical varint",
		"ring topology reply refused: a truncated table",
		"ring topology reply refused: a padded table",
		"ring topology reply refused: truncated",
		"ring topology reply refused: unchanged with blobs",
	} {
		vector := vectorNamed(t, file, name)
		if err := checkVector(vector); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if tag, held := conformance.Deferred(vector); held {
			t.Errorf("%s is still deferred as %s", name, tag)
		}
	}
}

// TestTheWidenedIntegerReplyIsAccepted: the node refuses a reply whose rf is
// not an int32, and this driver reads any width (A1 Owner question 1), so the
// row's payload decodes and the row stays deferred as server-canonical.
func TestTheWidenedIntegerReplyIsAccepted(t *testing.T) {
	file := loadVectors(t)
	vector := vectorNamed(t, file, "ring topology reply refused: a widened integer")
	payload, err := hex.DecodeString(vector.Response.PayloadHex)
	if err != nil {
		t.Fatalf("payload_hex: %v", err)
	}
	topology, err := hoarddbwire.DecodeRingTopology(payload)
	if err != nil {
		t.Fatalf("the widened-integer reply is refused: %v", err)
	}
	if len(topology.Databases) == 0 || topology.Databases[0].RF < 1 {
		t.Errorf("the widened reply decoded to databases %+v, want a database with a replication factor", topology.Databases)
	}
	if tag, held := conformance.Deferred(vector); !held || tag != "server-canonical" {
		t.Errorf("the row is deferred as %q (held %t), want server-canonical", tag, held)
	}
}
