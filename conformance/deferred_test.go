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
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/conformance"
)

// TestTheDeferralRulesPinTheSnapshot holds the rules to the 535 vectors of the
// corpus (../testdata/driver-vectors/v1.json, the one copy the server's
// generator writes): the rows held back are counted by tag, and the sum is the
// deferred count leg 1 prints. The ring rows are read with hoarddb-wire and are
// not held: what is left is the widened topology integer, the node-to-node
// probe and the legacy BATCH verb.
func TestTheDeferralRulesPinTheSnapshot(t *testing.T) {
	file := loadVectors(t)
	want := map[string]int{
		"server-canonical": 1, "cluster-probe": 4, "batch-verb": 24,
	}
	const wantDeferred = 29
	got := map[string]int{}
	trust, deferred := 0, 0
	for _, vector := range file.Vectors {
		if vector.Kind == "trust" {
			trust++
		}
		if tag, held := conformance.Deferred(vector); held {
			got[tag]++
			deferred++
		}
	}
	for tag, count := range want {
		if got[tag] != count {
			t.Errorf("tag %q holds %d vectors, the corpus has %d", tag, got[tag], count)
		}
	}
	for tag, count := range got {
		if _, listed := want[tag]; !listed {
			t.Errorf("tag %q holds %d vectors and this test does not list it", tag, count)
		}
	}
	if deferred != wantDeferred {
		t.Errorf("%d vectors are deferred, want %d", deferred, wantDeferred)
	}
	if len(file.Vectors) != 535 {
		t.Errorf("the corpus has %d vectors, want 535", len(file.Vectors))
	}
	if trust != 9 {
		t.Errorf("the corpus has %d trust vectors, want 9", trust)
	}
}

// TestEveryDeferredNameIsInTheFile catches a misspelt name in a1Owned or
// drift, which would hold nothing and hide a row.
func TestEveryDeferredNameIsInTheFile(t *testing.T) {
	file := loadVectors(t)
	names := conformance.DeferredNames()
	// The name tables are expected to be empty until phase B: the rows held by rule
	// (ring admin, cluster probe, server-canonical, batch) are pinned by count in
	// TestTheDeferralRulesPinTheSnapshot, and any other row must pass leg 1. So
	// this loop checks nothing today; it guards a name someone adds later.
	for _, name := range names {
		if _, found := file.Named(name); !found {
			t.Errorf("deferred name %q is not a vector of the file", name)
		}
	}
}

// TestTheSessionRefusalRowPasses: a session the server refuses is answered
// with an ERROR, which the driver reads without a response type, so the row
// passes and is no longer deferred.
func TestTheSessionRefusalRowPasses(t *testing.T) {
	file := loadVectors(t)
	vector, found := file.Named("ring topology: a session the server refuses")
	if !found {
		t.Fatal("the vector ring topology: a session the server refuses is not in the file")
	}
	if err := checkExchange(vector); err != nil {
		t.Errorf("checkExchange: %v", err)
	}
	if tag, held := conformance.Deferred(vector); held {
		t.Errorf("the row passes and is still deferred as %s", tag)
	}
}

// TestARefuseVectorWithNoCheckIsAnError: a refuse row nobody has built a
// decoder for is held back by name, and the harness says why in its own words.
// The probe is such a row: a driver has no decoder for a node-to-node opcode.
func TestARefuseVectorWithNoCheckIsAnError(t *testing.T) {
	file := loadVectors(t)
	const name = "cluster probe refused: a short body"
	vector, found := file.Named(name)
	if !found {
		t.Fatalf("the vector %s is not in the file", name)
	}
	for name, err := range map[string]error{"checkRefuse": checkRefuse(vector), "checkVector": checkVector(vector)} {
		if err == nil || !strings.Contains(err.Error(), "no refusal check is registered") {
			t.Errorf("%s answered %v, want an error containing %q", name, err, "no refusal check is registered")
		}
		if err != nil && strings.Contains(err.Error(), "does not know") {
			t.Errorf("%s answered %q: the harness does not know the refuse kind", name, err)
		}
	}
}
