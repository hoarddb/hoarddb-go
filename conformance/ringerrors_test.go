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
)

// TestRingErrorRowsDecodeAsTheVectorsSay holds the ring's ERROR answers to their
// vectors. The three ring rows pass as whole exchanges, read with hoarddb-wire,
// and the legacy client's error decoder reads the same payloads to the same
// code; the two insert refusals are legacy exchanges.
func TestRingErrorRowsDecodeAsTheVectorsSay(t *testing.T) {
	file := loadVectors(t)
	for _, name := range []string{
		"ring keyed: the slot belongs to another node",
		"ring keyed: the client's epoch is behind",
		"ring keyed: the client's declaration is stale",
	} {
		vector := vectorNamed(t, file, name)
		if err := checkVector(vector); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		payload, err := hex.DecodeString(vector.Response.PayloadHex)
		if err != nil {
			t.Fatalf("%s: payload_hex: %v", name, err)
		}
		if err := checkErrorAnswer(vector, payload); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for _, name := range []string{
		"insert of an existing id is refused",
		"btree insert of an existing key is refused",
	} {
		if err := checkExchange(vectorNamed(t, file, name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
