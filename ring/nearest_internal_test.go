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

package ring

import (
	"strings"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"
)

// TestNearestResultOfRefusesAReplyWithoutAScorePerElement: the codec refuses such a reply, and the client does not
// rely on that alone, since the CLI indexes Scores by element.
func TestNearestResultOfRefusesAReplyWithoutAScorePerElement(t *testing.T) {
	zero := int32(0)
	reply := ringwire.RingReply{Elements: [][]byte{{5, 0, 0, 0, 0}, {5, 0, 0, 0, 0}}, Scanned: &zero, Skipped: &zero}
	_, err := nearestResultOf("Friends", reply)
	if err == nil || !strings.Contains(err.Error(), "the server answered a nearest without a score per element") {
		t.Fatalf("error = %v, want the missing-score refusal", err)
	}
}
