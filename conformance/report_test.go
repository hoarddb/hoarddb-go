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

package conformance

import "testing"

func TestLegReportLine(t *testing.T) {
	got := LegReport{Leg: 1, Name: "codec", Vectors: 178, Passed: 177, Provisional: 1}.String()
	if want := "leg 1 codec: 178 vectors, 177 passed, 1 provisional"; got != want {
		t.Fatalf("report line %q, want %q", got, want)
	}
}

func TestLegReportCountsDeferred(t *testing.T) {
	got := LegReport{Leg: 1, Name: "codec", Vectors: 445, Passed: 300, Deferred: 132}.String()
	if want := "leg 1 codec: 445 vectors, 300 passed, 0 provisional, 132 deferred"; got != want {
		t.Fatalf("report line %q, want %q", got, want)
	}
	got = LegReport{Leg: 1, Name: "codec", Vectors: 178, Passed: 178}.String()
	if want := "leg 1 codec: 178 vectors, 178 passed, 0 provisional"; got != want {
		t.Fatalf("a report with nothing deferred reads %q, want %q", got, want)
	}
}
