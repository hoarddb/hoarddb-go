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

import "fmt"

// LegReport is the one line a leg prints (handbook/testing.md section 2).
type LegReport struct {
	// Leg is the leg's number and Name its short name ("codec").
	Leg  int
	Name string
	// Vectors is how many vectors the leg ran, Passed how many frozen ones
	// passed, and Provisional how many provisional ones ran: a provisional
	// vector's result is logged and never fails the leg.
	Vectors, Passed, Provisional int
	// Deferred is how many vectors a later build step owns (Deferred in
	// deferred.go): they ran, still fail, and are held back, not passed.
	Deferred int
}

// String renders "leg 1 codec: N vectors, N passed, P provisional", with
// ", D deferred" appended only when D is above zero.
func (report LegReport) String() string {
	line := fmt.Sprintf("leg %d %s: %d vectors, %d passed, %d provisional",
		report.Leg, report.Name, report.Vectors, report.Passed, report.Provisional)
	if report.Deferred > 0 {
		line += fmt.Sprintf(", %d deferred", report.Deferred)
	}
	return line
}
