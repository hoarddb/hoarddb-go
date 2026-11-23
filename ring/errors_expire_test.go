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
	"testing"
	"time"
)

func TestExpiresInIsTheClientsNowPlusTheDuration(t *testing.T) {
	before := time.Now()
	got := ExpiresIn(30 * time.Minute)
	after := time.Now()
	if got.Before(before.Add(30*time.Minute)) || got.After(after.Add(30*time.Minute)) {
		t.Fatalf("ExpiresIn(30m) = %v, want the client's now plus 30 minutes (between %v and %v)", got, before.Add(30*time.Minute), after.Add(30*time.Minute))
	}
}
