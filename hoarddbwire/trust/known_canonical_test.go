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

package trust

import "testing"

// TestCanonicalServerAddressForms pins the one form of every spelling the
// hoardDB command line tool writes, so a line it recorded is found when the
// driver spells the same server another way. An empty want is a form both the
// tool and the driver refuse.
func TestCanonicalServerAddressForms(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"db.internal:7433", "db.internal:7433"},
		{"DB.Internal:7433", "db.internal:7433"},
		{"Db.Internal.:7433", "db.internal:7433"},
		{"db.internal.", "db.internal:7433"},
		{"db.internal", "db.internal:7433"},
		{"LOCALHOST", "localhost:7433"},
		{"db:07433", "db:7433"},
		{"db:4434", "db:4434"},
		{"under_score-host:1", "under_score-host:1"},
		{"127.0.0.1:7433", "127.0.0.1:7433"},
		{"127.0.0.1", "127.0.0.1:7433"},
		{"[::1]:7433", "[::1]:7433"},
		{"[0:0:0:0:0:0:0:1]:7433", "[::1]:7433"},
		{"[::0001]", "[::1]:7433"},
		{"[2001:DB8:0:0:1:0:0:1]:7433", "[2001:db8::1:0:0:1]:7433"},
		{"[2001:db8:0:0:1:0:0:0]:7433", "[2001:db8:0:0:1::]:7433"},
		{"[2001:db8:0:1:1:1:1:1]", "[2001:db8:0:1:1:1:1:1]:7433"},
		{"[FE80::1%eth0]:7433", "[fe80::1%eth0]:7433"},
		{"[::FFFF:127.0.0.1]:7433", "[::ffff:127.0.0.1]:7433"},
		{"", ""},
		{"::1:7433", ""},
		{"[127.0.0.1]:7433", ""},
		{"[::1", ""},
		{"[::1]7433", ""},
		{"db:", ""},
		{":7433", ""},
		{"db:0", ""},
		{"db:65536", ""},
		{"127.000.000.001:7433", ""},
		{"db..internal:7433", ""},
		{"dé.internal:7433", ""},
	}
	for _, test := range tests {
		got, err := CanonicalServerAddress(test.input)
		if test.want == "" {
			if err == nil {
				t.Errorf("CanonicalServerAddress(%q) = %q; the command line tool refuses it, so the driver must too", test.input, got)
			}
			continue
		}
		if err != nil || got != test.want {
			t.Errorf("CanonicalServerAddress(%q) = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
}
