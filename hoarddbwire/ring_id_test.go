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
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// The three goldens are the string rows of entry-ring-conventions.md section 2
// as the vectors carry them: "ada" is 506164610000 in every RING_KEYED row.
func TestStringIDsEncodeAsTheConventionsTable(t *testing.T) {
	cases := []struct {
		name, id, wantHex string
	}{
		{"ada", "ada", "506164610000"},
		{"p1", "p1", "5070310000"},
		{"an id holding a zero byte", "a\x00b", "5061 00ff 62 0000"},
	}
	for _, tc := range cases {
		want, err := hex.DecodeString(strings.ReplaceAll(tc.wantHex, " ", ""))
		if err != nil {
			t.Fatal(err)
		}
		got, err := hoarddbwire.EncodeStringID(tc.id)
		if err != nil {
			t.Errorf("%s: EncodeStringID(%q): %v", tc.name, tc.id, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: EncodeStringID(%q) = % x, want % x", tc.name, tc.id, got, want)
		}
	}
}

func TestAStringIDOverTheCapIsRefused(t *testing.T) {
	cases := []struct {
		name    string
		id      string
		wantLen int // the encoded length when it passes; 0 when it is refused
	}{
		{"1021 ASCII bytes encode to 1024", strings.Repeat("a", 1021), 1024},
		{"1022 ASCII bytes encode to 1025", strings.Repeat("a", 1022), 0},
		{"510 zero bytes encode to 1023", strings.Repeat("\x00", 510), 1023},
		{"511 zero bytes encode to 1025", strings.Repeat("\x00", 511), 0},
		{"invalid UTF-8", "a\xffb", 0},
	}
	for _, tc := range cases {
		encoded, err := hoarddbwire.EncodeStringID(tc.id)
		if tc.wantLen == 0 {
			if !errors.Is(err, hoarddbwire.ErrRingRequest) {
				t.Errorf("%s: err = %v, want ErrRingRequest", tc.name, err)
			} else if !strings.Contains(err.Error(), "id") {
				t.Errorf("%s: message %q does not name the id", tc.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
		} else if len(encoded) != tc.wantLen {
			t.Errorf("%s: encoded to %d bytes, want %d", tc.name, len(encoded), tc.wantLen)
		}
	}
	if hoarddbwire.MaxRingIDBytes != 1024 {
		t.Errorf("MaxRingIDBytes = %d, want 1024", hoarddbwire.MaxRingIDBytes)
	}
}

func TestADecodedIDHandsBackWhatWasWritten(t *testing.T) {
	for _, id := range []string{"ada", "a\x00b", "\x00", "\x00\x00x", "grace hopper", "naïve"} {
		encoded, err := hoarddbwire.EncodeStringID(id)
		if err != nil {
			t.Fatalf("EncodeStringID(%q): %v", id, err)
		}
		// Bytes after the id belong to the next member and must stay unread.
		withTail := append(append([]byte{}, encoded...), 0x00, 0x04, 0xAA)
		got, width, err := hoarddbwire.DecodeStringID(withTail)
		if err != nil {
			t.Errorf("DecodeStringID of the id %q: %v", id, err)
			continue
		}
		if got != id || width != len(encoded) {
			t.Errorf("DecodeStringID of the id %q = (%q, %d), want (%q, %d)", id, got, width, id, len(encoded))
		}
	}
	refused := map[string][]byte{
		"a first byte of 0x30":          {0x30, 0x61, 0x00, 0x00},
		"no bytes":                      nil,
		"a tag and no terminator":       {0x50, 0x61},
		"a zero followed by a stray 01": {0x50, 0x61, 0x00, 0x01, 0x00, 0x00},
		"a zero at the very end":        {0x50, 0x61, 0x00},
		"invalid UTF-8":                 {0x50, 0xff, 0x00, 0x00},
		"over the cap":                  append(append([]byte{0x50}, bytes.Repeat([]byte{'a'}, 1022)...), 0x00, 0x00),
	}
	for name, encoded := range refused {
		_, _, err := hoarddbwire.DecodeStringID(encoded)
		if !errors.Is(err, hoarddbwire.ErrRingRequest) {
			t.Errorf("%s: err = %v, want ErrRingRequest", name, err)
		}
	}
	if _, _, err := hoarddbwire.DecodeStringID([]byte{0x30, 0x61, 0x00, 0x00}); err == nil || !strings.Contains(err.Error(), "0x30") {
		t.Errorf("a first byte of 0x30: err = %v, want a message naming 0x30", err)
	}
}
