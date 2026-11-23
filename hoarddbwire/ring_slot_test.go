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

package hoarddbwire

import (
	"fmt"
	"testing"
)

// countingBytes returns the n bytes 0x00, 0x01, ... that the known answers hash.
func countingBytes(n int) []byte {
	out := make([]byte, n)
	for index := range out {
		out[index] = byte(index)
	}
	return out
}

// mustEncodeStringID encodes a string id or fails the test.
func mustEncodeStringID(t *testing.T, id string) []byte {
	t.Helper()
	encoded, err := EncodeStringID(id)
	if err != nil {
		t.Fatalf("EncodeStringID(%q): %v", id, err)
	}
	return encoded
}

// TestXXH64KnownAnswers pins the xxHash64 of this repository (seed 0) to the
// published reference values and to the 32-byte four-lane path.
func TestXXH64KnownAnswers(t *testing.T) {
	cases := []struct {
		name  string
		input []byte
		want  uint64
	}{
		{"empty", []byte{}, 0xef46db3751d8e999},
		{"the letter a", []byte("a"), 0xd24ec4f1a98c6e5b},
		{"1 counting byte", countingBytes(1), 0xe934a84adb052768},
		{"3 counting bytes", countingBytes(3), 0xe5c7bb4533bc65dd},
		{"4 counting bytes", countingBytes(4), 0xffced8604453cc1e},
		{"8 counting bytes", countingBytes(8), 0x884a173614b81b8d},
		{"31 counting bytes", countingBytes(31), 0xc346d2b59b4d8ee1},
		{"32 counting bytes", countingBytes(32), 0xcbf59c5116ff32b4},
		{"33 counting bytes", countingBytes(33), 0x0c535d1acafb8ead},
		{"64 counting bytes", countingBytes(64), 0xf7c67301db6713f0},
		{"100 counting bytes", countingBytes(100), 0x6ac1e58032166597},
	}
	for _, c := range cases {
		if got := ringXXH64(c.input); got != c.want {
			t.Errorf("ringXXH64 of %s = %016x, want %016x", c.name, got, c.want)
		}
	}
}

// TestTheSlotRowsOfConventionsSection3 pins six slots of
// entry-ring-conventions.md section 3, the same six rows as
// storage/testdata/slot-vectors.json. The two last rows break a missing length
// prefix: without it ("a", "bc") and ("ab", "c") would hash alike.
func TestTheSlotRowsOfConventionsSection3(t *testing.T) {
	rows := []struct {
		name             string
		database, bucket string
		encodedID        []byte
		slot             uint16
	}{
		{"app.Person p1", "app", "Person", mustEncodeStringID(t, "p1"), 4962},
		{"app.Users ada", "app", "Users", mustEncodeStringID(t, "ada"), 5737},
		{"app.Users with a null id", "app", "Users", []byte{0x10}, 4259},
		{"empty database and bucket", "", "", mustEncodeStringID(t, "x"), 1089},
		{"a and bc with a null id", "a", "bc", []byte{0x10}, 16},
		{"ab and c with a null id", "ab", "c", []byte{0x10}, 7264},
	}
	for _, row := range rows {
		if got := RingSlot(row.database, row.bucket, row.encodedID); got != row.slot {
			t.Errorf("RingSlot of %s = %d, want %d", row.name, got, row.slot)
		}
	}
}

// TestAPlacementKeyIsLengthPrefixed pins the placement key bytes, which the
// server freezes in storage/testdata/slot-vectors.json.
func TestAPlacementKeyIsLengthPrefixed(t *testing.T) {
	const want = "0361707006506572736f6e5070310000"
	got := RingPlacementKey("app", "Person", mustEncodeStringID(t, "p1"))
	if fmt.Sprintf("%x", got) != want {
		t.Errorf("RingPlacementKey = %x, want %s", got, want)
	}
}

// TestTheSlotIsThirteenBits checks that no key lands past slot 8191: the slot
// is the top 13 bits of the hash, so 10,000 keys all fall below 8192.
func TestTheSlotIsThirteenBits(t *testing.T) {
	for index := range 10000 {
		encodedID := mustEncodeStringID(t, fmt.Sprintf("key-%d", index))
		if slot := RingSlot("app", "Users", encodedID); slot >= 8192 {
			t.Fatalf("RingSlot of key-%d = %d, want below 8192", index, slot)
		}
	}
}
