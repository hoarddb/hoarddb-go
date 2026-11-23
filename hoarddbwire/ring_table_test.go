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
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// tableOf writes a wire table of the layout in the topology note: for each of
// the 8192 slots a count, then that many rank indices, each a uvarint. The
// rows of the ranking come from rowOf(slot).
func tableOf(rowOf func(slot int) []uint64) []byte {
	var table []byte
	for slot := range ringTableSlots {
		row := rowOf(slot)
		table = binary.AppendUvarint(table, uint64(len(row)))
		for _, index := range row {
			table = binary.AppendUvarint(table, index)
		}
	}
	return table
}

// namedMembers is count members, listed by id as the node lists them.
func namedMembers(count int) []RingMember {
	members := make([]RingMember, count)
	for index := range members {
		members[index] = RingMember{ID: fmt.Sprintf("node-%05d", index), State: RingMemberActive, DataAddr: "x"}
	}
	return members
}

// rotation ranks the members of a cluster of count starting at slot modulo count.
func rotation(count int) func(slot int) []uint64 {
	return func(slot int) []uint64 {
		row := make([]uint64, count)
		for place := range row {
			row[place] = uint64((slot + place) % count)
		}
		return row
	}
}

func idsOf(members []RingMember) string {
	ids := make([]string, len(members))
	for index, member := range members {
		ids[index] = member.ID
	}
	return strings.Join(ids, ",")
}

// TestATableDecodesItsRanks reads a table of three members and one of 130,
// whose rank indices past 127 take two bytes, and checks the first, a middle
// and the last slot against the rotation that wrote them.
func TestATableDecodesItsRanks(t *testing.T) {
	for _, count := range []int{3, 130} {
		members := namedMembers(count)
		table, err := decodeRingSlotTable(tableOf(rotation(count)), members)
		if err != nil {
			t.Fatalf("%d members: the table is refused: %v", count, err)
		}
		for _, slot := range []int{0, 1, 4962, 8191} {
			ranking := table.Ranking(uint16(slot))
			if len(ranking) != count {
				t.Fatalf("%d members, slot %d: %d names ranked", count, slot, len(ranking))
			}
			for place, member := range ranking {
				if want := members[(slot+place)%count].ID; member.ID != want {
					t.Errorf("%d members, slot %d, place %d: %s, want %s", count, slot, place, member.ID, want)
				}
			}
		}
		if table.Ranking(8192) != nil || table.Ranking(65535) != nil {
			t.Errorf("%d members: a slot past 8191 has a ranking", count)
		}
	}
}

// TestATableOfNoMembersIsEightThousandZeroCounts reads the table a node with
// no ranked member writes: a zero count for each slot, and no ranking.
func TestATableOfNoMembersIsEightThousandZeroCounts(t *testing.T) {
	table, err := decodeRingSlotTable(make([]byte, ringTableSlots), nil)
	if err != nil {
		t.Fatalf("a table of zero counts is refused: %v", err)
	}
	if table.Ranking(0) != nil || table.Copies(0, 2) != nil {
		t.Errorf("a table of no members ranks %v", table.Ranking(0))
	}
	for _, length := range []int{ringTableSlots - 1, ringTableSlots + 1} {
		if _, err := decodeRingSlotTable(make([]byte, length), nil); err == nil {
			t.Errorf("a table of %d bytes and no members is accepted; each of the %d slots is one zero count", length, ringTableSlots)
		}
	}
	if _, found := table.Owner(0, 1); found {
		t.Error("a table of no members has an owner")
	}
}

// TestATableRefusalNamesItsBound refuses each way the node refuses a table:
// each refusal wraps ErrRingReply and names the table.
func TestATableRefusalNamesItsBound(t *testing.T) {
	members := namedMembers(3)
	good := tableOf(rotation(3))
	withRow := func(slot int, row []uint64) []byte {
		return tableOf(func(at int) []uint64 {
			if at == slot {
				return row
			}
			return rotation(3)(at)
		})
	}
	// 130 members: an index past 127 takes two bytes, so the table is longer than
	// the least a table can be and a cut or a padded byte reaches the slot loop.
	wide := namedMembers(130)
	goodWide := tableOf(rotation(130))
	cases := []struct {
		name    string
		members []RingMember
		table   []byte
	}{
		{"the table is shorter than a count and a rank for each slot", members, good[:ringTableSlots*4-1]},
		{"a byte follows slot 8191", members, append(append([]byte{}, good...), 0)},
		{"a slot lists fewer names than the members", members, withRow(7, []uint64{0, 1})},
		{"a slot lists more names than the members", members, withRow(7, []uint64{0, 1, 2, 0})},
		{"a rank index is not below the member count", members, withRow(7, []uint64{0, 1, 3})},
		{"a rank index is twice in a slot", members, withRow(7, []uint64{0, 1, 1})},
		{"a count is not the shortest varint", members, append([]byte{0x83, 0x00}, good[1:]...)},
		{"a rank index is not the shortest varint", members, append([]byte{0x03, 0x80, 0x00}, append([]byte{0x01, 0x02}, good[4:]...)...)},
		{"the table is empty", members, nil},
		{"a wide table ends one byte early", wide, goodWide[:len(goodWide)-1]},
		{"a wide table ends inside a two-byte rank index", wide, append(append([]byte{}, goodWide[:len(goodWide)-1]...), 0x80)},
		{"a byte follows a wide table", wide, append(append([]byte{}, goodWide...), 0)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := decodeRingSlotTable(testCase.table, testCase.members)
			if err == nil {
				t.Fatal("the table is accepted; the bound must refuse it")
			}
			if !errors.Is(err, ErrRingReply) {
				t.Errorf("the refusal %v does not wrap ErrRingReply", err)
			}
			if !strings.Contains(err.Error(), "table") {
				t.Errorf("the refusal %q does not name the table", err)
			}
		})
	}
}

// TestATableThatCannotFitIsRefusedBeforeItAllocates gives 65535 members and a
// table of 1 KiB: a table needs 8192 bytes for each member and one more, so the
// length refuses it, and nothing the size of 8192 ranks for each member is made
// first. A count of allocations alone would miss one large make, so the bytes
// allocated are held as well.
func TestATableThatCannotFitIsRefusedBeforeItAllocates(t *testing.T) {
	members := make([]RingMember, 65535)
	table := make([]byte, 1024)
	if _, err := decodeRingSlotTable(table, members); err == nil || !errors.Is(err, ErrRingReply) {
		t.Fatalf("a 1 KiB table of 65535 members: error %v, want a refusal wrapping ErrRingReply", err)
	}
	allocations := testing.AllocsPerRun(20, func() {
		_, _ = decodeRingSlotTable(table, members)
	})
	// The refusal's own message takes about ten allocations, and the race
	// detector moves that count by one, so the bound is loose on purpose: one
	// large make is caught by the bytes below, not by this count.
	if allocations >= 32 {
		t.Errorf("the refusal made %.0f allocations, want under 32", allocations)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _ = decodeRingSlotTable(table, members)
	runtime.ReadMemStats(&after)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 64<<10 {
		t.Errorf("the refusal allocated %d bytes, want under 64 KiB: the length must refuse before the ranks are made", grown)
	}
}
