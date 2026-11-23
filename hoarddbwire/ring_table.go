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
	"math"
	"slices"
)

// ringTableSlots is the slots of the table: cluster tableSlots.
const ringTableSlots = 8192

// RingSlotTable is the ranking of every slot of the ring, as a topology reply
// carries it: for each of the 8192 slots the ranked members, best first, down
// members included so a caller can see which one it skips. It holds the members
// once and 2 bytes of rank index for each member of each slot.
type RingSlotTable struct {
	members []RingMember
	ranks   []uint16 // 8192 rows of len(members) indices, best first
}

// row is the rank indices of a slot, or nil for a slot the table does not have
// or a table that ranks no member.
func (table RingSlotTable) row(slot uint16) []uint16 {
	count := len(table.members)
	if count == 0 || int(slot) >= ringTableSlots {
		return nil
	}
	return table.ranks[int(slot)*count : (int(slot)+1)*count]
}

// Ranking is the whole ranking of a slot, best first, down members included. It
// is nil for a slot past 8191 and for a table that ranks no member.
func (table RingSlotTable) Ranking(slot uint16) []RingMember {
	row := table.row(slot)
	if row == nil {
		return nil
	}
	ranking := make([]RingMember, len(row))
	for place, index := range row {
		ranking[place] = table.members[index]
	}
	return ranking
}

// Copies is the copy set of a slot: the first replicas names of its ranking, down
// names included, or all of them when the ranking is shorter. It is nil for a
// factor under 1.
func (table RingSlotTable) Copies(slot uint16, replicas int) []RingMember {
	row := table.row(slot)
	if row == nil || replicas < 1 {
		return nil
	}
	copies := make([]RingMember, 0, min(replicas, len(row)))
	for _, index := range row[:min(replicas, len(row))] {
		copies = append(copies, table.members[index])
	}
	return copies
}

// Owner is the first name of the copy set whose state is not down. A ranked
// member outside the copy set holds no copy, so it is never the owner even when
// every copy is down: the slot then has none and Owner reports false.
func (table RingSlotTable) Owner(slot uint16, replicas int) (RingMember, bool) {
	row := table.row(slot)
	if row == nil || replicas < 1 {
		return RingMember{}, false
	}
	for _, index := range row[:min(replicas, len(row))] {
		if member := table.members[index]; member.State != RingMemberDown {
			return member, true
		}
	}
	return RingMember{}, false
}

// decodeRingSlotTable reads a wire table whose rank indices count the members
// of the same reply. For each of the 8192 slots it holds a count equal to the
// member count and that many rank indices, each below the member count and none
// twice, every number the shortest form of its uvarint, and no byte after the
// last slot. The length is checked first: a table of m members takes at least
// one byte for the count and one for each rank of every slot, so a shorter one
// is refused before the ranks, 2 bytes for each member of each slot, are made.
func decodeRingSlotTable(raw []byte, members []RingMember) (RingSlotTable, error) {
	count := len(members)
	if count > math.MaxUint16 {
		return RingSlotTable{}, ringReplyRefusal("table", fmt.Sprintf("%d members are ranked and a table ranks at most %d", count, math.MaxUint16))
	}
	if least := ringTableSlots * (1 + count); len(raw) < least {
		return RingSlotTable{}, ringReplyRefusal("table", fmt.Sprintf("the table is %d bytes and the members listed need at least %d: a count and a rank index for each member of each of %d slots", len(raw), least, ringTableSlots))
	}
	ranks := make([]uint16, ringTableSlots*count)
	listed := make([]bool, count)
	position := 0
	for slot := range ringTableSlots {
		listing, used, ok := readShortestUvarint(raw[position:])
		if !ok {
			return RingSlotTable{}, ringReplyRefusal("table", fmt.Sprintf("slot %d starts at byte %d with a count that is cut short or is not the shortest varint", slot, position))
		}
		position += used
		if listing != uint64(count) {
			return RingSlotTable{}, ringReplyRefusal("table", fmt.Sprintf("slot %d lists %d names and the reply has %d ranked members: a slot lists every member", slot, listing, count))
		}
		row := ranks[slot*count : (slot+1)*count]
		clear(listed)
		for place := range row {
			index, used, ok := readShortestUvarint(raw[position:])
			if !ok {
				return RingSlotTable{}, ringReplyRefusal("table", fmt.Sprintf("slot %d has a rank index at byte %d that is cut short or is not the shortest varint", slot, position))
			}
			position += used
			if index >= uint64(count) || listed[index] {
				return RingSlotTable{}, ringReplyRefusal("table", fmt.Sprintf("slot %d names rank index %d, which is not below the %d members or is named twice", slot, index, count))
			}
			listed[index] = true
			row[place] = uint16(index)
		}
	}
	if position != len(raw) {
		return RingSlotTable{}, ringReplyRefusal("table", fmt.Sprintf("slot %d is the last, and %d more bytes follow it", ringTableSlots-1, len(raw)-position))
	}
	return RingSlotTable{members: slices.Clone(members), ranks: ranks}, nil
}
