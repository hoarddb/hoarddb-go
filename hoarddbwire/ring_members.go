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
	"fmt"
	"math"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// RingMemberState is the state of a member a topology reply lists. A members
// list holds only the three states below: a joining or dead member is on neither
// list.
type RingMemberState uint8

// The states a members list holds, as the node writes them in a members blob.
const (
	RingMemberActive  RingMemberState = 2
	RingMemberDown    RingMemberState = 3
	RingMemberLeaving RingMemberState = 4
)

// String prints the state's word, or its number for a state the list does not hold.
func (state RingMemberState) String() string {
	switch state {
	case RingMemberActive:
		return "active"
	case RingMemberDown:
		return "down"
	case RingMemberLeaving:
		return "leaving"
	}
	return fmt.Sprintf("state %d", uint8(state))
}

// RingMember is one member a topology reply lists. A witness is listed in the
// witnesses blob, is never ranked in the table, and has the same fields.
type RingMember struct {
	ID         string
	State      RingMemberState
	Votes      bool
	DataAddr   string
	ClientAddr string
}

// The bounds a members blob holds. Each is the node's own value.
const (
	ringMemberVoteBit       = 1 << 0         // flags bit 0: the member votes
	ringMemberMinBytes      = 6              // the least a member row takes
	ringMemberMaxText       = 256            // an id and a data address
	ringMemberMaxClientAddr = 256            // a client address
	ringMembersMaxCount     = math.MaxUint16 // the most members a table ranks
)

// DecodeRingMembers reads a members blob or a witnesses blob, which share one
// layout: the count, then per member a state byte, a flags byte, the id, the data
// address and the client address, each text behind a uvarint length. The members
// are listed by id bytes with no id twice, because a table's rank indices count in
// that order. Every length is checked against the bytes left before anything is
// made from it.
func DecodeRingMembers(blob []byte) ([]RingMember, error) {
	count, pos, ok := readShortestUvarint(blob)
	if !ok {
		return nil, ringReplyRefusal("members", "the member count is missing or is not the shortest varint")
	}
	if count > ringMembersMaxCount {
		return nil, ringReplyRefusal("members", fmt.Sprintf("%d members are claimed and a table ranks at most %d", count, ringMembersMaxCount))
	}
	if count > uint64(len(blob)-pos)/ringMemberMinBytes {
		return nil, ringReplyRefusal("members", fmt.Sprintf("%d members are claimed but %d bytes follow, and a member takes at least %d", count, len(blob)-pos, ringMemberMinBytes))
	}
	members := make([]RingMember, 0, count)
	for index := range count {
		member, used, err := decodeRingMember(blob[pos:], index)
		if err != nil {
			return nil, err
		}
		if len(members) > 0 && members[len(members)-1].ID >= member.ID {
			return nil, ringReplyRefusal("members", fmt.Sprintf("member %q follows %q: members are listed once each, sorted by id bytes, because a table's rank indices count in that order", member.ID, members[len(members)-1].ID))
		}
		members = append(members, member)
		pos += used
	}
	if pos != len(blob) {
		return nil, ringReplyRefusal("members", fmt.Sprintf("%d bytes follow the last member", len(blob)-pos))
	}
	return members, nil
}

// decodeRingMember reads the member row at the start of raw, which is row index
// of the blob, and returns it with the bytes the row takes.
func decodeRingMember(raw []byte, index uint64) (RingMember, int, error) {
	if len(raw) < 2 {
		return RingMember{}, 0, ringReplyRefusal("members", fmt.Sprintf("member %d is cut short: its state and flags bytes are missing", index))
	}
	state := RingMemberState(raw[0])
	if state != RingMemberActive && state != RingMemberDown && state != RingMemberLeaving {
		return RingMember{}, 0, ringReplyRefusal("members", fmt.Sprintf("member %d has state byte %d: a members list holds only active (2), down (3) and leaving (4)", index, raw[0]))
	}
	flags := raw[1]
	if reserved := flags &^ ringMemberVoteBit; reserved != 0 {
		return RingMember{}, 0, ringReplyRefusal("members", fmt.Sprintf("member %d sets flag bits %#x, which this format reserves: only bit 0 (votes) is defined", index, reserved))
	}
	pos := 2
	id, used, err := readMemberText(raw[pos:], "id", ringMemberMaxText, index)
	if err != nil {
		return RingMember{}, 0, err
	}
	pos += used
	dataAddr, used, err := readMemberText(raw[pos:], "data address", ringMemberMaxText, index)
	if err != nil {
		return RingMember{}, 0, err
	}
	pos += used
	clientAddr, used, err := readMemberText(raw[pos:], "client address", ringMemberMaxClientAddr, index)
	if err != nil {
		return RingMember{}, 0, err
	}
	pos += used
	switch {
	case id == "" || !utf8.ValidString(id):
		return RingMember{}, 0, ringReplyRefusal("members", fmt.Sprintf("member %d has an empty id or one that is not UTF-8", index))
	case dataAddr == "" || !plainMemberText(dataAddr):
		return RingMember{}, 0, ringReplyRefusal("members", fmt.Sprintf("member %d has an empty data address, or one that is not UTF-8 or holds a NUL byte", index))
	case !plainMemberText(clientAddr):
		return RingMember{}, 0, ringReplyRefusal("members", fmt.Sprintf("member %d has a client address that is not UTF-8 or holds a NUL byte", index))
	}
	return RingMember{ID: id, State: state, Votes: flags&ringMemberVoteBit != 0, DataAddr: dataAddr, ClientAddr: clientAddr}, pos, nil
}

// readMemberText reads one uvarint-prefixed text of at most limit bytes from raw.
// It returns the text and the bytes the prefix and the text take together.
func readMemberText(raw []byte, what string, limit uint64, index uint64) (string, int, error) {
	length, used, ok := readShortestUvarint(raw)
	if !ok || length > limit || length > uint64(len(raw)-used) {
		return "", 0, ringReplyRefusal("members", fmt.Sprintf("member %d's %s is cut short, longer than %d bytes, or its length is not the shortest varint", index, what, limit))
	}
	end := used + int(length)
	return string(raw[used:end]), end, nil
}

// readShortestUvarint reads a uvarint that is the shortest form of its number. A
// longer form of the same number is other bytes, and the node refuses them.
func readShortestUvarint(raw []byte) (uint64, int, bool) {
	number, used := binary.Uvarint(raw)
	if used <= 0 || used != len(binary.AppendUvarint(nil, number)) {
		return 0, 0, false
	}
	return number, used, true
}

// plainMemberText reports whether a text is valid UTF-8 with no NUL byte.
func plainMemberText(text string) bool {
	return utf8.ValidString(text) && !strings.ContainsRune(text, 0)
}

// DialAddr is the address a driver dials for the member: its client address, and
// nothing when that is not a usable host and port. The data address is never
// dialled: it is the node's internode listener and does not speak the driver protocol.
func (member RingMember) DialAddr() string {
	if usableClientAddr(member.ClientAddr) {
		return member.ClientAddr
	}
	return ""
}

// usableClientAddr holds the checks the node makes of a client address that need
// no data address: a host and a numeric port from 1 to 65535, no space, control
// character, user info, path or zone, and no wildcard address. The node also
// requires the client host to match the data host. The frozen vectors name a
// client host of their own, so this driver does not check it; the node checks it
// when it writes the row.
func usableClientAddr(addr string) bool {
	if len(addr) == 0 || len(addr) > ringMemberMaxClientAddr {
		return false
	}
	for _, character := range addr {
		if unicode.IsSpace(character) || unicode.IsControl(character) || strings.ContainsRune("@/%", character) {
			return false
		}
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if number, err := strconv.ParseUint(port, 10, 16); err != nil || number == 0 {
		return false
	}
	ip, err := netip.ParseAddr(host)
	return err != nil || !ip.Unmap().IsUnspecified()
}
