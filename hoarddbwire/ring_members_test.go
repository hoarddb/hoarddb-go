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
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// topologyBlobs reads the members and witnesses blobs of a RING_REPLY payload.
type topologyBlobs struct {
	Members   []byte `bson:"members"`
	Witnesses []byte `bson:"witnesses,omitempty"`
}

// vectorTopologyBlobs reads the blobs of a topology reply row from the row's
// own payload_hex, so no expected member is taken from an encoder under test.
func vectorTopologyBlobs(t *testing.T, name string) topologyBlobs {
	t.Helper()
	row := ringListVector(t, name)
	if row.Response == nil {
		t.Fatalf("%q has no response", name)
	}
	var blobs topologyBlobs
	if err := codec.Unmarshal(unhex(t, row.Response.PayloadHex), &blobs); err != nil {
		t.Fatalf("%q: the payload does not read as a topology reply: %v", name, err)
	}
	return blobs
}

// appendMemberRow appends one member row of the members layout with the bytes
// given, so a test can write a row the encoder would not.
func appendMemberRow(dst []byte, state, flags byte, id, dataAddr, clientAddr string) []byte {
	dst = append(dst, state, flags)
	for _, text := range []string{id, dataAddr, clientAddr} {
		dst = binary.AppendUvarint(dst, uint64(len(text)))
		dst = append(dst, text...)
	}
	return dst
}

// appendMembers appends a members blob: the count, then each member's row. The
// flags byte's bit 0 is the vote.
func appendMembers(dst []byte, members []hoarddbwire.RingMember) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(members)))
	for _, member := range members {
		var flags byte
		if member.Votes {
			flags = 1
		}
		dst = appendMemberRow(dst, byte(member.State), flags, member.ID, member.DataAddr, member.ClientAddr)
	}
	return dst
}

// blobOf is a members blob of the rows given, counted as the rows are.
func blobOf(rows ...[]byte) []byte {
	blob := binary.AppendUvarint(nil, uint64(len(rows)))
	for _, row := range rows {
		blob = append(blob, row...)
	}
	return blob
}

// TestTheFirstCallMembersDecode reads the members and witnesses of the first
// call's row and of the single-node row, and checks each against the row's bytes.
func TestTheFirstCallMembersDecode(t *testing.T) {
	first := vectorTopologyBlobs(t, "ring topology: the first call after AUTH")
	members, err := hoarddbwire.DecodeRingMembers(first.Members)
	if err != nil {
		t.Fatalf("the first call's members are refused: %v", err)
	}
	wantMembers := []hoarddbwire.RingMember{
		{ID: "node-a", State: hoarddbwire.RingMemberActive, Votes: true, DataAddr: "192.0.2.1:4433", ClientAddr: "a.example:7434"},
		{ID: "node-b", State: hoarddbwire.RingMemberDown, Votes: true, DataAddr: "192.0.2.2:4433"},
		{ID: "node-c", State: hoarddbwire.RingMemberActive, DataAddr: "192.0.2.3:4433"},
	}
	if !reflect.DeepEqual(members, wantMembers) {
		t.Errorf("the first call's members = %+v, want %+v", members, wantMembers)
	}

	witnesses, err := hoarddbwire.DecodeRingMembers(first.Witnesses)
	if err != nil {
		t.Fatalf("the first call's witnesses are refused: %v", err)
	}
	wantWitnesses := []hoarddbwire.RingMember{
		{ID: "node-w", State: hoarddbwire.RingMemberActive, Votes: true, DataAddr: "192.0.2.4:4433", ClientAddr: "w.example:7434"},
	}
	if !reflect.DeepEqual(witnesses, wantWitnesses) {
		t.Errorf("the first call's witnesses = %+v, want %+v", witnesses, wantWitnesses)
	}

	single := vectorTopologyBlobs(t, "ring topology: a single node")
	one, err := hoarddbwire.DecodeRingMembers(single.Members)
	if err != nil {
		t.Fatalf("the single node's members are refused: %v", err)
	}
	wantOne := []hoarddbwire.RingMember{
		{ID: "node-a", State: hoarddbwire.RingMemberActive, Votes: true, DataAddr: "127.0.0.1:4433"},
	}
	if !reflect.DeepEqual(one, wantOne) {
		t.Errorf("the single node's members = %+v, want %+v", one, wantOne)
	}
}

// TestAMembersBlobRefusalNamesItsBound refuses each bound a members blob holds,
// and the three vector rows that break one: each refusal wraps ErrRingReply and
// names the members.
func TestAMembersBlobRefusalNamesItsBound(t *testing.T) {
	valid := appendMemberRow(nil, 2, 1, "node-a", "192.0.2.1:4433", "")
	nodeB := appendMemberRow(nil, 2, 1, "node-b", "192.0.2.2:4433", "")
	cases := []struct {
		name string
		blob []byte
	}{
		{"the count is not the shortest varint", append([]byte{0x81, 0x00}, valid...)},
		{"the count is above 65535", append(binary.AppendUvarint(nil, math.MaxUint16+1), make([]byte, 10)...)},
		{"the count is more than the bytes after it can hold", append(binary.AppendUvarint(nil, 5), valid...)},
		{"the state byte is 0", blobOf(appendMemberRow(nil, 0, 1, "node-a", "192.0.2.1:4433", ""))},
		{"the state is joining", blobOf(appendMemberRow(nil, 1, 1, "node-a", "192.0.2.1:4433", ""))},
		{"the state is dead", blobOf(appendMemberRow(nil, 5, 1, "node-a", "192.0.2.1:4433", ""))},
		{"the state byte is above 5", blobOf(appendMemberRow(nil, 6, 1, "node-a", "192.0.2.1:4433", ""))},
		{"flags bit 1 is set", blobOf(appendMemberRow(nil, 2, 0x02, "node-a", "192.0.2.1:4433", ""))},
		{"flags bit 7 is set", blobOf(appendMemberRow(nil, 2, 0x80, "node-a", "192.0.2.1:4433", ""))},
		{"the id is empty", blobOf(appendMemberRow(nil, 2, 1, "", "192.0.2.1:4433", ""))},
		{"the id is 257 bytes", blobOf(appendMemberRow(nil, 2, 1, strings.Repeat("n", 257), "192.0.2.1:4433", ""))},
		{"the id is not UTF-8", blobOf(appendMemberRow(nil, 2, 1, "node-\xff", "192.0.2.1:4433", ""))},
		{"the data address is empty", blobOf(appendMemberRow(nil, 2, 1, "node-a", "", ""))},
		{"the data address is 257 bytes", blobOf(appendMemberRow(nil, 2, 1, "node-a", strings.Repeat("d", 257), ""))},
		{"the data address holds a NUL", blobOf(appendMemberRow(nil, 2, 1, "node-a", "192.0.2.1\x00:4433", ""))},
		{"the data address is not UTF-8", blobOf(appendMemberRow(nil, 2, 1, "node-a", "192.0.2.\xff:4433", ""))},
		{"the client address is 257 bytes", blobOf(appendMemberRow(nil, 2, 1, "node-a", "192.0.2.1:4433", strings.Repeat("c", 257)))},
		{"the client address holds a NUL", blobOf(appendMemberRow(nil, 2, 1, "node-a", "192.0.2.1:4433", "a.example\x00:7434"))},
		{"the client address is not UTF-8", blobOf(appendMemberRow(nil, 2, 1, "node-a", "192.0.2.1:4433", "a.\xff:7434"))},
		{"the ids are out of order", blobOf(nodeB, valid)},
		{"an id is listed twice", blobOf(valid, valid)},
		{"a byte follows the last member", append(blobOf(valid), 0x00)},
		{"a member is cut short", blobOf(valid)[:len(blobOf(valid))-3]},
		{"the count has no member", []byte{0x01}},
		{"the blob is empty", nil},
		{"the members out of order vector row", vectorTopologyBlobs(t, "ring topology reply refused: members out of order").Members},
		{"the reserved flag bit vector row", vectorTopologyBlobs(t, "ring topology reply refused: a reserved flag bit").Members},
		{"the non-canonical varint vector row", vectorTopologyBlobs(t, "ring topology reply refused: a non-canonical varint").Members},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := hoarddbwire.DecodeRingMembers(testCase.blob)
			if err == nil {
				t.Fatal("the blob is accepted; the bound must refuse it")
			}
			if !errors.Is(err, hoarddbwire.ErrRingReply) {
				t.Errorf("the refusal %v does not wrap ErrRingReply", err)
			}
			if !strings.Contains(err.Error(), "members") {
				t.Errorf("the refusal %q does not name the members", err)
			}
		})
	}
}

// TestAMemberAtItsLimitIsAccepted accepts the longest texts a bound allows and a
// leaving member with no client address, so the bounds are not off by one.
func TestAMemberAtItsLimitIsAccepted(t *testing.T) {
	longest := appendMemberRow(nil, 4, 0, strings.Repeat("n", 256), strings.Repeat("d", 256), strings.Repeat("c", 256))
	members, err := hoarddbwire.DecodeRingMembers(blobOf(longest))
	if err != nil {
		t.Fatalf("a member at every limit is refused: %v", err)
	}
	want := []hoarddbwire.RingMember{{
		ID:         strings.Repeat("n", 256),
		State:      hoarddbwire.RingMemberLeaving,
		DataAddr:   strings.Repeat("d", 256),
		ClientAddr: strings.Repeat("c", 256),
	}}
	if !reflect.DeepEqual(members, want) {
		t.Errorf("the members = %+v, want one leaving member at every limit", members)
	}
}

// TestAMemberCountThatCannotFitIsRefusedBeforeItAllocates refuses a count of
// 65535 that ten bytes cannot hold, and checks the refusal allocates no members
// slice. A count of allocations would not show one large make, so the bytes are
// measured.
func TestAMemberCountThatCannotFitIsRefusedBeforeItAllocates(t *testing.T) {
	blob := append(binary.AppendUvarint(nil, math.MaxUint16), make([]byte, 10)...)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := hoarddbwire.DecodeRingMembers(blob)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, hoarddbwire.ErrRingReply) {
		t.Fatalf("the refusal = %v, want an ErrRingReply", err)
	}
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 64<<10 {
		t.Errorf("the refusal allocated %d bytes: the members slice was made before the count was checked against the bytes", grown)
	}
}

// TestDialAddrIsTheClientAddressOrNothing dials a member's client address only:
// a usable host:port comes back unchanged, and every other address, including
// a data address the member has no client address for, gives nothing.
func TestDialAddrIsTheClientAddressOrNothing(t *testing.T) {
	first := vectorTopologyBlobs(t, "ring topology: the first call after AUTH")
	members, err := hoarddbwire.DecodeRingMembers(first.Members)
	if err != nil {
		t.Fatalf("the first call's members are refused: %v", err)
	}
	if len(members) < 2 {
		t.Fatalf("the first call lists %d members, want node-a and node-b", len(members))
	}
	if got := members[0].DialAddr(); got != "a.example:7434" {
		t.Errorf("node-a of the first call dials %q, want its client address a.example:7434", got)
	}
	if got := members[1].DialAddr(); got != "" {
		t.Errorf("node-b, with no client address, dials %q, want nothing", got)
	}

	cases := []struct {
		name       string
		clientAddr string
		want       string
	}{
		{"a host and port", "[2001:db8::1]:7434", "[2001:db8::1]:7434"},
		{"the highest port", "a.example:65535", "a.example:65535"},
		{"no client address", "", ""},
		{"a host with no port", "a.example", ""},
		{"port 0", "a.example:0", ""},
		{"port 65536", "a.example:65536", ""},
		{"user info", "x@a.example:7434", ""},
		{"no host", ":7434", ""},
		{"a space", "a.example:7434 ", ""},
		{"a path", "a.example:7434/x", ""},
		{"the IPv4 wildcard", "0.0.0.0:7434", ""},
		{"the IPv6 wildcard", "[::]:7434", ""},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			member := hoarddbwire.RingMember{ID: "node-x", DataAddr: "192.0.2.9:4433", ClientAddr: testCase.clientAddr}
			if got := member.DialAddr(); got != testCase.want {
				t.Errorf("DialAddr() = %q, want %q", got, testCase.want)
			}
		})
	}

	dataOnly := hoarddbwire.RingMember{ID: "node-x", DataAddr: "192.0.2.9:4433"}
	if got := dataOnly.DialAddr(); got != "" {
		t.Errorf("a member with only a data address dials %q, want nothing: the data address does not speak the driver protocol", got)
	}
}

// TestRingMemberStateWords names the three states a member is listed in and
// writes any other state as its number.
func TestRingMemberStateWords(t *testing.T) {
	cases := []struct {
		state hoarddbwire.RingMemberState
		want  string
	}{
		{hoarddbwire.RingMemberActive, "active"},
		{hoarddbwire.RingMemberDown, "down"},
		{hoarddbwire.RingMemberLeaving, "leaving"},
		{1, "state 1"},
		{5, "state 5"},
	}
	for _, testCase := range cases {
		if got := testCase.state.String(); got != testCase.want {
			t.Errorf("state %d prints %q, want %q", uint8(testCase.state), got, testCase.want)
		}
	}
}
