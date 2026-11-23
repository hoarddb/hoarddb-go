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
	"encoding/binary"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// topologyMembersBlob writes a members blob in the layout DecodeRingMembers reads: the count,
// then each member's state and flags bytes and its three uvarint-prefixed texts.
func topologyMembersBlob(members []hoarddbwire.RingMember) []byte {
	blob := binary.AppendUvarint(nil, uint64(len(members)))
	for _, member := range members {
		var flags byte
		if member.Votes {
			flags = 1
		}
		blob = append(blob, byte(member.State), flags)
		for _, text := range []string{member.ID, member.DataAddr, member.ClientAddr} {
			blob = binary.AppendUvarint(blob, uint64(len(text)))
			blob = append(blob, text...)
		}
	}
	return blob
}

// topologyTableBlob ranks every slot in the order the members are listed.
func topologyTableBlob(members int) []byte {
	var table []byte
	for range 8192 {
		table = binary.AppendUvarint(table, uint64(members))
		for index := range members {
			table = binary.AppendUvarint(table, uint64(index))
		}
	}
	return table
}

// topologyLeavingReply is the RING_REPLY a server encodes for a three-member cluster in which
// node-a is leaving, node-b is down and node-c is active, every slot ranked a, b, c, with RF 2.
// The server's SlotTable.Owner takes the first name of the copy set that is not down, so it
// names node-a: a leaving member still owns a slot; only a down one is skipped. A driver that
// skipped a leaving member as it skips a down one would name node-c instead.
//
// This module holds no topology encoder, and the server-side generator that writes
// testdata/topology-*.bin is outside this slice, so the bytes are built here from the layout
// hoarddbwire decodes; the layout is the one the committed fixtures use.
func topologyLeavingReply(t *testing.T) []byte {
	t.Helper()
	members := []hoarddbwire.RingMember{
		{ID: "node-a", State: hoarddbwire.RingMemberLeaving, DataAddr: "192.0.2.1:4433", ClientAddr: "a.example:7434"},
		{ID: "node-b", State: hoarddbwire.RingMemberDown, DataAddr: "192.0.2.2:4433"},
		{ID: "node-c", State: hoarddbwire.RingMemberActive, DataAddr: "192.0.2.3:4433", ClientAddr: "c.example:7434"},
	}
	payload, err := bson.Marshal(hoarddbwire.RingTopologyReply{Epoch: 4, StateSeq: 9,
		Members: topologyMembersBlob(members), Table: topologyTableBlob(len(members)),
		Databases: []hoarddbwire.RingDatabase{{Name: "app", ID: 1, RF: 2}}})
	if err != nil {
		t.Fatalf("encoding the leaving-member topology: %v", err)
	}
	return payload
}

// TestTopologyRoutesToALeavingMemberAndSkipsADownOne: [GUARD RingSlotTable.Owner names a leaving
// member as the server's SlotTable.Owner does; only a down member is skipped].
func TestTopologyRoutesToALeavingMemberAndSkipsADownOne(t *testing.T) {
	payload := topologyLeavingReply(t)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		conn.reply(frame, payload)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	topology, err := client.Topology(testContext(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, member := range topology.Members {
		states[member.ID] = member.State
	}
	if states["node-a"] != "leaving" || states["node-b"] != "down" || states["node-c"] != "active" {
		t.Fatalf("member states %v, want node-a leaving, node-b down, node-c active", states)
	}
	for slot := range uint16(8192) {
		owner, ok := topology.Owner("app", slot)
		if !ok || owner.ID != "node-a" {
			t.Fatalf("slot %d: owner %q/%v, want node-a: a leaving member owns, only the down node-b is skipped", slot, owner.ID, ok)
		}
	}
}
