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

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

func c16Topology(t *testing.T) *Topology {
	t.Helper()
	fixture := topologyFixtureBytes(t, "topology-client-addr-witness.bin")
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		conn.reply(frame, fixture)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	topology, err := client.Topology(testContext(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	return topology
}

// TestC16ADriverConnectsToTheClientAddrAndNeverToTheDataAddr: [GUARD Member.Addr, and so every owner the
// driver routes to, is the client address when the member published one and the data address when it did not].
func TestC16ADriverConnectsToTheClientAddrAndNeverToTheDataAddr(t *testing.T) {
	topology := c16Topology(t)
	if len(topology.Members) != 2 {
		t.Fatalf("members %+v", topology.Members)
	}
	a, b := topology.Members[0], topology.Members[1]
	if a.Addr != "a.example:7434" || a.ClientAddr != "a.example:7434" || a.DataAddr != "192.0.2.1:4433" || !a.Voter {
		t.Errorf("node-a is %+v", a)
	}
	if b.Addr != "" || b.ClientAddr != "" || b.DataAddr != "192.0.2.2:4433" || b.Voter {
		t.Errorf("node-b is %+v", b)
	}
	seen := map[string]bool{}
	for slot := range uint16(8192) {
		owner, ok := topology.Owner("app", slot)
		if !ok {
			t.Fatalf("slot %d has no owner", slot)
		}
		want := map[string]string{"node-a": "a.example:7434", "node-b": ""}[owner.ID]
		if owner.Addr != want {
			t.Fatalf("slot %d: owner %s routes to %q, want %q", slot, owner.ID, owner.Addr, want)
		}
		seen[owner.ID] = true
	}
	if len(seen) != 2 {
		t.Errorf("owners seen: %v", seen)
	}
}

// TestTopologyMemberAddressFallsBackAsDriverAddr: [GUARD Member.Addr of every ranked member and witness
// is what the server's Member.DriverAddr gives: the client address the member published, and empty when
// it published none. It never falls back to the data address, which serves no driver].
func TestTopologyMemberAddressFallsBackAsDriverAddr(t *testing.T) {
	topology := c16Topology(t)
	listed := append(append([]Member(nil), topology.Members...), topology.Witnesses...)
	if len(listed) != 3 {
		t.Fatalf("the fixture names two members and a witness, got %+v", listed)
	}
	withAddr, without := 0, 0
	for _, member := range listed {
		if member.Addr != member.ClientAddr {
			t.Errorf("%s: Addr %q is not its client address %q", member.ID, member.Addr, member.ClientAddr)
		}
		if member.DataAddr == "" || member.Addr == member.DataAddr {
			t.Errorf("%s: Addr %q fell back to the data address %q", member.ID, member.Addr, member.DataAddr)
		}
		if member.ClientAddr == "" {
			without++
		} else {
			withAddr++
		}
	}
	if withAddr != 2 || without != 1 {
		t.Errorf("the fixture holds two members with a client address and one without, got %d and %d", withAddr, without)
	}
}

// TestC5AWitnessIsListedAndNeverAnOwner: [GUARD witnesses come back apart from the ranked members, with their
// voter flag, and no slot is ever routed to one].
func TestC5AWitnessIsListedAndNeverAnOwner(t *testing.T) {
	topology := c16Topology(t)
	if len(topology.Witnesses) != 1 {
		t.Fatalf("witnesses %+v", topology.Witnesses)
	}
	if w := topology.Witnesses[0]; w.ID != "node-w" || !w.Voter || w.Addr != "w.example:7434" || w.State != "active" {
		t.Errorf("witness %+v", w)
	}
	for slot := range uint16(8192) {
		if owner, _ := topology.Owner("app", slot); owner.ID == "node-w" {
			t.Fatalf("slot %d routed to the witness", slot)
		}
	}
}

// TestC5TheTopologyCarriesTheVoterCount: [GUARD Topology.Voters is the reply's committed voter count, which includes a
// voter on neither list, and a matching view held keeps it].
func TestC5TheTopologyCarriesTheVoterCount(t *testing.T) {
	fixture, unchanged := topologyFixtureBytes(t, "topology-voters.bin"), topologyFixtureBytes(t, "topology-unchanged.bin")
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		request, ok := topologyRequestOf(t, frame.Payload)
		if !ok {
			return
		}
		reply := fixture
		if request.Epoch == 4 && request.StateSeq == 9 {
			reply = unchanged
		}
		conn.reply(frame, reply)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	topology, err := client.Topology(testContext(t), nil)
	if err != nil || topology.Voters != 3 {
		t.Fatalf("voters %d, %v", topology.Voters, err)
	}
	again, err := client.Topology(testContext(t), topology)
	if err != nil || again.Voters != 3 {
		t.Fatalf("the held view lost its voter count: %d, %v", again.Voters, err)
	}
}
