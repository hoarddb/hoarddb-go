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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// The topology replies these tests answer with are committed bytes, testdata/topology-*.bin.
// This module holds no topology encoder: the server's test
// hoardDB/ringclienttest/topology_fixture_test.go builds each reply with the server's own
// encoder and fails when a file differs, so a fixture cannot go stale.
//
//	topology-three-members.bin         epoch 4, state_seq 9; node-a active, node-b down, node-c active,
//	                                   none with a client address; databases app (rf 2) and logs (rf 1)
//	topology-three-members-owners.bin  for each slot, one byte for app and one for logs: the index of the
//	                                   owner the server names among the members sorted by id, 0xFF for none
//	topology-unchanged.bin             epoch 4, state_seq 9, unchanged
//	topology-client-addr-witness.bin   node-a (voter, client address), node-b (no client address) and the
//	                                   witness node-w; database app (rf 2)
//	topology-voters.bin                node-a alone, voters 3

// topologyFixtureNoOwner is the owners-fixture byte of a slot whose every copy is down.
const topologyFixtureNoOwner = 0xFF

// topologyFixtureBytes reads one committed fixture.
func topologyFixtureBytes(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading the topology fixture: %v", err)
	}
	return raw
}

// topologyRequestOf reads the two numbers of a topology request the client sent.
func topologyRequestOf(t *testing.T, payload []byte) (hoarddbwire.RingTopologyRequest, bool) {
	t.Helper()
	var request hoarddbwire.RingTopologyRequest
	if err := bson.Unmarshal(payload, &request); err != nil {
		t.Errorf("the topology request is not a document of epoch and state_seq: %v", err)
		return request, false
	}
	return request, true
}

// TestTopologyDecodesTheMembersAndRoutesAsTheServerDoes: [GUARD the owner of a slot is the first name of the
// first RF names of the ranking whose state is not down, per database].
func TestTopologyDecodesTheMembersAndRoutesAsTheServerDoes(t *testing.T) {
	fixture, unchanged := topologyFixtureBytes(t, "topology-three-members.bin"), topologyFixtureBytes(t, "topology-unchanged.bin")
	owners := topologyFixtureBytes(t, "topology-three-members-owners.bin")
	var asked []hoarddbwire.RingTopologyRequest
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		request, ok := topologyRequestOf(t, frame.Payload)
		if !ok {
			return
		}
		asked = append(asked, request)
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
	defer func() { _ = client.Close() }()
	topology, err := client.Topology(testContext(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if topology.Epoch != 4 || topology.StateSeq != 9 || len(topology.Members) != 3 || topology.Members[1].DataAddr != "192.0.2.2:4433" || topology.Members[1].Addr != "" || topology.Members[1].State != "down" {
		t.Fatalf("topology %+v", topology)
	}
	if len(topology.Databases) != 2 || topology.Databases[0] != (TopologyDatabase{Name: "app", ID: 1, RF: 2}) {
		t.Fatalf("databases %+v", topology.Databases)
	}
	if len(owners) != 8192*2 {
		t.Fatalf("the owners fixture is %d bytes, want one byte for each of two databases of 8192 slots", len(owners))
	}
	for slot := range uint16(8192) {
		for column, database := range []string{"app", "logs"} {
			var want Member
			wantOK := false
			if index := owners[int(slot)*2+column]; index != topologyFixtureNoOwner {
				want, wantOK = topology.Members[index], true
			}
			got, ok := topology.Owner(database, slot)
			if ok != wantOK || got.ID != want.ID || got.DataAddr != want.DataAddr || got.Addr != want.Addr {
				t.Fatalf("%s slot %d: owner %+v/%v, want %+v/%v", database, slot, got, ok, want, wantOK)
			}
		}
	}
	if _, ok := topology.Owner("missing", 0); ok {
		t.Error("a database the topology does not list has an owner")
	}
	again, err := client.Topology(testContext(t), topology)
	if err != nil || again != topology {
		t.Fatalf("a matching view should give back the topology held: %v", err)
	}
	if len(asked) != 2 || asked[0] != (hoarddbwire.RingTopologyRequest{}) || asked[1] != (hoarddbwire.RingTopologyRequest{Epoch: 4, StateSeq: 9}) {
		t.Fatalf("requests %+v", asked)
	}
}

// TestTopologyRefusalsAreTypedErrors: [GUARD an OpError becomes the typed *Error the other calls return].
func TestTopologyRefusalsAreTypedErrors(t *testing.T) {
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		conn.refuse(frame, ringwire.CodePermissionDenied, "this session cannot read the topology")
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Topology(testContext(t), nil)
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != ringwire.CodePermissionDenied {
		t.Fatalf("a refused topology call returned %v, want a PERMISSION_DENIED *Error", err)
	}
}
