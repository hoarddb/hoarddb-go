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
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// ringListVector finds a row of the frozen vector file by its name.
func ringListVector(t *testing.T, name string) conformance.Vector {
	t.Helper()
	file, err := conformance.Load(conformance.DriverVectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, vector := range file.Vectors {
		if vector.Name == name {
			return vector
		}
	}
	t.Fatalf("the vector file has no row named %q", name)
	return conformance.Vector{}
}

// unhex reads a hex literal from a vector or from the build note, so no
// expected byte is taken from an encoder under test.
func unhex(t *testing.T, text string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(text)
	if err != nil {
		t.Fatalf("%q is not hex: %v", text, err)
	}
	return decoded
}

const (
	firstCallRow   = "ring topology: the first call after AUTH"
	singleNodeRow  = "ring topology: a single node"
	currentViewRow = "ring topology: the view held is current"
	witnessRow     = "ring topology: a witness is listed apart from the members"
)

// topologyPayload is the payload of a topology row's reply, as the row has it.
func topologyPayload(t *testing.T, name string) []byte {
	t.Helper()
	row := ringListVector(t, name)
	if row.Response == nil {
		t.Fatalf("%q has no response", name)
	}
	return unhex(t, row.Response.PayloadHex)
}

// replyOf is a reply document as a map, so a test can add or break one member.
type replyOf map[string]any

// firstCall is the first-call row's reply as a map. Its blobs are the row's own
// bytes; its numbers are what the row's payload_json says (epoch 4, state_seq 9,
// voters 3, two databases), written with the types the node writes.
func firstCall(t *testing.T) replyOf {
	t.Helper()
	var blobs struct {
		Members   []byte `bson:"members"`
		Witnesses []byte `bson:"witnesses"`
		Table     []byte `bson:"table"`
	}
	if err := codec.Unmarshal(topologyPayload(t, firstCallRow), &blobs); err != nil {
		t.Fatalf("the first call's reply does not read: %v", err)
	}
	return replyOf{
		"epoch": int64(4), "state_seq": int64(9), "voters": int32(3),
		"members": blobs.Members, "witnesses": blobs.Witnesses, "table": blobs.Table,
		"databases": []any{
			map[string]any{"name": "app", "id": int64(1), "rf": int32(2)},
			map[string]any{"name": "logs", "id": int64(2), "rf": int32(1)},
		},
	}
}

// unchangedReply is the "view held is current" reply as a map.
func unchangedReply() replyOf {
	return replyOf{"epoch": int64(4), "state_seq": int64(9), "unchanged": true}
}

func (reply replyOf) payload(t *testing.T) []byte {
	t.Helper()
	payload, err := codec.Marshal(map[string]any(reply))
	if err != nil {
		t.Fatalf("writing the reply: %v", err)
	}
	return payload
}

func (reply replyOf) with(member string, value any) replyOf {
	copied := replyOf{}
	for name, kept := range reply {
		copied[name] = kept
	}
	if value == nil {
		delete(copied, member)
	} else {
		copied[member] = value
	}
	return copied
}

func decodedFirstCall(t *testing.T) hoarddbwire.RingTopology {
	t.Helper()
	topology, err := hoarddbwire.DecodeRingTopology(topologyPayload(t, firstCallRow))
	if err != nil {
		t.Fatalf("the first call's reply is refused: %v", err)
	}
	return topology
}

// TestTheFirstCallRowDecodesToItsViewOfTheCluster reads the four exchange rows
// and checks each member against what the row's own bytes hold.
func TestTheFirstCallRowDecodesToItsViewOfTheCluster(t *testing.T) {
	first := decodedFirstCall(t)
	if first.Epoch != 4 || first.StateSeq != 9 || first.Voters != 3 || first.Unchanged {
		t.Errorf("epoch %d, state_seq %d, voters %d, unchanged %t; the row says 4, 9, 3, false", first.Epoch, first.StateSeq, first.Voters, first.Unchanged)
	}
	wantMembers := []hoarddbwire.RingMember{
		{ID: "node-a", State: hoarddbwire.RingMemberActive, Votes: true, DataAddr: "192.0.2.1:4433", ClientAddr: "a.example:7434"},
		{ID: "node-b", State: hoarddbwire.RingMemberDown, Votes: true, DataAddr: "192.0.2.2:4433"},
		{ID: "node-c", State: hoarddbwire.RingMemberActive, DataAddr: "192.0.2.3:4433"},
	}
	if !reflect.DeepEqual(first.Members, wantMembers) {
		t.Errorf("members = %+v, want %+v", first.Members, wantMembers)
	}
	wantWitnesses := []hoarddbwire.RingMember{
		{ID: "node-w", State: hoarddbwire.RingMemberActive, Votes: true, DataAddr: "192.0.2.4:4433", ClientAddr: "w.example:7434"},
	}
	if !reflect.DeepEqual(first.Witnesses, wantWitnesses) {
		t.Errorf("witnesses = %+v, want %+v", first.Witnesses, wantWitnesses)
	}
	wantDatabases := []hoarddbwire.RingDatabase{{Name: "app", ID: 1, RF: 2}, {Name: "logs", ID: 2, RF: 1}}
	if !reflect.DeepEqual(first.Databases, wantDatabases) {
		t.Errorf("databases = %+v, want %+v", first.Databases, wantDatabases)
	}

	single, err := hoarddbwire.DecodeRingTopology(topologyPayload(t, singleNodeRow))
	if err != nil {
		t.Fatalf("the single node's reply is refused: %v", err)
	}
	if single.Epoch != 1 || single.StateSeq != 3 || single.Voters != 0 || len(single.Members) != 1 || len(single.Witnesses) != 0 {
		t.Errorf("single node: %+v, want epoch 1, state_seq 3, no voters count, one member, no witnesses", single)
	}

	current, err := hoarddbwire.DecodeRingTopology(topologyPayload(t, currentViewRow))
	if err != nil {
		t.Fatalf("the current-view reply is refused: %v", err)
	}
	if !current.Unchanged || current.Epoch != 4 || current.StateSeq != 9 || current.Members != nil || current.Witnesses != nil || current.Databases != nil {
		t.Errorf("the unchanged reply decoded to %+v, want only the epoch 4 and state_seq 9 and unchanged", current)
	}

	apart, err := hoarddbwire.DecodeRingTopology(topologyPayload(t, witnessRow))
	if err != nil {
		t.Fatalf("the witness row's reply is refused: %v", err)
	}
	if len(apart.Members) != 2 || len(apart.Witnesses) != 1 || apart.Witnesses[0].ID != "node-w" || apart.Voters != 3 {
		t.Errorf("witness row: %d members, witnesses %+v, voters %d; the row has two members, node-w, 3 voters", len(apart.Members), apart.Witnesses, apart.Voters)
	}
}

// TestTheFirstCallTableRanksAsTheVectorDoes reads the rankings the first-call
// row's table gives, which were decoded from its own bytes by an independent
// script: slot 0 ranks a, b, c; 1 ranks c, a, b; 2 ranks b, a, c; 4962 ranks
// a, b, c; 5737 ranks b, c, a.
func TestTheFirstCallTableRanksAsTheVectorDoes(t *testing.T) {
	table := decodedFirstCall(t).Table
	for slot, want := range map[uint16]string{
		0: "node-a,node-b,node-c", 1: "node-c,node-a,node-b", 2: "node-b,node-a,node-c",
		4962: "node-a,node-b,node-c", 5737: "node-b,node-c,node-a", 8191: "node-c,node-b,node-a",
	} {
		if got := idsOf(table.Ranking(slot)); got != want {
			t.Errorf("Ranking(%d) = %s, want %s", slot, got, want)
		}
	}
	if table.Ranking(8192) != nil {
		t.Error("Ranking(8192) is not nil: the table has slots 0 to 8191")
	}
	single, err := hoarddbwire.DecodeRingTopology(topologyPayload(t, singleNodeRow))
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(single.Table.Ranking(5737)); got != "node-a" {
		t.Errorf("the single node's Ranking(5737) = %s, want node-a", got)
	}
}

func idsOf(members []hoarddbwire.RingMember) string {
	ids := make([]string, len(members))
	for index, member := range members {
		ids[index] = member.ID
	}
	return strings.Join(ids, ",")
}

// TestTheOwnerIsTheFirstLiveNameOfTheCopySet: with a replication factor of 2,
// the owner of slot 0 is node-a, of slot 1 node-c, of slot 2 node-a (node-b is
// down) and of slot 5737 node-c. With a factor of 1 slots 2 and 5737 have no
// owner: the first live name of the whole ranking (node-a, node-c) is outside
// the copy set and holds no copy.
func TestTheOwnerIsTheFirstLiveNameOfTheCopySet(t *testing.T) {
	table := decodedFirstCall(t).Table
	for slot, want := range map[uint16]string{0: "node-a", 1: "node-c", 2: "node-a", 5737: "node-c"} {
		owner, found := table.Owner(slot, 2)
		if !found || owner.ID != want {
			t.Errorf("Owner(%d, 2) = %q (found %t), want %s", slot, owner.ID, found, want)
		}
	}
	for slot, want := range map[uint16]string{0: "node-a", 1: "node-c"} {
		owner, found := table.Owner(slot, 1)
		if !found || owner.ID != want {
			t.Errorf("Owner(%d, 1) = %q (found %t), want %s", slot, owner.ID, found, want)
		}
	}
	for _, slot := range []uint16{2, 5737} {
		if owner, found := table.Owner(slot, 1); found {
			t.Errorf("Owner(%d, 1) = %s: its copy set is the down node-b, so the slot has no owner", slot, owner.ID)
		}
	}
	if _, found := table.Owner(0, 0); found {
		t.Error("Owner(0, 0) has an owner; a factor under 1 holds no copy")
	}
	if _, found := table.Owner(8192, 2); found {
		t.Error("Owner(8192, 2) has an owner; the table has slots 0 to 8191")
	}
	if got := idsOf(table.Copies(2, 2)); got != "node-b,node-a" {
		t.Errorf("Copies(2, 2) = %s, want node-b,node-a: a down name stays in the copy set", got)
	}
	if got := idsOf(table.Copies(0, 9)); got != "node-a,node-b,node-c" {
		t.Errorf("Copies(0, 9) = %s, want the whole ranking", got)
	}
	if table.Copies(0, 0) != nil || table.Copies(0, -1) != nil || table.Copies(8192, 2) != nil {
		t.Error("Copies is not nil for a factor under 1 or a slot past 8191")
	}
}

// TestOwnerOfNeedsTheDatabase: the database's own replication factor decides
// the copy set. app has 2, logs has 1, and a database the reply does not list
// has no owner for any slot.
func TestOwnerOfNeedsTheDatabase(t *testing.T) {
	topology := decodedFirstCall(t)
	for _, probe := range []struct {
		database string
		slot     uint16
		want     string
	}{
		{"app", 2, "node-a"}, {"app", 1, "node-c"}, {"logs", 0, "node-a"}, {"logs", 1, "node-c"},
		{"logs", 2, ""}, {"nope", 2, ""}, {"nope", 0, ""}, {"", 0, ""},
	} {
		owner, found := topology.OwnerOf(probe.database, probe.slot)
		if probe.want == "" && found {
			t.Errorf("OwnerOf(%q, %d) = %s, want no owner", probe.database, probe.slot, owner.ID)
		}
		if probe.want != "" && (!found || owner.ID != probe.want) {
			t.Errorf("OwnerOf(%q, %d) = %q (found %t), want %s", probe.database, probe.slot, owner.ID, found, probe.want)
		}
	}
	if _, found := (hoarddbwire.RingTopology{}).OwnerOf("app", 0); found {
		t.Error("a zero topology has an owner")
	}
}

// TestAMissingMembersBlobWithoutUnchangedIsRefused: a reply that is not
// unchanged carries the members and the table.
func TestAMissingMembersBlobWithoutUnchangedIsRefused(t *testing.T) {
	for member, value := range map[string]any{"members": nil, "table": nil, "members empty": []byte{}} {
		name := strings.TrimSuffix(member, " empty")
		reply := firstCall(t).with(name, value)
		_, err := hoarddbwire.DecodeRingTopology(reply.payload(t))
		if err == nil || !errors.Is(err, hoarddbwire.ErrRingReply) || !strings.Contains(err.Error(), name) {
			t.Errorf("a reply with no %s (%s): error %v, want a refusal wrapping ErrRingReply and naming %s", name, member, err, name)
		}
		if value == nil && err != nil && !strings.Contains(err.Error(), "carries the "+name) {
			t.Errorf("a reply with no %s: %q does not say the reply carries the %s", name, err, name)
		}
	}
}

// refusal runs DecodeRingTopology on a payload and checks the refusal wraps
// ErrRingReply and names the member it is about.
func refusal(t *testing.T, payload []byte, member string) {
	t.Helper()
	_, err := hoarddbwire.DecodeRingTopology(payload)
	if err == nil {
		t.Errorf("the reply is accepted; the bound on %s must refuse it", member)
		return
	}
	if !errors.Is(err, hoarddbwire.ErrRingReply) {
		t.Errorf("the refusal %v does not wrap ErrRingReply", err)
	}
	if !strings.Contains(err.Error(), member) {
		t.Errorf("the refusal %q does not name %s", err, member)
	}
}

func accepted(t *testing.T, payload []byte) hoarddbwire.RingTopology {
	t.Helper()
	topology, err := hoarddbwire.DecodeRingTopology(payload)
	if err != nil {
		t.Fatalf("the reply is refused: %v", err)
	}
	return topology
}

// TestTopologyEpochStateSeqAndVotersAreHeldToTheirRanges is bound 1: epoch and
// state_seq are 0 or more; voters is 0 to 2147483647.
func TestTopologyEpochStateSeqAndVotersAreHeldToTheirRanges(t *testing.T) {
	refusals := []struct {
		member string
		value  any
	}{
		{"epoch", int64(-1)}, {"state_seq", int64(-1)},
		{"voters", int32(-1)}, {"voters", int64(math.MaxInt32) + 1},
	}
	for _, testCase := range refusals {
		t.Run(fmt.Sprintf("%s is %v", testCase.member, testCase.value), func(t *testing.T) {
			refusal(t, firstCall(t).with(testCase.member, testCase.value).payload(t), testCase.member)
		})
	}
	topology := accepted(t, firstCall(t).with("epoch", int64(0)).with("state_seq", int64(0)).with("voters", int64(math.MaxInt32)).payload(t))
	if topology.Epoch != 0 || topology.StateSeq != 0 || topology.Voters != math.MaxInt32 {
		t.Errorf("epoch %d, state_seq %d, voters %d; want 0, 0, 2147483647", topology.Epoch, topology.StateSeq, topology.Voters)
	}
}

// TestAnUnchangedReplyCarriesNothingElse is bound 2: unchanged with any blob, a
// databases member or a voters count is refused, written even when empty.
func TestAnUnchangedReplyCarriesNothingElse(t *testing.T) {
	full := firstCall(t)
	for _, member := range []string{"members", "witnesses", "table", "databases", "voters"} {
		refusal(t, unchangedReply().with(member, full[member]).payload(t), "unchanged")
	}
	refusal(t, unchangedReply().with("members", []byte{}).payload(t), "unchanged")
	refusal(t, unchangedReply().with("databases", []any{}).payload(t), "unchanged")
	topology := accepted(t, unchangedReply().payload(t))
	if !topology.Unchanged || topology.Table.Ranking(0) != nil {
		t.Errorf("a bare unchanged reply decoded to %+v", topology)
	}
}

// TestTheWitnessesBlobIsAbsentOrHoldsAWitness is bound 3: a witnesses member
// that is present with no witness is refused, and so is a witness that is also
// a ranked member.
func TestTheWitnessesBlobIsAbsentOrHoldsAWitness(t *testing.T) {
	nodeW := appendMemberRow(nil, 2, 1, "node-w", "w.example:1", "")
	nodeX := appendMemberRow(nil, 2, 0, "node-x", "x.example:1", "")
	refusals := map[string][]byte{
		"a zero-length blob":                    {},
		"a count of zero":                       {0x00},
		"a blob that does not decode":           {0xff},
		"a witness that is also a member":       blobOf(appendMemberRow(nil, 2, 1, "node-a", "w.example:1", "")),
		"a witness that is also a down member":  blobOf(appendMemberRow(nil, 3, 1, "node-b", "w.example:1", "")),
		"witnesses out of order":                blobOf(nodeX, nodeW),
		"a witness listed twice":                blobOf(nodeW, nodeW),
		"a witness in the joining state (byte)": blobOf(appendMemberRow(nil, 1, 1, "node-w", "w.example:1", "")),
	}
	for name, blob := range refusals {
		t.Run(name, func(t *testing.T) {
			refusal(t, firstCall(t).with("witnesses", blob).payload(t), "witnesses")
		})
	}
	absent := accepted(t, firstCall(t).with("witnesses", nil).payload(t))
	if absent.Witnesses != nil {
		t.Errorf("a reply with no witnesses member decoded to witnesses %+v", absent.Witnesses)
	}
	two := accepted(t, firstCall(t).with("witnesses", blobOf(nodeW, nodeX)).payload(t))
	if len(two.Witnesses) != 2 {
		t.Errorf("two witnesses decoded to %+v", two.Witnesses)
	}
}

// TestATopologyTableThatDoesNotMatchItsMembersIsRefused is bound 4 through the
// whole reply: the table is the one the members blob counts.
func TestATopologyTableThatDoesNotMatchItsMembersIsRefused(t *testing.T) {
	full := firstCall(t)
	table, _ := full["table"].([]byte)
	two := blobOf(appendMemberRow(nil, 2, 1, "node-a", "d:1", ""), appendMemberRow(nil, 2, 1, "node-b", "d:1", ""))
	cases := map[string]replyOf{
		"a table of three over two members":    full.with("members", two),
		"a table one byte short":               full.with("table", table[:len(table)-1]),
		"a table with a byte after slot 8191":  full.with("table", append(append([]byte{}, table...), 0)),
		"a table of one kilobyte":              full.with("table", table[:1024]),
		"a zero-length table":                  full.with("table", []byte{}),
		"a table that ranks a member twice":    full.with("table", append([]byte{0x03, 0x00, 0x00, 0x01}, table[4:]...)),
		"a table with an index past the count": full.with("table", append([]byte{0x03, 0x00, 0x01, 0x03}, table[4:]...)),
	}
	for name, reply := range cases {
		t.Run(name, func(t *testing.T) { refusal(t, reply.payload(t), "table") })
	}
	empty := accepted(t, full.with("members", []byte{0x00}).with("witnesses", nil).with("table", make([]byte, 8192)).payload(t))
	if len(empty.Members) != 0 || empty.Table.Ranking(0) != nil {
		t.Errorf("a reply of no ranked members decoded to %+v", empty)
	}
}

// TestSixtyFiveThousandMembersAndAKilobyteOfTableAreRefused sends the whole
// reply of a member count at the limit with a 1 KiB table: the table's length
// refuses it, and the refusal names the table.
func TestSixtyFiveThousandMembersAndAKilobyteOfTableAreRefused(t *testing.T) {
	members := make([]hoarddbwire.RingMember, math.MaxUint16)
	for index := range members {
		members[index] = hoarddbwire.RingMember{ID: fmt.Sprintf("m%05d", index), State: hoarddbwire.RingMemberActive, DataAddr: "d:1"}
	}
	reply := replyOf{"epoch": int64(1), "state_seq": int64(1), "members": appendMembers(nil, members), "table": make([]byte, 1024)}
	refusal(t, reply.payload(t), "table")
}

// TestTopologyDatabasesAreHeldToTheNodesBounds is bound 5: name 1 to 1021 bytes
// of UTF-8, id strictly ascending from 1, rf 1 to 2147483647, no name twice.
func TestTopologyDatabasesAreHeldToTheNodesBounds(t *testing.T) {
	database := func(name string, id int64, rf any) map[string]any {
		return map[string]any{"name": name, "id": id, "rf": rf}
	}
	cases := map[string][]any{
		"an empty name":                 {database("", 1, int32(1))},
		"a name of 1022 bytes":          {database(strings.Repeat("d", 1022), 1, int32(1))},
		"a name that is not UTF-8":      {database("d\xff", 1, int32(1))},
		"an id of 0":                    {database("app", 0, int32(1))},
		"a negative id":                 {database("app", -3, int32(1))},
		"an id equal to the one before": {database("app", 1, int32(1)), database("logs", 1, int32(1))},
		"an id below the one before":    {database("app", 2, int32(1)), database("logs", 1, int32(1))},
		"a replication factor of 0":     {database("app", 1, int32(0))},
		"a negative replication factor": {database("app", 1, int32(-1))},
		"a factor above 2147483647":     {database("app", 1, int64(math.MaxInt32)+1)},
		"a name listed twice":           {database("app", 1, int32(1)), database("app", 2, int32(1))},
	}
	for name, databases := range cases {
		t.Run(name, func(t *testing.T) { refusal(t, firstCall(t).with("databases", databases).payload(t), "databases") })
	}
	topology := accepted(t, firstCall(t).with("databases", []any{
		database(strings.Repeat("d", 1021), 7, int64(math.MaxInt32)), database("é", math.MaxInt64, int32(1)),
	}).payload(t))
	if len(topology.Databases) != 2 || topology.Databases[0].RF != math.MaxInt32 || topology.Databases[1].ID != math.MaxInt64 {
		t.Errorf("databases at their limits decoded to %d rows: %+v", len(topology.Databases), topology.Databases)
	}
	none := accepted(t, firstCall(t).with("databases", nil).payload(t))
	if len(none.Databases) != 0 {
		t.Errorf("a reply with no databases decoded to %+v", none.Databases)
	}
}

// TestTopologyPayloadIsOneBoundedDocument is bound 6: a valid BSON document, no
// deeper than 4 levels, no larger than a frame holds.
func TestTopologyPayloadIsOneBoundedDocument(t *testing.T) {
	nested := func(levels int) any {
		var inner any = int32(1)
		for range levels {
			inner = map[string]any{"a": inner}
		}
		return inner
	}
	nestedArrays := func(levels int) any {
		var inner any = int32(1)
		for range levels {
			inner = []any{inner}
		}
		return inner
	}
	// The root is level 1 and zz holding n nested documents reaches level n+1, so
	// 3 is the deepest the node takes and 4 is one level too many.
	refusal(t, firstCall(t).with("zz", nested(4)).payload(t), "payload")
	refusal(t, firstCall(t).with("zz", nestedArrays(4)).payload(t), "payload")
	accepted(t, firstCall(t).with("zz", nested(3)).payload(t))
	accepted(t, firstCall(t).with("zz", nestedArrays(3)).payload(t))

	refusal(t, nil, "payload")
	refusal(t, []byte{0x05, 0x00, 0x00}, "payload")
	refusal(t, []byte("not a document, not even close to one"), "payload")
	whole := firstCall(t).payload(t)
	refusal(t, whole[:len(whole)-3], "payload")
	refusal(t, append(append([]byte{}, whole...), 0), "payload")

	huge := firstCall(t).with("zz", make([]byte, hoarddbwire.MaxPayload)).payload(t)
	if len(huge) <= hoarddbwire.MaxPayload {
		t.Fatalf("the oversized payload is %d bytes, not over the %d a frame holds", len(huge), hoarddbwire.MaxPayload)
	}
	refusal(t, huge, "payload")
}

// TestTheTopologyReplyToleratesAnExtraMemberAndEveryWidth reads each exchange
// row's own payload with an unknown member and with every integer as int32,
// int64 and a double that holds the integer: the view decodes the same. The
// node's strict decoder refuses all of these, and this driver does not (A1
// Owner question 1).
func TestTheTopologyReplyToleratesAnExtraMemberAndEveryWidth(t *testing.T) {
	variants := map[string]func(conformance.Shape, map[string]any) map[string]any{
		"an extra member": conformance.Shape.WithExtraMembers,
		"integers as int32": func(shape conformance.Shape, tree map[string]any) map[string]any {
			return shape.WithIntegerWidth(tree, "int32")
		},
		"integers as int64": func(shape conformance.Shape, tree map[string]any) map[string]any {
			return shape.WithIntegerWidth(tree, "int64")
		},
		"integers as double": func(shape conformance.Shape, tree map[string]any) map[string]any {
			return shape.WithIntegerWidth(tree, "double")
		},
	}
	shape := conformance.ShapeOf("RING_TOPOLOGY", true)
	for _, row := range []string{firstCallRow, singleNodeRow, currentViewRow, witnessRow} {
		payload := topologyPayload(t, row)
		want := accepted(t, payload)
		if want.Epoch == 0 {
			t.Fatalf("%s: the plain payload decoded to %+v, an empty view, so the variants test nothing", row, want)
		}
		tree, err := conformance.WireTree(payload)
		if err != nil {
			t.Fatalf("%s: %v", row, err)
		}
		for name, vary := range variants {
			text, err := conformance.ExtendedJSON(vary(shape, tree))
			if err != nil {
				t.Fatal(err)
			}
			varied, err := codec.FromExtendedJSON(text)
			if err != nil {
				t.Fatalf("%s, %s: %v", row, name, err)
			}
			if name == "an extra member" && !bytes.Contains(varied, []byte(conformance.ExtraMember)) {
				t.Fatalf("%s: the variant carries no extra member, so it tests nothing", row)
			}
			if name == "integers as double" && bytes.Equal(varied, payload) {
				t.Fatalf("%s: the double variant is the original payload, so it tests nothing", row)
			}
			got, err := hoarddbwire.DecodeRingTopology(varied)
			if err != nil {
				t.Errorf("%s with %s is refused: %v", row, name, err)
				continue
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s with %s decoded to %+v, want %+v", row, name, got, want)
			}
		}
	}
}

// bsonLE32 is a 32-bit little-endian length or int32, written by hand from the
// BSON layout so no byte comes from the encoder under test.
func bsonLE32(number int) []byte {
	return []byte{byte(number), byte(number >> 8), byte(number >> 16), byte(number >> 24)}
}

func bsonElement(kind byte, name string, value ...[]byte) []byte {
	element := append([]byte{kind}, name...)
	element = append(element, 0)
	for _, part := range value {
		element = append(element, part...)
	}
	return element
}

func bsonText(text string) []byte {
	return append(append(bsonLE32(len(text)+1), text...), 0)
}

func bsonDocument(elements ...[]byte) []byte {
	var body []byte
	for _, element := range elements {
		body = append(body, element...)
	}
	return append(append(bsonLE32(len(body)+5), body...), 0)
}

// withElements adds elements to the end of a reply document.
func withElements(payload []byte, elements ...[]byte) []byte {
	body := append([]byte{}, payload[4:len(payload)-1]...)
	for _, element := range elements {
		body = append(body, element...)
	}
	return append(append(bsonLE32(len(body)+5), body...), 0)
}

// TestTheDepthWalkReadsEveryBSONTypeAndCountsScopes puts one element of each
// BSON type in an unknown member of the reply, which the walk must step over by
// the type's own size, and nests the scope of a code-with-scope value to the
// limit and one past it.
func TestTheDepthWalkReadsEveryBSONTypeAndCountsScopes(t *testing.T) {
	plain := firstCall(t).payload(t)
	every := [][]byte{
		bsonElement(0x01, "d", make([]byte, 8)),
		bsonElement(0x02, "s", bsonText("text")),
		bsonElement(0x05, "b", bsonLE32(3), []byte{0}, []byte("abc")),
		bsonElement(0x06, "u"),
		bsonElement(0x07, "o", make([]byte, 12)),
		bsonElement(0x08, "t", []byte{1}),
		bsonElement(0x09, "m", make([]byte, 8)),
		bsonElement(0x0A, "n"),
		bsonElement(0x0B, "r", []byte("a.b\x00i\x00")),
		bsonElement(0x0C, "p", bsonText("db"), make([]byte, 12)),
		bsonElement(0x0D, "c", bsonText("code")),
		bsonElement(0x0E, "y", bsonText("sym")),
		bsonElement(0x10, "i", bsonLE32(7)),
		bsonElement(0x11, "x", make([]byte, 8)),
		bsonElement(0x12, "l", make([]byte, 8)),
		bsonElement(0x13, "q", make([]byte, 16)),
		bsonElement(0x7F, "max"),
		bsonElement(0xFF, "min"),
		bsonElement(0x04, "arr", bsonDocument(bsonElement(0x10, "0", bsonLE32(1)))),
	}
	// The elements sit in the reply itself, so the document keeps its depth of 1.
	accepted(t, withElements(plain, every...))

	scoped := func(levels int) []byte {
		scope := bsonDocument(bsonElement(0x10, "v", bsonLE32(1)))
		for range levels {
			scope = bsonDocument(bsonElement(0x03, "a", scope))
		}
		code := append(bsonText("f()"), scope...)
		return bsonElement(0x0F, "w", bsonLE32(4+len(code)), code)
	}
	// The scope is level 2 and each nested document adds one: 2 nested is level 4.
	accepted(t, withElements(plain, scoped(2)))
	refusal(t, withElements(plain, scoped(3)), "payload")

	broken := map[string][]byte{
		"a type byte no BSON type has":     bsonElement(0x20, "z"),
		"a string longer than the bytes":   bsonElement(0x02, "s", bsonLE32(99), []byte("x\x00")),
		"a binary longer than the bytes":   bsonElement(0x05, "b", bsonLE32(99), []byte{0}, []byte("x")),
		"an int64 cut short":               bsonElement(0x12, "l", make([]byte, 4)),
		"a negative length":                bsonElement(0x02, "s", bsonLE32(-1)),
		"a regular expression with no end": bsonElement(0x0B, "r", []byte("a.b")),
		"an element name with no end":      {0x10, 'z', 'z'},
	}
	for name, element := range broken {
		t.Run(name, func(t *testing.T) {
			refusal(t, withElements(plain, element), "payload")
			// The walk refuses before the typed read does, and says so in its words.
			if _, err := hoarddbwire.DecodeRingTopology(withElements(plain, element)); err != nil && !strings.Contains(err.Error(), "not a BSON document") {
				t.Errorf("the refusal %q does not come from the walk of the document", err)
			}
		})
	}
}
