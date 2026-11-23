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
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Member is one ranked data member of the cluster.
type Member struct {
	// ID is the node id and Addr the address a driver connects to: the member's client address,
	// or empty when it published none (the data address is the internode listener and serves no
	// driver, so a caller skips a member whose Addr is empty).
	ID, Addr string
	// DataAddr and ClientAddr are the two committed addresses; ClientAddr is empty when
	// the member published none.
	DataAddr, ClientAddr string
	// Voter is true when the member votes in the cluster state group.
	Voter bool
	// State is active, down or leaving; a down member keeps its rank and is skipped as owner.
	State string
}

// TopologyDatabase is a database the session may see, with its replication factor.
type TopologyDatabase struct {
	Name string
	ID   int64
	RF   int32
}

// Topology is the cluster view a smart driver routes with: the members, the full ranking
// of all 8192 slots and the databases the session may see. It is immutable; ask the
// server again with [Client.Topology] when a request is refused STALE_EPOCH or NOT_OWNER.
type Topology struct {
	// Epoch and StateSeq identify the view; send them back to learn whether it changed.
	Epoch, StateSeq int64
	// Voters is how many members vote in the cluster state group, from the committed voter list, so
	// it counts a voter that is joining or dead and so is in neither Members nor Witnesses. Derive
	// the quorum from it, not from the Voter flags alone.
	Voters int32
	// Members are the ranked data members, sorted by id.
	Members []Member
	// Witnesses are the witnesses (voting or not; members that hold no documents), sorted by id. A driver
	// never routes to one.
	Witnesses []Member
	// Databases are the databases this session may see, by id.
	Databases []TopologyDatabase

	table hoarddbwire.RingSlotTable
}

// Topology asks the node for the cluster view. Pass nil for the first call. Pass the view
// you hold to learn whether it is still current: the server then answers with the numbers
// alone and the same *Topology comes back. Any authenticated session may call it; the
// databases listed are the ones the session holds a role on.
func (c *Client) Topology(ctx context.Context, have *Topology) (*Topology, error) {
	var request hoarddbwire.RingTopologyRequest
	if have != nil {
		request = hoarddbwire.RingTopologyRequest{Epoch: have.Epoch, StateSeq: have.StateSeq}
	}
	body, err := request.Encode()
	if err != nil {
		return nil, fmt.Errorf("ring: encoding the topology request: %w", err)
	}
	payload, err := c.call(ctx, hoarddbwire.OpRingTopology, c.NextRequestID(), body)
	if err != nil {
		return nil, fmt.Errorf("topology: %w", err)
	}
	reply, err := hoarddbwire.DecodeRingTopology(payload)
	if err != nil {
		return nil, fmt.Errorf("ring: decoding the topology reply: %w", err)
	}
	if reply.Unchanged {
		if have == nil {
			return nil, errors.New("ring: the server answered unchanged to a request that held nothing")
		}
		return have, nil
	}
	topology := &Topology{Epoch: reply.Epoch, StateSeq: reply.StateSeq, Voters: int32(reply.Voters), table: reply.Table} //nolint:gosec // the decoder holds the voter count to 0..MaxInt32
	for _, member := range reply.Members {
		topology.Members = append(topology.Members, memberOf(member))
	}
	for _, witness := range reply.Witnesses {
		topology.Witnesses = append(topology.Witnesses, memberOf(witness))
	}
	for _, database := range reply.Databases {
		topology.Databases = append(topology.Databases, TopologyDatabase{Name: database.Name, ID: database.ID, RF: int32(database.RF)}) //nolint:gosec // the decoder holds a factor to 1..MaxInt32
	}
	return topology, nil
}

// memberOf is the driver's view of a committed member: it connects to the client address the
// member published, and to nothing when it published none. It never falls back to the data
// address, which is the internode listener and serves no driver.
func memberOf(member hoarddbwire.RingMember) Member {
	return Member{ID: member.ID, Addr: member.ClientAddr, DataAddr: member.DataAddr, ClientAddr: member.ClientAddr,
		Voter: member.Votes, State: member.State.String()}
}

// Owner is the member that serves slot of database: the first of the first RF names of the
// slot's ranking whose state is not down. It reports false when the
// database is not in the view, the slot is out of range or every copy is down.
func (t *Topology) Owner(database string, slot uint16) (Member, bool) {
	for _, listed := range t.Databases {
		if listed.Name != database {
			continue
		}
		owner, ok := t.table.Owner(slot, int(listed.RF))
		if !ok {
			return Member{}, false
		}
		return memberOf(owner), true
	}
	return Member{}, false
}
