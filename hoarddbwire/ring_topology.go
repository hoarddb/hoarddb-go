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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// ErrRingRequest is a ring request this package refuses to encode: a string
// id or a topology request with a member the server would refuse.
var ErrRingRequest = errors.New("ring request")

// ringRequestRefusal is the one shape of every ring request refusal: the
// member, what is wrong, what to send. It wraps ErrRingRequest.
func ringRequestRefusal(member, problem, send string) error {
	return fmt.Errorf("%w: %s: %s. %s", ErrRingRequest, member, problem, send)
}

// ErrRingReply is wrapped by every refusal DecodeRingTopology makes.
var ErrRingReply = errors.New("ring reply")

// ringReplyRefusal says which member of a reply is wrong and what to do about
// it; the caller of a ring request cannot act on a reply it cannot trust.
func ringReplyRefusal(member, problem string) error {
	return fmt.Errorf("%w: %s: %s. Report this reply, with the version of the node that sent it, to the node's operator", ErrRingReply, member, problem)
}

// RingTopologyRequest is a RING_TOPOLOGY request: the epoch and state sequence
// the client holds. Both are written even at zero, because the node reads the
// two members and a zero pair asks for the whole view. The node answers
// unchanged only to a non-zero epoch that matches its own.
type RingTopologyRequest struct {
	Epoch    int64 `bson:"epoch"`
	StateSeq int64 `bson:"state_seq"`
}

// Validate refuses a negative epoch or state sequence. The node holds both to
// 0 or more (cluster/wire checkRingTopologyRequest), so a negative one is
// refused at the call and named.
func (request RingTopologyRequest) Validate() error {
	switch {
	case request.Epoch < 0:
		return ringRequestRefusal("Epoch", fmt.Sprintf("%d, and an epoch counts from 0", request.Epoch), "Send the epoch the last reply returned, or 0 for the whole view")
	case request.StateSeq < 0:
		return ringRequestRefusal("StateSeq", fmt.Sprintf("%d, and a state sequence counts from 0", request.StateSeq), "Send the state_seq the last reply returned, or 0 for the whole view")
	}
	return nil
}

// Encode validates the request and returns its RING_TOPOLOGY payload.
func (request RingTopologyRequest) Encode() ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	return codec.Marshal(request)
}

// The bounds a topology reply holds. Each is the node's own value
// (cluster/wire ringtopology.go and ringlist.go ringMaxName).
const (
	ringTopologyDepth   = 4    // levels: the reply, the databases array, one database document
	ringDatabaseMaxName = 1021 // bytes of a database name
)

// RingDatabase is one database row of a topology reply: its name, its id, and
// the replication factor that sizes the copy set of each of its slots.
type RingDatabase struct {
	Name string `bson:"name"`
	ID   int64  `bson:"id"`
	RF   int64  `bson:"rf"`
}

// RingTopologyReply is the wire form of a topology reply, the type a response
// is decoded into and written back from. Callers use RingTopology, which
// DecodeRingTopology builds after checking every bound the node holds.
type RingTopologyReply struct {
	Epoch     int64          `bson:"epoch"`
	StateSeq  int64          `bson:"state_seq"`
	Unchanged bool           `bson:"unchanged,omitempty"`
	Members   []byte         `bson:"members,omitempty"`
	Witnesses []byte         `bson:"witnesses,omitempty"`
	Voters    int64          `bson:"voters,omitempty"`
	Table     []byte         `bson:"table,omitempty"`
	Databases []RingDatabase `bson:"databases,omitempty"`
}

// RingTopology is a topology reply decoded: the members the table ranks, the
// witnesses that are never ranked, the table, and the databases the session may
// see. An unchanged reply holds only the epoch and state sequence, and the
// caller keeps the view it has.
type RingTopology struct {
	Epoch, StateSeq    int64
	Unchanged          bool
	Members, Witnesses []RingMember
	Voters             int
	Table              RingSlotTable
	Databases          []RingDatabase
}

// OwnerOf is the member that owns a slot of a database: the first live name of
// the first RF names of the slot's ranking, RF being the database's own factor.
// It reports false when the database is not listed or no copy is live.
func (topology RingTopology) OwnerOf(database string, slot uint16) (RingMember, bool) {
	for _, row := range topology.Databases {
		if row.Name == database {
			return topology.Table.Owner(slot, int(row.RF))
		}
	}
	return RingMember{}, false
}

// DecodeRingTopology reads a RING_REPLY payload that answers a topology request.
// It reads an unknown member and an integer of any width, as every reply decoder
// of this package does, and then refuses what the node refuses: a bad number, an
// unchanged reply that carries more, a members, witnesses or table blob that
// breaks its layout, a database row out of order, and a document that is not BSON
// or nests too deep. Every size is checked before anything is made from it.
func DecodeRingTopology(payload []byte) (RingTopology, error) {
	if len(payload) == 0 || len(payload) > MaxPayload {
		return RingTopology{}, ringReplyRefusal("payload", fmt.Sprintf("the reply is %d bytes, and a topology reply is one document of 1 to %d bytes", len(payload), MaxPayload))
	}
	if err := checkBSONNesting(payload, ringTopologyDepth); err != nil {
		return RingTopology{}, ringReplyRefusal("payload", err.Error())
	}
	var wire RingTopologyReply
	if err := codec.Unmarshal(payload, &wire); err != nil {
		return RingTopology{}, ringReplyRefusal("payload", err.Error())
	}
	switch {
	case wire.Epoch < 0:
		return RingTopology{}, ringReplyRefusal("epoch", fmt.Sprintf("%d, and an epoch counts from 0", wire.Epoch))
	case wire.StateSeq < 0:
		return RingTopology{}, ringReplyRefusal("state_seq", fmt.Sprintf("%d, and a state sequence counts from 0", wire.StateSeq))
	case wire.Voters < 0 || wire.Voters > math.MaxInt32:
		return RingTopology{}, ringReplyRefusal("voters", fmt.Sprintf("%d, and a voter count is 0 to %d", wire.Voters, math.MaxInt32))
	}
	if wire.Unchanged {
		if wire.Members != nil || wire.Witnesses != nil || wire.Table != nil || wire.Databases != nil || wire.Voters != 0 {
			return RingTopology{}, ringReplyRefusal("unchanged", "an unchanged reply carries no members, witnesses, table, databases or voters: the view held stays as it is")
		}
		return RingTopology{Epoch: wire.Epoch, StateSeq: wire.StateSeq, Unchanged: true}, nil
	}
	if wire.Members == nil {
		return RingTopology{}, ringReplyRefusal("members", "a reply that is not unchanged carries the members blob")
	}
	if wire.Table == nil {
		return RingTopology{}, ringReplyRefusal("table", "a reply that is not unchanged carries the table")
	}
	members, err := DecodeRingMembers(wire.Members)
	if err != nil {
		return RingTopology{}, err
	}
	witnesses, err := decodeRingWitnesses(wire.Witnesses, members)
	if err != nil {
		return RingTopology{}, err
	}
	table, err := decodeRingSlotTable(wire.Table, members)
	if err != nil {
		return RingTopology{}, err
	}
	if err := checkRingDatabases(wire.Databases); err != nil {
		return RingTopology{}, err
	}
	return RingTopology{
		Epoch: wire.Epoch, StateSeq: wire.StateSeq, Members: members, Witnesses: witnesses,
		Voters: int(wire.Voters), Table: table, Databases: wire.Databases,
	}, nil
}

// decodeRingWitnesses reads the witnesses blob, which is absent when the
// cluster has no witness. A blob that is present holds at least one, and none of
// them is a ranked member.
func decodeRingWitnesses(blob []byte, members []RingMember) ([]RingMember, error) {
	if blob == nil {
		return nil, nil
	}
	witnesses, err := DecodeRingMembers(blob)
	if err != nil {
		return nil, fmt.Errorf("witnesses blob: %w", err)
	}
	if len(witnesses) == 0 {
		return nil, ringReplyRefusal("witnesses", "the blob is present with no witness: it is left out when there is none")
	}
	ranked := make(map[string]bool, len(members))
	for _, member := range members {
		ranked[member.ID] = true
	}
	for _, witness := range witnesses {
		if ranked[witness.ID] {
			return nil, ringReplyRefusal("witnesses", fmt.Sprintf("%q is both a ranked member and a witness: a witness is never ranked", witness.ID))
		}
	}
	return witnesses, nil
}

// checkRingDatabases holds the database rows to the node's rules: a name of 1 to
// 1021 bytes of UTF-8 listed once, ids ascending from 1, and a factor of 1 to
// 2147483647.
func checkRingDatabases(databases []RingDatabase) error {
	seen := make(map[string]bool, len(databases))
	var previous int64
	for _, database := range databases {
		switch {
		case database.Name == "" || len(database.Name) > ringDatabaseMaxName || !utf8.ValidString(database.Name):
			return ringReplyRefusal("databases", fmt.Sprintf("a database name is %d bytes, and a name is 1 to %d bytes of UTF-8", len(database.Name), ringDatabaseMaxName))
		case database.ID <= previous:
			return ringReplyRefusal("databases", fmt.Sprintf("database %q has id %d after id %d: databases are listed by id ascending, from 1", database.Name, database.ID, previous))
		case database.RF < 1 || database.RF > math.MaxInt32:
			return ringReplyRefusal("databases", fmt.Sprintf("database %q has replication factor %d, and a factor is 1 to %d", database.Name, database.RF, math.MaxInt32))
		case seen[database.Name]:
			return ringReplyRefusal("databases", fmt.Sprintf("database %q is listed twice", database.Name))
		}
		seen[database.Name] = true
		previous = database.ID
	}
	return nil
}

// checkBSONNesting walks a document once and refuses one whose lengths do not
// add up, whose element has a type no BSON type has, or that nests deeper than
// limit levels, the document itself being level 1. The codec's own validation is
// shallow and has no depth, and a depth is the one bound it cannot give.
func checkBSONNesting(document []byte, limit int) error {
	return walkBSONDocument(document, 1, limit)
}

func walkBSONDocument(document []byte, level, limit int) error {
	if level > limit {
		return fmt.Errorf("the document nests deeper than %d levels", limit)
	}
	if len(document) < 5 || uint64(binary.LittleEndian.Uint32(document)) != uint64(len(document)) || document[len(document)-1] != 0 {
		return errors.New("the bytes are not a BSON document: its length or its terminator is wrong")
	}
	end := len(document) - 1
	for position := 4; position < end; {
		kind := document[position]
		position++
		nameLength := bytes.IndexByte(document[position:end], 0)
		if nameLength < 0 {
			return errors.New("the bytes are not a BSON document: an element name has no terminator")
		}
		position += nameLength + 1
		size, err := bsonValueSize(kind, document[position:end], level, limit)
		if err != nil {
			return err
		}
		position += size
	}
	return nil
}

// bsonValueSize is the bytes a value of the given type takes at the start of
// raw. A document, an array and the scope of code with scope are walked, one
// level deeper.
func bsonValueSize(kind byte, raw []byte, level, limit int) (int, error) {
	cut := errors.New("the bytes are not a BSON document: a value is cut short")
	prefixed := func(extra int) (int, error) {
		if len(raw) < 4 {
			return 0, cut
		}
		length := int64(int32(binary.LittleEndian.Uint32(raw)))
		if length < 0 || length+int64(extra) > int64(len(raw)) {
			return 0, cut
		}
		return int(length), nil
	}
	fixed := func(size int) (int, error) {
		if len(raw) < size {
			return 0, cut
		}
		return size, nil
	}
	switch kind {
	case 0x01, 0x09, 0x11, 0x12: // double, datetime, timestamp, int64
		return fixed(8)
	case 0x10: // int32
		return fixed(4)
	case 0x13: // decimal128
		return fixed(16)
	case 0x07: // object id
		return fixed(12)
	case 0x08: // boolean
		return fixed(1)
	case 0x06, 0x0A, 0x7F, 0xFF: // undefined, null, max key, min key
		return 0, nil
	case 0x02, 0x0D, 0x0E: // string, code, symbol
		length, err := prefixed(4)
		return 4 + length, err
	case 0x05: // binary: the length, a subtype byte, then the bytes
		length, err := prefixed(5)
		return 5 + length, err
	case 0x0C: // database pointer: a string and 12 bytes
		length, err := prefixed(16)
		return 4 + length + 12, err
	case 0x0B: // regular expression: two terminated strings
		first := bytes.IndexByte(raw, 0)
		if first < 0 {
			return 0, cut
		}
		second := bytes.IndexByte(raw[first+1:], 0)
		if second < 0 {
			return 0, cut
		}
		return first + second + 2, nil
	case 0x03, 0x04: // document, array
		length, err := prefixed(0)
		if err != nil {
			return 0, err
		}
		return length, walkBSONDocument(raw[:length], level+1, limit)
	case 0x0F: // code with scope: the total, a string, then the scope document
		length, err := prefixed(0)
		if err != nil {
			return 0, err
		}
		if length < 4+4+1+5 {
			return 0, cut
		}
		codeLength := int64(int32(binary.LittleEndian.Uint32(raw[4:])))
		scope := 8 + codeLength
		if codeLength < 1 || scope+5 > int64(length) {
			return 0, cut
		}
		return length, walkBSONDocument(raw[scope:length], level+1, limit)
	}
	return 0, fmt.Errorf("the bytes are not a BSON document: an element has type byte %#x, which is no BSON type", kind)
}
