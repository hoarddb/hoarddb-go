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

package conformance_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// refuseCheck decodes a payload the way the driver would and returns the
// driver's own refusal; nil means the driver accepted the payload.
type refuseCheck func(payload []byte) error

// refuseChecks is keyed by direction + ":" + opcode name, for example
// "request:RING_KEYED" or "error:ERROR". The ring's requests are refused by the
// decoders of hoarddb-wire, which are the server's own; a topology request by
// this module's rule for one.
var refuseChecks = map[string]refuseCheck{
	"error:ERROR":           refuseErrorPayload,
	"request:RING_KEYED":    refusedBy(ringwire.DecodeRingKeyed),
	"request:RING_BATCH":    refusedBy(ringwire.DecodeRingBatch),
	"request:RING_GET_MANY": refusedBy(ringwire.DecodeRingGetMany),
	"request:RING_LIST":     refusedBy(ringwire.DecodeRingList),
	"request:RING_COUNT":    refusedBy(ringwire.DecodeRingCount),
	"request:RING_ADMIN":    refusedBy(ringwire.DecodeRingAdmin),
	"request:RING_TOPOLOGY": canonicalTopologyRequest,
}

// namedRefuseChecks answers by the start of a vector's name, for a row whose
// opcode cannot say which decoder it needs: one RING_REPLY answers a topology,
// an admin command, a batch or a keyed request. It is consulted before
// refuseChecks.
var namedRefuseChecks = []struct {
	prefix string
	check  refuseCheck
}{
	{"ring topology reply refused", refusedBy(hoarddbwire.DecodeRingTopology)},
	{"ring admin reply refused", refusedBy(ringwire.DecodeRingAdminReply)},
	{"ring batch reply refused", refusedBy(ringwire.DecodeRingReply)},
	{"ring reply refused", refusedBy(ringwire.DecodeRingReply)},
}

// namedCheckOf returns the check of the first prefix the vector's name starts with.
func namedCheckOf(name string) (refuseCheck, bool) {
	for _, named := range namedRefuseChecks {
		if strings.HasPrefix(name, named.prefix) {
			return named.check, true
		}
	}
	return nil, false
}

// canonicalTopologyRequest is the server's rule for a RING_TOPOLOGY request
// (cluster/wire DecodeRingTopology), written as a test helper: the payload must
// decode, and it must be the canonical encoding of what it decodes to. A member
// out of order, an extra member and a widened integer fail the byte comparison;
// a cut payload fails the decode.
func canonicalTopologyRequest(payload []byte) error {
	var request hoarddbwire.RingTopologyRequest
	if err := codec.Unmarshal(payload, &request); err != nil {
		return err
	}
	canonical, err := codec.Marshal(request)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, payload) {
		return fmt.Errorf("the payload is %x, and its canonical encoding is %x", payload, canonical)
	}
	return nil
}

// refuseErrorPayload refuses an ERROR payload neither of the driver's readers
// may act on. The ring client reads an ERROR with hoarddb-wire, which refuses
// it. The legacy client's decoder answers INTERNAL with a sentence saying it
// could not decode the payload, and an error the driver must not act on is a
// refusal, not a code. The check answers nil, "accepted", if either took it.
func refuseErrorPayload(payload []byte) error {
	_, refusal := ringwire.DecodeRingError(payload)
	if refusal == nil {
		return nil
	}
	decoded := hoarddbwire.DecodeError(payload)
	if decoded.Code != hoarddbwire.CodeInternal || !strings.Contains(decoded.Message, "could not decode") {
		return nil
	}
	return refusal
}

// TestTheFourRefusedTopologyRequestsAreNotCanonical: each refused RING_TOPOLOGY
// request is refused by the driver and is no longer deferred. Its bytes are
// not the canonical encoding of what they decode to, or they do not decode.
func TestTheFourRefusedTopologyRequestsAreNotCanonical(t *testing.T) {
	file := loadVectors(t)
	rows := []string{
		"ring topology refused: members out of order",
		"ring topology refused: an extra member",
		"ring topology refused: a widened integer",
		"ring topology refused: truncated",
	}
	for _, name := range rows {
		vector, found := file.Named(name)
		if !found {
			t.Fatalf("the vector %q is not in the file", name)
		}
		if err := checkRefuse(vector); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if tag, held := conformance.Deferred(vector); held {
			t.Errorf("%s is still deferred as %s", name, tag)
		}
	}
}

// TestTheBatchRequestRefusalRowsAreRefusedAndNotDeferred: each refused
// RING_BATCH request is refused by the driver and is no longer deferred, and
// so is the frame the node answers with an ERROR (its request half is a
// payload the driver's decoder refuses, which an ERROR answer allows).
func TestTheBatchRequestRefusalRowsAreRefusedAndNotDeferred(t *testing.T) {
	file := loadVectors(t)
	rows := []string{
		"ring batch refused: a count of zero",
		"ring batch refused: a count past the operations",
		"ring batch refused: a read class",
		"ring batch refused: a trailing byte",
		"ring batch refused: truncated",
		"ring batch: a frame that does not decode is refused whole",
	}
	for _, name := range rows {
		vector, found := file.Named(name)
		if !found {
			t.Fatalf("the vector %q is not in the file", name)
		}
		if err := checkVector(vector); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if tag, held := conformance.Deferred(vector); held {
			t.Errorf("%s is still deferred as %s", name, tag)
		}
	}
}

// TestTheBatchExchangesDecodeAsTheVectorsSay: each RING_BATCH exchange row
// passes the harness, which reads the reply with hoarddb-wire and holds it to
// one answer per operation of its own request, and none is deferred. The
// 255-operation row has no payload_json; the bytes are what is compared, so it
// is held like the others.
func TestTheBatchExchangesDecodeAsTheVectorsSay(t *testing.T) {
	file := loadVectors(t)
	rows := map[string]int{
		"ring batch: a batch of one answers as the lone request would":                1,
		"ring batch: three writes, one refused":                                       3,
		"ring batch: every operation applied":                                         0,
		"ring batch: 255 operations, the most a frame holds":                          255,
		"ring batch: a reply over the budget drops a claim's elements, not the claim": 0,
		"ring batch: the deadline leaves a not-attempted tail":                        0,
	}
	for name, operations := range rows {
		vector, found := file.Named(name)
		if !found {
			t.Fatalf("the vector %q is not in the file", name)
		}
		if err := checkVector(vector); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		if tag, held := conformance.Deferred(vector); held {
			t.Errorf("%s is still deferred as %s", name, tag)
		}
		reply, err := hex.DecodeString(vector.Response.PayloadHex)
		if err != nil {
			t.Fatalf("%s: response payload_hex: %v", name, err)
		}
		decoded, err := ringwire.DecodeRingReply(reply)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		// The count is written out for the rows whose name states it, so a
		// harness that stopped counting would be caught here.
		if operations != 0 && len(decoded.Answers) != operations {
			t.Errorf("%s: %d answers, the row is a batch of %d", name, len(decoded.Answers), operations)
		}
		if len(decoded.Answers) == 0 {
			t.Errorf("%s: a batch reply with no answers", name)
		}
	}
}

// TestTheBatchReplyRefusalRowIsRefusedAndNotDeferred: the reply that says ok
// over a refused answer is refused by the driver, and is no longer deferred.
func TestTheBatchReplyRefusalRowIsRefusedAndNotDeferred(t *testing.T) {
	file := loadVectors(t)
	vector, found := file.Named("ring batch reply refused: ok over a refused answer")
	if !found {
		t.Fatal("the vector ring batch reply refused: ok over a refused answer is not in the file")
	}
	if err := checkVector(vector); err != nil {
		t.Error(err)
	}
	if tag, held := conformance.Deferred(vector); held {
		t.Errorf("the row is still deferred as %s", tag)
	}
}

// checkRefuse shows the driver refuses the payload a refuse vector carries.
func checkRefuse(vector conformance.Vector) error {
	if vector.Refuse == nil {
		return errors.New("a refuse vector without a refuse member")
	}
	direction := vector.Refuse.Direction
	var payloadHex, opcodeName string
	switch {
	case direction == "request" && vector.Request != nil:
		payloadHex, opcodeName = vector.Request.PayloadHex, vector.Request.OpcodeName
	case direction != "request" && vector.Response != nil:
		payloadHex, opcodeName = vector.Response.PayloadHex, vector.Response.OpcodeName
	default:
		return fmt.Errorf("a refuse vector in direction %q has no payload to check", direction)
	}
	payload, err := hex.DecodeString(payloadHex)
	if err != nil {
		return fmt.Errorf("payload_hex: %w", err)
	}
	key := direction + ":" + opcodeName
	check, registered := namedCheckOf(vector.Name)
	if !registered {
		check, registered = refuseChecks[key]
	}
	if !registered {
		return fmt.Errorf("no refusal check is registered for %s: a step builds the decoder", key)
	}
	refusal := check(payload)
	if refusal == nil {
		return fmt.Errorf("the driver accepted a payload the vector refuses (%s)", vector.Notes)
	}
	return checkRefusalCode(vector, refusal)
}
