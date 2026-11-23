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
	"slices"
	"strings"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// The ring rows are held to hoarddb-wire, the codec module the server reads
// with too: the ring client sends and reads with exactly these functions, so a
// row that passes here is a row the client and the server agree on. Only the
// topology request and reply are read with this module's own decoder, which is
// the one the ring client uses for them.

// ringCodec is one ring payload's decoder and encoder as a pair: the bytes a
// vector carries must decode, and what they decode to must encode back to the
// same bytes.
type ringCodec func(payload []byte) (again []byte, err error)

func ringPair[T any](decode func([]byte) (T, error), encode func(T) ([]byte, error)) ringCodec {
	return func(payload []byte) ([]byte, error) {
		decoded, err := decode(payload)
		if err != nil {
			return nil, err
		}
		return encode(decoded)
	}
}

// ringRequestCodecs is keyed by the request's opcode name.
var ringRequestCodecs = map[string]ringCodec{
	"RING_KEYED":    ringPair(ringwire.DecodeRingKeyed, ringwire.EncodeRingKeyed),
	"RING_BATCH":    ringPair(ringwire.DecodeRingBatch, ringwire.EncodeRingBatch),
	"RING_GET_MANY": ringPair(ringwire.DecodeRingGetMany, ringwire.EncodeRingGetMany),
	"RING_LIST":     ringPair(ringwire.DecodeRingList, ringwire.EncodeRingList),
	"RING_COUNT":    ringPair(ringwire.DecodeRingCount, ringwire.EncodeRingCount),
	"RING_ADMIN":    ringPair(ringwire.DecodeRingAdmin, ringwire.EncodeRingAdmin),
	"RING_TOPOLOGY": func(payload []byte) ([]byte, error) {
		if err := canonicalTopologyRequest(payload); err != nil {
			return nil, err
		}
		return payload, nil
	},
}

// ringReplyCodecs is keyed by the request's opcode name: one RING_REPLY opcode
// answers every ring request, and only the request says which document it is.
var ringReplyCodecs = map[string]ringCodec{
	"RING_KEYED":    ringReplyCodec,
	"RING_BATCH":    ringReplyCodec,
	"RING_GET_MANY": ringReplyCodec,
	"RING_LIST":     ringReplyCodec,
	"RING_COUNT":    ringReplyCodec,
	"RING_ADMIN":    ringPair(ringwire.DecodeRingAdminReply, ringwire.EncodeRingAdminReply),
	"RING_TOPOLOGY": func(payload []byte) ([]byte, error) {
		// This module has a topology decoder and no encoder: the server writes
		// the reply and a client only reads it.
		if _, err := hoarddbwire.DecodeRingTopology(payload); err != nil {
			return nil, err
		}
		return payload, nil
	},
}

var (
	ringReplyCodec = ringPair(ringwire.DecodeRingReply, ringwire.EncodeRingReply)
	ringErrorCodec = ringPair(ringwire.DecodeRingError, ringwire.EncodeRingError)
)

// isRingRequest reports whether a request is one of the ring's, by its opcode name.
func isRingRequest(request *conformance.Request) bool {
	return request != nil && strings.HasPrefix(request.OpcodeName, "RING_")
}

// roundTrip runs a codec over the payload and fails unless the bytes come back.
func roundTrip(codec ringCodec, payload []byte) error {
	again, err := codec(payload)
	if err != nil {
		return err
	}
	if !bytes.Equal(again, payload) {
		return fmt.Errorf("the payload is %x and it encodes back as %x", payload, again)
	}
	return nil
}

// checkRingExchange is checkExchange for a ring row. The request must be one
// the codec reads and writes back byte for byte, and so must the answer. A
// request the server answered with an ERROR may be one the codec refuses as
// well: several rows carry a frame that does not decode, and the codec that
// cannot build it is correct.
func checkRingExchange(vector conformance.Vector) error {
	request, response := vector.Request, vector.Response
	requestCodec, known := ringRequestCodecs[request.OpcodeName]
	if !known {
		return fmt.Errorf("request %s: the harness has no ring codec for this opcode", request.OpcodeName)
	}
	payload, err := hex.DecodeString(request.PayloadHex)
	if err != nil {
		return fmt.Errorf("request payload_hex: %w", err)
	}
	refused := hoarddbwire.Opcode(response.Opcode) == hoarddbwire.OpError
	requestErr := roundTrip(requestCodec, payload)
	if requestErr != nil && !refused {
		return fmt.Errorf("request %s: %w", request.OpcodeName, requestErr)
	}
	if requestErr == nil {
		if err := checkRingAllowedFields(request, payload); err != nil {
			return fmt.Errorf("request %s: %w", request.OpcodeName, err)
		}
	}
	answer, err := hex.DecodeString(response.PayloadHex)
	if err != nil {
		return fmt.Errorf("response payload_hex: %w", err)
	}
	if refused {
		if err := checkRingErrorAnswer(vector, answer); err != nil {
			return fmt.Errorf("response ERROR to %s: %w", request.OpcodeName, err)
		}
		return nil
	}
	if hoarddbwire.Opcode(response.Opcode) != hoarddbwire.OpRingReply {
		return fmt.Errorf("response %s to %s: a ring request is answered with RING_REPLY or ERROR", response.OpcodeName, request.OpcodeName)
	}
	if err := roundTrip(ringReplyCodecs[request.OpcodeName], answer); err != nil {
		return fmt.Errorf("response RING_REPLY to %s: %w", request.OpcodeName, err)
	}
	if err := checkRingAnswerCount(request.OpcodeName, payload, answer); err != nil {
		return fmt.Errorf("response RING_REPLY to %s: %w", request.OpcodeName, err)
	}
	return nil
}

// checkRingAllowedFields holds a BSON request to the vector's allowed_fields.
// The binary requests (keyed, batch, get-many) have no members and an empty list.
func checkRingAllowedFields(request *conformance.Request, payload []byte) error {
	switch request.OpcodeName {
	case "RING_KEYED", "RING_BATCH", "RING_GET_MANY":
		return nil
	}
	tree, err := conformance.WireTree(payload)
	if err != nil {
		return err
	}
	for _, name := range conformance.TopLevelNames(tree) {
		if !slices.Contains(request.AllowedFields, name) {
			return fmt.Errorf("the request carries %q, which is not in allowed_fields %v", name, request.AllowedFields)
		}
	}
	return nil
}

// checkRingAnswerCount: a batch and a get-many are answered with exactly one
// answer per operation or key, in request order.
func checkRingAnswerCount(opcodeName string, request, answer []byte) error {
	var asked int
	switch opcodeName {
	case "RING_BATCH":
		batch, err := ringwire.DecodeRingBatch(request)
		if err != nil {
			return err
		}
		asked = len(batch.Ops)
	case "RING_GET_MANY":
		many, err := ringwire.DecodeRingGetMany(request)
		if err != nil {
			return err
		}
		asked = len(many.Reads)
	default:
		return nil
	}
	reply, err := ringwire.DecodeRingReply(answer)
	if err != nil {
		return err
	}
	if len(reply.Answers) != asked {
		return fmt.Errorf("%d answers for %d operations", len(reply.Answers), asked)
	}
	return nil
}

// checkRingErrorAnswer reads an ERROR that answers a ring request and holds it
// to the vector's code and name. The message is compared only by the parts the
// vector lists: its wording may change before a release, its code may not.
func checkRingErrorAnswer(vector conformance.Vector, payload []byte) error {
	if vector.Error == nil {
		return errors.New("an ERROR answer without an error expectation")
	}
	if err := roundTrip(ringErrorCodec, payload); err != nil {
		return err
	}
	decoded, err := ringwire.DecodeRingError(payload)
	if err != nil {
		return err
	}
	expect := vector.Error
	if int(decoded.Code) != expect.Code || decoded.Code.String() != expect.Name {
		return fmt.Errorf("reported as %s (%d), the vector says %s (%d)", decoded.Code, decoded.Code, expect.Name, expect.Code)
	}
	for _, part := range expect.MessageContains {
		if !strings.Contains(decoded.Message, part) {
			return fmt.Errorf("message %q lacks %q", decoded.Message, part)
		}
	}
	return nil
}

// refusedBy turns a ring decoder into a refuseCheck: the decoder's refusal, or
// nil when it accepted the payload.
func refusedBy[T any](decode func([]byte) (T, error)) refuseCheck {
	return func(payload []byte) error {
		_, err := decode(payload)
		return err
	}
}

// checkRefusalCode holds a ring decoder's refusal to the code the vector says
// the server answered with. The text of a refusal is never compared.
func checkRefusalCode(vector conformance.Vector, refusal error) error {
	var coded *ringwire.Error
	if !errors.As(refusal, &coded) {
		return nil // this module's own decoder (topology), which carries no ring code
	}
	if int(coded.Code) != vector.Refuse.Code || coded.Code.String() != vector.Refuse.Name {
		return fmt.Errorf("refused as %s (%d), the vector says %s (%d)", coded.Code, coded.Code, vector.Refuse.Name, vector.Refuse.Code)
	}
	return nil
}
