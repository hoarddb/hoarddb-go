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
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

func loadVectors(t *testing.T) *conformance.File {
	t.Helper()
	file, err := conformance.Load(conformance.DriverVectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

// TestLeg1Codec is conformance leg 1 (handbook/testing.md section 2): every
// frame, exchange, decode and refuse vector, with no server. A provisional
// vector's result is logged and never fails the test. A deferred vector
// (conformance/deferred.go) is held back while it fails and fails once it passes.
func TestLeg1Codec(t *testing.T) {
	file := loadVectors(t)
	report := conformance.LegReport{Leg: 1, Name: "codec"}
	seen := map[string]bool{}
	for _, vector := range file.Vectors {
		if vector.Kind == "trust" {
			continue // leg 2
		}
		if seen[vector.Name] {
			t.Errorf("two vectors are named %q; names are unique", vector.Name)
		}
		seen[vector.Name] = true
		report.Vectors++
		if vector.Provisional() {
			report.Provisional++
		}
		tag, held := conformance.Deferred(vector)
		t.Run(vector.Kind+"/"+vector.Name, func(t *testing.T) {
			err := checkVector(vector)
			switch {
			case held && err == nil:
				t.Errorf("vector %q passes and is deferred as %s: delete its line from conformance/deferred.go (or narrow the rule)", vector.Name, tag)
			case held:
				t.Logf("deferred as %s: %v", tag, err)
				report.Deferred++
			case err == nil && !vector.Provisional():
				report.Passed++
			case err == nil:
				t.Logf("provisional vector (%s) passes", vector.Status)
			case vector.Provisional():
				t.Logf("provisional vector (%s) does not pass, and that is not a failure: %v", vector.Status, err)
			default:
				t.Error(err)
			}
		})
	}
	if report.Vectors == 0 {
		t.Fatal("no vectors ran")
	}
	t.Log(report)
}

// checkVector runs the check of the vector's kind.
func checkVector(vector conformance.Vector) error {
	switch vector.Kind {
	case "frame":
		return checkFrame(vector)
	case "exchange":
		return checkExchange(vector)
	case "decode":
		return checkDecode(vector)
	case "refuse":
		return checkRefuse(vector)
	default:
		return fmt.Errorf("a vector of kind %q: this harness does not know it", vector.Kind)
	}
}

func checkFrame(vector conformance.Vector) error {
	var expect conformance.FrameExpect
	if err := unmarshalExpect(vector, &expect); err != nil {
		return err
	}
	encoded, err := hex.DecodeString(vector.FrameHex)
	if err != nil {
		return fmt.Errorf("frame_hex: %w", err)
	}
	counter := &countingReader{reader: bytes.NewReader(encoded)}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	frame, err := hoarddbwire.ReadFrame(counter)
	runtime.ReadMemStats(&after)
	if expect.Result == "refuse" {
		return checkRefusal(expect, counter.read, int64(after.TotalAlloc-before.TotalAlloc), err)
	}
	if err != nil {
		return fmt.Errorf("the decoder refused a frame the vector accepts: %w", err)
	}
	if hoarddbwire.ProtocolVersion != byte(expect.Version) {
		return fmt.Errorf("version %d, the vector says %d", hoarddbwire.ProtocolVersion, expect.Version)
	}
	if got := len(frame.Payload) + 1 + hoarddbwire.RequestIDLen; got != expect.Length {
		return fmt.Errorf("length %d, the vector says %d", got, expect.Length)
	}
	if int(frame.Op) != expect.Opcode {
		return fmt.Errorf("opcode %d, the vector says %d", frame.Op, expect.Opcode)
	}
	if got := hex.EncodeToString(frame.ID[:]); got != expect.RequestIDHex {
		return fmt.Errorf("request ID %s, the vector says %s", got, expect.RequestIDHex)
	}
	if got := hex.EncodeToString(frame.Payload); got != expect.PayloadHex {
		return fmt.Errorf("payload %s, the vector says %s", got, expect.PayloadHex)
	}
	reencoded, err := frame.AppendTo(nil)
	if err != nil {
		return fmt.Errorf("encoding the decoded frame: %w", err)
	}
	if !bytes.Equal(reencoded, encoded) {
		return fmt.Errorf("the encoder wrote %x, the vector's frame is %x", reencoded, encoded)
	}
	return nil
}

// checkRefusal checks that the decoder refused for the vector's reason and,
// for a bad header, did so without reading or allocating the body.
func checkRefusal(expect conformance.FrameExpect, bytesRead int, allocated int64, err error) error {
	if err == nil {
		return fmt.Errorf("the decoder accepted a frame the vector refuses (%s)", expect.RefuseBecause)
	}
	var wantErr error
	switch expect.RefuseBecause {
	case "length-too-small", "bad-version":
		wantErr = hoarddbwire.ErrBadFrame
	case "length-too-large":
		wantErr = hoarddbwire.ErrFrameTooLarge
	case "truncated":
		if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return fmt.Errorf("a truncated frame was refused with %v, want an end-of-input error", err)
		}
		return nil
	default:
		return fmt.Errorf("a refusal the harness does not know: %q", expect.RefuseBecause)
	}
	if !errors.Is(err, wantErr) {
		return fmt.Errorf("refused with %v, want %v (%s)", err, wantErr, expect.RefuseBecause)
	}
	if bytesRead > 5 {
		return fmt.Errorf("read %d bytes before refusing; the header's 5 bytes decide it and the body is not read", bytesRead)
	}
	if allocated > 1<<20 {
		return fmt.Errorf("allocated %d bytes refusing a bad header; the body must not be allocated", allocated)
	}
	return nil
}

type countingReader struct {
	reader io.Reader
	read   int
}

func (counter *countingReader) Read(buffer []byte) (int, error) {
	count, err := counter.reader.Read(buffer)
	counter.read += count
	return count, err
}

func unmarshalExpect(vector conformance.Vector, into any) error {
	if err := json.Unmarshal(vector.Expect, into); err != nil {
		return fmt.Errorf("reading expect: %w", err)
	}
	return nil
}

func checkExchange(vector conformance.Vector) error {
	if vector.Request == nil || vector.Response == nil {
		return errors.New("an exchange without a request or a response")
	}
	if err := checkOpcodeName(vector.Request.Opcode, vector.Request.OpcodeName); err != nil {
		return fmt.Errorf("request: %w", err)
	}
	if err := checkOpcodeName(vector.Response.Opcode, vector.Response.OpcodeName); err != nil {
		return fmt.Errorf("response: %w", err)
	}
	if isRingRequest(vector.Request) {
		return checkRingExchange(vector)
	}
	if err := checkRequest(vector.Request, hoarddbwire.Opcode(vector.Response.Opcode) == hoarddbwire.OpError); err != nil {
		return fmt.Errorf("request %s: %w", vector.Request.OpcodeName, err)
	}
	if err := checkResponse(vector); err != nil {
		return fmt.Errorf("response %s to %s: %w", vector.Response.OpcodeName, vector.Request.OpcodeName, err)
	}
	return nil
}

// checkOpcodeName holds the driver's opcode names to the vectors', for every
// opcode the driver names; 0x05 and GOSSIP are opcodes a driver never sends.
func checkOpcodeName(opcode int, name string) error {
	got := hoarddbwire.Opcode(opcode).String()
	if strings.HasPrefix(got, "0x") {
		return nil
	}
	if got != name {
		return fmt.Errorf("the driver names opcode %d %q, the vector %q", opcode, got, name)
	}
	return nil
}

// checkRequest builds the typed request from the vector's payload_json, encodes
// it, and shows the bytes decode to a document that uses only allowed_fields
// and carries every non-empty member of payload_json with an equal value
// (handbook/testing.md section 1.1).
//
// A binary payload the server refused (refused is true) may be one the driver's
// own decoder refuses too: GET_FAST's malformed payload is raw bytes on purpose,
// and a driver that cannot build it is correct.
func checkRequest(request *conformance.Request, refused bool) error {
	typed, known := hoarddbwire.RequestFor(hoarddbwire.Opcode(request.Opcode))
	if !known {
		if len(request.PayloadHex) == 0 && len(request.PayloadJSON) == 0 {
			return nil // an opcode a driver never sends: GOSSIP, 0x05
		}
		return errors.New("the driver has no request type for this opcode")
	}
	if binary, isBinary := typed.(encoding.BinaryMarshaler); isBinary {
		err := checkBinary(request.PayloadHex, typed, binary)
		if err != nil && refused {
			return nil
		}
		return err
	}
	want, err := conformance.ParseExtended(request.PayloadJSON)
	if err != nil {
		return err
	}
	if len(want) > 0 {
		payload, err := codec.FromExtendedJSON(request.PayloadJSON)
		if err != nil {
			return err
		}
		if err := codec.Unmarshal(payload, typed); err != nil {
			return fmt.Errorf("filling the typed request from payload_json: %w", err)
		}
	}
	encoded, err := codec.Marshal(typed)
	if err != nil {
		return fmt.Errorf("encoding the typed request: %w", err)
	}
	got, err := conformance.WireTree(encoded)
	if err != nil {
		return err
	}
	for _, name := range conformance.TopLevelNames(got) {
		if !slices.Contains(request.AllowedFields, name) {
			return fmt.Errorf("the encoded request carries %q, which is not in allowed_fields %v", name, request.AllowedFields)
		}
	}
	shape := conformance.ShapeOf(request.OpcodeName, false)
	if difference := conformance.Diff(shape.Normalise(want), shape.Normalise(got)); difference != "" {
		return fmt.Errorf("the encoded request differs from payload_json: %s", difference)
	}
	return nil
}

// checkBinary covers the one payload that is not BSON (GET_FAST): the bytes
// decode into the typed value and encode back unchanged.
func checkBinary(payloadHex string, typed any, marshaler encoding.BinaryMarshaler) error {
	payload, err := hex.DecodeString(payloadHex)
	if err != nil {
		return fmt.Errorf("payload_hex: %w", err)
	}
	unmarshaler, ok := typed.(encoding.BinaryUnmarshaler)
	if !ok {
		return fmt.Errorf("%T marshals as bytes and cannot read them back", typed)
	}
	if err := unmarshaler.UnmarshalBinary(payload); err != nil {
		return fmt.Errorf("decoding %x: %w", payload, err)
	}
	reencoded, err := marshaler.MarshalBinary()
	if err != nil {
		return fmt.Errorf("encoding %+v: %w", typed, err)
	}
	if !bytes.Equal(reencoded, payload) {
		return fmt.Errorf("encoded %x, the vector's payload is %x", reencoded, payload)
	}
	return nil
}

func checkResponse(vector conformance.Vector) error {
	request, response := vector.Request, vector.Response
	payload, err := hex.DecodeString(response.PayloadHex)
	if err != nil {
		return fmt.Errorf("payload_hex: %w", err)
	}
	switch hoarddbwire.Opcode(response.Opcode) {
	case hoarddbwire.OpError:
		return checkErrorAnswer(vector, payload)
	case hoarddbwire.OpAuthFail:
		return checkAuthFailAnswer(vector, payload)
	}
	typed, known := hoarddbwire.ResponseFor(hoarddbwire.Opcode(request.Opcode))
	if !known {
		return errors.New("the driver has no response type for this request's opcode")
	}
	for _, absent := range response.AbsentFields {
		if tree, err := conformance.WireTree(payload); err == nil {
			if _, present := tree[absent]; present {
				return fmt.Errorf("the vector's own answer carries %q, which absent_fields forbids", absent)
			}
		}
	}
	if binary, isBinary := typed.(encoding.BinaryMarshaler); isBinary {
		return checkBinary(response.PayloadHex, typed, binary)
	}
	return checkTypedAnswer(conformance.ShapeOf(request.OpcodeName, true), typed, payload, response.PayloadJSON, response.Vary)
}

// checkTypedAnswer decodes payload into typed, encodes the typed value back,
// and shows it equals the vector's payload_json except the varying values,
// whose presence and BSON type are checked instead.
func checkTypedAnswer(shape conformance.Shape, typed any, payload []byte, payloadJSON []byte, vary []string) error {
	if err := codec.Unmarshal(payload, typed); err != nil {
		return fmt.Errorf("decoding the answer: %w", err)
	}
	reencoded, err := codec.Marshal(typed)
	if err != nil {
		return fmt.Errorf("encoding the typed answer: %w", err)
	}
	want, err := conformance.ParseExtended(payloadJSON)
	if err != nil {
		return err
	}
	got, err := conformance.WireTree(reencoded)
	if err != nil {
		return err
	}
	wire, err := conformance.WireTree(payload)
	if err != nil {
		return err
	}
	if len(want) == 0 && len(wire) > 0 {
		// Four rows have a null payload_json: the payload's own tree is what the
		// typed answer must give back (handbook/testing.md does not say).
		want = wire
	}
	if err := conformance.CheckVary(want, wire, vary); err != nil {
		return err
	}
	if err := conformance.CheckVaryTyped(want, got, vary); err != nil {
		return fmt.Errorf("the typed value lost a varying member: %w", err)
	}
	wantTree, gotTree := shape.Normalise(want), shape.Normalise(got)
	conformance.StripVary(wantTree, vary)
	conformance.StripVary(gotTree, vary)
	if difference := conformance.Diff(wantTree, gotTree); difference != "" {
		return fmt.Errorf("the typed answer differs from payload_json: %s", difference)
	}
	return nil
}

func checkErrorAnswer(vector conformance.Vector, payload []byte) error {
	if vector.Error == nil {
		return errors.New("an ERROR answer without an error expectation")
	}
	decoded := hoarddbwire.DecodeError(payload)
	expect := vector.Error
	if int(decoded.Code) != expect.Code || decoded.Code.String() != expect.Name {
		return fmt.Errorf("reported as %s (%d), the vector says %s (%d)", decoded.Code, decoded.Code, expect.Name, expect.Code)
	}
	if string(decoded.WriteOutcome) != expect.WriteOutcome {
		return fmt.Errorf("write outcome %q, the vector says %q", decoded.WriteOutcome, expect.WriteOutcome)
	}
	for _, part := range expect.MessageContains {
		if !strings.Contains(decoded.Message, part) {
			return fmt.Errorf("message %q lacks %q", decoded.Message, part)
		}
	}
	return compareError(decoded, vector.Response.PayloadJSON)
}

// compareError shows the decoded error carries every member of payload_json.
func compareError(decoded hoarddbwire.ErrorResponse, payloadJSON []byte) error {
	want, err := conformance.ParseExtended(payloadJSON)
	if err != nil {
		return err
	}
	got := map[string]any{
		"code":               number(decoded.RawCode),
		"message":            decoded.Message,
		"write_outcome":      string(decoded.WriteOutcome),
		"constraint_refusal": decoded.ConstraintRefusal,
		"max_result_bytes":   number(decoded.MaxResultBytes),
		"retained_bytes":     number(decoded.RetainedBytes),
	}
	if decoded.CopiesConfirmed != nil {
		got["copies_confirmed"] = number(*decoded.CopiesConfirmed)
	}
	if decoded.ReplicationFactor != nil {
		got["replication_factor"] = number(*decoded.ReplicationFactor)
	}
	if len(decoded.Detail.Raw) > 0 {
		tree, err := conformance.WireTree(decoded.Detail.Raw)
		if err != nil {
			return fmt.Errorf("the error's detail document: %w", err)
		}
		got["detail"] = tree
	}
	if len(decoded.Results) > 0 {
		encoded, err := codec.Marshal(struct {
			Results []hoarddbwire.WriteResult `bson:"results"`
		}{decoded.Results})
		if err != nil {
			return fmt.Errorf("the error's results: %w", err)
		}
		tree, err := conformance.WireTree(encoded)
		if err != nil {
			return fmt.Errorf("the error's results: %w", err)
		}
		got["results"] = tree["results"]
	}
	shape := conformance.ShapeOf("ERROR", true)
	if difference := conformance.Diff(shape.Normalise(want), shape.Normalise(got)); difference != "" {
		return fmt.Errorf("the decoded error differs from payload_json: %s", difference)
	}
	return nil
}

func number(value int64) json.Number { return json.Number(strconv.FormatInt(value, 10)) }

func checkAuthFailAnswer(vector conformance.Vector, payload []byte) error {
	if vector.AuthFail == nil {
		return errors.New("an AUTH_FAIL answer without an auth_fail expectation")
	}
	decoded := hoarddbwire.DecodeAuthFail(payload)
	expect := vector.AuthFail
	if int(decoded.Reason) != expect.Reason || decoded.Reason.String() != expect.Name {
		return fmt.Errorf("reported as %s (%d), the vector says %s (%d)", decoded.Reason, decoded.Reason, expect.Name, expect.Reason)
	}
	for _, part := range expect.MessageContains {
		if !strings.Contains(decoded.Message, part) {
			return fmt.Errorf("message %q lacks %q", decoded.Message, part)
		}
	}
	return nil
}

func checkDecode(vector conformance.Vector) error {
	var expect conformance.DecodeExpect
	if err := unmarshalExpect(vector, &expect); err != nil {
		return err
	}
	encoded, err := hex.DecodeString(vector.FrameHex)
	if err != nil {
		return fmt.Errorf("frame_hex: %w", err)
	}
	frame, err := hoarddbwire.ReadFrame(bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("reading the frame: %w", err)
	}
	if frame.ID.IsZero() != expect.RequestIDZero {
		return fmt.Errorf("request ID zero is %t, the vector says %t", frame.ID.IsZero(), expect.RequestIDZero)
	}
	switch expect.Result {
	case "error":
		return checkDecodedError(vector, frame, expect)
	case "auth_fail":
		if frame.Op != hoarddbwire.OpAuthFail {
			return fmt.Errorf("opcode %s, want AUTH_FAIL", frame.Op)
		}
		decoded := hoarddbwire.DecodeAuthFail(frame.Payload)
		if int(decoded.Reason) != expect.AuthFailReason || decoded.Reason.String() != expect.Name {
			return fmt.Errorf("reported as %s (%d), the vector says %s (%d)", decoded.Reason, decoded.Reason, expect.Name, expect.AuthFailReason)
		}
		return nil
	case "answer":
		typed, known := hoarddbwire.ResponseFor(frame.Op)
		if !known {
			return fmt.Errorf("the driver has no response type for %s", frame.Op)
		}
		return checkTypedAnswer(conformance.ShapeOf("", true), typed, frame.Payload, expect.PayloadJSON, nil)
	default:
		return fmt.Errorf("a decode result the harness does not know: %q", expect.Result)
	}
}

func checkDecodedError(vector conformance.Vector, frame hoarddbwire.Frame, expect conformance.DecodeExpect) error {
	if frame.Op != hoarddbwire.OpError {
		return fmt.Errorf("opcode %s, want ERROR", frame.Op)
	}
	decoded := hoarddbwire.DecodeError(frame.Payload)
	if decoded.RawCode != int64(expect.Code) || decoded.Code.String() != expect.Name {
		return fmt.Errorf("reported as %s (raw code %d), the vector says %s (%d)", decoded.Code, decoded.RawCode, expect.Name, expect.Code)
	}
	if decoded.Code == hoarddbwire.CodeOK {
		return errors.New("an error frame was read as success")
	}
	for _, part := range expect.MessageContains {
		if !strings.Contains(decoded.Message, part) {
			return fmt.Errorf("message %q lacks %q", decoded.Message, part)
		}
	}
	if vector.RequestHeaderHex != "" {
		// The server closes after answering an oversize request header; what a
		// driver must do is never send such a frame, and refuse it by its length.
		header, err := hex.DecodeString(vector.RequestHeaderHex)
		if err != nil {
			return fmt.Errorf("request_header_hex: %w", err)
		}
		if _, err := hoarddbwire.ReadFrame(bytes.NewReader(header)); !errors.Is(err, hoarddbwire.ErrFrameTooLarge) {
			return fmt.Errorf("the oversize request header was read with %v, want ErrFrameTooLarge", err)
		}
		return nil
	}
	// A refusal at the connection level carries a zero request ID and the
	// connection closes after it; any other error frame leaves it to the caller.
	if fatal := frame.ID.IsZero(); fatal != expect.ConnectionFatal {
		return fmt.Errorf("connection fatal is %t, the vector says %t", fatal, expect.ConnectionFatal)
	}
	return nil
}
