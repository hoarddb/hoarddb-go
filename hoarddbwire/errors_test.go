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
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/internal/codec"
)

func mustMarshal(t *testing.T, document map[string]any) []byte {
	t.Helper()
	encoded, err := codec.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestDecodeErrorCodeWidthsAndTolerance(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		wantCode ErrorCode
		wantRaw  int64
		wantMsg  string
	}{
		{"int32 code", mustMarshal(t, map[string]any{"code": int32(1), "message": "no bucket"}), CodeNotFound, 1, "no bucket"},
		{"int64 code", mustMarshal(t, map[string]any{"code": int64(0x0F), "message": "dup"}), CodeDuplicateKey, 0x0F, "dup"},
		{"double code", mustMarshal(t, map[string]any{"code": float64(13), "message": "busy"}), CodeOverloaded, 13, "busy"},
		{"zero code is INTERNAL, never success", mustMarshal(t, map[string]any{"code": int32(0), "message": "boom"}), CodeInternal, 0, "boom"},
		{"absent code is INTERNAL", mustMarshal(t, map[string]any{"message": "boom"}), CodeInternal, 0, "boom"},
		{"code from a newer server keeps its number", mustMarshal(t, map[string]any{"code": int32(0x40), "message": "new"}), ErrorCode(0x40), 0x40, "new"},
		{"code beyond a byte is INTERNAL", mustMarshal(t, map[string]any{"code": int32(5000), "message": "x"}), CodeInternal, 5000, "x"},
		{"code of the wrong type is INTERNAL", mustMarshal(t, map[string]any{"code": "one", "message": "x"}), CodeInternal, 0, "x"},
		{"message survives verbatim", mustMarshal(t, map[string]any{"code": int32(1), "message": "Bucket \"users\" not found. Run 'hoardDB bucket list app'."}), CodeNotFound, 1, "Bucket \"users\" not found. Run 'hoardDB bucket list app'."},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := DecodeError(test.payload)
			if got.Code != test.wantCode || got.RawCode != test.wantRaw || got.Message != test.wantMsg {
				t.Fatalf("got code=%v raw=%d message=%q; want code=%v raw=%d message=%q",
					got.Code, got.RawCode, got.Message, test.wantCode, test.wantRaw, test.wantMsg)
			}
		})
	}
}

func TestDecodeErrorEmptyAndGarbagePayloads(t *testing.T) {
	if got := DecodeError(nil); got.Code != CodeInternal {
		t.Errorf("empty payload: code %v, want INTERNAL", got.Code)
	}
	got := DecodeError([]byte{0xFF, 0xFF, 0xFF})
	if got.Code != CodeInternal || !strings.Contains(got.Message, "could not decode") {
		t.Errorf("garbage payload: %+v, want INTERNAL with a message saying so", got)
	}
}

func TestDecodeErrorResultTooLargeCarriesLimits(t *testing.T) {
	got := DecodeError(mustMarshal(t, map[string]any{
		"code": int32(0x0E), "message": "page it", "max_result_bytes": int64(16 << 20), "retained_bytes": int32(20 << 20),
	}))
	if got.Code != CodeResultTooLarge || got.MaxResultBytes != 16<<20 || got.RetainedBytes != 20<<20 {
		t.Fatalf("got %+v", got)
	}
}

func TestDecodeAuthFail(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    AuthFailReason
		message string
	}{
		{"empty payload from an older server is CREDENTIALS", nil, AuthFailCredentials, ""},
		{"int32 reason", mustMarshal(t, map[string]any{"reason": int32(6), "message": "slow down"}), AuthFailRateLimited, "slow down"},
		{"every named reason", mustMarshal(t, map[string]any{"reason": int64(2)}), AuthFailTokenExpired, ""},
		{"reason zero is not valid, so CREDENTIALS", mustMarshal(t, map[string]any{"reason": int32(0)}), AuthFailCredentials, ""},
		{"absent reason", mustMarshal(t, map[string]any{"message": "no"}), AuthFailCredentials, "no"},
		{"reason from a newer server", mustMarshal(t, map[string]any{"reason": int32(99)}), AuthFailReason(99), ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := DecodeAuthFail(test.payload)
			if got.Reason != test.want || got.Message != test.message {
				t.Fatalf("got %+v, want reason %v message %q", got, test.want, test.message)
			}
		})
	}
	if got := DecodeAuthFail([]byte{1, 2}); got.Reason != AuthFailInternal {
		t.Errorf("undecodable AUTH_FAIL: reason %v, want INTERNAL", got.Reason)
	}
}

func TestErrorCodeNamesAndValues(t *testing.T) {
	// The numeric values are the protocol.
	want := map[ErrorCode]string{
		0x01: "NOT_FOUND", 0x02: "ALREADY_EXISTS", 0x03: "PERMISSION_DENIED", 0x04: "INVALID_ARGUMENT",
		0x05: "INTERNAL", 0x06: "UNAVAILABLE", 0x07: "TIMEOUT", 0x08: "CONFLICT", 0x09: "QUOTA_EXCEEDED",
		0x0A: "UNSUPPORTED_VERSION", 0x0B: "EARLY_DATA_REJECTED", 0x0C: "NOT_AUTHENTICATED",
		0x0D: "OVERLOADED", 0x0E: "RESULT_TOO_LARGE", 0x0F: "DUPLICATE_KEY", 0x10: "FIELD_TYPE",
		0x11: "BATCH_TOO_LARGE",
	}
	for code, name := range want {
		if code.String() != name {
			t.Errorf("code 0x%02X is %q, want %q", uint8(code), code.String(), name)
		}
	}
	if got := ErrorCode(0x33).String(); got != "UNKNOWN(51)" {
		t.Errorf("unknown code String = %q", got)
	}
	reasons := map[AuthFailReason]string{
		1: "CREDENTIALS", 2: "TOKEN_EXPIRED", 3: "TOKEN_INVALID", 4: "TOKEN_UNSUPPORTED",
		5: "PROTOCOL_MISMATCH", 6: "RATE_LIMITED", 7: "ALREADY_AUTHED", 8: "INTERNAL",
	}
	for reason, name := range reasons {
		if reason.String() != name {
			t.Errorf("reason %d is %q, want %q", uint8(reason), reason.String(), name)
		}
	}
}

func TestAuthRequestWireNames(t *testing.T) {
	encoded, err := codec.Marshal(AuthRequest{
		Username: "root", Password: "pw", ProtocolVersion: 1, Capabilities: []string{"x"}, Client: "hoarddb-go/0.0.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := codec.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"username", "password", "protocol_version", "capabilities", "client"} {
		if _, present := fields[name]; !present {
			t.Errorf("AuthRequest has no wire field %q; fields are %v", name, fields)
		}
	}
	if _, present := fields["token"]; present {
		t.Error("an empty token must be omitted so a password AUTH frame stays byte-identical")
	}
	withToken, err := codec.Marshal(AuthRequest{Username: "root", Token: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	var tokenFields map[string]any
	if err := codec.Unmarshal(withToken, &tokenFields); err != nil {
		t.Fatal(err)
	}
	if tokenFields["token"] != "abc" {
		t.Errorf("token field = %v, want abc", tokenFields["token"])
	}
}

func TestAuthResponseDecodesAnyIntegerWidth(t *testing.T) {
	payload := mustMarshal(t, map[string]any{
		"success": true, "token": "tok", "token_expires_at": int32(1000), "token_deadline": int64(5000),
		"protocol_version": int32(1), "server_version": "v9", "capabilities": []string{"auth_token", "get_fast"},
	})
	var response AuthResponse
	if err := codec.Unmarshal(payload, &response); err != nil {
		t.Fatal(err)
	}
	if !response.Success || response.Token != "tok" || response.TokenExpiresAt != 1000 || response.TokenDeadline != 5000 ||
		response.ServerVersion != "v9" || len(response.Capabilities) != 2 {
		t.Fatalf("decoded %+v", response)
	}
}

func TestRingErrorCodeNamesAndValues(t *testing.T) {
	tests := []struct {
		code  ErrorCode
		value uint8
		name  string
	}{
		{CodeDocumentExists, 0x12, "DOCUMENT_EXISTS"}, {CodeNotOwner, 0x13, "NOT_OWNER"},
		{CodeStaleEpoch, 0x14, "STALE_EPOCH"}, {CodeOwnerDown, 0x15, "OWNER_DOWN"},
		{CodeNoQuorum, 0x16, "NO_QUORUM"}, {CodeRFUnsupported, 0x17, "RF_UNSUPPORTED"},
		{CodeNodeLeaseLapsed, 0x18, "LEASE_LAPSED"}, {CodeEpochFenced, 0x19, "EPOCH_FENCED"},
		{CodeSeqGap, 0x1A, "SEQ_GAP"}, {CodeLogBelowHorizon, 0x1B, "LOG_BELOW_HORIZON"},
		{CodeStateMoved, 0x1C, "STATE_MOVED"}, {CodeNodeConflict, 0x1D, "CONFLICT"},
		{CodeClusterMismatch, 0x1E, "CLUSTER_MISMATCH"}, {CodeNotADataNode, 0x1F, "NOT_A_DATA_NODE"},
		{CodeSchemaNotCurrent, 0x20, "SCHEMA_NOT_CURRENT"}, {CodeBucketDropping, 0x21, "BUCKET_DROPPING"},
		{CodeDeclUnavailable, 0x22, "DECL_UNAVAILABLE"}, {CodeBucketExists, 0x23, "BUCKET_EXISTS"},
		{CodeReservedName, 0x24, "RESERVED_NAME"}, {CodeBucketIDCollision, 0x25, "BUCKET_ID_COLLISION"},
		{CodeSeedNotHeld, 0x26, "SEED_NOT_HELD"}, {CodeReplaceTargetHasData, 0x27, "REPLACE_TARGET_HAS_DATA"},
		{CodeMintFailed, 0x28, "MINT_FAILED"}, {CodeCorruptKey, 0x29, "CORRUPT_KEY"},
		{CodeSeedHeld, 0x2A, "SEED_HELD"}, {CodeClaimExpired, 0x2B, "CLAIM_EXPIRED"},
		{CodeClaimPending, 0x2C, "CLAIM_PENDING"},
	}
	for _, test := range tests {
		if uint8(test.code) != test.value || test.code.String() != test.name {
			t.Errorf("code 0x%02X is %s, want %s", uint8(test.code), test.code, test.name)
		}
	}
}

func TestAnErrorDetailCarriesTheOwner(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    ErrorCode
		owner   string
		address string
		epoch   int64
		slot    *int64
	}{
		{"the slot belongs to another node",
			mustMarshal(t, map[string]any{"code": int32(0x13), "message": "slot 5737 belongs to node-b.",
				"detail": map[string]any{"owner": "node-b", "owner_addr": "192.0.2.2:4433", "epoch": int64(4), "slot": int32(5737)}}),
			CodeNotOwner, "node-b", "192.0.2.2:4433", 4, ptrTo(int64(5737))},
		{"the client's epoch is behind",
			mustMarshal(t, map[string]any{"code": int32(0x14), "message": "fetch the topology again",
				"detail": map[string]any{"epoch": int64(4), "owner": "node-a", "owner_addr": "a.example:7434"}}),
			CodeStaleEpoch, "node-a", "a.example:7434", 4, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := DecodeError(test.payload)
			detail := got.Detail
			if got.Code != test.want || detail.Owner != test.owner || detail.OwnerAddr != test.address ||
				detail.Epoch == nil || *detail.Epoch != test.epoch || !sameInt64(detail.Slot, test.slot) || len(detail.Raw) == 0 {
				t.Fatalf("got code %v detail %+v; want code %v owner %q address %q epoch %d",
					got.Code, detail, test.want, test.owner, test.address, test.epoch)
			}
		})
	}
}

func TestAnErrorWithoutDetailHasNone(t *testing.T) {
	got := DecodeError(mustMarshal(t, map[string]any{"code": int32(0x20), "message": "describe the bucket again"}))
	detail := got.Detail
	if got.Code != CodeSchemaNotCurrent || detail.Raw != nil || detail.Owner != "" || detail.OwnerAddr != "" ||
		detail.Epoch != nil || detail.Slot != nil {
		t.Fatalf("got code %v detail %+v; want SCHEMA_NOT_CURRENT with no detail", got.Code, detail)
	}
}

func TestAnUnreadableDetailKeepsTheCode(t *testing.T) {
	got := DecodeError(mustMarshal(t, map[string]any{"code": int32(0x13), "message": "m", "detail": "text"}))
	detail := got.Detail
	if got.Code != CodeNotOwner || detail.Raw != nil || detail.Owner != "" {
		t.Fatalf("got code %v detail %+v; want NOT_OWNER with a zero detail", got.Code, detail)
	}
}

func TestATruncatedRingErrorIsNeverActedOn(t *testing.T) {
	full := mustMarshal(t, map[string]any{"code": int32(0x13), "message": "slot 5737 belongs to node-b.",
		"detail": map[string]any{"owner": "node-b", "owner_addr": "192.0.2.2:4433", "epoch": int64(4), "slot": int32(5737)}})
	got := DecodeError(full[:len(full)-8])
	if got.Code != CodeInternal || !strings.Contains(got.Message, "could not decode") {
		t.Fatalf("got code %v message %q; want INTERNAL saying it could not decode", got.Code, got.Message)
	}
}

func ptrTo[T any](pointee T) *T { return &pointee }

func sameInt64(got, want *int64) bool {
	if got == nil || want == nil {
		return got == want
	}
	return *got == *want
}
