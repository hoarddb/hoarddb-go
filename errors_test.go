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

package hoarddb_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

var allSentinels = map[string]*hoarddb.Error{
	"NotFound": hoarddb.ErrNotFound, "AlreadyExists": hoarddb.ErrAlreadyExists,
	"PermissionDenied": hoarddb.ErrPermissionDenied, "InvalidArgument": hoarddb.ErrInvalidArgument,
	"Internal": hoarddb.ErrInternal, "Unavailable": hoarddb.ErrUnavailable, "Timeout": hoarddb.ErrTimeout,
	"Conflict": hoarddb.ErrConflict, "QuotaExceeded": hoarddb.ErrQuotaExceeded,
	"UnsupportedVersion": hoarddb.ErrUnsupportedVersion, "NotAuthenticated": hoarddb.ErrNotAuthenticated,
	"Overloaded": hoarddb.ErrOverloaded, "ResultTooLarge": hoarddb.ErrResultTooLarge, "DuplicateKey": hoarddb.ErrDuplicateKey, "FieldType": hoarddb.ErrFieldType,
	"BatchTooLarge": hoarddb.ErrBatchTooLarge,
	"AuthFailed":    hoarddb.ErrAuthFailed, "UntrustedServer": hoarddb.ErrUntrustedServer,
	"ServerKeyChanged": hoarddb.ErrServerKeyChanged, "FingerprintMismatch": hoarddb.ErrFingerprintMismatch,
	"InvalidURI": hoarddb.ErrInvalidURI, "Closed": hoarddb.ErrClosed, "Unreachable": hoarddb.ErrUnreachable,
}

func TestEverySentinelMatchesItselfAndNothingElse(t *testing.T) {
	for name, sentinel := range allSentinels {
		failure := &hoarddb.Error{Code: sentinel.Code, Op: "x", Message: "m"}
		for otherName, other := range allSentinels {
			if got, want := errors.Is(failure, other), name == otherName; got != want {
				t.Errorf("errors.Is(%s error, Err%s) = %v, want %v", name, otherName, got, want)
			}
		}
	}
}

func TestErrorsMatchThroughWrapping(t *testing.T) {
	failure := &hoarddb.Error{Code: hoarddb.CodeNotFound, Op: "get", Message: "no such bucket"}
	wrapped := fmt.Errorf("loading the profile: %w", failure)
	if !errors.Is(wrapped, hoarddb.ErrNotFound) {
		t.Fatal("errors.Is must see through fmt.Errorf %w")
	}
	var found *hoarddb.Error
	if !errors.As(wrapped, &found) || found.Op != "get" {
		t.Fatal("errors.As must recover the structured error")
	}
}

func TestFieldTypeCodeMapsToItsSentinel(t *testing.T) {
	failure := &hoarddb.Error{Code: hoarddb.Code(0x10), Op: "put", Message: `field "a.b" is not allowed`}
	if hoarddb.CodeFieldType != 0x10 || !errors.Is(failure, hoarddb.ErrFieldType) {
		t.Fatal("wire code 0x10 must map to ErrFieldType")
	}
	if errors.Is(failure, hoarddb.ErrInternal) || errors.Is(failure, hoarddb.ErrInvalidArgument) {
		t.Error("FIELD_TYPE must not fold into another class")
	}
	if !strings.Contains(hoarddb.ErrFieldType.Error(), "rename the field") {
		t.Errorf("the sentinel must name the fix, got %q", hoarddb.ErrFieldType.Error())
	}
}

func TestBatchTooLargeCodeMapsToItsSentinel(t *testing.T) {
	failure := &hoarddb.Error{Code: hoarddb.Code(0x11), Op: "insert many", Message: "the batch is too large"}
	if hoarddb.CodeBatchTooLarge != 0x11 || !errors.Is(failure, hoarddb.ErrBatchTooLarge) {
		t.Fatal("wire code 0x11 must map to ErrBatchTooLarge")
	}
	if errors.Is(failure, hoarddb.ErrInternal) {
		t.Error("BATCH_TOO_LARGE must not fold into INTERNAL")
	}
	if got := hoarddb.CodeBatchTooLarge.String(); got != "batch too large" {
		t.Errorf("CodeBatchTooLarge.String() = %q", got)
	}
}

// ringCodes is the ring's error table, 0x12 to 0x2C, without the two
// node-to-node codes 0x19 and 0x1A, which have no sentinel.
var ringCodes = []struct {
	code     hoarddb.Code
	sentinel *hoarddb.Error
	name     string
}{
	{hoarddb.CodeDocumentExists, hoarddb.ErrDocumentExists, "document exists"},
	{hoarddb.CodeNotOwner, hoarddb.ErrNotOwner, "not owner"},
	{hoarddb.CodeStaleEpoch, hoarddb.ErrStaleEpoch, "stale epoch"},
	{hoarddb.CodeOwnerDown, hoarddb.ErrOwnerDown, "owner down"},
	{hoarddb.CodeNoQuorum, hoarddb.ErrNoQuorum, "no quorum"},
	{hoarddb.CodeRFUnsupported, hoarddb.ErrRFUnsupported, "replication factor unsupported"},
	{hoarddb.CodeNodeLeaseLapsed, hoarddb.ErrNodeLeaseLapsed, "node lease lapsed"},
	{hoarddb.CodeLogBelowHorizon, hoarddb.ErrLogBelowHorizon, "log below horizon"},
	{hoarddb.CodeStateMoved, hoarddb.ErrStateMoved, "state moved"},
	{hoarddb.CodeNodeConflict, hoarddb.ErrNodeConflict, "node conflict"},
	{hoarddb.CodeClusterMismatch, hoarddb.ErrClusterMismatch, "cluster mismatch"},
	{hoarddb.CodeNotADataNode, hoarddb.ErrNotADataNode, "not a data node"},
	{hoarddb.CodeSchemaNotCurrent, hoarddb.ErrSchemaNotCurrent, "schema not current"},
	{hoarddb.CodeBucketDropping, hoarddb.ErrBucketDropping, "bucket dropping"},
	{hoarddb.CodeDeclUnavailable, hoarddb.ErrDeclUnavailable, "declaration unavailable"},
	{hoarddb.CodeBucketExists, hoarddb.ErrBucketExists, "bucket exists"},
	{hoarddb.CodeReservedName, hoarddb.ErrReservedName, "reserved name"},
	{hoarddb.CodeBucketIDCollision, hoarddb.ErrBucketIDCollision, "bucket id collision"},
	{hoarddb.CodeSeedNotHeld, hoarddb.ErrSeedNotHeld, "seed not held"},
	{hoarddb.CodeReplaceTargetHasData, hoarddb.ErrReplaceTargetHasData, "replace target has data"},
	{hoarddb.CodeMintFailed, hoarddb.ErrMintFailed, "mint failed"},
	{hoarddb.CodeCorruptKey, hoarddb.ErrCorruptKey, "corrupt key"},
	{hoarddb.CodeSeedHeld, hoarddb.ErrSeedHeld, "seed held"},
	{hoarddb.CodeClaimExpired, hoarddb.ErrClaimExpired, "claim expired"},
	{hoarddb.CodeClaimPending, hoarddb.ErrClaimPending, "claim pending"},
}

// TestEveryRingCodeHasASentinelAndAName checks each ring code's name and that
// its sentinel matches that code and no other ring sentinel.
func TestEveryRingCodeHasASentinelAndAName(t *testing.T) {
	for _, row := range ringCodes {
		if got := row.code.String(); got != row.name {
			t.Errorf("Code %#x String() = %q, want %q", uint16(row.code), got, row.name)
		}
		failure := &hoarddb.Error{Code: row.code, Op: "x", Message: "m"}
		if !errors.Is(failure, row.sentinel) {
			t.Errorf("a %s error does not match its sentinel", row.name)
		}
		for _, other := range ringCodes {
			if other.sentinel != row.sentinel && errors.Is(failure, other.sentinel) {
				t.Errorf("a %s error matches the %s sentinel", row.name, other.name)
			}
		}
	}
	if errors.Is(&hoarddb.Error{Code: hoarddb.CodeStaleEpoch}, hoarddb.ErrNotOwner) {
		t.Error("a stale epoch error must not match ErrNotOwner")
	}
}

func TestUnknownAndDeadCodesAreInternal(t *testing.T) {
	for _, code := range []hoarddb.Code{0, 0x0B, 0x19, 0x1A, 0x2D, 0x41, 0xFF} {
		failure := &hoarddb.Error{Code: code, Message: "from a newer server"}
		if !errors.Is(failure, hoarddb.ErrInternal) {
			t.Errorf("code %#x should behave as ErrInternal", uint16(code))
		}
		if failure.Code != code {
			t.Errorf("the code %#x was not kept verbatim", uint16(code))
		}
	}
	if errors.Is(&hoarddb.Error{Code: hoarddb.CodeUnreachable}, hoarddb.ErrInternal) {
		t.Error("a driver code must not fold into ErrInternal")
	}
}

func TestErrorUnwrapsItsCause(t *testing.T) {
	failure := &hoarddb.Error{Code: hoarddb.CodeUnreachable, Op: "connect", Err: context.DeadlineExceeded}
	if !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatal("a caller's deadline must stay visible as context.DeadlineExceeded")
	}
	if errors.Is(failure, hoarddb.ErrTimeout) {
		t.Fatal("the caller's own deadline is not the server's TIMEOUT")
	}
}

func TestErrorString(t *testing.T) {
	tests := []struct {
		failure *hoarddb.Error
		want    string
	}{
		{&hoarddb.Error{Code: hoarddb.CodeNotFound, Op: "get", Target: "app/users", Message: "Bucket \"users\" not found."}, `get app/users: Bucket "users" not found.`},
		{&hoarddb.Error{Code: hoarddb.CodeUnreachable, Op: "connect", Message: "nobody home"}, "connect: nobody home"},
		{&hoarddb.Error{Code: hoarddb.CodeInternal, Err: errors.New("boom")}, "boom"},
		{&hoarddb.Error{Code: hoarddb.CodeConflict}, "conflict"},
		{hoarddb.ErrNotFound, "not found"},
	}
	for _, test := range tests {
		if got := test.failure.Error(); got != test.want {
			t.Errorf("Error() = %q, want %q", got, test.want)
		}
	}
}

func TestCodeNamesAndValuesArePinned(t *testing.T) {
	// Wire codes equal the protocol's numbers; driver codes start at 0x100.
	if hoarddb.CodeNotFound != 0x01 || hoarddb.CodeDuplicateKey != 0x0F || hoarddb.CodeOverloaded != 0x0D {
		t.Fatal("wire codes drifted from the protocol")
	}
	if hoarddb.CodeAuthFailed != 0x100 {
		t.Fatalf("first driver code = %#x, want 0x100", uint16(hoarddb.CodeAuthFailed))
	}
	if hoarddb.Code(0x777).String() == "" || hoarddb.AuthReasonRateLimited.String() != "RATE_LIMITED" {
		t.Fatal("names")
	}
}

// config is unexported, so redaction is checked through the only place it is
// logged: the client's own log lines never carry the password. The value-level
// check for String and LogValue lives in options_internal_test.go.
func TestLoggerReceivesNoSecretsAtDebug(t *testing.T) {
	server := startFake(t, serveAuthThen(nil))
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := mustConnect(t, "hoarddb://admin:p4ssw0rd-xyz@"+server.address+"/?fingerprint="+server.fingerprint, hoarddb.WithLogger(logger))
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "p4ssw0rd-xyz") || strings.Contains(logs.String(), "token-from") {
		t.Fatalf("secret in log:\n%s", logs.String())
	}
}
