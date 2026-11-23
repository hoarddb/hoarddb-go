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
	"fmt"

	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// ErrorCode is the code in an OpError frame (handbook/errors.md).
type ErrorCode uint8

// Wire error codes. 0x0B is dead: it belonged to the removed 0-RTT transport
// and a server never sends it.
const (
	CodeOK                 ErrorCode = 0x00
	CodeNotFound           ErrorCode = 0x01
	CodeAlreadyExists      ErrorCode = 0x02
	CodePermissionDenied   ErrorCode = 0x03
	CodeInvalidArgument    ErrorCode = 0x04
	CodeInternal           ErrorCode = 0x05
	CodeUnavailable        ErrorCode = 0x06
	CodeTimeout            ErrorCode = 0x07
	CodeConflict           ErrorCode = 0x08
	CodeQuotaExceeded      ErrorCode = 0x09
	CodeUnsupportedVersion ErrorCode = 0x0A
	CodeEarlyDataRejected  ErrorCode = 0x0B
	CodeNotAuthenticated   ErrorCode = 0x0C
	CodeOverloaded         ErrorCode = 0x0D
	CodeResultTooLarge     ErrorCode = 0x0E
	CodeDuplicateKey       ErrorCode = 0x0F
	CodeFieldType          ErrorCode = 0x10
	CodeBatchTooLarge      ErrorCode = 0x11
)

// Ring error codes, 0x12 to 0x2C. Only 0x12, 0x13, 0x14, 0x20, 0x26 and 0x2A are
// pinned by a vector; the other names are the ring's own table and are not
// verified against a server. CodeNodeLeaseLapsed is the node's lease, not a
// queue lease. CodeNodeConflict keeps the table's name CONFLICT, which code 0x08
// also carries.
const (
	CodeDocumentExists       ErrorCode = 0x12
	CodeNotOwner             ErrorCode = 0x13
	CodeStaleEpoch           ErrorCode = 0x14
	CodeOwnerDown            ErrorCode = 0x15
	CodeNoQuorum             ErrorCode = 0x16
	CodeRFUnsupported        ErrorCode = 0x17
	CodeNodeLeaseLapsed      ErrorCode = 0x18
	CodeEpochFenced          ErrorCode = 0x19
	CodeSeqGap               ErrorCode = 0x1A
	CodeLogBelowHorizon      ErrorCode = 0x1B
	CodeStateMoved           ErrorCode = 0x1C
	CodeNodeConflict         ErrorCode = 0x1D
	CodeClusterMismatch      ErrorCode = 0x1E
	CodeNotADataNode         ErrorCode = 0x1F
	CodeSchemaNotCurrent     ErrorCode = 0x20
	CodeBucketDropping       ErrorCode = 0x21
	CodeDeclUnavailable      ErrorCode = 0x22
	CodeBucketExists         ErrorCode = 0x23
	CodeReservedName         ErrorCode = 0x24
	CodeBucketIDCollision    ErrorCode = 0x25
	CodeSeedNotHeld          ErrorCode = 0x26
	CodeReplaceTargetHasData ErrorCode = 0x27
	CodeMintFailed           ErrorCode = 0x28
	CodeCorruptKey           ErrorCode = 0x29
	CodeSeedHeld             ErrorCode = 0x2A
	CodeClaimExpired         ErrorCode = 0x2B
	CodeClaimPending         ErrorCode = 0x2C
)

var codeNames = map[ErrorCode]string{
	CodeOK: "OK", CodeNotFound: "NOT_FOUND", CodeAlreadyExists: "ALREADY_EXISTS",
	CodePermissionDenied: "PERMISSION_DENIED", CodeInvalidArgument: "INVALID_ARGUMENT",
	CodeInternal: "INTERNAL", CodeUnavailable: "UNAVAILABLE", CodeTimeout: "TIMEOUT",
	CodeConflict: "CONFLICT", CodeQuotaExceeded: "QUOTA_EXCEEDED",
	CodeUnsupportedVersion: "UNSUPPORTED_VERSION", CodeEarlyDataRejected: "EARLY_DATA_REJECTED",
	CodeNotAuthenticated: "NOT_AUTHENTICATED", CodeOverloaded: "OVERLOADED",
	CodeResultTooLarge: "RESULT_TOO_LARGE", CodeDuplicateKey: "DUPLICATE_KEY", CodeFieldType: "FIELD_TYPE",
	CodeBatchTooLarge:  "BATCH_TOO_LARGE",
	CodeDocumentExists: "DOCUMENT_EXISTS", CodeNotOwner: "NOT_OWNER", CodeStaleEpoch: "STALE_EPOCH",
	CodeOwnerDown: "OWNER_DOWN", CodeNoQuorum: "NO_QUORUM", CodeRFUnsupported: "RF_UNSUPPORTED",
	CodeNodeLeaseLapsed: "LEASE_LAPSED", CodeEpochFenced: "EPOCH_FENCED", CodeSeqGap: "SEQ_GAP",
	CodeLogBelowHorizon: "LOG_BELOW_HORIZON", CodeStateMoved: "STATE_MOVED", CodeNodeConflict: "CONFLICT",
	CodeClusterMismatch: "CLUSTER_MISMATCH", CodeNotADataNode: "NOT_A_DATA_NODE",
	CodeSchemaNotCurrent: "SCHEMA_NOT_CURRENT", CodeBucketDropping: "BUCKET_DROPPING",
	CodeDeclUnavailable: "DECL_UNAVAILABLE", CodeBucketExists: "BUCKET_EXISTS", CodeReservedName: "RESERVED_NAME",
	CodeBucketIDCollision: "BUCKET_ID_COLLISION", CodeSeedNotHeld: "SEED_NOT_HELD",
	CodeReplaceTargetHasData: "REPLACE_TARGET_HAS_DATA", CodeMintFailed: "MINT_FAILED",
	CodeCorruptKey: "CORRUPT_KEY", CodeSeedHeld: "SEED_HELD", CodeClaimExpired: "CLAIM_EXPIRED",
	CodeClaimPending: "CLAIM_PENDING",
}

// String is the spec's name for the code, or UNKNOWN(n) for one a newer server
// sent: a driver keeps the number so the code is not lost.
func (code ErrorCode) String() string {
	if name, known := codeNames[code]; known {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", uint8(code))
}

// AuthFailReason is the reason in an OpAuthFail frame. Its zero value is
// deliberately not a valid reason.
type AuthFailReason uint8

// Reasons an AUTH was refused (handbook/auth.md section 1).
const (
	AuthFailCredentials      AuthFailReason = 1
	AuthFailTokenExpired     AuthFailReason = 2
	AuthFailTokenInvalid     AuthFailReason = 3
	AuthFailTokenUnsupported AuthFailReason = 4
	AuthFailProtocolMismatch AuthFailReason = 5
	AuthFailRateLimited      AuthFailReason = 6
	AuthFailAlreadyAuthed    AuthFailReason = 7
	AuthFailInternal         AuthFailReason = 8
)

var authFailNames = map[AuthFailReason]string{
	AuthFailCredentials: "CREDENTIALS", AuthFailTokenExpired: "TOKEN_EXPIRED",
	AuthFailTokenInvalid: "TOKEN_INVALID", AuthFailTokenUnsupported: "TOKEN_UNSUPPORTED",
	AuthFailProtocolMismatch: "PROTOCOL_MISMATCH", AuthFailRateLimited: "RATE_LIMITED",
	AuthFailAlreadyAuthed: "ALREADY_AUTHED", AuthFailInternal: "INTERNAL",
}

// String is the spec's name for the reason, or UNKNOWN(n).
func (reason AuthFailReason) String() string {
	if name, known := authFailNames[reason]; known {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", uint8(reason))
}

// ErrorResponse is the decoded payload of an OpError frame.
type ErrorResponse struct {
	// Code is the wire code. A code of zero or an absent code is reported as
	// CodeInternal: it is never success.
	Code ErrorCode
	// RawCode is the number as received, so a code a newer server added keeps
	// its value.
	RawCode int64
	// Message is the server's sentence, verbatim. The fix is inside it.
	Message string
	// MaxResultBytes and RetainedBytes accompany CodeResultTooLarge.
	MaxResultBytes int64
	RetainedBytes  int64
	// WriteOutcome says whether a refused write happened; empty when the
	// server did not say.
	WriteOutcome WriteOutcome
	// CopiesConfirmed and ReplicationFactor are nil when the server did not
	// report them, which is not the same as zero.
	CopiesConfirmed   *int64
	ReplicationFactor *int64
	// ConstraintRefusal marks an INVALID_ARGUMENT that refuses the document
	// itself, the same on every copy, rather than a malformed request.
	ConstraintRefusal bool
	// Detail is the ring's detail document: the owner and the epoch a refused
	// request needs. It is zero when the server sent none.
	Detail ErrorDetail
	// Results is one entry per request position of a refused batch; nil when
	// the server sent none.
	Results []WriteResult
}

// ErrorDetail is the optional detail document of a ring error. Raw is the
// document as received; the members are read from it and stay zero when it is
// absent or unreadable.
type ErrorDetail struct {
	Raw       Document
	Owner     string
	OwnerAddr string
	Epoch     *int64
	Slot      *int64
}

// WriteOutcome is whether a write happened (handbook/errors.md section 4).
type WriteOutcome string

// errorPayload is the wire shape. The code is read at any numeric width
// because the server writes it as int32 from one path and the transport's own
// writer from another.
type errorPayload struct {
	Code           any    `bson:"code"`
	Message        string `bson:"message"`
	MaxResultBytes int64  `bson:"max_result_bytes"`
	RetainedBytes  int64  `bson:"retained_bytes"`
	// ConstraintRefusal is optional and travels with INVALID_ARGUMENT.
	ConstraintRefusal bool `bson:"constraint_refusal"`
	WriteReport       `bson:",inline"`
	Results           []WriteResult `bson:"results"`
	// Detail is the ring's optional detail document.
	Detail Value `bson:"detail,omitempty"`
}

// typeEmbeddedDocument is the BSON type byte of an embedded document.
const typeEmbeddedDocument = 0x03

// detailPayload is the members of a ring error's detail document.
type detailPayload struct {
	Owner     string `bson:"owner"`
	OwnerAddr string `bson:"owner_addr"`
	Epoch     any    `bson:"epoch"`
	Slot      any    `bson:"slot"`
}

// readDetail reads the detail member of an error. A member that is not a
// document is dropped; a document whose members do not read keeps its bytes and
// leaves the members zero. Neither changes the code the error carries.
func readDetail(member Value) ErrorDetail {
	if member.Type != typeEmbeddedDocument {
		return ErrorDetail{}
	}
	detail := ErrorDetail{Raw: Document(member.Value)}
	var members detailPayload
	if err := codec.Unmarshal(member.Value, &members); err != nil {
		return detail
	}
	detail.Owner, detail.OwnerAddr = members.Owner, members.OwnerAddr
	if epoch, ok := numeric(members.Epoch); ok {
		detail.Epoch = &epoch
	}
	if slot, ok := numeric(members.Slot); ok {
		detail.Slot = &slot
	}
	return detail
}

// DecodeError decodes the payload of an OpError frame. Decoding is tolerant:
// an undecodable payload becomes an INTERNAL error that says so rather than a
// second error for the caller to handle.
func DecodeError(payload []byte) ErrorResponse {
	var decoded errorPayload
	if err := codec.Unmarshal(payload, &decoded); err != nil {
		return ErrorResponse{
			Code:    CodeInternal,
			Message: fmt.Sprintf("the server sent an error frame this driver could not decode (%v)", err),
		}
	}
	raw, _ := numeric(decoded.Code)
	response := ErrorResponse{
		RawCode:        raw,
		Message:        decoded.Message,
		MaxResultBytes: decoded.MaxResultBytes,
		RetainedBytes:  decoded.RetainedBytes,

		WriteOutcome:      decoded.WriteOutcome,
		CopiesConfirmed:   decoded.CopiesConfirmed,
		ReplicationFactor: decoded.ReplicationFactor,
		ConstraintRefusal: decoded.ConstraintRefusal,
		Detail:            readDetail(decoded.Detail),
		Results:           decoded.Results,
	}
	switch {
	case raw <= 0 || raw > 0xFF:
		response.Code = CodeInternal
	default:
		response.Code = ErrorCode(raw)
	}
	return response
}

// AuthFailResponse is the decoded payload of an OpAuthFail frame.
type AuthFailResponse struct {
	// Reason is never zero: an empty payload, which older servers send, is
	// CREDENTIALS.
	Reason  AuthFailReason
	Message string
}

type authFailPayload struct {
	Reason  any    `bson:"reason"`
	Message string `bson:"message"`
}

// DecodeAuthFail decodes the payload of an OpAuthFail frame.
func DecodeAuthFail(payload []byte) AuthFailResponse {
	var decoded authFailPayload
	if err := codec.Unmarshal(payload, &decoded); err != nil {
		return AuthFailResponse{Reason: AuthFailInternal, Message: fmt.Sprintf("the server sent an AUTH_FAIL this driver could not decode (%v)", err)}
	}
	raw, _ := numeric(decoded.Reason)
	if raw <= 0 || raw > 0xFF {
		raw = int64(AuthFailCredentials)
	}
	return AuthFailResponse{Reason: AuthFailReason(raw), Message: decoded.Message}
}

// numeric reads any BSON number as an int64.
func numeric(value any) (int64, bool) {
	switch number := value.(type) {
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case float64:
		return int64(number), true
	default:
		return 0, false
	}
}
