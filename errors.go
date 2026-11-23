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

package hoarddb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Code classifies an [Error]. Codes 0x01 to 0x11 and 0x12 to 0x2C are the
// server's wire codes; codes from 0x100 are raised by the driver itself and
// never appear on the wire.
type Code uint16

// Wire codes, as the server sends them.
const (
	CodeNotFound           Code = 0x01
	CodeAlreadyExists      Code = 0x02
	CodePermissionDenied   Code = 0x03
	CodeInvalidArgument    Code = 0x04
	CodeInternal           Code = 0x05
	CodeUnavailable        Code = 0x06
	CodeTimeout            Code = 0x07
	CodeConflict           Code = 0x08
	CodeQuotaExceeded      Code = 0x09
	CodeUnsupportedVersion Code = 0x0A
	CodeNotAuthenticated   Code = 0x0C
	CodeOverloaded         Code = 0x0D
	CodeResultTooLarge     Code = 0x0E
	CodeDuplicateKey       Code = 0x0F
	// CodeFieldType is a document the server refused for one of its fields: a
	// name no path can address, or a value of the wrong declared type. A retry
	// of the same document is refused again.
	CodeFieldType Code = 0x10
	// CodeBatchTooLarge is a batch put over the largest batch the store commits
	// atomically. A standalone node reports not_applied; a cluster owner's
	// refusal is unknown. The caller halves the batch and resends it, never the
	// driver.
	CodeBatchTooLarge Code = 0x11
	// The ring codes, 0x12 to 0x2C. CodeNodeConflict is the ring's CONFLICT, not
	// the legacy CodeConflict. CodeNodeLeaseLapsed is the node's own lease, not a
	// queue lease. 0x19 and 0x1A are node-to-node only and have no code here.
	CodeDocumentExists       Code = 0x12
	CodeNotOwner             Code = 0x13
	CodeStaleEpoch           Code = 0x14
	CodeOwnerDown            Code = 0x15
	CodeNoQuorum             Code = 0x16
	CodeRFUnsupported        Code = 0x17
	CodeNodeLeaseLapsed      Code = 0x18
	CodeLogBelowHorizon      Code = 0x1B
	CodeStateMoved           Code = 0x1C
	CodeNodeConflict         Code = 0x1D
	CodeClusterMismatch      Code = 0x1E
	CodeNotADataNode         Code = 0x1F
	CodeSchemaNotCurrent     Code = 0x20
	CodeBucketDropping       Code = 0x21
	CodeDeclUnavailable      Code = 0x22
	CodeBucketExists         Code = 0x23
	CodeReservedName         Code = 0x24
	CodeBucketIDCollision    Code = 0x25
	CodeSeedNotHeld          Code = 0x26
	CodeReplaceTargetHasData Code = 0x27
	CodeMintFailed           Code = 0x28
	CodeCorruptKey           Code = 0x29
	CodeSeedHeld             Code = 0x2A
	CodeClaimExpired         Code = 0x2B
	CodeClaimPending         Code = 0x2C
)

// Driver-side codes.
const (
	// CodeAuthFailed is a login the server refused, or no credentials to log in
	// with. See [Error.Reason].
	CodeAuthFailed Code = 0x100 + iota
	// CodeUntrustedServer is a server that presented a key nothing vouches for.
	CodeUntrustedServer
	// CodeServerKeyChanged is a known server that now presents a different key.
	CodeServerKeyChanged
	// CodeFingerprintMismatch is a server whose key is not the pinned one.
	CodeFingerprintMismatch
	// CodeInvalidURI is a connection string or option the driver refused.
	CodeInvalidURI
	// CodeClosed is a call on a closed [Client].
	CodeClosed
	// CodeUnreachable is no server answering at any address given.
	CodeUnreachable
	// CodeUnsupportedByServer is an option or call the connected server did not
	// advertise a capability for. Nothing was sent.
	CodeUnsupportedByServer
	// CodeInterrupted is a call that ended before its answer arrived because
	// the caller's context was cancelled or its deadline passed. Match it by its
	// cause, errors.Is(err, context.DeadlineExceeded) or errors.Is(err,
	// context.Canceled). A write that may have reached the server carries
	// [OutcomeUnknown]; one that never left the driver carries
	// [OutcomeNotApplied].
	CodeInterrupted
	// CodeLeaseUnknown is a lease verb on a lease the bucket never issued, or
	// one already acknowledged, released or returned. Nothing was changed.
	CodeLeaseUnknown
	// CodeLeaseExpired is a lease verb on a lease whose time ran out: its
	// entries went back to the queue and will be delivered again.
	CodeLeaseExpired
)

// AuthReason says why a login was refused. The zero value means the error is
// not an authentication refusal.
type AuthReason uint8

// Reasons the server gives (handbook/auth.md section 1).
const (
	AuthReasonCredentials      AuthReason = 1
	AuthReasonTokenExpired     AuthReason = 2
	AuthReasonTokenInvalid     AuthReason = 3
	AuthReasonTokenUnsupported AuthReason = 4
	AuthReasonProtocolMismatch AuthReason = 5
	AuthReasonRateLimited      AuthReason = 6
	AuthReasonAlreadyAuthed    AuthReason = 7
	AuthReasonInternal         AuthReason = 8
)

// String is the server's name for the reason.
func (reason AuthReason) String() string { return hoarddbwire.AuthFailReason(reason).String() }

// Error is the one error type every call returns. Match its class with
// errors.Is against the Err sentinels, and read its fields with errors.As:
//
//	if errors.Is(err, hoarddb.ErrUntrustedServer) { ... }
//
//	var failure *hoarddb.Error
//	if errors.As(err, &failure) { log.Println(failure.Op, failure.Target) }
type Error struct {
	// Code is the class: a wire code (0x01 to 0x11) or a driver code (0x100 and
	// up).
	Code Code
	// Op is the operation that failed: "connect", "ping", "create bucket".
	Op string
	// Target is what it was aimed at: a host address, or "app/users" for a bucket.
	Target string
	// Message is the server's sentence verbatim, or the driver's own with its
	// fix. It never contains a password or token.
	Message string
	// Reason is set for [CodeAuthFailed] when the server gave one.
	Reason AuthReason
	// Owner and OwnerAddr: the node that owns the slot and its address, when the
	// server named them; "" otherwise.
	Owner, OwnerAddr string
	// Outcome says whether a refused write happened. A server stamps every
	// refusal of a data write, on a single node as on a cluster, and the stamp is
	// passed through untouched. An ordinary error frame on a data write with no
	// stamp (an older server, or a single node whose own store failed the write)
	// is [OutcomeUnknown], and the message ends with the driver's advice to read
	// the data back; so is a data write or a DDL request answered OVERLOADED under
	// its own request ID (never resent), and one whose connection is lost
	// or whose context ends in flight (a DDL request's advice says to read the
	// state back). It is [OutcomeNotReported] for a read, a login, a DDL request
	// the server refused with an error frame, and an in-band refusal the server
	// did not stamp.
	Outcome WriteOutcome
	// CopiesConfirmed and ReplicationFactor are how many copies hold a write and
	// how many the bucket keeps, when the server said; 0 is "not reported".
	CopiesConfirmed, ReplicationFactor int
	// MaxResultBytes and RetainedBytes accompany [ErrResultTooLarge]: the ceiling
	// and a lower bound of what the query held. 0 is "not reported".
	MaxResultBytes, RetainedBytes int64
	// Results is what the server said of each document of a refused batch, in
	// the order the batch was sent: which one was refused and why, and that the
	// others were not applied either when the batch is refused whole. Nil for
	// any other error. ResultsOffset is the position in the caller's slice of
	// Results[0], non-zero only when InsertMany or UpsertMany had already stored earlier parts.
	// A position the server names in a sentence counts from the first document of
	// the part that answered; add ResultsOffset to find it in your slice.
	Results       []WriteResult
	ResultsOffset int
	// Err is the underlying cause: a network error, context.DeadlineExceeded.
	Err error

	// claimTooLarge marks an INVALID_ARGUMENT that is a claim refused for an
	// entry over the hard cap. The code stays the wire's; the mark is what
	// [ErrClaimEntryTooLarge] matches.
	claimTooLarge bool
}

// WriteOutcome is whether a write that was refused or interrupted happened
// (handbook/errors.md section 4).
type WriteOutcome uint8

// The outcomes a server reports. A caller may resend after [OutcomeNotApplied];
// after [OutcomeApplied] a resent push or increment would apply twice; after
// [OutcomeUnknown] it reads the data back first.
const (
	// OutcomeNotReported: no outcome applies or none was given: a read, a login,
	// a DDL request the server refused with an error frame, or an in-band refusal
	// the server did not stamp. A data write whose error frame says nothing, and
	// any write or DDL request lost in flight, is [OutcomeUnknown], not this.
	OutcomeNotReported WriteOutcome = iota
	// OutcomeApplied: the write is stored, though the request was refused.
	OutcomeApplied
	// OutcomeNotApplied: nothing was written.
	OutcomeNotApplied
	// OutcomeUnknown: the server cannot tell.
	OutcomeUnknown
)

// String is the server's word for the outcome.
func (outcome WriteOutcome) String() string {
	switch outcome {
	case OutcomeApplied:
		return "applied"
	case OutcomeNotApplied:
		return "not_applied"
	case OutcomeUnknown:
		return "unknown"
	default:
		return "not reported"
	}
}

// Error renders "op target: message", leaving out what is empty.
func (e *Error) Error() string {
	var text strings.Builder
	if e.Op != "" {
		text.WriteString(e.Op)
	}
	if e.Target != "" {
		if text.Len() > 0 {
			text.WriteString(" ")
		}
		text.WriteString(e.Target)
	}
	body := e.Message
	if body == "" && e.Err != nil {
		body = e.Err.Error()
	}
	if body == "" {
		body = e.Code.String()
	}
	if text.Len() > 0 {
		text.WriteString(": ")
	}
	text.WriteString(body)
	return text.String()
}

// Unwrap returns the underlying cause, so errors.Is(err, context.DeadlineExceeded)
// works when a deadline caused the failure.
func (e *Error) Unwrap() error { return e.Err }

// Is reports whether target is a sentinel of the same class. Codes the driver
// does not know, the dead code 0x0B and the node-to-node codes 0x19 and 0x1A are
// [ErrInternal].
func (e *Error) Is(target error) bool {
	sentinel, ok := target.(*Error)
	if !ok {
		return false
	}
	if sentinel.claimTooLarge {
		return e.claimTooLarge
	}
	return e.Code.class() == sentinel.Code
}

// class folds the codes a driver must treat as INTERNAL into it: 0, the dead
// 0x0B, the node-to-node codes 0x19 and 0x1A, and every code above the ring's.
func (code Code) class() Code {
	switch {
	case code == 0, code == Code(hoarddbwire.CodeEarlyDataRejected),
		code == Code(hoarddbwire.CodeEpochFenced), code == Code(hoarddbwire.CodeSeqGap):
		return CodeInternal
	case code < 0x100 && code > CodeClaimPending:
		return CodeInternal
	default:
		return code
	}
}

var codeNames = map[Code]string{
	CodeNotFound: "not found", CodeAlreadyExists: "already exists", CodePermissionDenied: "permission denied",
	CodeInvalidArgument: "invalid argument", CodeInternal: "internal error", CodeUnavailable: "unavailable",
	CodeTimeout: "timeout", CodeConflict: "conflict", CodeQuotaExceeded: "quota exceeded",
	CodeUnsupportedVersion: "unsupported version", CodeNotAuthenticated: "not authenticated",
	CodeOverloaded: "overloaded", CodeResultTooLarge: "result too large", CodeDuplicateKey: "duplicate key",
	CodeFieldType: "field type", CodeBatchTooLarge: "batch too large",
	CodeAuthFailed: "authentication failed", CodeUntrustedServer: "untrusted server",
	CodeServerKeyChanged: "server key changed", CodeFingerprintMismatch: "fingerprint mismatch",
	CodeInvalidURI: "invalid connection string", CodeClosed: "client closed", CodeUnreachable: "server unreachable",
	CodeUnsupportedByServer: "unsupported by server", CodeInterrupted: "interrupted",
	CodeLeaseUnknown: "lease unknown", CodeLeaseExpired: "lease expired",
	CodeDocumentExists: "document exists", CodeNotOwner: "not owner", CodeStaleEpoch: "stale epoch",
	CodeOwnerDown: "owner down", CodeNoQuorum: "no quorum", CodeRFUnsupported: "replication factor unsupported",
	CodeNodeLeaseLapsed: "node lease lapsed", CodeLogBelowHorizon: "log below horizon", CodeStateMoved: "state moved",
	CodeNodeConflict: "node conflict", CodeClusterMismatch: "cluster mismatch", CodeNotADataNode: "not a data node",
	CodeSchemaNotCurrent: "schema not current", CodeBucketDropping: "bucket dropping",
	CodeDeclUnavailable: "declaration unavailable", CodeBucketExists: "bucket exists", CodeReservedName: "reserved name",
	CodeBucketIDCollision: "bucket id collision", CodeSeedNotHeld: "seed not held",
	CodeReplaceTargetHasData: "replace target has data", CodeMintFailed: "mint failed", CodeCorruptKey: "corrupt key",
	CodeSeedHeld: "seed held", CodeClaimExpired: "claim expired", CodeClaimPending: "claim pending",
}

// String is a short name for the class.
func (code Code) String() string {
	if name, known := codeNames[code]; known {
		return name
	}
	return fmt.Sprintf("code %#x", uint16(code))
}

func sentinel(code Code) *Error { return &Error{Code: code, Message: code.String()} }

// Sentinels for errors.Is. Each is an *Error with only its class set.
var (
	ErrNotFound           = sentinel(CodeNotFound)
	ErrAlreadyExists      = sentinel(CodeAlreadyExists)
	ErrPermissionDenied   = sentinel(CodePermissionDenied)
	ErrInvalidArgument    = sentinel(CodeInvalidArgument)
	ErrInternal           = sentinel(CodeInternal)
	ErrUnavailable        = sentinel(CodeUnavailable)
	ErrTimeout            = sentinel(CodeTimeout)
	ErrConflict           = sentinel(CodeConflict)
	ErrQuotaExceeded      = sentinel(CodeQuotaExceeded)
	ErrUnsupportedVersion = sentinel(CodeUnsupportedVersion)
	ErrNotAuthenticated   = sentinel(CodeNotAuthenticated)
	ErrOverloaded         = sentinel(CodeOverloaded)
	ErrResultTooLarge     = sentinel(CodeResultTooLarge)
	ErrDuplicateKey       = sentinel(CodeDuplicateKey)
	// ErrFieldType: the server refused a document for a field. The message
	// names the fix because a retry cannot succeed.
	ErrFieldType = &Error{Code: CodeFieldType, Message: "field type: a field name contains '.', '[' or ']', or a value does not match the bucket's declared type; rename the field or correct the value, then resend (retrying the same document will fail again)"}
	// ErrBatchTooLarge: the store refused a batch put whole because it is over
	// the largest batch it commits atomically. Nothing was written on a
	// standalone node; a cluster owner's refusal is unknown. Halve the batch and
	// resend it; the driver never retries it.
	ErrBatchTooLarge        = sentinel(CodeBatchTooLarge)
	ErrDocumentExists       = sentinel(CodeDocumentExists)
	ErrNotOwner             = sentinel(CodeNotOwner)
	ErrStaleEpoch           = sentinel(CodeStaleEpoch)
	ErrOwnerDown            = sentinel(CodeOwnerDown)
	ErrNoQuorum             = sentinel(CodeNoQuorum)
	ErrRFUnsupported        = sentinel(CodeRFUnsupported)
	ErrNodeLeaseLapsed      = sentinel(CodeNodeLeaseLapsed)
	ErrLogBelowHorizon      = sentinel(CodeLogBelowHorizon)
	ErrStateMoved           = sentinel(CodeStateMoved)
	ErrNodeConflict         = sentinel(CodeNodeConflict)
	ErrClusterMismatch      = sentinel(CodeClusterMismatch)
	ErrNotADataNode         = sentinel(CodeNotADataNode)
	ErrSchemaNotCurrent     = sentinel(CodeSchemaNotCurrent)
	ErrBucketDropping       = sentinel(CodeBucketDropping)
	ErrDeclUnavailable      = sentinel(CodeDeclUnavailable)
	ErrBucketExists         = sentinel(CodeBucketExists)
	ErrReservedName         = sentinel(CodeReservedName)
	ErrBucketIDCollision    = sentinel(CodeBucketIDCollision)
	ErrSeedNotHeld          = sentinel(CodeSeedNotHeld)
	ErrReplaceTargetHasData = sentinel(CodeReplaceTargetHasData)
	ErrMintFailed           = sentinel(CodeMintFailed)
	ErrCorruptKey           = sentinel(CodeCorruptKey)
	ErrSeedHeld             = sentinel(CodeSeedHeld)
	ErrClaimExpired         = sentinel(CodeClaimExpired)
	ErrClaimPending         = sentinel(CodeClaimPending)

	// ErrAuthFailed: the server refused the login, or there were no credentials.
	ErrAuthFailed = sentinel(CodeAuthFailed)
	// ErrUntrustedServer: no pinned fingerprint or recorded key vouches for the server.
	ErrUntrustedServer = sentinel(CodeUntrustedServer)
	// ErrServerKeyChanged: a recorded server now presents a different key.
	ErrServerKeyChanged = sentinel(CodeServerKeyChanged)
	// ErrFingerprintMismatch: the server's key is not the pinned fingerprint.
	ErrFingerprintMismatch = sentinel(CodeFingerprintMismatch)
	// ErrInvalidURI: the connection string or an option was refused.
	ErrInvalidURI = sentinel(CodeInvalidURI)
	// ErrClosed: the client was closed.
	ErrClosed = sentinel(CodeClosed)
	// ErrUnreachable: no server answered at any address given.
	ErrUnreachable = sentinel(CodeUnreachable)
	// ErrUnsupportedByServer: the server did not advertise what the call needs.
	ErrUnsupportedByServer = sentinel(CodeUnsupportedByServer)
	// ErrLeaseUnknown: Ack, Release or Extend named a lease nothing holds: it was
	// acknowledged, released or returned already, or the bucket never issued it.
	// The call changed nothing, and the result returned beside the error says so.
	ErrLeaseUnknown = sentinel(CodeLeaseUnknown)
	// ErrLeaseExpired: Ack, Release or Extend named a lease whose time ran out.
	// Its entries are back in the queue and will be delivered again.
	ErrLeaseExpired = sentinel(CodeLeaseExpired)
	// ErrClaimEntryTooLarge: the entry at the front of the queue is over the hard
	// cap a claim moves, so Claim took nothing. Pop it, which removes it at most
	// once, and claims work again. It is also an ErrInvalidArgument.
	ErrClaimEntryTooLarge = &Error{Code: CodeInvalidArgument, Message: "claim entry too large", claimTooLarge: true}
)

// wireError converts the server's error frame into an [Error]. The server's
// message is passed through verbatim because the fix is inside it.
func wireError(op, target string, response hoarddbwire.ErrorResponse) *Error {
	failure := &Error{
		Code: Code(response.Code), Op: op, Target: target, Message: response.Message,
		Owner: response.Detail.Owner, OwnerAddr: response.Detail.OwnerAddr,
		Outcome:        outcomeOf(response.WriteOutcome),
		MaxResultBytes: response.MaxResultBytes, RetainedBytes: response.RetainedBytes,
	}
	failure.Results = writeResultsOf(response.Results)
	if response.CopiesConfirmed != nil {
		failure.CopiesConfirmed = int(*response.CopiesConfirmed)
	}
	if response.ReplicationFactor != nil {
		failure.ReplicationFactor = int(*response.ReplicationFactor)
	}
	return failure
}

// WriteResult is the server's word on one document of a batch.
type WriteResult struct {
	// ID is the document's key as the server read it.
	ID string
	// Outcome is [OutcomeApplied] or [OutcomeNotApplied]; [OutcomeNotReported]
	// when the server did not say.
	Outcome WriteOutcome
	// Code is the refusal's class for a position the server refused; 0 for a
	// position it applied or only held back because another was refused.
	Code Code
	// Message is the server's sentence for the position, empty when it sent none.
	Message string
}

func writeResultsOf(results []hoarddbwire.WriteResult) []WriteResult {
	if len(results) == 0 {
		return nil
	}
	converted := make([]WriteResult, len(results))
	for position, result := range results {
		converted[position] = WriteResult{ID: result.ID, Outcome: outcomeOf(result.WriteOutcome), Code: Code(result.Code), Message: result.Message}
	}
	return converted
}

func outcomeOf(reported hoarddbwire.WriteOutcome) WriteOutcome {
	switch reported {
	case hoarddbwire.WriteApplied:
		return OutcomeApplied
	case hoarddbwire.WriteNotApplied:
		return OutcomeNotApplied
	case hoarddbwire.WriteUnknown:
		return OutcomeUnknown
	default:
		return OutcomeNotReported
	}
}

// isFinal reports whether an error means "do not try another host": a trust
// failure or a refused login says something about the server or the caller, not
// about one host being down.
func isFinal(err error) bool {
	for _, class := range []*Error{ErrUntrustedServer, ErrServerKeyChanged, ErrFingerprintMismatch, ErrInvalidURI} {
		if errors.Is(err, class) {
			return true
		}
	}
	var failure *Error
	// AUTH_FAIL INTERNAL means this node has no user configuration yet; another
	// node may answer. Every other refusal is about the credentials.
	return errors.As(err, &failure) && failure.Code == CodeAuthFailed && failure.Reason != AuthReasonInternal
}
