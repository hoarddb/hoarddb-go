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
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestRetryTableIsTheHandbooks holds retry.go's table to handbook/errors.md
// section 6, one case per row of that table, each asked for a read and for a
// write.
func TestRetryTableIsTheHandbooks(t *testing.T) {
	type answer struct {
		stage   sentStage
		code    Code
		outcome WriteOutcome
	}
	rows := []struct {
		row         string // the handbook's words for the answer
		answers     []answer
		read, write retryAction
	}{
		{"UNAVAILABLE on a read", []answer{{answered, CodeUnavailable, OutcomeNotReported}}, backOff, giveUp},
		{"UNAVAILABLE with write_outcome: not_applied", []answer{{answered, CodeUnavailable, OutcomeNotApplied}}, backOff, backOff},
		{"any answer with write_outcome: unknown", []answer{
			{answered, CodeUnavailable, OutcomeUnknown}, {answered, CodeInternal, OutcomeUnknown},
			{answered, CodeInvalidArgument, OutcomeUnknown}, {answered, CodeNotAuthenticated, OutcomeUnknown},
		}, giveUp, giveUp},
		{"TIMEOUT (write_outcome: applied)", []answer{{answered, CodeTimeout, OutcomeApplied}, {answered, CodeTimeout, OutcomeNotReported}}, giveUp, giveUp},
		{"OVERLOADED (\"that one dial\": nothing was sent)", []answer{{notSent, CodeOverloaded, OutcomeNotReported}}, redial, redial},
		// OVERLOADED is a connection-level refusal (handbook/wire.md section 8): the
		// server serves nothing on that connection, so any call backs off and goes
		// again, a write included ("yes, that one dial, with backoff").
		{"OVERLOADED after the request was sent", []answer{{answered, CodeOverloaded, OutcomeNotReported}}, backOff, backOff},
		{"NOT_AUTHENTICATED", []answer{{answered, CodeNotAuthenticated, OutcomeNotReported}}, reauthenticate, reauthenticate},
		{"RESULT_TOO_LARGE", []answer{{answered, CodeResultTooLarge, OutcomeNotReported}}, giveUp, giveUp},
		{"BATCH_TOO_LARGE, whether not_applied or unknown", []answer{
			{answered, CodeBatchTooLarge, OutcomeNotApplied}, {answered, CodeBatchTooLarge, OutcomeUnknown},
		}, giveUp, giveUp},
		{"NOT_FOUND, ALREADY_EXISTS, PERMISSION_DENIED, INVALID_ARGUMENT, DUPLICATE_KEY, FIELD_TYPE, CONFLICT, UNSUPPORTED_VERSION", []answer{
			{answered, CodeNotFound, OutcomeNotReported}, {answered, CodeAlreadyExists, OutcomeNotReported},
			{answered, CodePermissionDenied, OutcomeNotReported}, {answered, CodeInvalidArgument, OutcomeNotReported},
			{answered, CodeDuplicateKey, OutcomeNotReported}, {answered, CodeFieldType, OutcomeNotReported},
			{answered, CodeConflict, OutcomeNotReported}, {answered, CodeUnsupportedVersion, OutcomeNotReported},
			// "never: the same request fails the same way", even when nothing was
			// written (an unknown write-concern word is INVALID_ARGUMENT,
			// not_applied).
			{answered, CodeInvalidArgument, OutcomeNotApplied}, {answered, CodeDuplicateKey, OutcomeNotApplied},
			{answered, CodeFieldType, OutcomeNotApplied},
		}, giveUp, giveUp},
		{"INTERNAL, UNKNOWN(n)", []answer{
			{answered, CodeInternal, OutcomeNotReported}, {answered, Code(0x63), OutcomeNotReported}, {answered, 0, OutcomeNotReported},
			{answered, CodeQuotaExceeded, OutcomeNotReported},
		}, giveUp, giveUp},
		{"the connection failed before a complete response arrived", []answer{{inFlight, CodeUnavailable, OutcomeNotReported}}, resendOnce, giveUp},
		{"the caller's deadline or cancellation ended the call", []answer{
			{inFlight, CodeInterrupted, OutcomeNotReported}, {notSent, CodeInterrupted, OutcomeNotReported},
			{inFlight, CodeClosed, OutcomeNotReported}, {notSent, CodeClosed, OutcomeNotReported},
		}, giveUp, giveUp},
		{"an AUTH_FAIL", []answer{{notSent, CodeAuthFailed, OutcomeNotReported}, {answered, CodeAuthFailed, OutcomeNotReported}}, giveUp, giveUp},
		{"a dial that did not reach the server, or a trust failure", []answer{
			{notSent, CodeUnreachable, OutcomeNotReported}, {notSent, CodeUntrustedServer, OutcomeNotReported},
			{notSent, CodeServerKeyChanged, OutcomeNotReported}, {notSent, CodeFingerprintMismatch, OutcomeNotReported},
		}, giveUp, giveUp},
	}
	for _, row := range rows {
		for _, given := range row.answers {
			failure := &Error{Code: given.code, Outcome: given.outcome}
			if got := retryRule(given.stage, failure, true); got != row.read {
				t.Errorf("%s: %s (stage %d, outcome %s) on a read: %s, want %s", row.row, given.code, given.stage, given.outcome, got, row.read)
			}
			if got := retryRule(given.stage, failure, false); got != row.write {
				t.Errorf("%s: %s (stage %d, outcome %s) on a write: %s, want %s", row.row, given.code, given.stage, given.outcome, got, row.write)
			}
		}
	}
}

// The reads handbook/api-go.md section 7 names are the ones the driver may send
// twice, and no write is among them.
func TestOnlyReadsAreIdempotent(t *testing.T) {
	reads := map[string]hoarddbwire.Opcode{
		"Get": hoarddbwire.OpGet, "Get with Consistent": hoarddbwire.OpConsistentGet, "Exists": hoarddbwire.OpExists,
		"Find pages": hoarddbwire.OpScan, "Search": hoarddbwire.OpSearch, "Count": hoarddbwire.OpCount,
		"Distinct": hoarddbwire.OpDistinct, "Range": hoarddbwire.OpBTreeRange, "First": hoarddbwire.OpBTreeFirst,
		"Last": hoarddbwire.OpBTreeLast, "Peek": hoarddbwire.OpFIFOPeek, "Stats": hoarddbwire.OpBucketStats, "Info": hoarddbwire.OpBucketInfo,
		"GetBlob": hoarddbwire.OpBlobRead, "ListDatabases": hoarddbwire.OpListDBs, "ListBuckets": hoarddbwire.OpListBuckets,
		"Health": hoarddbwire.OpHealthCheck, "ListNodes": hoarddbwire.OpListNodes, "Ping": hoarddbwire.OpPing,
		"FieldStore.Peek": hoarddbwire.OpFieldPeek, "FieldStore.Get": hoarddbwire.OpFieldGet,
		"FieldStore.Len": hoarddbwire.OpFieldLen, "FieldStore.List": hoarddbwire.OpFieldList,
		"DeadList": hoarddbwire.OpDeadList,
	}
	writes := map[string]hoarddbwire.Opcode{
		"Put and Insert": hoarddbwire.OpPut, "InsertMany": hoarddbwire.OpBatchPut, "Push": hoarddbwire.OpFIFOPush,
		"Pop": hoarddbwire.OpFIFOPop, "Update": hoarddbwire.OpUpdate, "Delete": hoarddbwire.OpDelete,
		"PutBlob": hoarddbwire.OpBlobWrite, "FieldStore.Push": hoarddbwire.OpFieldPush, "FieldStore.Pop": hoarddbwire.OpFieldPop,
		"FieldStore.Put": hoarddbwire.OpFieldPut, "FieldStore.Delete": hoarddbwire.OpFieldDelete,
		"CreateDatabase": hoarddbwire.OpCreateDB, "CreateBucket": hoarddbwire.OpCreateBucket, "DropDatabase": hoarddbwire.OpDeleteDB,
		"DropBucket": hoarddbwire.OpDeleteBucket, "DeclareStoreFields": hoarddbwire.OpDeclareStoreFields,
		"a login": hoarddbwire.OpAuth,
		"Claim":   hoarddbwire.OpQueueClaim, "Ack": hoarddbwire.OpQueueAck, "Release": hoarddbwire.OpQueueRelease,
		"Extend": hoarddbwire.OpQueueExtend, "Requeue": hoarddbwire.OpDeadRequeue, "Discard": hoarddbwire.OpDeadDiscard,
	}
	for verb, opcode := range reads {
		if !isRead(opcode) {
			t.Errorf("%s (%s) is a read and may be sent twice", verb, opcode)
		}
	}
	for verb, opcode := range writes {
		if isRead(opcode) {
			t.Errorf("%s (%s) changes the server and must never be resent on an unknown outcome", verb, opcode)
		}
	}
	if len(idempotentReads) != len(reads) {
		t.Errorf("the table holds %d reads, the handbook's list %d: every entry needs its verb here", len(idempotentReads), len(reads))
	}
}

// Backoff is 100 ms doubling to 5 s, with full jitter.
func TestBackoffDoublesToFiveSecondsWithFullJitter(t *testing.T) {
	limits := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
		1600 * time.Millisecond, 3200 * time.Millisecond, 5 * time.Second, 5 * time.Second, 5 * time.Second}
	for index, limit := range limits {
		failures := index + 1
		var longest time.Duration
		for range 2000 {
			delay := backoffDelay(failures)
			if delay < 0 || delay >= limit {
				t.Fatalf("after %d failures: %v, want within [0, %v)", failures, delay, limit)
			}
			longest = max(longest, delay)
		}
		if longest < limit/2 {
			t.Errorf("after %d failures the longest of 2000 delays was %v: the jitter must span [0, %v)", failures, longest, limit)
		}
	}
}

// Every data-write opcode of handbook/errors.md section 4 is named here
// individually, so a typo that drops one from dataWrites is caught: the public
// verbs send only some of them (every queue is FIFO_*), and the rest are
// reachable only through the table.
func TestEveryDataWriteOpcodeIsMarkedUnknownWhenUnstamped(t *testing.T) {
	data := map[string]hoarddbwire.Opcode{
		"OpPut": hoarddbwire.OpPut, "OpBatchPut": hoarddbwire.OpBatchPut, "OpBTreeInsert": hoarddbwire.OpBTreeInsert,
		"OpUpsert": hoarddbwire.OpUpsert, "OpBatchUpsert": hoarddbwire.OpBatchUpsert,
		"OpFIFOPush": hoarddbwire.OpFIFOPush, "OpFIFOPop": hoarddbwire.OpFIFOPop,
		"OpLIFOPush": hoarddbwire.OpLIFOPush, "OpLIFOPop": hoarddbwire.OpLIFOPop,
		"OpHeapPush": hoarddbwire.OpHeapPush, "OpHeapPop": hoarddbwire.OpHeapPop,
		"OpDelete": hoarddbwire.OpDelete, "OpUpdate": hoarddbwire.OpUpdate, "OpBlobWrite": hoarddbwire.OpBlobWrite,
		"OpFieldPush": hoarddbwire.OpFieldPush, "OpFieldPop": hoarddbwire.OpFieldPop,
		"OpFieldPut": hoarddbwire.OpFieldPut, "OpFieldDelete": hoarddbwire.OpFieldDelete,
		"OpQueueClaim": hoarddbwire.OpQueueClaim, "OpQueueAck": hoarddbwire.OpQueueAck,
		"OpQueueRelease": hoarddbwire.OpQueueRelease, "OpQueueExtend": hoarddbwire.OpQueueExtend,
		"OpDeadRequeue": hoarddbwire.OpDeadRequeue, "OpDeadDiscard": hoarddbwire.OpDeadDiscard,
	}
	for name, opcode := range data {
		unstamped := unstampedWriteIsUnknown(call{opcode: opcode}, &Error{Code: CodeInternal, Message: "refused"})
		if unstamped.Outcome != OutcomeUnknown || !strings.Contains(unstamped.Message, outcomeUnknownAdvice) {
			t.Errorf("%s unstamped = %+v; want unknown with the data-write advice", name, unstamped)
		}
		stamped := unstampedWriteIsUnknown(call{opcode: opcode}, &Error{Code: CodeInternal, Message: "refused", Outcome: OutcomeNotApplied})
		if stamped.Outcome != OutcomeNotApplied || stamped.Message != "refused" {
			t.Errorf("%s stamped not_applied = %+v; want it untouched", name, stamped)
		}
	}
	if len(dataWrites) != len(data) {
		t.Errorf("dataWrites holds %d opcodes, this list %d: name every one", len(dataWrites), len(data))
	}
	for name, opcode := range map[string]hoarddbwire.Opcode{
		"CreateDB": hoarddbwire.OpCreateDB, "CreateBucket": hoarddbwire.OpCreateBucket, "DeleteDB": hoarddbwire.OpDeleteDB,
		"DeleteBucket": hoarddbwire.OpDeleteBucket, "DeclareStoreFields": hoarddbwire.OpDeclareStoreFields,
		"Get": hoarddbwire.OpGet, "Auth": hoarddbwire.OpAuth,
	} {
		if got := unstampedWriteIsUnknown(call{opcode: opcode}, &Error{Code: CodeInternal, Message: "refused"}); got.Outcome != OutcomeNotReported || got.Message != "refused" {
			t.Errorf("%s unstamped = %+v; a DDL request, a read and a login are left as the server sent them", name, got)
		}
	}
	if adviceFor(call{opcode: hoarddbwire.OpLIFOPush}) != outcomeUnknownAdvice || adviceFor(call{opcode: hoarddbwire.OpCreateBucket}) != ddlOutcomeUnknownAdvice {
		t.Error("adviceFor must give a data write its advice and a DDL request its own")
	}
}
