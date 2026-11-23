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
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// The retry policy of handbook/errors.md section 6, in one place (D-10). It
// is driven by the answer's code and write outcome and by whether the call is
// a read, never by the text of a message. A retry is invisible to the caller:
// the call returns the answer of its last attempt.

// idempotentReads are the opcodes the driver may send twice: reads, which
// change nothing on the server. Everything else, every write and every DDL
// request, is sent again only when the server said nothing was applied. A pop
// consumes, so it is a write (D-2).
var idempotentReads = map[hoarddbwire.Opcode]bool{
	hoarddbwire.OpGet:           true,
	hoarddbwire.OpExists:        true,
	hoarddbwire.OpConsistentGet: true,
	hoarddbwire.OpScan:          true,
	hoarddbwire.OpCount:         true,
	hoarddbwire.OpDistinct:      true,
	hoarddbwire.OpSearch:        true,
	hoarddbwire.OpBTreeRange:    true,
	hoarddbwire.OpBTreeFirst:    true,
	hoarddbwire.OpBTreeLast:     true,
	hoarddbwire.OpFIFOPeek:      true,
	hoarddbwire.OpBlobRead:      true,
	hoarddbwire.OpBucketStats:   true,
	hoarddbwire.OpBucketInfo:    true,
	hoarddbwire.OpListDBs:       true,
	hoarddbwire.OpListBuckets:   true,
	hoarddbwire.OpHealthCheck:   true,
	hoarddbwire.OpListNodes:     true,
	hoarddbwire.OpPing:          true,
	hoarddbwire.OpFieldPeek:     true,
	hoarddbwire.OpFieldGet:      true,
	hoarddbwire.OpFieldLen:      true,
	hoarddbwire.OpFieldList:     true,
	hoarddbwire.OpDeadList:      true,
}

// dataWrites are the opcodes of the data mutations: the verbs whose refusal
// says whether the write happened (handbook/errors.md section 4). A DDL request
// and a login are not among them.
var dataWrites = map[hoarddbwire.Opcode]bool{
	hoarddbwire.OpPut: true, hoarddbwire.OpBatchPut: true, hoarddbwire.OpBTreeInsert: true,
	hoarddbwire.OpUpsert: true, hoarddbwire.OpBatchUpsert: true,
	hoarddbwire.OpFIFOPush: true, hoarddbwire.OpFIFOPop: true, hoarddbwire.OpLIFOPush: true, hoarddbwire.OpLIFOPop: true,
	hoarddbwire.OpHeapPush: true, hoarddbwire.OpHeapPop: true,
	hoarddbwire.OpDelete: true, hoarddbwire.OpUpdate: true, hoarddbwire.OpBlobWrite: true,
	hoarddbwire.OpFieldPush: true, hoarddbwire.OpFieldPop: true, hoarddbwire.OpFieldPut: true, hoarddbwire.OpFieldDelete: true,
	hoarddbwire.OpQueueClaim: true, hoarddbwire.OpQueueAck: true, hoarddbwire.OpQueueRelease: true, hoarddbwire.OpQueueExtend: true,
	hoarddbwire.OpDeadRequeue: true, hoarddbwire.OpDeadDiscard: true,
}

// isRead reports whether a request with this opcode may be sent twice.
func isRead(opcode hoarddbwire.Opcode) bool { return idempotentReads[opcode] }

// Backoff between attempts the server said may succeed later: 100 ms doubling
// to 5 s, with full jitter, bounded by the caller's deadline
// (handbook/errors.md section 5).
const (
	retryBackoffBase = 100 * time.Millisecond
	retryBackoffMax  = 5 * time.Second
)

// retryAction is what the driver does after an attempt failed.
type retryAction uint8

const (
	// giveUp returns the failure to the caller.
	giveUp retryAction = iota
	// backOff waits and sends the request again, for as long as the caller's
	// deadline allows.
	backOff
	// resendOnce sends a read once more, on a fresh connection.
	resendOnce
	// reauthenticate logs the connection in again, once, and sends a read once
	// more on it; a write is not resent.
	reauthenticate
	// redial waits and dials again: the server refused the connection for load,
	// before anything was sent, and the connections already open are fine.
	redial
)

func (action retryAction) String() string {
	return [...]string{"give up", "back off and resend", "resend once on a fresh connection", "re-authenticate once", "redial with backoff"}[action]
}

// retryRule is the table. stage is how far the failed attempt got, failure
// what it returned, and read whether the request may be sent twice.
func retryRule(stage sentStage, failure *Error, read bool) retryAction {
	switch stage {
	case notSent:
		// Only a refusal for load is worth another dial; a trust failure, a
		// refused login or an unreachable host would fail the same way.
		if failure.Code == CodeOverloaded {
			return redial
		}
		return giveUp
	case inFlight:
		// The connection failed before a complete answer: a read once, on a
		// fresh connection; a write never, its outcome is unknown.
		if read && failure.Code == CodeUnavailable {
			return resendOnce
		}
		return giveUp
	}
	if failure.Outcome == OutcomeUnknown || failure.Outcome == OutcomeApplied {
		return giveUp // the caller decides, after reading back; never resend what is stored
	}
	switch failure.Code {
	case CodeUnavailable:
		if read || failure.Outcome == OutcomeNotApplied {
			return backOff
		}
		return giveUp // a write the server did not say was not applied: treat as unknown
	case CodeOverloaded:
		// A connection-level refusal (handbook/wire.md section 8): the server
		// serves nothing on that connection, so whatever was sent was not
		// applied, and every kind of call backs off and goes again
		// (handbook/errors.md section 6: "that one dial, with backoff").
		return backOff
	case CodeNotAuthenticated:
		return reauthenticate
	default:
		// TIMEOUT (applied), RESULT_TOO_LARGE (not as sent: the cursor pages
		// smaller), BATCH_TOO_LARGE (the caller halves it; the driver never
		// resends it), the refusals a resend repeats, INTERNAL and unknown codes.
		return giveUp
	}
}

// retryBudget counts the retries that are allowed once per call.
type retryBudget struct {
	resent          bool
	reauthenticated bool
}

// send runs attempts until one succeeds or the table says to stop.
func (c *Client) send(ctx context.Context, request call, payload []byte) error {
	var (
		budget   retryBudget
		failures int
		// waiting is the last answer the driver is backing off from: the error
		// returned when the caller's deadline ends the waiting.
		waiting *Error
		// waitingStage is how far the attempt behind waiting got: an answered one
		// meant the request was sent.
		waitingStage sentStage
		// lost is a read's first connection failure, kept for the error when
		// its one resend fails too.
		lost *Error
	)
	for {
		stage, err := c.attempt(ctx, request, payload, &budget)
		if err == nil {
			return nil
		}
		var failure *Error
		if !errors.As(err, &failure) {
			return err
		}
		if waiting != nil && stage == notSent && failure.Code == CodeInterrupted {
			return gaveUpWaitingOn(request, waiting, waitingStage, ctx.Err())
		}
		if lost != nil && stage != answered {
			return lostTwice(lost, failure)
		}
		lost = nil // a resend the server answered is that answer, under the table like any other
		switch retryRule(stage, failure, request.readOnly()) {
		case resendOnce:
			if budget.resent {
				return failure
			}
			budget.resent, lost = true, failure
		case backOff, redial:
			waiting, waitingStage = failure, stage
			failures++
			if err := sleepContext(ctx, backoffDelay(failures)); err != nil {
				return gaveUpWaitingOn(request, waiting, waitingStage, err)
			}
		default:
			return failure
		}
		c.retries.Add(1)
	}
}

// backoffDelay is the wait before the next attempt after failures consecutive
// failures: a uniformly random duration below a limit that starts at 100 ms
// and doubles up to 5 s.
func backoffDelay(failures int) time.Duration {
	limit := retryBackoffBase
	for step := 1; step < failures && limit < retryBackoffMax; step++ {
		limit *= 2
	}
	return rand.N(min(limit, retryBackoffMax))
}

// gaveUpWaiting is the answer the driver was backing off from, once the
// caller's deadline or cancellation has ended the waiting. The server's
// sentence is kept verbatim, because it says what is happening and what to do;
// the context error is its cause.
func gaveUpWaiting(waiting *Error, cause error) *Error {
	final := *waiting
	final.Err = cause
	return &final
}

// gaveUpWaitingOn is gaveUpWaiting for a call that was sent: a data write that
// was answered OVERLOADED and never got through, with no stamp from the server,
// ends unknown (handbook/errors.md section 4: absent means treat as unknown),
// where a stamped refusal such as UNAVAILABLE with not_applied is untouched.
func gaveUpWaitingOn(request call, waiting *Error, stage sentStage, cause error) *Error {
	final := gaveUpWaiting(waiting, cause)
	if stage == answered {
		return unstampedWriteIsUnknown(request, final)
	}
	return final
}

// lostTwice is a read whose connection failed, and whose one resend on a fresh
// connection did not get an answer either. The first failure says what
// happened to the request, so it is kept, and the second is added.
func lostTwice(first, second *Error) *Error {
	final := *first
	final.Message = fmt.Sprintf("%s The read was sent once more on a fresh connection, and that failed too: %s", first.Message, second.Message)
	final.Err = errors.Join(first.Err, second)
	return &final
}

// sleepContext waits for wait, or until ctx ends, whichever comes first.
func sleepContext(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
