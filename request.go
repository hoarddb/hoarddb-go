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

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
	"github.com/hoarddb/hoarddb-go/internal/pool"
)

// lane is which pool a call is checked out of. The point lane carries calls a
// user waits on; the bulk lane carries the ones that can run long, so a scan
// cannot sit in front of a point read (handbook/wire.md section 7).
type lane uint8

const (
	lanePoint lane = iota
	laneBulk
)

// call is one request: what to send, which pool to send it through, and where
// the answer goes.
type call struct {
	// op names the operation in errors ("create bucket"); target is what it was
	// aimed at ("app/users").
	op, target string
	// opcode is the request's opcode and answer the opcode a success carries:
	// the request's own for most operations, GET_RESPONSE for HEALTH_CHECK and
	// LIST_NODES (handbook/wire.md section 4).
	opcode, answer hoarddbwire.Opcode
	lane           lane
	// request is encoded as the payload, nil for none; response receives the
	// decoded answer, nil to discard it.
	request, response any
	// oversizeAdvice replaces the usual remedy of a request over the frame
	// limit, for a verb to which "store it as a blob" is no help.
	oversizeAdvice string
	// popLimit is, for a pop, how many entries it may have removed (its limit,
	// 1 when it sent none); 0 for every other call. It words the advice of a pop
	// whose outcome is not known.
	popLimit int64
}

// readOnly reports whether the request may be sent twice.
func (request call) readOnly() bool {
	return isRead(request.opcode)
}

// dataWrite reports whether the request is a data mutation.
func (request call) dataWrite() bool {
	return dataWrites[request.opcode]
}

func (c *Client) poolFor(which lane) *pool.Pool[*hoarddbwire.Conn] {
	if which == laneBulk {
		return c.bulk
	}
	return c.point
}

// do sends one request and decodes its answer. It applies the request timeout
// when the caller set no deadline, refuses a request no frame can carry, and
// hands the rest to send, which retries by the table in retry.go.
func (c *Client) do(ctx context.Context, request call) error {
	ctx, cancel := c.requestContext(ctx)
	defer cancel()

	var payload []byte
	if request.request != nil {
		encoded, err := codec.Marshal(request.request)
		if err != nil {
			return &Error{Code: CodeInvalidArgument, Op: request.op, Target: request.target, Err: err,
				Message: fmt.Sprintf("the request could not be encoded (%v); nothing was sent", err)}
		}
		payload = encoded
	}
	if len(payload) > hoarddbwire.MaxPayload {
		advice := request.oversizeAdvice
		if advice == "" {
			advice = "Split a batch into smaller ones, or store a large value as a blob"
		}
		return &Error{Code: CodeInvalidArgument, Op: request.op, Target: request.target, Message: fmt.Sprintf(
			"the request is %d bytes and one frame carries at most %d (1 byte of opcode + %d of request ID + payload must fit in %d). "+
				"Nothing was sent. %s",
			len(payload), hoarddbwire.MaxPayload, hoarddbwire.RequestIDLen, hoarddbwire.MaxFrameLength, advice)}
	}
	return c.send(ctx, request, payload)
}

// sentStage is how far one attempt got before it failed, which decides what
// may be done about it.
type sentStage uint8

const (
	// notSent: no byte of the request was written (checkout, dial, token
	// refresh). Nothing can have happened on the server.
	notSent sentStage = iota
	// inFlight: the request may have reached the server, and no complete answer
	// came back. A write's outcome is unknown.
	inFlight
	// answered: the server answered with an error frame.
	answered
)

// attempt sends the request once on a connection from the call's lane and
// reads its answer, reporting how far it got when it fails.
//
// A connection goes back to its pool only when it is known to be in step with
// the server: after a success, and after an error frame that is documented to
// leave the connection open. After a timeout, a cancellation, a transport
// failure or an answer that is not the one asked for it is closed, because a
// late answer would otherwise be taken for the next request's.
//
// NOT_AUTHENTICATED is the one answer handled here, because its remedy needs
// the connection it came on (handbook/errors.md section 6): the connection is
// logged in again, once per call, and a read is sent again on it.
func (c *Client) attempt(ctx context.Context, request call, payload []byte, budget *retryBudget) (sentStage, error) {
	lease, err := c.poolFor(request.lane).Acquire(ctx)
	if err != nil {
		return notSent, c.notSentError(request, err)
	}
	conn := lease.Conn()
	if c.session.NeedsRefresh() {
		if err := c.session.Refresh(ctx, conn); err != nil {
			lease.Discard()
			return notSent, c.notSentError(request, err)
		}
	}
	for {
		reply, err := conn.RoundTrip(ctx, hoarddbwire.Frame{Op: request.opcode, Payload: payload})
		if err != nil {
			lease.Discard()
			var refused *hoarddbwire.RefusedError
			if errors.As(err, &refused) {
				// A refusal with a zero request ID is read as the answer to what
				// was in flight (handbook/wire.md section 2). It reports no write
				// outcome, and the request may already have been written.
				return answered, answeredFailure(request, wireError(request.op, request.target, refused.Response))
			}
			return inFlight, c.lostError(ctx, request, err)
		}
		switch reply.Op {
		case hoarddbwire.OpError:
			failure := wireError(request.op, request.target, hoarddbwire.DecodeError(reply.Payload))
			if failure.Code == CodeOverloaded && !request.readOnly() {
				// OVERLOADED with the request's own ID is not the connection
				// refusal of handbook/wire.md section 8 (that has an all-zero ID
				// and is read as an error of the connection, above): it answers
				// this request, which may have been applied, so a write is never
				// sent again. A stamp from the server is left as it is.
				failure = unreportedIsUnknown(request, failure)
			}
			if retryRule(answered, failure, request.readOnly()) != reauthenticate || budget.reauthenticated {
				if keepsConnection(failure.Code) {
					lease.Release()
				} else {
					lease.Discard()
				}
				return answered, answeredFailure(request, failure)
			}
			budget.reauthenticated = true
			if err := c.reauthenticate(ctx, conn); err != nil {
				lease.Discard()
				return answered, err
			}
			if !request.readOnly() {
				lease.Release()
				return answered, answeredFailure(request, failure)
			}
			c.retries.Add(1)
		case request.answer:
			if request.response != nil {
				if err := codec.Unmarshal(reply.Payload, request.response); err != nil {
					lease.Release() // the frame was whole, so the connection is still in step
					return answered, &Error{Code: CodeInternal, Op: request.op, Target: request.target, Err: err, Message: fmt.Sprintf(
						"the server's %s answer could not be read (%v); this driver and the server disagree about its shape", reply.Op, err)}
				}
			}
			lease.Release()
			return answered, nil
		default:
			lease.Discard()
			return answered, &Error{Code: CodeInternal, Op: request.op, Target: request.target, Message: fmt.Sprintf(
				"the server answered %s with %s instead of %s; is this a hoardDB driver port?", request.opcode, reply.Op, request.answer)}
		}
	}
}

// reauthenticate logs conn in again after the server answered
// NOT_AUTHENTICATED on it. A refusal is reported as a login refusal is at
// connect, with the reason and what to do.
func (c *Client) reauthenticate(ctx context.Context, conn *hoarddbwire.Conn) error {
	greeting, err := c.session.Reauthenticate(ctx, conn)
	if err != nil {
		return c.dialer.classify(conn.RemoteAddr().String(), err, false)
	}
	c.serverMu.Lock()
	c.server = ServerInfo{Version: greeting.ServerVersion, ProtocolVersion: greeting.ProtocolVersion, Capabilities: greeting.Capabilities}
	c.serverMu.Unlock()
	return nil
}

// notSentError is the error of an attempt that failed before any byte of the
// request was written: a write's outcome is known, it was not applied.
func (c *Client) notSentError(request call, err error) error {
	failure := c.callError(request.op, request.target, err)
	var typed *Error
	if errors.As(failure, &typed) && typed.Code == CodeInterrupted && !request.readOnly() {
		typed.Outcome = OutcomeNotApplied
		typed.Message += " Nothing was sent, so the write was not applied."
	}
	return failure
}

// lostError is the error of an attempt whose request may have reached the
// server and whose answer did not arrive: the caller's context ended, the
// client was closed, or the connection was lost. For a write the outcome is
// unknown, and the error says so, because the driver will not resend it.
func (c *Client) lostError(ctx context.Context, request call, err error) error {
	write := !request.readOnly()
	var failure *Error
	switch {
	case ctx.Err() != nil:
		failure = interrupted(request.op, request.target, ctx.Err())
	case errors.Is(err, hoarddbwire.ErrConnClosed):
		failure = &Error{Code: CodeClosed, Op: request.op, Target: request.target, Err: err,
			Message: "the client was closed while the request was in flight; create a new one with hoarddb.Connect."}
	default:
		failure = &Error{Code: CodeUnavailable, Op: request.op, Target: request.target, Err: err, Message: fmt.Sprintf(
			"the connection to the server was lost before its answer arrived (%v). Check that the server is still running; the next call dials a new connection.", err)}
	}
	if write {
		failure.Outcome = OutcomeUnknown
		failure.Message += " " + adviceFor(request)
	}
	return failure
}

// adviceFor is the sentence that ends the error of a request whose outcome is
// unknown: a data write's says never to resend a push or an increment, a DDL
// request's says to read the state back, and a pop's says how many entries it
// may have removed.
func adviceFor(request call) string {
	if request.popLimit > 0 {
		return fmt.Sprintf("outcome unknown: the pop may have removed up to %d entries; run peek to see what is left", request.popLimit)
	}
	if request.dataWrite() {
		return outcomeUnknownAdvice
	}
	return ddlOutcomeUnknownAdvice
}

// unreportedIsUnknown marks a refused write whose answer reports no outcome as
// OutcomeUnknown, with the advice (handbook/errors.md section 4: "absent: treat
// as unknown for a write"). Used where the answer cannot vouch for the write:
// a zero-ID refusal, and OVERLOADED after the request was sent.
func unreportedIsUnknown(request call, failure *Error) *Error {
	if request.readOnly() || failure.Outcome != OutcomeNotReported {
		return failure
	}
	failure.Outcome = OutcomeUnknown
	failure.Message += " " + adviceFor(request)
	return failure
}

// answeredFailure is the error of a request the server answered with a refusal.
// OVERLOADED is left as it came: it is a connection-level refusal after which
// the retry table backs off and sends the request again (handbook/errors.md
// section 6), and send marks a data write unknown only if the backing off ends
// without an answer. Every other refusal of a data write that carries no stamp
// is unknown.
func answeredFailure(request call, failure *Error) *Error {
	if failure.Code == CodeOverloaded {
		return failure
	}
	if request.popLimit > 0 && failure.Outcome == OutcomeApplied {
		// The error frame carries no entries, and the pop was applied: what it
		// removed is gone from the store and not in the answer.
		failure.Message += fmt.Sprintf(" The pop was applied or refused and its answer carried no entries: it may have removed up to %d entries; run peek to see what is left", request.popLimit)
		return failure
	}
	return unstampedWriteIsUnknown(request, failure)
}

// unstampedWriteIsUnknown applies handbook/errors.md section 4 to an ordinary
// ERROR frame: a refused data write whose answer carries no write_outcome (an
// older server, or a single node whose own store failed the write) is unknown,
// with the advice after the server's sentence. A stamped outcome is left
// untouched, and so is any answer to a read, a DDL request or a login. It runs
// after the retry rule has decided, so a NOT_AUTHENTICATED write is still logged
// in again first.
func unstampedWriteIsUnknown(request call, failure *Error) *Error {
	if !request.dataWrite() {
		return failure
	}
	return unreportedIsUnknown(request, failure)
}

// ddlOutcomeUnknownAdvice ends the error of a DDL request (create or drop a
// database or bucket, declare store fields) whose connection was lost before
// its answer: it may have been applied, and the driver has not sent it again.
const ddlOutcomeUnknownAdvice = "Whether the request was applied is unknown, and the driver has not resent it: " +
	"read it back (list the databases or buckets, or inspect the one it names) before sending it again."

// outcomeUnknownAdvice ends every error of a data write whose outcome the driver
// cannot know (handbook/errors.md sections 4 and 7).
const outcomeUnknownAdvice = "Whether the write was applied is unknown, and the driver has not resent it: " +
	"read the data back before sending it again, and never resend a push or an increment without checking, because it would apply twice."

// interrupted is the error of a call whose context ended before its answer.
func interrupted(op, target string, cause error) *Error {
	message := "the call was cancelled before the server's answer arrived."
	if errors.Is(cause, context.DeadlineExceeded) {
		message = "the call's deadline passed before the server's answer arrived. " +
			"Give the call a longer deadline, or raise WithRequestTimeout for calls that set none."
	}
	return &Error{Code: CodeInterrupted, Op: op, Target: target, Err: cause, Message: message}
}

// keepsConnection reports whether the server leaves a connection open after an
// error frame with this code. Only answers verified or documented to are
// listed (handbook/errors.md section 6); the safe default is to close, which
// costs one dial.
func keepsConnection(code Code) bool {
	switch code {
	case CodeNotFound, CodeAlreadyExists, CodeResultTooLarge, CodeBatchTooLarge, CodeFieldType:
		return true
	default:
		return false
	}
}

// refusal is the error for an in-band refusal: a successful frame that says
// success false. The server's sentence is passed through verbatim. A message
// the vectors pin to another class is classed here; none of the database and
// bucket operations has one, so every refusal is an invalid argument.
func refusal(op, target, message string) *Error {
	return &Error{Code: CodeInvalidArgument, Op: op, Target: target, Message: message}
}
