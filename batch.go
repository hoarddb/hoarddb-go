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
	"strings"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// BatchReceipt is the answer to [Bucket.InsertMany].
type BatchReceipt struct {
	// IDs are the keys of the stored documents, in the order they were given.
	IDs []string
	// Count is how many documents were stored.
	Count int
	// CopiesConfirmed and ReplicationFactor are how many copies hold the batch
	// and how many the bucket keeps, when the server said; 0 is "not reported".
	CopiesConfirmed, ReplicationFactor int
	// Results is one entry per document in the order given, when the server
	// reports them; nil from an older server. A position the server names in a
	// sentence counts from the first document of the part that answered.
	Results []WriteResult
}

// batchNoneWritten is the phrase an older server put in an in-band refusal of
// a batch over its atomic bound; the current server sends an ERROR frame with
// [CodeBatchTooLarge] instead. The phrase is kept for an older server, which
// has no code to split on, and is the server's promise that nothing was stored.
// This path belongs to that older server generation only (the current server
// refuses with an ERROR frame), and can be deleted with the next sweep.
const batchNoneWritten = "none of it was written"

// InsertMany stores every document of docs, each under a key the server
// assigns, and returns the keys in the order the documents were given. Each is
// a struct with `bson:"..."` tags, a [Map] or a [Document].
//
// The server commits a batch atomically up to a size that depends on the
// node's memory (its health answer reports it as memtable.batch_bytes). A
// batch over it is refused with nothing written, and InsertMany then halves it
// and sends the halves, down to one document, whose refusal is returned. So a
// batch the server takes is one request, and a larger one is stored in as few
// atomic parts as the node allows: it is atomic per part, not as a whole.
//
// When a later part fails after an earlier one was stored, the error says how
// many documents were stored and the receipt that comes with it lists their
// keys; nothing is rolled back. When a part fails in flight (the connection is
// lost, or the caller's deadline passes, before its answer), whether it was
// stored is unknown: the error is [OutcomeUnknown], names the documents the
// part held, and the part is neither in the receipt nor resent. A document the
// server refuses (over its size limit, or of the wrong type for a typed
// bucket) is refused with the server's sentence. Nothing is sent for an empty slice, a value that is not a document,
// or a batch that cannot fit one 32 MiB frame. It travels on the bulk lane and
// is never resent after a lost connection.
func (b *Bucket) InsertMany(ctx context.Context, docs []any, opts ...WriteOption) (BatchReceipt, error) {
	return b.writeMany(ctx, batchVerb{op: "insert many", name: "InsertMany", opcode: hoarddbwire.OpBatchPut, answer: hoarddbwire.OpBatchPutOK}, docs, opts)
}

// UpsertMany stores every document of docs under its _id_ and replaces the
// document already stored under it: [Bucket.InsertMany] refuses a held _id_ and
// UpsertMany overwrites it, as [Bucket.Upsert] does for one key. Each document
// carries its _id_ as a member named "_id_"; a document repeating an _id_ of
// the same call keeps the last one. A replaced document keeps its store fields.
//
// It is sent as one atomic batch up to the node's bound and halved above it,
// exactly as InsertMany is, and the receipt, the per-document results and the
// errors read the same. A bucket that has no keys to replace (fifo, lifo, heap)
// or no documents (blob) is refused with [ErrInvalidArgument] and the server's
// sentence names the verb to use. Like every write it is never resent after a
// lost connection. Resending after a lost answer stores the same documents
// again, which is safe only when nothing else writes those keys in between:
// the server cannot tell a resend from a new write.
func (b *Bucket) UpsertMany(ctx context.Context, docs []any, opts ...WriteOption) (BatchReceipt, error) {
	return b.writeMany(ctx, batchVerb{op: "upsert many", name: "UpsertMany", opcode: hoarddbwire.OpBatchUpsert, answer: hoarddbwire.OpBatchUpsertOK}, docs, opts)
}

// batchVerb is what differs between the two batch writes: the operation an
// error names, the method it is called by, and the wire opcodes.
type batchVerb struct {
	op, name       string
	opcode, answer hoarddbwire.Opcode
}

func (b *Bucket) writeMany(ctx context.Context, verb batchVerb, docs []any, opts []WriteOption) (BatchReceipt, error) {
	op := verb.op
	target := b.path()
	config := applyWriteOptions(opts)
	if err := config.refusePriority(op, target); err != nil {
		return BatchReceipt{}, err
	}
	concern, err := b.db.client.writeConcern(op, target, config)
	if err != nil {
		return BatchReceipt{}, err
	}
	if len(docs) == 0 {
		return BatchReceipt{}, refusal(op, target, verb.name+" was given no documents. Nothing was sent. Pass at least one, or skip the call")
	}
	encoded := make([]hoarddbwire.Document, len(docs))
	for position, doc := range docs {
		raw, err := encodeDocument(op, target, doc)
		if err != nil {
			var failure *Error
			if errors.As(err, &failure) {
				failure.Message = fmt.Sprintf("document %d: %s", position+1, failure.Message)
			}
			return BatchReceipt{}, err
		}
		encoded[position] = raw
	}
	receipt, failed, err := b.insertBatch(ctx, batchRequest{verb: verb, target: target, concern: concern}, encoded, 0)
	var failure *Error
	if err == nil || !errors.As(err, &failure) {
		return receipt, err
	}
	var account []string
	if receipt.Count > 0 {
		account = append(account, fmt.Sprintf("%d of %d documents were stored before this failure (their keys are in the receipt, nothing is rolled back).",
			receipt.Count, len(docs)))
	}
	if failure.Outcome == OutcomeUnknown {
		// The part in flight may or may not have been stored: it is neither in
		// the receipt nor counted as stored, and it is never resent.
		account = append(account, fmt.Sprintf("Documents %d to %d of %d were in the part in flight, and whether they were stored is unknown: "+
			"the driver has not resent them. Read them back before sending them again.", failed.first+1, failed.end, len(docs)))
	}
	if len(failure.Results) > 0 {
		failure.ResultsOffset = failed.first
	}
	if len(account) > 0 {
		failure.Message = strings.Join(account, " ") + " " + failure.Message
	}
	return receipt, err
}

// batchPart is a run of a batch's documents: positions first up to, not
// including, end.
type batchPart struct{ first, end int }

// batchRequest is what every part of one InsertMany shares: the target an
// error names and the write concern each part is sent at.
type batchRequest struct {
	verb            batchVerb
	target, concern string
}

// insertBatch sends one batch, whose first document is at position offset of
// the caller's slice, and, when the server refuses it whole as over its atomic
// bound, the two halves in order. A receipt returned with an error holds what
// was stored before the error, and the part is the one that failed.
func (b *Bucket) insertBatch(ctx context.Context, shared batchRequest, batch []hoarddbwire.Document, offset int) (BatchReceipt, batchPart, error) {
	op, target := shared.verb.op, shared.target
	part := batchPart{first: offset, end: offset + len(batch)}
	var answer hoarddbwire.BatchPutResponse
	err := b.db.client.do(ctx, call{op: op, target: target, opcode: shared.verb.opcode, answer: shared.verb.answer,
		lane:     laneBulk,
		request:  batchBody(shared.verb, hoarddbwire.BatchPutRequest{Database: b.db.name, Bucket: b.name, Data: batch, WriteConcern: shared.concern}),
		response: &answer})
	if err != nil {
		// The current server refuses an oversize batch with an ERROR frame and
		// the code, so split on the code rather than on its message. A refusal
		// whose outcome is unknown is not split: a member with a larger
		// memtable may have applied the batch, and the caller must read it back
		// before sending the halves.
		var failure *Error
		if len(batch) > 1 && errors.As(err, &failure) && failure.Code == CodeBatchTooLarge && failure.Outcome != OutcomeUnknown {
			return b.splitBatch(ctx, shared, batch, offset)
		}
		return BatchReceipt{}, part, err
	}
	if answer.Success {
		receipt := BatchReceipt{IDs: answer.IDs, Count: int(answer.Count), Results: writeResultsOf(answer.Results)}
		receipt.CopiesConfirmed, receipt.ReplicationFactor = copiesOf(answer.WriteReport)
		return receipt, batchPart{}, nil
	}
	refused := writeRefusal(op, target, answer.Message, answer.WriteReport)
	refused.Results = writeResultsOf(answer.Results)
	if len(batch) == 1 || answer.WriteOutcome == hoarddbwire.WriteUnknown || !strings.Contains(answer.Message, batchNoneWritten) {
		return BatchReceipt{}, part, refused
	}
	return b.splitBatch(ctx, shared, batch, offset)
}

// batchBody is the request value of the verb's opcode: the two batch requests
// carry the same members and differ only in type.
func batchBody(verb batchVerb, body hoarddbwire.BatchPutRequest) any {
	if verb.opcode == hoarddbwire.OpBatchUpsert {
		return hoarddbwire.BatchUpsertRequest(body)
	}
	return body
}

// splitBatch sends the two halves of a batch the server refused whole, in
// order, and merges what they stored.
func (b *Bucket) splitBatch(ctx context.Context, shared batchRequest, batch []hoarddbwire.Document, offset int) (BatchReceipt, batchPart, error) {
	middle := len(batch) / 2
	first, failed, err := b.insertBatch(ctx, shared, batch[:middle], offset)
	if err != nil {
		return first, failed, err
	}
	second, failed, err := b.insertBatch(ctx, shared, batch[middle:], offset+middle)
	return first.merge(second), failed, err
}

// merge joins the receipts of two consecutive parts of one batch. The copies
// confirmed are the fewest of the parts, since that is all the whole batch can
// claim.
func (receipt BatchReceipt) merge(next BatchReceipt) BatchReceipt {
	receipt.IDs = append(receipt.IDs, next.IDs...)
	receipt.Results = append(receipt.Results, next.Results...)
	receipt.Count += next.Count
	if next.CopiesConfirmed != 0 && (receipt.CopiesConfirmed == 0 || next.CopiesConfirmed < receipt.CopiesConfirmed) {
		receipt.CopiesConfirmed = next.CopiesConfirmed
	}
	receipt.ReplicationFactor = max(receipt.ReplicationFactor, next.ReplicationFactor)
	return receipt
}
