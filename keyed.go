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
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// The verbs of the keyed stores, hash and btree (handbook/operations.md
// sections 2 and 3). A verb a store does not answer is refused by the server,
// and the refusal is passed through: the driver never looks a bucket's type up.

// The server's absence messages (handbook/errors.md section 3), pinned by the
// vectors. An in-band answer with any other message is a refusal.
const (
	messageDocumentNotFound = "document not found"
	messageBlobNotFound     = "blob not found"
	messageNotFoundIn       = " not found in " // `document "k" not found in app.users - insert it first ...`, `blob "k" not found in app.files - use ...`
)

// Update says how [Bucket.Update] changes a document. All nine operators are
// documented (handbook/operations.md section 2) and tested; each is sent only
// when its field is set. The server applies them in its own order (set, unset,
// inc, push, addtoset, mul, min, max, rename), and an update is all or nothing:
// when one operator is refused, nothing is written. A key may be a path into a
// value: "Address.City", "Lines[0].Qty".
type Update struct {
	// Set assigns fields, Unset removes them, and Inc adds to a number.
	Set   Map
	Unset []string
	Inc   map[string]int64
	// Push appends to an array, AddToSet appends unless the value is already
	// there, Min and Max keep the smaller or larger value (an absent field is
	// set), Mul multiplies a number (an absent field stays absent; the factors
	// are doubles) and Rename moves a field (a new name that exists does
	// nothing).
	Push, AddToSet, Min, Max Map
	Mul                      map[string]float64
	Rename                   map[string]string
}

// path is "database/bucket", what an error names as its target.
func (b *Bucket) path() string { return b.db.name + "/" + b.name }

// keyPath is "database/bucket/key".
func (b *Bucket) keyPath(key string) string { return b.path() + "/" + key }

// Put stores doc under key as a new document: it INSERTS. A key that already
// holds a document is refused with [ErrDocumentExists] and nothing is written,
// so a lost answer can be retried without overwriting anything; the error says
// to use [Bucket.Upsert] to replace the document or [Bucket.Update] to change
// some of its fields. doc is a struct with `bson:"..."` tags, a [Map], or a
// [Document], which is sent untouched. The receipt's ID is key.
//
// An empty key is refused: the server would read it as "assign one", which is
// [Bucket.Insert].
func (b *Bucket) Put(ctx context.Context, key string, doc any, opts ...WriteOption) (Receipt, error) {
	if key == "" {
		return Receipt{}, refusal("put", b.path(), "Put needs a key, and an empty one would store the document under a key the server picks. "+
			"Use Insert to let the server assign one, or pass the key you want")
	}
	return b.store(ctx, "put", hoarddbwire.OpPut, b.keyPath(key), key, doc, opts)
}

// Upsert stores doc under key, replacing the document that is there and
// creating it when there is none. The replaced document's fields are replaced
// and its store fields are kept. Use [Bucket.Put] to refuse a held key
// instead. A bucket with no keys to replace (fifo, lifo, heap) is refused with
// [ErrInvalidArgument], and the server's sentence names the verb to use.
//
// Resending an upsert after a lost answer stores the same document again. That
// is safe only when nothing else writes the key in between: UPSERT carries no
// request id, so the server cannot tell a resend from a new write, and a resend
// overwrites any Update, Upsert or Delete that landed meanwhile. The driver
// never resends it for you; the error says the outcome is unknown and the
// decision is yours. An empty key is refused locally. doc is
// encoded as for Put, and the receipt's ID is key.
func (b *Bucket) Upsert(ctx context.Context, key string, doc any, opts ...WriteOption) (Receipt, error) {
	if key == "" {
		return Receipt{}, refusal("upsert", b.path(), "Upsert needs the key of the document to replace. Pass it, or use Insert to let the server assign a new key")
	}
	return b.store(ctx, "upsert", hoarddbwire.OpUpsert, b.keyPath(key), key, doc, opts)
}

// Insert stores doc under a key the server assigns and returns it in the
// receipt. Use [Bucket.Put] to choose the key.
func (b *Bucket) Insert(ctx context.Context, doc any, opts ...WriteOption) (Receipt, error) {
	return b.store(ctx, "insert", hoarddbwire.OpPut, b.path(), "", doc, opts)
}

func (b *Bucket) store(ctx context.Context, op string, opcode hoarddbwire.Opcode, target, key string, doc any, opts []WriteOption) (Receipt, error) {
	config := applyWriteOptions(opts)
	if err := config.refusePriority(op, target); err != nil {
		return Receipt{}, err
	}
	concern, err := b.db.client.writeConcern(op, target, config)
	if err != nil {
		return Receipt{}, err
	}
	encoded, err := encodeDocument(op, target, doc)
	if err != nil {
		return Receipt{}, err
	}
	var answer hoarddbwire.InsertResponse
	answerOp, request := hoarddbwire.OpPut, any(hoarddbwire.PutRequest{Database: b.db.name, Bucket: b.name, Key: key, Data: encoded, WriteConcern: concern})
	if opcode == hoarddbwire.OpUpsert {
		answerOp, request = hoarddbwire.OpUpsertOK, hoarddbwire.UpsertRequest{Database: b.db.name, Bucket: b.name, Key: key, Data: encoded, WriteConcern: concern}
	}
	err = b.db.client.do(ctx, call{op: op, target: target, opcode: opcode, answer: answerOp, request: request, response: &answer})
	if err != nil {
		return Receipt{}, withBlobHint(err)
	}
	if !answer.Success {
		return Receipt{}, withBlobHint(writeRefusal(op, target, answer.Message, answer.WriteReport))
	}
	return receiptOf(answer), nil
}

// blobWrittenWithPut is the server's sentence for a document written to a blob
// bucket; it names the CLI verb, which in Go is the method that just failed.
const blobWrittenWithPut = "a blob bucket is written with put"

// withBlobHint appends the Go method to the server's blob refusal, keeping the
// server's sentence verbatim before it.
func withBlobHint(err error) error {
	var failure *Error
	if errors.As(err, &failure) && strings.Contains(failure.Message, blobWrittenWithPut) {
		failure.Message += " In Go that is Bucket.PutBlob."
	}
	return err
}

// Get reads the document stored under key. A key that is not there is
// [ErrNotFound]; Get never returns an empty document for a miss. With
// [Consistent] it is a quorum read, the read-your-writes verb on a cluster.
func (b *Bucket) Get(ctx context.Context, key string, opts ...ReadOption) (Document, error) {
	var config readConfig
	for _, option := range opts {
		option(&config)
	}
	if config.consistent {
		return b.consistentGet(ctx, key)
	}
	target := b.keyPath(key)
	var answer hoarddbwire.GetResponse
	err := b.db.client.do(ctx, call{op: "get", target: target, opcode: hoarddbwire.OpGet, answer: hoarddbwire.OpGetResponse,
		request:  hoarddbwire.KeyRequest{Database: b.db.name, Bucket: b.name, Key: key},
		response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Found {
		return nil, absence("get", target, answer.Message, CodeNotFound, messageDocumentNotFound)
	}
	return Document(answer.Data), nil
}

// Exists reports whether a document is stored under key. False always means the
// server looked and the key is not there: a bucket type with no keys (fifo,
// lifo, heap) and an empty key are refused with [ErrInvalidArgument], never
// answered false.
func (b *Bucket) Exists(ctx context.Context, key string) (bool, error) {
	var answer hoarddbwire.ExistsResponse
	err := b.db.client.do(ctx, call{op: "exists", target: b.keyPath(key), opcode: hoarddbwire.OpExists, answer: hoarddbwire.OpExistsResponse,
		request:  hoarddbwire.KeyRequest{Database: b.db.name, Bucket: b.name, Key: key},
		response: &answer})
	if err != nil {
		return false, err
	}
	return answer.Exists, nil
}

// Delete removes the document stored under key and reports whether there was
// one: false with no error means nothing was there, so repeating a delete is
// safe. An error means the server refused.
func (b *Bucket) Delete(ctx context.Context, key string, opts ...WriteOption) (bool, error) {
	target := b.keyPath(key)
	config := applyWriteOptions(opts)
	if err := config.refusePriority("delete", target); err != nil {
		return false, err
	}
	concern, err := b.db.client.writeConcern("delete", target, config)
	if err != nil {
		return false, err
	}
	var answer hoarddbwire.DeleteResponse
	err = b.db.client.do(ctx, call{op: "delete", target: target, opcode: hoarddbwire.OpDelete, answer: hoarddbwire.OpDelete,
		request:  hoarddbwire.DeleteRequest{Database: b.db.name, Bucket: b.name, Key: key, WriteConcern: concern},
		response: &answer})
	if err != nil {
		return false, err
	}
	if answer.Success {
		return true, nil
	}
	if answer.Message == messageDocumentNotFound || answer.Message == messageBlobNotFound {
		return false, nil
	}
	return false, writeRefusal("delete", target, answer.Message, answer.WriteReport)
}

// Update changes the document stored under key with the operators of update and
// returns it as it is afterwards. A key that is not there is [ErrNotFound]: an
// update never creates a document.
func (b *Bucket) Update(ctx context.Context, key string, update Update, opts ...WriteOption) (Document, error) {
	target := b.keyPath(key)
	config := applyWriteOptions(opts)
	if err := config.refusePriority("update", target); err != nil {
		return nil, err
	}
	concern, err := b.db.client.writeConcern("update", target, config)
	if err != nil {
		return nil, err
	}
	request, err := update.request(b.db.name, b.name, key)
	if err != nil {
		return nil, refusal("update", target, err.Error())
	}
	request.WriteConcern = concern
	var answer hoarddbwire.UpdateResponse
	err = b.db.client.do(ctx, call{op: "update", target: target, opcode: hoarddbwire.OpUpdate, answer: hoarddbwire.OpUpdate,
		request: request, response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Success {
		if strings.Contains(answer.Message, messageNotFoundIn) {
			return nil, &Error{Code: CodeNotFound, Op: "update", Target: target, Message: answer.Message, Outcome: outcomeOf(answer.WriteOutcome)}
		}
		return nil, writeRefusal("update", target, answer.Message, answer.WriteReport)
	}
	return Document(answer.Doc), nil
}

// request is the wire form of the update: each operator is a document.
func (update Update) request(database, bucket, key string) (hoarddbwire.UpdateRequest, error) {
	request := hoarddbwire.UpdateRequest{Database: database, Bucket: bucket, Key: key, Unset: update.Unset}
	operators := []struct {
		name  string
		into  *hoarddbwire.Document
		given any
		count int
	}{
		{"Set", &request.Set, update.Set, len(update.Set)},
		{"Inc", &request.Inc, update.Inc, len(update.Inc)},
		{"Push", &request.Push, update.Push, len(update.Push)},
		{"AddToSet", &request.AddToSet, update.AddToSet, len(update.AddToSet)},
		{"Min", &request.Min, update.Min, len(update.Min)},
		{"Max", &request.Max, update.Max, len(update.Max)},
		{"Mul", &request.Mul, update.Mul, len(update.Mul)},
		{"Rename", &request.Rename, update.Rename, len(update.Rename)},
	}
	changes := len(update.Unset)
	for _, operator := range operators {
		if operator.count == 0 {
			continue
		}
		encoded, err := codec.Marshal(operator.given)
		if err != nil {
			return request, fmt.Errorf("the Update's %s operator could not be written (%w); nothing was sent. Give field names and values the document can hold", operator.name, err)
		}
		*operator.into = encoded
		changes += operator.count
	}
	if changes == 0 {
		return request, fmt.Errorf("the Update has nothing to change, so nothing was sent. Set at least one operator, " +
			"for example hoarddb.Update{Set: hoarddb.Map{\"Name\": \"Ada\"}}")
	}
	return request, nil
}

// writeRefusal is an in-band refusal of a write: success false and a message
// that is not an absence.
func writeRefusal(op, target, message string, report hoarddbwire.WriteReport) *Error {
	failure := &Error{Code: CodeInvalidArgument, Op: op, Target: target, Message: message, Outcome: outcomeOf(report.WriteOutcome)}
	failure.CopiesConfirmed, failure.ReplicationFactor = copiesOf(report)
	return failure
}

// absence classifies an in-band found:false. The server's absence messages
// (handbook/errors.md section 3) are a miss of the given class; any other
// message is a refusal, for example a verb the store type does not answer, and
// is reported as one with the server's sentence verbatim.
func absence(op, target, message string, miss Code, absent ...string) error {
	if isAbsence(message, absent...) {
		return &Error{Code: miss, Op: op, Target: target, Message: message}
	}
	return &Error{Code: CodeInvalidArgument, Op: op, Target: target, Message: message}
}

// isAbsence reports whether message is one of the server's absence sentences.
func isAbsence(message string, absent ...string) bool {
	for _, expected := range absent {
		if message == expected {
			return true
		}
	}
	return false
}

// emptyList is the answer of a list verb that found nothing to return: an empty
// list and no error when the server said the store is empty, and a refusal
// carrying the server's sentence for any other message (a verb the store type
// does not answer).
func emptyList(op, target, message string) ([]Document, error) {
	if isAbsence(message, messageQueueEmpty, messageHeapEmpty) {
		return []Document{}, nil
	}
	return nil, &Error{Code: CodeInvalidArgument, Op: op, Target: target, Message: message}
}
