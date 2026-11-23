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
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// FieldStore is one store field of one document: the Purchases queue of the
// user "ada", or the Steps queue inside her "pie" recipe
// (handbook/operations.md section 5). Its verbs are the buckets': Push, Pop and
// Peek on a fifo, lifo or heap field; Put, Get and Delete on a hash, btree or
// blob field; Len and List on any.
//
// A FieldStore is a handle: a bucket pointer and a few strings. Making one does
// no I/O and keeps nothing on the client, so a program may make one per
// document, per call, for millions of documents. The driver does not look the
// field's engine up: a verb the engine does not answer is the server's
// refusal, passed through, and a field the bucket does not declare, or a
// parent document that does not exist, is [ErrNotFound].
//
//	users.Field("ada", "Purchases").Push(ctx, order)
//	users.Field("ada", "Recipes").Field("pie", "Steps").Push(ctx, step)
type FieldStore struct {
	bucket *Bucket
	// key is the parent document's key. path is the outer field and element
	// pairs that reach a nested store, and field the store itself.
	key   string
	path  []string
	field string
}

// Field returns the store field named field of the document stored under key.
// It does no I/O and does not check that the field is declared. Note the
// order: the document's key first, then the field.
func (b *Bucket) Field(key, field string) *FieldStore {
	return &FieldStore{bucket: b, key: key, field: field}
}

// Field returns the store named field that is nested inside the element named
// element of this store, for a store field declared with its own store fields.
func (f *FieldStore) Field(element, field string) *FieldStore {
	path := make([]string, 0, len(f.path)+2)
	path = append(append(path, f.path...), f.field, element)
	return &FieldStore{bucket: f.bucket, key: f.key, path: path, field: field}
}

// target is what an error names: "app/User/ada/Recipes/pie/Steps".
func (f *FieldStore) target() string {
	parts := append([]string{f.bucket.keyPath(f.key)}, f.path...)
	return strings.Join(append(parts, f.field), "/")
}

// request is the part of every field request that names the store.
func (f *FieldStore) request() hoarddbwire.FieldRequest {
	request := hoarddbwire.FieldRequest{Database: f.bucket.db.name, Bucket: f.bucket.name, Key: f.key, Field: f.field}
	if len(f.path) > 0 {
		request.Path = make([]any, len(f.path))
		for position, step := range f.path {
			request.Path[position] = step
		}
	}
	return request
}

// writeRequest is the request of a field write that takes no priority: the
// store's address and the write concern, after refusing a priority.
func (f *FieldStore) writeRequest(op string, config writeConfig) (hoarddbwire.FieldRequest, error) {
	if err := config.refusePriority(op, f.target()); err != nil {
		return hoarddbwire.FieldRequest{}, err
	}
	concern, err := f.bucket.db.client.writeConcern(op, f.target(), config)
	if err != nil {
		return hoarddbwire.FieldRequest{}, err
	}
	request := f.request()
	request.WriteConcern = concern
	return request, nil
}

// fieldCall is one field verb's request.
type fieldCall struct {
	// popLimit is how many entries a pop may have removed, 0 for any other verb.
	popLimit int64
	op       string
	opcode   hoarddbwire.Opcode
	request  any
	lane     lane
}

// send sends one field request and returns the answer. An in-band refusal (success
// false) is an error here; absence, which is success with found false, is the
// verb's to classify.
func (f *FieldStore) send(ctx context.Context, verb fieldCall) (hoarddbwire.FieldResponse, error) {
	var answer hoarddbwire.FieldResponse
	target := f.target()
	err := f.bucket.db.client.do(ctx, call{op: verb.op, target: target, opcode: verb.opcode, answer: verb.opcode,
		lane: verb.lane, popLimit: verb.popLimit, request: verb.request, response: &answer})
	if err != nil {
		return answer, err
	}
	if !answer.Success {
		if isRead(verb.opcode) {
			return answer, refusal(verb.op, target, answer.Message)
		}
		return answer, writeRefusal(verb.op, target, answer.Message, answer.WriteReport)
	}
	return answer, nil
}

// missError is the get of an element that is not there.
func (f *FieldStore) missError(op, element, message string) *Error {
	if message == "" {
		message = fmt.Sprintf("the store field %q of %s has no element %q", f.field, f.bucket.keyPath(f.key), element)
	}
	return &Error{Code: CodeNotFound, Op: op, Target: f.target(), Message: message}
}

// fieldReceipt is the receipt of a write: id is the entry id of a push or the
// element key of a put.
func fieldReceipt(id string, report hoarddbwire.WriteReport) Receipt {
	receipt := Receipt{ID: id}
	receipt.CopiesConfirmed, receipt.ReplicationFactor = copiesOf(report)
	return receipt
}

// Push adds doc to a fifo, lifo or heap field and returns the entry's id in the
// receipt's ID. A heap takes [WithPriority]; without it the element goes in at
// priority 0.
func (f *FieldStore) Push(ctx context.Context, doc any, opts ...WriteOption) (Receipt, error) {
	target := f.target()
	config := applyWriteOptions(opts)
	concern, err := f.bucket.db.client.writeConcern("push", target, config)
	if err != nil {
		return Receipt{}, err
	}
	encoded, err := encodeDocument("push", target, doc)
	if err != nil {
		return Receipt{}, err
	}
	request := f.request()
	request.Data, request.WriteConcern = encoded, concern
	if config.hasPriority {
		request.Priority = &config.priority
	}
	answer, err := f.send(ctx, fieldCall{op: "push", opcode: hoarddbwire.OpFieldPush, request: request})
	if err != nil {
		return Receipt{}, err
	}
	return fieldReceipt(answer.EntryID, answer.WriteReport), nil
}

// Pop removes the next documents of a fifo, lifo or heap field and returns them
// in pop order: the oldest, the newest, or the highest priority. One by default;
// [WithLimit] asks for up to 100 in one request. An empty store is an empty list and a nil error: check len.
// A pop consumes, so it is a write, and the driver never resends one whose
// outcome it cannot tell. A field pop shows no element's position, priority or
// entry id, as a field Peek does not.
func (f *FieldStore) Pop(ctx context.Context, opts ...PopOption) ([]Document, error) {
	config := applyPopOptions(opts)
	if config.invalid != nil {
		return nil, refusal("pop", f.target(), config.invalid.Error()+"; nothing was sent")
	}
	base, err := f.writeRequest("pop", config.write)
	if err != nil {
		return nil, err
	}
	request := hoarddbwire.FieldPopRequest{FieldRequest: base, Limit: config.limit}
	answer, err := f.send(ctx, fieldCall{op: "pop", opcode: hoarddbwire.OpFieldPop, request: request, popLimit: max(config.limit, 1)})
	if err != nil {
		return nil, err
	}
	if !answer.Found || len(answer.Items) == 0 {
		return []Document{}, nil
	}
	documents := make([]Document, len(answer.Items))
	for position, entry := range answer.Items {
		documents[position] = Document(entry.Element)
	}
	return documents, nil
}

// Peek returns the next documents of a fifo, lifo or heap field without
// removing them, in the order pops would return them: one by default, or as
// many as [WithLimit] asks, up to 1000. An empty store is an empty list and a nil error: check len.
func (f *FieldStore) Peek(ctx context.Context, opts ...PeekOption) ([]Document, error) {
	var config peekConfig
	for _, option := range opts {
		option.applyPeek(&config)
	}
	if config.invalid != nil {
		return nil, refusal("peek", f.target(), config.invalid.Error()+"; nothing was sent")
	}
	request := hoarddbwire.FieldPeekRequest{FieldRequest: f.request(), Limit: config.limit}
	answer, err := f.send(ctx, fieldCall{op: "peek", opcode: hoarddbwire.OpFieldPeek, request: request})
	if err != nil {
		return nil, err
	}
	if !answer.Found || len(answer.Items) == 0 {
		return []Document{}, nil
	}
	documents := make([]Document, len(answer.Items))
	for position, entry := range answer.Items {
		documents[position] = Document(entry.Element)
	}
	return documents, nil
}

// Put stores doc under element in a hash or btree field, replacing the element
// that is there. The receipt's ID is element.
func (f *FieldStore) Put(ctx context.Context, element string, doc any, opts ...WriteOption) (Receipt, error) {
	request, err := f.writeRequest("put", applyWriteOptions(opts))
	if err != nil {
		return Receipt{}, err
	}
	encoded, err := encodeDocument("put", f.target(), doc)
	if err != nil {
		return Receipt{}, err
	}
	request.Element, request.Data = element, encoded
	answer, err := f.send(ctx, fieldCall{op: "put", opcode: hoarddbwire.OpFieldPut, request: request})
	if err != nil {
		return Receipt{}, err
	}
	return fieldReceipt(element, answer.WriteReport), nil
}

// Get reads the element stored under element in a hash or btree field. An
// element that is not there is [ErrNotFound]; Get never returns an empty
// document for a miss.
func (f *FieldStore) Get(ctx context.Context, element string) (Document, error) {
	request := f.request()
	request.Element = element
	answer, err := f.send(ctx, fieldCall{op: "get", opcode: hoarddbwire.OpFieldGet, request: request})
	if err != nil {
		return nil, err
	}
	if !answer.Found {
		return nil, f.missError("get", element, answer.Message)
	}
	return Document(answer.Element), nil
}

// Delete removes the element stored under element in a hash, btree or blob
// field and reports whether there was one: false with no error means nothing
// was there, so repeating a delete is safe.
func (f *FieldStore) Delete(ctx context.Context, element string, opts ...WriteOption) (bool, error) {
	request, err := f.writeRequest("delete", applyWriteOptions(opts))
	if err != nil {
		return false, err
	}
	request.Element = element
	answer, err := f.send(ctx, fieldCall{op: "delete", opcode: hoarddbwire.OpFieldDelete, request: request})
	if err != nil {
		return false, err
	}
	return answer.Found, nil
}

// PutBlob stores content under element in a blob field, with the metadata of
// [WithMetadata] when given, replacing what is there. It travels on the bulk
// lane.
func (f *FieldStore) PutBlob(ctx context.Context, element string, content []byte, opts ...BlobOption) (BlobReceipt, error) {
	var config blobConfig
	for _, option := range opts {
		option.applyBlob(&config)
	}
	if err := config.write.refusePriority("put blob", f.target()); err != nil {
		return BlobReceipt{}, err
	}
	concern, err := f.bucket.db.client.writeConcern("put blob", f.target(), config.write)
	if err != nil {
		return BlobReceipt{}, err
	}
	request := f.request()
	request.Element, request.Content, request.WriteConcern = element, content, concern
	if config.hasMetadata {
		encoded, err := encodeDocument("put blob", f.target(), config.metadata)
		if err != nil {
			return BlobReceipt{}, err
		}
		request.Data = encoded
	}
	answer, err := f.send(ctx, fieldCall{op: "put blob", opcode: hoarddbwire.OpFieldPut, request: request, lane: laneBulk})
	if err != nil {
		return BlobReceipt{}, err
	}
	return BlobReceipt{Receipt: fieldReceipt(element, answer.WriteReport), BlobID: answer.BlobID, Size: int64(len(content))}, nil
}

// GetBlob reads the blob stored under element in a blob field. An element that
// is not there is [ErrNotFound]. It travels on the bulk lane.
func (f *FieldStore) GetBlob(ctx context.Context, element string) (*StoredBlob, error) {
	request := f.request()
	request.Element = element
	answer, err := f.send(ctx, fieldCall{op: "get blob", opcode: hoarddbwire.OpFieldGet, request: request, lane: laneBulk})
	if err != nil {
		return nil, err
	}
	if !answer.Found {
		return nil, f.missError("get blob", element, answer.Message)
	}
	return &StoredBlob{Key: element, BlobID: answer.BlobID, Data: answer.Content, Size: int64(len(answer.Content)),
		Metadata: Document(answer.Element)}, nil
}

// Len returns how many elements the store holds, whatever its engine.
func (f *FieldStore) Len(ctx context.Context) (int64, error) {
	answer, err := f.send(ctx, fieldCall{op: "len", opcode: hoarddbwire.OpFieldLen, request: f.request()})
	if err != nil {
		return 0, err
	}
	return answer.Len, nil
}

// fieldListMax is the most elements one FIELD_LIST page holds
// (handbook/operations.md section 5).
const fieldListMax = 1000

// List returns a [Cursor] over the store's elements, in the engine's own order
// (a queue's is its pop order), a page at a time: each page is one request of
// [PageSize] elements, from 1 to 1000, default 100. A blob field lists each
// blob's metadata. The server keeps nothing between pages, so, as for any
// cursor, it is not a snapshot. [Limit] stops it early; [Skip], [SortBy] and
// [Projection] are refused, because the request has no place for them. A
// field that does not exist is [ErrNotFound]; one with no elements is an empty
// cursor.
func (f *FieldStore) List(ctx context.Context, opts ...FindOption) (*Cursor, error) {
	target := f.target()
	config := applyFindOptions(opts)
	if err := config.refuse("list", target, "A list takes only Limit and PageSize"); err != nil {
		return nil, err
	}
	if config.pageSize > fieldListMax {
		return nil, refusal("list", target, fmt.Sprintf("PageSize(%d): a list page holds at most %d elements, so nothing was sent. "+
			"Ask for a smaller page; the cursor fetches the rest as you read", config.pageSize, fieldListMax))
	}
	pager := &fieldListPager{store: f, sizer: pageSizer{size: config.pageSize}, limited: config.limit > 0, remaining: config.limit}
	cursor := &Cursor{fetch: pager.next, total: -1}
	if err := cursor.advance(ctx); err != nil {
		return nil, err
	}
	return cursor, nil
}

// fieldListPager fetches the pages of one List. The server answers each page
// with an opaque continuation, which the next request carries back unchanged.
type fieldListPager struct {
	store     *FieldStore
	sizer     pageSizer
	limited   bool
	remaining int64
	after     []byte
}

func (p *fieldListPager) next(ctx context.Context) (page, error) {
	target := p.store.target()
	ceiling := int64(0)
	if p.limited {
		ceiling = p.remaining
	}
	var answer hoarddbwire.FieldListResponse
	err := p.sizer.fetch(ceiling, func(want int64) error {
		request := hoarddbwire.FieldListRequest{FieldRequest: p.store.request(), Limit: want}
		if p.after != nil {
			request.After = p.after
		}
		answer = hoarddbwire.FieldListResponse{}
		return p.store.bucket.db.client.do(ctx, call{op: "list", target: target, opcode: hoarddbwire.OpFieldList, answer: hoarddbwire.OpFieldList,
			lane: laneBulk, request: request, response: &answer})
	})
	if err != nil {
		return page{}, err
	}
	if !answer.Success {
		return page{}, refusal("list", target, answer.Message)
	}
	documents := make([]Document, len(answer.Entries))
	for position, entry := range answer.Entries {
		documents[position] = Document(entry.Element)
	}
	if p.limited {
		p.remaining -= int64(len(documents))
	}
	more := answer.HasMore && len(documents) > 0 && (!p.limited || p.remaining > 0)
	if more && len(answer.Next) == 0 {
		return page{}, &Error{Code: CodeInternal, Op: "list", Target: target, Message: "the server said more elements follow but gave no continuation; " +
			"this driver and the server disagree about the list, and reading on would start again from the first element"}
	}
	p.after = slices.Clone(answer.Next)
	return page{documents: documents, more: more}, nil
}
