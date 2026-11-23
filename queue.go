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

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// The verbs of the queues and the heap: fifo, lifo and heap buckets
// (handbook/operations.md sections 2 and 3). The server routes a push, pop or
// peek by the bucket's store type, not by the opcode, so the driver sends the
// FIFO family for all three.

// The server's absence messages for a store with nothing to take.
const (
	messageQueueEmpty = "queue is empty"
	messageHeapEmpty  = "heap is empty"
)

// Push adds doc to a fifo, lifo or heap bucket and returns the key the server
// assigned in the receipt. A heap takes [WithPriority] and pops the highest
// priority first; without it the element goes in at priority 0.
func (b *Bucket) Push(ctx context.Context, doc any, opts ...WriteOption) (Receipt, error) {
	target := b.path()
	config := applyWriteOptions(opts)
	concern, err := b.db.client.writeConcern("push", target, config)
	if err != nil {
		return Receipt{}, err
	}
	encoded, err := encodeDocument("push", target, doc)
	if err != nil {
		return Receipt{}, err
	}
	request := hoarddbwire.PushRequest{Database: b.db.name, Bucket: b.name, Data: encoded, WriteConcern: concern}
	if config.hasPriority {
		request.Priority = &config.priority
	}
	var answer hoarddbwire.InsertResponse
	err = b.db.client.do(ctx, call{op: "push", target: target, opcode: hoarddbwire.OpFIFOPush, answer: hoarddbwire.OpFIFOPush,
		request: request, response: &answer})
	if err != nil {
		return Receipt{}, err
	}
	if !answer.Success {
		return Receipt{}, writeRefusal("push", target, answer.Message, answer.WriteReport)
	}
	return receiptOf(answer), nil
}

// Pop removes the next documents and returns them in pop order: the oldest of
// a fifo, the newest of a lifo, the highest priority of a heap. One by default;
// [WithLimit] asks for up to 100 in one request. Fewer than asked for is not an
// error: all that existed was taken. A store with nothing to take is an empty
// list and a nil error: check len. A pop consumes, so it is a write, and the
// driver never resends one whose outcome it cannot tell: the error then says
// how many entries it may have removed, and Peek shows what is left. The write
// options are Pop options
// too.
func (b *Bucket) Pop(ctx context.Context, opts ...PopOption) ([]Document, error) {
	target := b.path()
	config := applyPopOptions(opts)
	if config.invalid != nil {
		return nil, refusal("pop", target, config.invalid.Error()+"; nothing was sent")
	}
	if err := config.write.refusePriority("pop", target); err != nil {
		return nil, err
	}
	concern, err := b.db.client.writeConcern("pop", target, config.write)
	if err != nil {
		return nil, err
	}
	var answer hoarddbwire.PopResponse
	err = b.db.client.do(ctx, call{op: "pop", target: target, opcode: hoarddbwire.OpFIFOPop, answer: hoarddbwire.OpFIFOPopResp,
		popLimit: max(config.limit, 1),
		request:  hoarddbwire.PopRequest{Database: b.db.name, Bucket: b.name, Limit: config.limit, WriteConcern: concern}, response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Found || len(answer.Items) == 0 {
		return emptyList("pop", target, answer.Message)
	}
	return documentsOf(answer.Items), nil
}

// documentsOf hands the server's documents over as they arrived.
func documentsOf(stored []hoarddbwire.Document) []Document {
	documents := make([]Document, len(stored))
	for position, one := range stored {
		documents[position] = Document(one)
	}
	return documents
}

// Peek returns the next documents without removing them, in the order pops
// would return them: one by default, or as many as [WithLimit] asks, up to
// 1000. An empty store is an empty list and a nil error: check len.
func (b *Bucket) Peek(ctx context.Context, opts ...PeekOption) ([]Document, error) {
	target := b.path()
	var config peekConfig
	for _, option := range opts {
		option.applyPeek(&config)
	}
	if config.invalid != nil {
		return nil, refusal("peek", target, config.invalid.Error()+"; nothing was sent")
	}
	var answer hoarddbwire.PeekResponse
	err := b.db.client.do(ctx, call{op: "peek", target: target, opcode: hoarddbwire.OpFIFOPeek, answer: hoarddbwire.OpFIFOPeek,
		request:  hoarddbwire.PeekRequest{Database: b.db.name, Bucket: b.name, Limit: config.limit},
		response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Found || len(answer.Items) == 0 {
		return emptyList("peek", target, answer.Message)
	}
	return documentsOf(answer.Items), nil
}
