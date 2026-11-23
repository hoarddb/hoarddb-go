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
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// The dead list (handbook/api-go.md section 5, handbook/operations.md "Leases
// and the dead list"). An entry claimed 5 times without an ack, or a heap
// element pushed again while claimed, leaves the queue for the dead list.
// DeadList reads it; Requeue puts entries back; Discard drops them.

const (
	deadListLimitMax = 1000
)

// DeadEntry is one entry on the dead list.
type DeadEntry struct {
	// DeadID is the handle Requeue and Discard name, and what [After] pages by.
	DeadID   string
	Document Document
	// Deliveries is how many times the entry was delivered before it died.
	Deliveries int
	DeadAt     time.Time
	// Reason is why it died: max_deliveries or collided.
	Reason string
}

// DeadPage is one page of the dead list.
type DeadPage struct {
	// Items are the dead entries, in dead-list order. Empty when there are none.
	Items []DeadEntry
	// Next is the last dead id listed, when more remain: pass it to [After] for
	// the next page. Empty on the last page.
	Next string
}

type deadListConfig struct {
	limit   int64
	after   string
	invalid error
}

// DeadListOption changes one [Bucket.DeadList]: [WithLimit] and [After].
type DeadListOption interface{ applyDeadList(*deadListConfig) }

func (option LimitOption) applyDeadList(config *deadListConfig) {
	config.limit, config.invalid = option.within("a dead list shows", deadListLimitMax, "; page with After for more")
}

type afterOption string

// After starts a dead list past the dead id deadID, which is not repeated: the
// Next of the page before.
func After(deadID string) DeadListOption { return afterOption(deadID) }

func (option afterOption) applyDeadList(config *deadListConfig) { config.after = string(option) }

// DeadList reads the bucket's dead entries, 10 by default or [WithLimit] up to
// 1000, past [After]. It is a read: it changes nothing and is safe to retry. A
// bucket with no dead entries answers an empty page, not an error.
func (b *Bucket) DeadList(ctx context.Context, opts ...DeadListOption) (*DeadPage, error) {
	target := b.path()
	var config deadListConfig
	for _, option := range opts {
		option.applyDeadList(&config)
	}
	if config.invalid != nil {
		return nil, refusal("dead list", target, config.invalid.Error()+"; nothing was sent")
	}
	var answer hoarddbwire.DeadListResponse
	err := b.db.client.do(ctx, call{op: "dead list", target: target, opcode: hoarddbwire.OpDeadList, answer: hoarddbwire.OpDeadList,
		request:  hoarddbwire.DeadListRequest{Database: b.db.name, Bucket: b.name, Limit: config.limit, After: config.after},
		response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Found {
		if answer.Message != "" { // a message means the read was refused, not that the list is empty
			return nil, refusal("dead list", target, answer.Message)
		}
		return &DeadPage{}, nil
	}
	page := &DeadPage{Next: answer.Next, Items: make([]DeadEntry, len(answer.Items))}
	for position, entry := range answer.Items {
		page.Items[position] = DeadEntry{DeadID: entry.DeadID, Document: Document(entry.Data),
			Deliveries: int(entry.Deliveries), DeadAt: entry.DeadAt, Reason: entry.Reason}
	}
	return page, nil
}

// DeadSelector says which dead entries a [Bucket.Requeue] or [Bucket.Discard]
// moves: one by its dead id, or all of them.
type DeadSelector struct {
	deadID string
	all    bool
	after  string
}

// DeadByID selects the one dead entry with this dead id (from [DeadEntry]).
func DeadByID(deadID string) DeadSelector { return DeadSelector{deadID: deadID} }

// AllDead selects every dead entry, from the head of the list. One request
// considers at most 100 entries, so repeat it while [DeadMoveResult.Done] is
// above 0; [DeadSelector.After] steps past entries a request left behind.
func AllDead() DeadSelector { return DeadSelector{all: true} }

// After starts an [AllDead] request past the dead id deadID (exclusive), as
// DeadList pages: a request that was refused some entries steps past them with
// the last refused id. The server refuses it on a selector for one entry.
func (s DeadSelector) After(deadID string) DeadSelector {
	s.after = deadID
	return s
}

// Refusal is one dead entry a requeue left where it was, and why.
type Refusal struct {
	DeadID  string
	Message string
}

// DeadMoveResult is what a requeue or discard did.
type DeadMoveResult struct {
	// Done entries moved. Left remain on the dead list after an all request.
	Done, Left int64
	// Refused names the entries that stayed, for example a heap element whose key
	// a live element holds; those that moved stay moved.
	Refused []Refusal
	Message string
}

// Requeue moves dead entries back into the queue to be delivered again. When
// the server refuses some, the error says which, and the result beside it holds
// the counts and the refusals; the entries that moved stay moved. A write: it is
// never resent.
func (b *Bucket) Requeue(ctx context.Context, sel DeadSelector, opts ...WriteOption) (DeadMoveResult, error) {
	return b.deadMove(ctx, "requeue", hoarddbwire.OpDeadRequeue, sel, opts)
}

// Discard drops dead entries for good. Results and errors are Requeue's.
func (b *Bucket) Discard(ctx context.Context, sel DeadSelector, opts ...WriteOption) (DeadMoveResult, error) {
	return b.deadMove(ctx, "discard", hoarddbwire.OpDeadDiscard, sel, opts)
}

func (b *Bucket) deadMove(ctx context.Context, op string, opcode hoarddbwire.Opcode, sel DeadSelector, opts []WriteOption) (DeadMoveResult, error) {
	target := b.path()
	config := applyWriteOptions(opts)
	if err := config.refusePriority(op, target); err != nil {
		return DeadMoveResult{}, err
	}
	concern, err := b.db.client.writeConcern(op, target, config)
	if err != nil {
		return DeadMoveResult{}, err
	}
	var answer hoarddbwire.DeadMoveResponse
	err = b.db.client.do(ctx, call{op: op, target: target, opcode: opcode, answer: opcode,
		request: hoarddbwire.DeadMoveRequest{Database: b.db.name, Bucket: b.name, DeadID: sel.deadID, All: sel.all,
			After: sel.after, WriteConcern: concern},
		response: &answer})
	if err != nil {
		return DeadMoveResult{}, err
	}
	result := DeadMoveResult{Done: answer.Done, Left: answer.Left, Message: answer.Message}
	for _, refused := range answer.Refused {
		result.Refused = append(result.Refused, Refusal{DeadID: refused.DeadID, Message: refused.Message})
	}
	if answer.Success {
		return result, nil
	}
	return result, writeRefusal(op, target, refusedMessage(answer), answer.WriteReport)
}

// refusedMessage is the sentence of a requeue or discard the server refused:
// its own message, else the refusals it named, verbatim.
func refusedMessage(answer hoarddbwire.DeadMoveResponse) string {
	if answer.Message != "" {
		return answer.Message
	}
	sentences := make([]string, len(answer.Refused))
	for position, refused := range answer.Refused {
		sentences[position] = refused.Message
	}
	if len(sentences) == 0 {
		return fmt.Sprintf("the server refused the request and moved %d entries", answer.Done)
	}
	return strings.Join(sentences, "\n")
}
