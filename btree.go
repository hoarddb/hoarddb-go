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

// The verbs of the btree: ordered reads by key (handbook/operations.md
// section 3).

// Bounds limits a btree range by key. Both bounds are inclusive and an empty
// side is open.
type Bounds struct{ Gte, Lte string }

// Range reads a btree bucket's documents in key order, from Bounds.Gte to
// Bounds.Lte, through a [Cursor] that fetches them a page at a time. The first
// page is fetched before Range returns, so a missing bucket is reported here.
// Each document's key is in its "_id_" field.
//
// The cursor is not a snapshot: between two pages a concurrent insert or
// delete can change what the next page holds.
func (b *Bucket) Range(ctx context.Context, bounds Bounds, opts ...FindOption) (*Cursor, error) {
	target := b.path()
	config := applyFindOptions(opts)
	if err := config.refuse("range", target, "A range reads by key; use Find to skip, sort or project"); err != nil {
		return nil, err
	}
	pager := &rangePager{bucket: b, bounds: bounds, sizer: pageSizer{size: config.pageSize}, limited: config.limit > 0, remaining: config.limit}
	cursor := &Cursor{fetch: pager.next, total: -1}
	if err := cursor.advance(ctx); err != nil {
		return nil, err
	}
	return cursor, nil
}

// rangePager fetches the pages of one Range. BTREE_RANGE has no skip and no
// continuation token, so a page after the first starts at the last key already
// returned (the bounds are inclusive), asks for one entry more, and drops that
// repeated first entry. When limited, remaining counts down the documents a
// Limit still allows.
type rangePager struct {
	bucket    *Bucket
	bounds    Bounds
	sizer     pageSizer
	limited   bool
	remaining int64

	started bool
	lastKey string
}

func (p *rangePager) next(ctx context.Context) (page, error) {
	var (
		entries []hoarddbwire.RangeEntry
		want    int64
		filled  bool
	)
	ceiling := int64(0)
	if p.limited {
		ceiling = p.remaining
	}
	err := p.sizer.fetch(ceiling, func(asked int64) error {
		want = asked
		low, ask := p.bounds.Gte, asked
		if p.started {
			low, ask = p.lastKey, asked+1
		}
		var err error
		entries, _, err = p.bucket.rangeEntries(ctx, rangeCall{op: "range", lane: laneBulk, opcode: hoarddbwire.OpBTreeRange, answer: hoarddbwire.OpBTreeResponse,
			request: hoarddbwire.RangeRequest{Database: p.bucket.db.name, Bucket: p.bucket.name, Low: low, High: p.bounds.Lte, Limit: ask}})
		filled = int64(len(entries)) >= ask
		return err
	})
	if err != nil {
		return page{}, err
	}
	if p.started && len(entries) > 0 && entries[0].Key == p.lastKey {
		entries = entries[1:]
	}
	if int64(len(entries)) > want {
		entries = entries[:want] // the repeated entry was deleted meanwhile, so the page has one too many
	}
	documents := make([]Document, len(entries))
	for position, entry := range entries {
		documents[position] = Document(entry.Value)
	}
	if len(entries) > 0 {
		p.lastKey = entries[len(entries)-1].Key
	}
	p.started = true
	if p.limited {
		p.remaining -= int64(len(documents))
	}
	more := filled && len(documents) > 0 && (!p.limited || p.remaining > 0)
	return page{documents: documents, more: more}, nil
}

// First returns the entry with the smallest key. An empty tree is [ErrNotFound].
func (b *Bucket) First(ctx context.Context) (Entry, error) {
	return b.end(ctx, "first", hoarddbwire.OpBTreeFirst)
}

// Last returns the entry with the largest key. An empty tree is [ErrNotFound].
func (b *Bucket) Last(ctx context.Context) (Entry, error) {
	return b.end(ctx, "last", hoarddbwire.OpBTreeLast)
}

// end is First and Last, which answer with their own opcode.
func (b *Bucket) end(ctx context.Context, op string, opcode hoarddbwire.Opcode) (Entry, error) {
	entries, message, err := b.rangeEntries(ctx, rangeCall{op: op, lane: lanePoint, opcode: opcode, answer: opcode,
		request: hoarddbwire.RangeRequest{Database: b.db.name, Bucket: b.name}})
	if err != nil {
		return Entry{}, err
	}
	if len(entries) == 0 {
		return Entry{}, &Error{Code: CodeNotFound, Op: op, Target: b.path(), Message: message}
	}
	return Entry{Key: entries[0].Key, Value: Document(entries[0].Value)}, nil
}

// rangeCall is one request of the range family.
type rangeCall struct {
	op             string
	lane           lane
	opcode, answer hoarddbwire.Opcode
	request        hoarddbwire.RangeRequest
}

// rangeEntries sends one request of the range family and returns its entries
// and the server's message, which on an empty answer says why.
func (b *Bucket) rangeEntries(ctx context.Context, request rangeCall) ([]hoarddbwire.RangeEntry, string, error) {
	var answer hoarddbwire.RangeResponse
	err := b.db.client.do(ctx, call{op: request.op, target: b.path(), opcode: request.opcode, answer: request.answer,
		lane: request.lane, request: request.request, response: &answer})
	if err != nil {
		return nil, "", err
	}
	return answer.Entries, answer.Message, nil
}
