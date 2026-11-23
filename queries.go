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
	"math/bits"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// The query verbs: find, count, distinct, search and the consistent read
// (handbook/operations.md section 3). A filter is a document: a struct with
// `bson:"..."` tags, a [Map] or a [Document]; nil, or an empty one, matches
// every document.

// messageBucketMissing starts the server's sentence for a bucket that is not
// there, the one in-band absence of a query (handbook/errors.md section 3).
const messageBucketMissing = "bucket not found"

// encodeFilter turns the caller's filter into the bytes of a request member, or
// nil when the filter matches everything and the member is left off the wire.
func encodeFilter(op, target string, filter any) (codec.Raw, error) {
	if filter == nil {
		return nil, nil
	}
	if bare, isDocument := filter.(Document); isDocument && len(bare) == 0 {
		return nil, nil
	}
	encoded, err := encodeDocument(op, target, filter)
	if err != nil {
		return nil, err
	}
	if len(encoded) == emptyDocumentLen {
		return nil, nil
	}
	return encoded, nil
}

// emptyDocumentLen is the size of the BSON document {}: a length and a
// terminator.
const emptyDocumentLen = 5

// absentBucket is the miss of a query on a bucket that is not there.
func absentBucket(op, target, message string) *Error {
	return &Error{Code: CodeNotFound, Op: op, Target: target, Message: message}
}

// Find returns a [Cursor] over the documents that match filter, a page at a
// time: each page is one request of [PageSize] documents (default 100), and the
// server keeps nothing between pages, so a cursor is not a snapshot. The first
// page is fetched before Find returns, so a bucket that is not there is
// [ErrNotFound] here, and [Cursor.Total] already says how many documents match
// in all, whatever [Skip] and [Limit] leave. A store type that cannot be
// queried (a blob bucket) is the server's refusal, passed through.
//
// Without [SortBy] the documents come in the store's natural order: for a queue
// its pop order. If a page is over the server's result ceiling the cursor
// halves its page size and retries as often as needed, down to one document: a
// size that worked is kept for the pages after, and a later page may halve
// again. If a single document is still over the ceiling the error carries both
// numbers and says how to ask for less ([Projection], a smaller [PageSize], a
// narrower filter).
//
//	cursor, err := people.Find(ctx, hoarddb.Map{"Field": "computing"},
//		hoarddb.SortBy("Born", hoarddb.Ascending), hoarddb.Limit(10))
func (b *Bucket) Find(ctx context.Context, filter any, opts ...FindOption) (*Cursor, error) {
	target := b.path()
	config := applyFindOptions(opts)
	if config.invalid != nil {
		return nil, refusal("find", target, config.invalid.Error()+"; nothing was sent")
	}
	encodedFilter, err := encodeFilter("find", target, filter)
	if err != nil {
		return nil, err
	}
	request := hoarddbwire.ScanRequest{Database: b.db.name, Bucket: b.name, Filter: encodedFilter, Skip: config.skip}
	if config.sortBy != "" {
		if request.Sort, err = codec.Marshal(map[string]int32{config.sortBy: int32(config.sortDir)}); err != nil {
			return nil, refusal("find", target, fmt.Sprintf("the sort could not be written (%v); nothing was sent", err))
		}
	}
	if len(config.project) > 0 {
		fields := make(map[string]int32, len(config.project))
		for _, field := range config.project {
			fields[field] = 1
		}
		if request.Projection, err = codec.Marshal(fields); err != nil {
			return nil, refusal("find", target, fmt.Sprintf("the projection could not be written (%v); nothing was sent", err))
		}
	}
	pager := &findPager{bucket: b, request: request, sizer: pageSizer{size: config.pageSize}, limited: config.limit > 0, remaining: config.limit}
	cursor := &Cursor{fetch: pager.next, total: -1}
	if err := cursor.advance(ctx); err != nil {
		return nil, err
	}
	return cursor, nil
}

// findPager fetches the pages of one Find: the same SCAN with a larger skip each
// time. When limited, remaining counts down the documents a Limit still allows.
type findPager struct {
	bucket    *Bucket
	request   hoarddbwire.ScanRequest
	sizer     pageSizer
	limited   bool
	remaining int64

	started bool
}

func (p *findPager) next(ctx context.Context) (page, error) {
	target := p.bucket.path()
	var answer hoarddbwire.ScanResponse
	err := p.sizer.fetch(p.remainingCeiling(), func(want int64) error {
		request := p.request
		request.Limit = want
		answer = hoarddbwire.ScanResponse{}
		return p.bucket.db.client.do(ctx, call{op: "find", target: target, opcode: hoarddbwire.OpScan, answer: hoarddbwire.OpScan,
			lane: laneBulk, request: request, response: &answer})
	})
	if err != nil {
		return page{}, err
	}
	if !answer.Found {
		if strings.HasPrefix(answer.Message, messageBucketMissing) {
			return page{}, absentBucket("find", target, answer.Message)
		}
		return page{}, refusal("find", target, answer.Message)
	}
	if len(answer.Documents) == 0 && answer.Message != "" {
		return page{}, refusal("find", target, answer.Message) // a store type that cannot be queried says why, with found true
	}
	documents := make([]Document, len(answer.Documents))
	for position, stored := range answer.Documents {
		documents[position] = Document(stored)
	}
	reply := page{documents: documents, total: answer.Total, hasTotal: !p.started}
	if !p.started {
		// The pages after the first name the bucket they continue, so one dropped
		// and created again in between is refused rather than read as more of
		// the same scan.
		p.request.ExpectBucketID = answer.BucketID
	}
	p.started = true
	p.request.Skip += int64(len(documents))
	if p.limited {
		p.remaining -= int64(len(documents))
	}
	reply.more = answer.HasMore && len(documents) > 0 && (!p.limited || p.remaining > 0)
	return reply, nil
}

// remainingCeiling is the most the next page may ask for: what a Limit still
// allows, or 0 for no ceiling.
func (p *findPager) remainingCeiling() int64 {
	if p.limited {
		return p.remaining
	}
	return 0
}

// Count returns how many documents match filter; nil counts them all. A bucket
// that is not there is [ErrNotFound].
func (b *Bucket) Count(ctx context.Context, filter any) (int64, error) {
	target := b.path()
	encodedFilter, err := encodeFilter("count", target, filter)
	if err != nil {
		return 0, err
	}
	var answer hoarddbwire.CountResponse
	err = b.db.client.do(ctx, call{op: "count", target: target, opcode: hoarddbwire.OpCount, answer: hoarddbwire.OpCount,
		request:  hoarddbwire.CountRequest{Database: b.db.name, Bucket: b.name, Filter: encodedFilter},
		response: &answer})
	if err != nil {
		return 0, err
	}
	return answer.Count, nil
}

// Distinct returns the distinct values field holds among the documents that
// match filter, nil for all of them, in no particular order. A value is
// whatever type the field holds; read it with the accessor for the type you
// expect. A bucket that is not there is [ErrNotFound].
func (b *Bucket) Distinct(ctx context.Context, field string, filter any) ([]Value, error) {
	target := b.path()
	if field == "" {
		return nil, refusal("distinct", target, "Distinct needs the name of the field whose values to list, so nothing was sent. "+
			`For example bucket.Distinct(ctx, "Field", nil)`)
	}
	encodedFilter, err := encodeFilter("distinct", target, filter)
	if err != nil {
		return nil, err
	}
	var answer hoarddbwire.DistinctResponse
	err = b.db.client.do(ctx, call{op: "distinct", target: target, opcode: hoarddbwire.OpDistinct, answer: hoarddbwire.OpDistinct, lane: laneBulk,
		request:  hoarddbwire.DistinctRequest{Database: b.db.name, Bucket: b.name, Field: field, Filter: encodedFilter},
		response: &answer})
	if err != nil {
		return nil, err
	}
	values := make([]Value, len(answer.Values))
	for position, stored := range answer.Values {
		values[position] = Value{raw: stored}
	}
	return values, nil
}

// Search returns a [Cursor] over the documents that match filter, answered
// from a secondary index when one covers the filter and by a full scan when
// none does (hash buckets are the only store with indexes; every other type but
// blob, which is refused, is always a full scan). The
// server answers a search in one reply, so [Limit] is the only option it
// takes: [Skip], [SortBy], [Projection] and [PageSize] are refused, and
// [Bucket.Find] is the query that takes them. [Cursor.Total] is how many
// documents the server returned, which a Limit caps: unlike Find's, it is not
// the match count of the whole bucket. [Cursor.HasMore] says whether a further
// match exists beyond the Limit.
func (b *Bucket) Search(ctx context.Context, filter any, opts ...FindOption) (*Cursor, error) {
	target := b.path()
	config := applyFindOptions(opts)
	if err := config.refuse("search", target, "Search answers in one reply and takes only Limit; use Find to skip, sort, project or page"); err != nil {
		return nil, err
	}
	encodedFilter, err := encodeFilter("search", target, filter)
	if err != nil {
		return nil, err
	}
	if encodedFilter == nil {
		encodedFilter = emptyDocument() // SEARCH's filter member is required
	}
	var answer hoarddbwire.SearchResponse
	err = b.db.client.do(ctx, call{op: "search", target: target, opcode: hoarddbwire.OpSearch, answer: hoarddbwire.OpSearch, lane: laneBulk,
		request:  hoarddbwire.SearchRequest{Database: b.db.name, Bucket: b.name, Filter: encodedFilter, Limit: config.limit},
		response: &answer})
	if err != nil {
		return nil, err
	}
	documents := make([]Document, len(answer.Results))
	for position, stored := range answer.Results {
		documents[position] = Document(stored)
	}
	return &Cursor{pending: documents, total: answer.Total, cut: answer.HasMore}, nil
}

// emptyDocument is the BSON document {}.
func emptyDocument() codec.Raw { return codec.Raw{5, 0, 0, 0, 0} }

// consistentGet is Get through CONSISTENT_GET, which answers with its own
// opcode (handbook/wire.md section 4).
func (b *Bucket) consistentGet(ctx context.Context, key string) (Document, error) {
	target := b.keyPath(key)
	if serverInfo := b.db.client.ServerInfo(); !serverInfo.Has("consistent_get") {
		return nil, &Error{Code: CodeUnsupportedByServer, Op: "get", Target: target, Message: fmt.Sprintf(
			"Consistent() needs the server capability consistent_get, which this server (version %s) did not advertise; nothing was sent. "+
				"Upgrade the server, or read without Consistent()", serverInfo.Version)}
	}
	var answer hoarddbwire.ConsistentGetResponse
	err := b.db.client.do(ctx, call{op: "get", target: target, opcode: hoarddbwire.OpConsistentGet, answer: hoarddbwire.OpConsistentGet,
		request:  hoarddbwire.KeyRequest{Database: b.db.name, Bucket: b.name, Key: key},
		response: &answer})
	if err != nil {
		return nil, err
	}
	if answer.Found {
		return Document(answer.Data), nil
	}
	switch {
	case answer.Message == "":
		return nil, &Error{Code: CodeNotFound, Op: "get", Target: target, Message: messageDocumentNotFound}
	case strings.HasPrefix(answer.Message, messageBucketMissing):
		return nil, absentBucket("get", target, answer.Message)
	default:
		return nil, refusal("get", target, answer.Message)
	}
}

// pageSizer holds the page size of one query and shrinks it when the server
// says a page is over its result ceiling.
type pageSizer struct{ size int64 }

// fetch calls ask with a page of at most the page size, or ceiling when that is
// smaller and not zero. On RESULT_TOO_LARGE it halves the size and asks again,
// as often as needed down to one document: at most log2(size)+1 attempts for
// this page, which is also what ends the loop for a document that can never
// fit. The size that worked is kept for the pages after, and a later page may
// halve again. A page of one that is still refused is returned with both
// numbers the server gave and the way out.
func (s *pageSizer) fetch(ceiling int64, ask func(want int64) error) error {
	first := s.size
	if ceiling > 0 && ceiling < first {
		first = ceiling
	}
	attempts := bits.Len64(uint64(first))
	for want := first; ; want /= 2 {
		err := ask(want)
		attempts--
		if !errors.Is(err, ErrResultTooLarge) {
			if err == nil && want < first {
				s.size = want
			}
			return err
		}
		if attempts <= 0 {
			return tooLarge(err, first, want)
		}
	}
}

// tooLarge is the error of a page the server refused as over its result
// ceiling, after the cursor has done what it can: the server's two numbers and
// what to change.
func tooLarge(refused error, asked, last int64) error {
	var original *Error
	if !errors.As(refused, &original) {
		return refused
	}
	tried := fmt.Sprintf("a page of %d documents", asked)
	switch {
	case last == 1 && asked > 1:
		tried += " (halved down to 1 document)"
	case last != asked:
		tried += fmt.Sprintf(" (halved down to %d)", last)
	}
	held := ""
	if original.RetainedBytes > 0 {
		held = fmt.Sprintf(" (the query held at least %d bytes before it stopped)", original.RetainedBytes)
	}
	failure := *original
	failure.Message = fmt.Sprintf("%s was over the server's result ceiling of %d bytes%s. "+
		"Ask for less per request: a smaller hoarddb.PageSize(n), hoarddb.Projection(...) to return fewer fields, or a narrower filter. The server said: %s",
		tried, original.MaxResultBytes, held, original.Message)
	return &failure
}

// FindOne returns the first document that matches filter, with the options of
// [Bucket.Find]: it is a Find that asks the server for one document, so a
// [Limit] or [PageSize] passed here is overridden, and [Skip], [SortBy] and
// [Projection] apply. A nil filter matches every document. When nothing matches
// the error matches [ErrNotFound] and the document is nil: a miss is an error
// here, as it is for [Bucket.Get], so a found document is never confused with
// an absent one (a document whose field holds null is found). A bucket that is
// not there is also [ErrNotFound]. To read a list that may be empty without an
// error, use [Bucket.Find].
//
//	ada, err := people.FindOne(ctx, hoarddb.Map{"Name": "Ada"})
//	if errors.Is(err, hoarddb.ErrNotFound) { /* no such person */ }
func (b *Bucket) FindOne(ctx context.Context, filter any, opts ...FindOption) (Document, error) {
	target := b.path()
	// Limit(1) goes last so it wins over any Limit the caller passed.
	cursor, err := b.Find(ctx, filter, append(slices.Clone(opts), Limit(1))...)
	if err != nil {
		var failure *Error
		if errors.As(err, &failure) && failure.Op == "find" {
			failure.Op = "findOne"
		}
		return nil, err
	}
	if !cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return nil, err
		}
		return nil, &Error{Code: CodeNotFound, Op: "findOne", Target: target,
			Message: fmt.Sprintf("No document in %s matches the filter. Use Find to get a list that may be empty, or check errors.Is(err, hoarddb.ErrNotFound)", target)}
	}
	return slices.Clone(cursor.Document()), nil
}
