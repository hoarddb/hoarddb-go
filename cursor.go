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
	"iter"
)

// Cursor reads the documents of a query one page at a time: [Bucket.Find],
// [Bucket.Search] and [Bucket.Range] return one. It starts no goroutine and
// holds nothing on the server, so abandoning one needs no cleanup, though
// [Cursor.Close] is the habit that keeps a program correct if a later release
// gives a cursor something to release. A Cursor is for one goroutine at a time.
//
// A cursor is not a snapshot. The server has no cursor of its own: each page is
// a new request, so a document inserted or deleted between two pages can be
// shown twice or missed.
//
//	cursor, err := bucket.Find(ctx, hoarddb.Map{"Field": "computing"})
//	if err != nil { ... }
//	defer cursor.Close()
//	for cursor.Next(ctx) {
//		doc := cursor.Document()
//		...
//	}
//	if err := cursor.Err(); err != nil { ... }
type Cursor struct {
	fetch   func(context.Context) (page, error)
	current Document
	pending []Document
	more    bool
	total   int64
	cut     bool
	err     error
	closed  bool
}

// page is one request's worth of a query. more says another request may have
// documents.
type page struct {
	documents []Document
	more      bool
	// total is how many documents the query matches in all, when the server
	// reports it; the cursor keeps the first page's.
	total    int64
	hasTotal bool
}

// advance reads the next page into the cursor. It is called for the first page
// before the cursor is returned, so the query's first error is the query's.
func (c *Cursor) advance(ctx context.Context) error {
	next, err := c.fetch(ctx)
	if err != nil {
		c.err = err
		return err
	}
	c.pending, c.more = next.documents, next.more
	if next.hasTotal {
		c.total = next.total
	}
	return nil
}

// Next moves to the next document, fetching the next page when the current one
// is spent, and reports whether there is one. When it reports false, [Cursor.Err]
// says whether the cursor ended or failed.
func (c *Cursor) Next(ctx context.Context) bool {
	for {
		if c.closed || c.err != nil {
			c.current = nil
			return false
		}
		if len(c.pending) > 0 {
			c.current, c.pending = c.pending[0], c.pending[1:]
			return true
		}
		if !c.more {
			c.current = nil
			return false
		}
		if err := c.advance(ctx); err != nil {
			c.current = nil
			return false
		}
	}
}

// Document is the document [Cursor.Next] moved to. It is valid until the next
// call to Next.
func (c *Cursor) Document() Document { return c.current }

// Err is the error that stopped the cursor, or nil when it ran to its end or
// was closed.
func (c *Cursor) Err() error { return c.err }

// Close ends the cursor: Next reports false afterwards. It releases nothing on
// the server today, and is safe to call more than once.
func (c *Cursor) Close() error {
	c.closed = true
	c.current, c.pending = nil, nil
	return nil
}

// Total is a count the first page reported: [Bucket.Find]'s is the exact
// number of documents the query matches in all, whatever [Skip] and [Limit]
// leave; [Bucket.Search]'s is the number it returned (at most its [Limit]),
// not the number that match, and [Cursor.HasMore] says whether more match. A
// btree range does not say, and reports -1.
func (c *Cursor) Total() int64 { return c.total }

// HasMore reports whether a [Bucket.Search] was cut at its [Limit]: at least
// one further document matches beyond the ones the cursor holds. A search
// answers in one reply and cannot be paged, so the way on is a larger Limit,
// or [Bucket.Find] when the match count matters. It is false for a search that
// returned every match, and always false for [Bucket.Find] and [Bucket.Range],
// whose [Cursor.Next] fetches the next page itself.
func (c *Cursor) HasMore() bool { return c.cut }

// All reads every remaining document into a slice. A query that can match
// millions of documents should be read with [Cursor.Next] instead.
func (c *Cursor) All(ctx context.Context) ([]Document, error) {
	var documents []Document
	for c.Next(ctx) {
		documents = append(documents, c.Document())
	}
	return documents, c.Err()
}

// Documents iterates the remaining documents with a range loop. A failure is
// yielded once, as the error of the last iteration, and the loop ends.
//
//	for doc, err := range cursor.Documents(ctx) {
//		if err != nil { ... }
//	}
func (c *Cursor) Documents(ctx context.Context) iter.Seq2[Document, error] {
	return func(yield func(Document, error) bool) {
		for c.Next(ctx) {
			if !yield(c.Document(), nil) {
				return
			}
		}
		if err := c.Err(); err != nil {
			yield(nil, err)
		}
	}
}
