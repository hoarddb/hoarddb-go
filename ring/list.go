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

package ring

import (
	"context"
	"fmt"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// List reads one page of the documents of the bucket, optionally those whose scalar fields
// equal the members of filter (a bson.D or any value that marshals to a document), and
// optionally only the scalar fields page.Fields names besides _id_. A filter still sees
// every declared field, whatever is returned. It is the
// one read that is O(documents): page through it with the returned Cursor while More is true.
func (b *Bucket) List(ctx context.Context, filter any, page PageRequest) (EntryPage, error) {
	raw, err := filterBytes(filter)
	if err != nil {
		return EntryPage{}, err
	}
	options, err := readOptions(page.Expand, page.Local)
	if err != nil {
		return EntryPage{}, err
	}
	request := ringwire.RingListRequest{Database: b.database, Bucket: b.bucket, Filter: raw, Fields: page.Fields, Cursor: page.Cursor, Options: options}
	if page.Limit != 0 {
		request.Limit = &page.Limit
	}
	body, err := ringwire.EncodeRingList(request)
	if err != nil {
		return EntryPage{}, fmt.Errorf("ring: encoding the list of %s: %w", b.where(), err)
	}
	payload, err := b.client.call(ctx, hoarddbwire.OpRingList, b.client.NextRequestID(), body)
	if err != nil {
		return EntryPage{}, err
	}
	reply, err := ringwire.DecodeRingReply(payload)
	if err != nil {
		return EntryPage{}, fmt.Errorf("ring: decoding the list reply from %s: %w", b.where(), err)
	}
	entries, err := ringwire.DecodeEntries(reply.Entries)
	if err != nil {
		return EntryPage{}, fmt.Errorf("ring: decoding the entries of %s: %w", b.where(), err)
	}
	out := EntryPage{Entries: make([]Entry, len(entries)), More: reply.More, Cursor: reply.Cursor, Expanded: expandedList(reply.Expanded), Served: reply.Served}
	if reply.ExpandFiltered != nil {
		out.ExpandFiltered = int(*reply.ExpandFiltered)
	}
	for i, entry := range entries {
		out.Entries[i] = Entry{ID: decodeID(entry.EncID), Body: entry.Body}
	}
	return out, nil
}

// Count returns how many documents the bucket holds, or how many match filter.
func (b *Bucket) Count(ctx context.Context, filter any) (int64, error) {
	raw, err := filterBytes(filter)
	if err != nil {
		return 0, err
	}
	body, err := ringwire.EncodeRingCount(ringwire.RingCountRequest{Database: b.database, Bucket: b.bucket, Filter: raw})
	if err != nil {
		return 0, fmt.Errorf("ring: encoding the count of %s: %w", b.where(), err)
	}
	payload, err := b.client.call(ctx, hoarddbwire.OpRingCount, b.client.NextRequestID(), body)
	if err != nil {
		return 0, err
	}
	reply, err := ringwire.DecodeRingReply(payload)
	if err != nil {
		return 0, fmt.Errorf("ring: decoding the count reply from %s: %w", b.where(), err)
	}
	if reply.Count == nil {
		return 0, fmt.Errorf("ring: the server answered a count of %s without a count", b.where())
	}
	return *reply.Count, nil
}

// readOptions builds the options trailer of an expanded or local read. The cap of 8 expanded
// fields is checked here, before anything is sent.
func readOptions(expand []ExpandRequest, local bool) (ringwire.RingReadOptions, error) {
	options := ringwire.RingReadOptions{Local: local}
	for _, request := range expand {
		where, err := filterBytes(request.Where)
		if err != nil {
			return ringwire.RingReadOptions{}, fmt.Errorf("ring: encoding the where of expand %q: %w", request.Field, err)
		}
		options.Expand = append(options.Expand, ringwire.RingExpandSpec{Field: request.Field, Database: request.Database, Bucket: request.Bucket, Fields: request.Fields, Where: where})
	}
	if _, err := ringwire.EncodeRingReadOptions(options); err != nil {
		return ringwire.RingReadOptions{}, fmt.Errorf("ring: %w", err)
	}
	return options, nil
}
