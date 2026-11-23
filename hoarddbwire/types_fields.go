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

package hoarddbwire

// Store fields: a store that each document of a bucket carries
// (handbook/operations.md section 5).

// FieldRequest is the request of every field verb but the list. Key is the
// parent document's key and Element the element's: the two are easy to swap
// (handbook/build-go.md section 2, failure 14).
//
// Path addresses a store nested inside an element: the outer field, then the
// outer element's key or position or {priority, id}; its members differ in
// type, so it is a list of values.
type FieldRequest struct {
	Database     string   `bson:"database"`
	Bucket       string   `bson:"bucket"`
	Key          string   `bson:"key"`
	Field        string   `bson:"field"`
	Element      string   `bson:"element,omitempty"`
	Path         []any    `bson:"path,omitempty"`
	Data         Document `bson:"data,omitempty"`
	Content      []byte   `bson:"content,omitempty"`
	Priority     *int64   `bson:"priority,omitempty"`
	WriteConcern string   `bson:"writeconcern,omitempty"`
}

// FieldPopRequest is FIELD_POP: a FieldRequest and how many entries to remove
// and return. Limit is absent for one; a negative one, or one above 100, is
// INVALID_ARGUMENT.
type FieldPopRequest struct {
	FieldRequest `bson:",inline"`
	Limit        int64 `bson:"limit,omitempty"`
}

// FieldPeekRequest is FIELD_PEEK: a FieldRequest and how many entries to
// return. Limit is absent for one; a negative one, or one above 1000, is
// INVALID_ARGUMENT. No other field verb has the member.
type FieldPeekRequest struct {
	FieldRequest `bson:",inline"`
	Limit        int64 `bson:"limit,omitempty"`
}

// FieldListRequest is FIELD_LIST: a FieldRequest and the page. After is where
// the page starts, exclusive: an element key, a position, or the Next of the
// previous page, unchanged. Limit defaults to 100 and is at most 1000.
type FieldListRequest struct {
	FieldRequest `bson:",inline"`
	After        any   `bson:"after,omitempty"`
	Limit        int64 `bson:"limit,omitempty"`
}

// FieldEntry says where an element is and what it is. Which of the position
// members is set depends on the store's engine.
type FieldEntry struct {
	Element    Document `bson:"element,omitempty"`
	BlobID     string   `bson:"blob_id,omitempty"`
	ElementKey string   `bson:"element_key,omitempty"`
	Position   int64    `bson:"position,omitempty"`
	Priority   int64    `bson:"priority,omitempty"`
	EntryID    string   `bson:"entry_id,omitempty"`
}

// FieldResponse answers every field verb but the list. A pop or get fills
// Element and the members that say where it was; a peek fills Items instead
// and leaves those unset. The wire name of BlobID has an underscore here,
// where the blob verbs' has none.
type FieldResponse struct {
	Success     bool         `bson:"success"`
	Message     string       `bson:"message,omitempty"`
	Found       bool         `bson:"found"`
	Element     Document     `bson:"element,omitempty"`
	ElementKey  string       `bson:"element_key,omitempty"`
	Position    int64        `bson:"position,omitempty"`
	Priority    int64        `bson:"priority,omitempty"`
	EntryID     string       `bson:"entry_id,omitempty"`
	Len         int64        `bson:"len,omitempty"`
	Items       []FieldEntry `bson:"items,omitempty"`
	BlobID      string       `bson:"blob_id,omitempty"`
	Content     []byte       `bson:"content,omitempty"`
	WriteReport `bson:",inline"`
}

// FieldListResponse answers FIELD_LIST. Next is opaque: pass it back as After.
type FieldListResponse struct {
	Success bool         `bson:"success"`
	Message string       `bson:"message,omitempty"`
	Entries []FieldEntry `bson:"entries,omitempty"`
	HasMore bool         `bson:"has_more,omitempty"`
	Next    []byte       `bson:"next,omitempty"`
}
