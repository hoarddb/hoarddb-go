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

package hoarddb_test

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Component tests of the document verbs against the in-process fake server:
// what a real server cannot be made to do on demand (count frames, answer a
// page of a chosen shape), and the local refusals that must send nothing.

// countingServer answers every data frame it is sent with an error, and counts
// the data frames, so a test can show that a local refusal sent nothing.
func countingServer(t *testing.T) (*fakeServer, *atomic.Int32) {
	t.Helper()
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.errorFrame(request, int32(hoarddbwire.CodeInternal), "the fake answers nothing")
		return true
	}))
	return server, &frames
}

func TestWithPriorityOnPutIsInvalidArgument(t *testing.T) {
	server, frames := countingServer(t)
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	bucket := client.Database("app").Bucket("users")
	doc := hoarddb.Map{"Name": "Ada"}
	priority := hoarddb.WithPriority(3)
	calls := map[string]func() error{
		"Put":    func() error { _, err := bucket.Put(ctx, "ada", doc, priority); return err },
		"Insert": func() error { _, err := bucket.Insert(ctx, doc, priority); return err },
		"Pop":    func() error { _, err := bucket.Pop(ctx, priority); return err },
		"Delete": func() error { _, err := bucket.Delete(ctx, "ada", priority); return err },
		"Update": func() error {
			_, err := bucket.Update(ctx, "ada", hoarddb.Update{Set: doc}, priority)
			return err
		},
		"Claim": func() error { _, err := bucket.Claim(ctx, priority); return err },
		"Ack": func() error {
			lease, _ := hoarddb.ParseLeaseID(aLeaseID)
			_, err := bucket.Ack(ctx, lease, priority)
			return err
		},
		"Release": func() error {
			lease, _ := hoarddb.ParseLeaseID(aLeaseID)
			_, err := bucket.Release(ctx, lease, priority)
			return err
		},
		"Extend": func() error {
			lease, _ := hoarddb.ParseLeaseID(aLeaseID)
			_, err := bucket.Extend(ctx, lease, time.Minute, priority)
			return err
		},
		"Requeue": func() error { _, err := bucket.Requeue(ctx, hoarddb.AllDead(), priority); return err },
		"Discard": func() error { _, err := bucket.Discard(ctx, hoarddb.AllDead(), priority); return err },
		// A WriteOption is a BlobOption, so a priority reaches a blob write
		// too, where it must be refused rather than dropped.
		"PutBlob": func() error { _, err := bucket.PutBlob(ctx, "hello.txt", []byte("hello"), priority); return err },
		"FieldStore.PutBlob": func() error {
			_, err := bucket.Field("ada", "Data").PutBlob(ctx, "avatar", []byte("png"), priority)
			return err
		},
	}
	for name, call := range calls {
		err := call()
		var failure *hoarddb.Error
		if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) || !strings.Contains(failure.Message, "WithPriority(3)") ||
			!strings.Contains(failure.Message, "nothing was sent") {
			t.Errorf("%s with WithPriority: %v; want ErrInvalidArgument that says what to do, and nothing sent", name, err)
		}
	}
	if frames.Load() != 0 {
		t.Fatalf("%d frames were sent; a priority on a verb that takes none is refused locally", frames.Load())
	}
}

func TestLocalRefusalsSendNothing(t *testing.T) {
	server, frames := countingServer(t)
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	bucket := client.Database("app").Bucket("users")
	truncated := hoarddb.Document{0x10, 0x00, 0x00, 0x00}
	tests := map[string]struct {
		call func() error
		want string
	}{
		"Put without a key": {func() error { _, err := bucket.Put(ctx, "", hoarddb.Map{"a": 1}); return err }, "Insert"},
		"Put of a string":   {func() error { _, err := bucket.Put(ctx, "k", "text"); return err }, "document"},
		"Put of nil":        {func() error { _, err := bucket.Put(ctx, "k", nil); return err }, "document"},
		"Put of bad bytes":  {func() error { _, err := bucket.Put(ctx, "k", truncated); return err }, "document"},
		"Insert of a list":  {func() error { _, err := bucket.Insert(ctx, []int{1, 2}); return err }, "document"},
		"Push of nil":       {func() error { _, err := bucket.Push(ctx, nil); return err }, "document"},
		"Update with no operator": {func() error {
			_, err := bucket.Update(ctx, "k", hoarddb.Update{})
			return err
		}, "nothing to change"},
		"Peek limit 0":      {func() error { _, err := bucket.Peek(ctx, hoarddb.WithLimit(0)); return err }, "WithLimit(0)"},
		"Peek limit 1001":   {func() error { _, err := bucket.Peek(ctx, hoarddb.WithLimit(1001)); return err }, "1000"},
		"Range limit 0":     {func() error { _, err := bucket.Range(ctx, hoarddb.Bounds{}, hoarddb.Limit(0)); return err }, "Limit(0)"},
		"Range page size 0": {func() error { _, err := bucket.Range(ctx, hoarddb.Bounds{}, hoarddb.PageSize(0)); return err }, "PageSize(0)"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.call()
			var failure *hoarddb.Error
			if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) || !strings.Contains(failure.Message, test.want) {
				t.Fatalf("err = %v; want ErrInvalidArgument mentioning %q", err, test.want)
			}
		})
	}
	if frames.Load() != 0 {
		t.Fatalf("%d frames were sent; every one of these is refused before anything is sent", frames.Load())
	}
}

// A document goes in as a struct, a Map, or a Document; the last is passed
// through byte for byte.
func TestDocumentsGoInAsStructsMapsAndRawBytes(t *testing.T) {
	var mu sync.Mutex
	var seen [][]byte
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var put hoarddbwire.PutRequest
		if err := unmarshal(request.Payload, &put); err != nil {
			t.Error(err)
		}
		mu.Lock()
		seen = append(seen, bytes.Clone(put.Data))
		mu.Unlock()
		c.send(request, hoarddbwire.OpPut, hoarddbwire.InsertResponse{ID: put.Key, Success: true, Seq: 7})
		return true
	}))
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	bucket := client.Database("app").Bucket("users")

	type person struct {
		Name string `bson:"Name"`
		Born int32  `bson:"Born"`
	}
	raw, err := marshal(hoarddb.Map{"z": int32(1), "a": "last"}) // field order z, a: a re-encoding would sort or reorder
	if err != nil {
		t.Fatal(err)
	}
	rawPointer := hoarddb.Document(raw)
	// The order matters: the server records the documents in the order they are sent.
	for _, sent := range []struct {
		name string
		doc  any
	}{
		{"struct", person{Name: "Ada", Born: 1815}},
		{"pointer", &person{Name: "Ada", Born: 1815}},
		{"map", hoarddb.Map{"Name": "Ada", "Born": int32(1815)}},
		{"document", hoarddb.Document(raw)},
		{"document pointer", &rawPointer},
	} {
		receipt, err := bucket.Put(ctx, sent.name, sent.doc)
		if err != nil || receipt.ID != sent.name || receipt.Seq != 7 {
			t.Fatalf("Put of a %s = %+v, %v", sent.name, receipt, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 5 {
		t.Fatalf("%d documents reached the server, want 5", len(seen))
	}
	for index, name := range []string{"struct", "pointer", "map"} {
		var decoded hoarddb.Map
		if err := hoarddb.Document(seen[index]).Decode(&decoded); err != nil || decoded["Name"] != "Ada" {
			t.Errorf("the %s arrived as %s (%v)", name, hoarddb.Document(seen[index]), err)
		}
	}
	for _, index := range []int{3, 4} {
		if !bytes.Equal(seen[index], raw) {
			t.Errorf("a Document was re-encoded on the way: sent %x, arrived %x", raw, seen[index])
		}
	}
}

// fakeBTree answers BTREE_RANGE as the real server does (both bounds inclusive,
// key order, an empty bound open) and records each request.
type fakeBTree struct {
	keys     []string
	mu       sync.Mutex
	requests []hoarddbwire.RangeRequest
}

func (tree *fakeBTree) serve() func(*fakeConn) {
	sort.Strings(tree.keys)
	return serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var asked hoarddbwire.RangeRequest
		if err := unmarshal(request.Payload, &asked); err != nil {
			c.errorFrame(request, int32(hoarddbwire.CodeInvalidArgument), err.Error())
			return true
		}
		tree.mu.Lock()
		tree.requests = append(tree.requests, asked)
		tree.mu.Unlock()
		var answer hoarddbwire.RangeResponse
		for _, key := range tree.keys {
			if (asked.Low != "" && key < asked.Low) || (asked.High != "" && key > asked.High) {
				continue
			}
			value, _ := marshal(hoarddb.Map{"_id_": key})
			answer.Entries = append(answer.Entries, hoarddbwire.RangeEntry{Key: key, Value: value})
			if asked.Limit > 0 && int64(len(answer.Entries)) == asked.Limit {
				break
			}
		}
		c.send(request, hoarddbwire.OpBTreeResponse, answer)
		return true
	})
}

func (tree *fakeBTree) asked() []hoarddbwire.RangeRequest {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	return slices.Clone(tree.requests)
}

func TestRangeCursorPagesWithoutRepeatsOrGaps(t *testing.T) {
	keys := make([]string, 0, 20)
	for index := range 20 {
		keys = append(keys, fmt.Sprintf("k%02d", index))
	}
	tests := []struct {
		name         string
		opts         []hoarddb.FindOption
		want         []string
		wantRequests int
	}{
		{"one page that is not full", nil, keys, 1},
		{"pages of 7", []hoarddb.FindOption{hoarddb.PageSize(7)}, keys, 3},
		{"a limit inside the first page asks for no more", []hoarddb.FindOption{hoarddb.PageSize(10), hoarddb.Limit(3)}, keys[:3], 1},
		{"a limit that ends on a page boundary asks for no more", []hoarddb.FindOption{hoarddb.PageSize(5), hoarddb.Limit(10)}, keys[:10], 2},
		{"a page that exactly fits the count needs one more request to know", []hoarddb.FindOption{hoarddb.PageSize(20)}, keys, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := &fakeBTree{keys: slices.Clone(keys)}
			server := startFake(t, tree.serve())
			client := mustConnect(t, server.uri())
			cursor, err := client.Database("app").Bucket("tree").Range(testContext(t), hoarddb.Bounds{}, test.opts...)
			if err != nil {
				t.Fatal(err)
			}
			documents, err := cursor.All(testContext(t))
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, 0, len(documents))
			for _, document := range documents {
				got = append(got, stringField(t, document, "_id_"))
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("keys = %v, want %v", got, test.want)
			}
			if asked := tree.asked(); len(asked) != test.wantRequests {
				t.Fatalf("%d requests %+v, want %d", len(asked), asked, test.wantRequests)
			}
		})
	}
}

func TestRangeCursorIterationAndClose(t *testing.T) {
	tree := &fakeBTree{keys: []string{"a", "b", "c", "d", "e"}}
	server := startFake(t, tree.serve())
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	bucket := client.Database("app").Bucket("tree")

	cursor, err := bucket.Range(ctx, hoarddb.Bounds{}, hoarddb.PageSize(2))
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	for cursor.Next(ctx) {
		seen = append(seen, stringField(t, cursor.Document(), "_id_"))
	}
	if err := cursor.Err(); err != nil || !slices.Equal(seen, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("Next loop = %v, %v", seen, err)
	}
	if cursor.Next(ctx) {
		t.Fatal("Next after the end returned true")
	}

	cursor, err = bucket.Range(ctx, hoarddb.Bounds{}, hoarddb.PageSize(2))
	if err != nil {
		t.Fatal(err)
	}
	var iterated []string
	for document, err := range cursor.Documents(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		iterated = append(iterated, stringField(t, document, "_id_"))
		if len(iterated) == 3 {
			break
		}
	}
	if !slices.Equal(iterated, []string{"a", "b", "c"}) {
		t.Fatalf("Documents with an early break = %v", iterated)
	}

	cursor, err = bucket.Range(ctx, hoarddb.Bounds{}, hoarddb.PageSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next(ctx) {
		t.Fatal("no first document")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if cursor.Next(ctx) {
		t.Fatal("Next after Close returned true")
	}
	if err := cursor.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	if total := cursor.Total(); total != -1 {
		t.Fatalf("Total of a range = %d; BTREE_RANGE reports no count, so it is -1", total)
	}
}

func TestRangeCursorReportsAnErrorOnALaterPage(t *testing.T) {
	var requests atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		if requests.Add(1) == 1 {
			var answer hoarddbwire.RangeResponse
			for _, key := range []string{"a", "b"} {
				value, _ := marshal(hoarddb.Map{"_id_": key})
				answer.Entries = append(answer.Entries, hoarddbwire.RangeEntry{Key: key, Value: value})
			}
			c.send(request, hoarddbwire.OpBTreeResponse, answer)
			return true
		}
		c.errorFrame(request, int32(hoarddbwire.CodeUnavailable), "the cluster is busy")
		return true
	}))
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	cursor, err := client.Database("app").Bucket("tree").Range(ctx, hoarddb.Bounds{}, hoarddb.PageSize(2))
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(ctx)
	if !errors.Is(err, hoarddb.ErrUnavailable) || len(documents) != 2 {
		t.Fatalf("All = %d documents, %v; want the two read, then the error", len(documents), err)
	}
	if !errors.Is(cursor.Err(), hoarddb.ErrUnavailable) || cursor.Next(ctx) {
		t.Fatalf("the cursor must stay failed: Err=%v", cursor.Err())
	}
}

func TestACursorStartsNoGoroutine(t *testing.T) {
	tree := &fakeBTree{keys: []string{"a", "b", "c", "d"}}
	server := startFake(t, tree.serve())
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	// The first Range dials the bulk lane's connection, which has a reader of its
	// own; that goroutine is the pool's, so it is counted before the cursors.
	if _, err := client.Database("app").Bucket("tree").Range(ctx, hoarddb.Bounds{}); err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	for range 50 {
		cursor, err := client.Database("app").Bucket("tree").Range(ctx, hoarddb.Bounds{}, hoarddb.PageSize(1))
		if err != nil {
			t.Fatal(err)
		}
		cursor.Next(ctx) // abandoned half read, never closed
	}
	time.Sleep(10 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("%d goroutines before 50 abandoned cursors, %d after; a cursor must hold none", before, after)
	}
}

func TestDocumentLookupAndValueAccessors(t *testing.T) {
	doc, err := marshal(hoarddb.Map{
		"Name":   "Ada",
		"Small":  int32(7),
		"Big":    int64(1 << 40),
		"Pi":     3.5,
		"Active": true,
		"At":     time.UnixMilli(1767225600123).UTC(),
		"Nested": hoarddb.Map{"City": "London", "Zip": int32(42)},
	})
	if err != nil {
		t.Fatal(err)
	}
	document := hoarddb.Document(doc)

	if text, ok := mustLookup(t, document, "Name").AsString(); !ok || text != "Ada" {
		t.Errorf("Name = %q, %v", text, ok)
	}
	for field, want := range map[string]int64{"Small": 7, "Big": 1 << 40} {
		if number, ok := mustLookup(t, document, field).Int64(); !ok || number != want {
			t.Errorf("%s as Int64 = %d, %v; an integer reads as Int64 at either width", field, number, ok)
		}
	}
	if number, ok := mustLookup(t, document, "Pi").Float64(); !ok || number != 3.5 {
		t.Errorf("Pi = %v, %v", number, ok)
	}
	if number, ok := mustLookup(t, document, "Small").Float64(); !ok || number != 7 {
		t.Errorf("an integer reads as Float64: %v, %v", number, ok)
	}
	if flag, ok := mustLookup(t, document, "Active").Bool(); !ok || !flag {
		t.Errorf("Active = %v, %v", flag, ok)
	}
	if moment, ok := mustLookup(t, document, "At").Time(); !ok || moment.UnixMilli() != 1767225600123 {
		t.Errorf("At = %v, %v; a datetime is exact to the millisecond", moment, ok)
	}
	if text, ok := mustLookup(t, document, "Nested", "City").AsString(); !ok || text != "London" {
		t.Errorf("Nested.City = %q, %v", text, ok)
	}

	// A wrong guess is false, never a silent zero.
	if _, ok := mustLookup(t, document, "Name").Int64(); ok {
		t.Error("a string read as an integer")
	}
	if _, ok := mustLookup(t, document, "Pi").Int64(); ok {
		t.Error("a double read as an integer")
	}
	if _, ok := mustLookup(t, document, "Small").AsString(); ok {
		t.Error("an integer read as a string")
	}
	if _, ok := mustLookup(t, document, "Active").Time(); ok {
		t.Error("a boolean read as a time")
	}
	if _, found := document.Lookup("Missing"); found {
		t.Error("a missing field was found")
	}
	if _, found := document.Lookup("Nested", "Missing"); found {
		t.Error("a missing nested field was found")
	}
	if _, found := document.Lookup("Name", "Deeper"); found {
		t.Error("a path through a string was found")
	}

	var person struct {
		Name  string `bson:"Name"`
		Small int32  `bson:"Small"`
	}
	if err := document.Decode(&person); err != nil || person.Name != "Ada" || person.Small != 7 {
		t.Errorf("Decode = %+v, %v", person, err)
	}
	var city string
	if err := mustLookup(t, document, "Nested", "City").Decode(&city); err != nil || city != "London" {
		t.Errorf("Value.Decode = %q, %v", city, err)
	}
	if text := document.String(); !strings.Contains(text, `"Name":"Ada"`) && !strings.Contains(text, `"Name": "Ada"`) {
		t.Errorf("String = %s; want relaxed Extended JSON", text)
	}
	if strings.Contains(document.String(), "$numberInt") {
		t.Errorf("String = %s; the readable form has plain integers", document.String())
	}
	if text := hoarddb.Document([]byte{1, 2, 3}).String(); text == "" {
		t.Error("a malformed document must still print something for a log line")
	}
	if text := mustLookup(t, document, "Small").String(); text != "7" {
		t.Errorf("Value.String = %q", text)
	}
}

func mustLookup(t *testing.T, document hoarddb.Document, path ...string) hoarddb.Value {
	t.Helper()
	value, found := document.Lookup(path...)
	if !found {
		t.Fatalf("no field %v in %s", path, document)
	}
	return value
}

// Each public field of Update reaches the wire under the name the handbook
// gives it (handbook/operations.md section 2), alone in its request: a field
// sent under another operator's name would change a document the caller did
// not mean to.
func TestEveryUpdateFieldReachesItsWireName(t *testing.T) {
	var mu sync.Mutex
	var sent []hoarddb.Document
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		mu.Lock()
		sent = append(sent, bytes.Clone(request.Payload))
		mu.Unlock()
		c.send(request, hoarddbwire.OpUpdate, hoarddbwire.UpdateResponse{Success: true})
		return true
	}))
	client := mustConnect(t, server.uri())
	bucket := client.Database("app").Bucket("users")
	tests := []struct {
		field  string
		wire   string
		update hoarddb.Update
	}{
		{"Set", "set", hoarddb.Update{Set: hoarddb.Map{"A": 1}}},
		{"Unset", "unset", hoarddb.Update{Unset: []string{"A"}}},
		{"Inc", "inc", hoarddb.Update{Inc: map[string]int64{"A": 1}}},
		{"Push", "push", hoarddb.Update{Push: hoarddb.Map{"A": 1}}},
		{"AddToSet", "addtoset", hoarddb.Update{AddToSet: hoarddb.Map{"A": 1}}},
		{"Mul", "mul", hoarddb.Update{Mul: map[string]float64{"A": 1.5}}},
		{"Min", "min", hoarddb.Update{Min: hoarddb.Map{"A": 1}}},
		{"Max", "max", hoarddb.Update{Max: hoarddb.Map{"A": 1}}},
		{"Rename", "rename", hoarddb.Update{Rename: map[string]string{"A": "B"}}},
	}
	operators := []string{"set", "unset", "inc", "push", "addtoset", "mul", "min", "max", "rename"}
	for _, test := range tests {
		t.Run(test.field, func(t *testing.T) {
			mu.Lock()
			sent = nil
			mu.Unlock()
			if _, err := bucket.Update(testContext(t), "ada", test.update); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(sent) != 1 {
				t.Fatalf("%d requests were sent, want 1", len(sent))
			}
			for _, name := range operators {
				_, present := sent[0].Lookup(name)
				if present != (name == test.wire) {
					t.Errorf("Update.%s: the request has the member %q = %v, want only %q: %s", test.field, name, present, test.wire, sent[0])
				}
			}
		})
	}
}
