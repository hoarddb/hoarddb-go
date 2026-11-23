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
	"strings"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// Real-server tests of store fields (handbook/build-go.md step 9). Each starts
// its own server and needs HOARDDB_SERVER_BIN.

// userStores is the User bucket of the handbook's example: one store field of
// each of the six types, and a store nested inside the elements of Recipes.
var userStores = []hoarddb.StoreSpec{
	{Name: "Purchases", Type: hoarddb.FIFO},
	{Name: "Photos", Type: hoarddb.LIFO},
	{Name: "Friends", Type: hoarddb.Hash},
	{Name: "Recipes", Type: hoarddb.BTree, Stores: []hoarddb.StoreSpec{{Name: "Steps", Type: hoarddb.FIFO}}},
	{Name: "Books", Type: hoarddb.Heap},
	{Name: "Data", Type: hoarddb.Blob},
}

// usersWith creates bucket User with every store field and the named parent
// documents in it.
func usersWith(t *testing.T, client *hoarddb.Client, keys ...string) *hoarddb.Bucket {
	t.Helper()
	ctx := testContext(t)
	users, err := client.Database("app").CreateBucket(ctx, "User", hoarddb.Hash, hoarddb.WithStores(userStores...))
	if err != nil {
		t.Fatalf("CreateBucket User: %v", err)
	}
	for _, key := range keys {
		if _, err := users.Put(ctx, key, hoarddb.Map{"Name": key}); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	return users
}

// Failure 14 of handbook/build-go.md section 2: key is the parent document's
// key and element the element's. Two users with a queue and a table each, so a
// driver that sent the element as the key, or dropped the key, would cross or
// lose them.
func TestEachDocumentHasItsOwnStore(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada", "bob")

	for _, owner := range []string{"ada", "bob"} {
		purchases := users.Field(owner, "Purchases")
		for _, item := range []string{"tea", "ink"} {
			if _, err := purchases.Push(ctx, hoarddb.Map{"Item": owner + "-" + item}); err != nil {
				t.Fatalf("%s Push: %v", owner, err)
			}
		}
		if _, err := users.Field(owner, "Friends").Put(ctx, "friend", hoarddb.Map{"Name": owner + "'s friend"}); err != nil {
			t.Fatalf("%s Put: %v", owner, err)
		}
	}
	for _, owner := range []string{"ada", "bob"} {
		first, err := onlyDocument(users.Field(owner, "Purchases").Pop(ctx))
		if err != nil || stringField(t, first, "Item") != owner+"-tea" {
			t.Fatalf("%s popped %s, %v; want %s-tea, the oldest of its own queue", owner, first, err, owner)
		}
		if length, err := users.Field(owner, "Purchases").Len(ctx); err != nil || length != 1 {
			t.Fatalf("%s Purchases Len = %d, %v; want 1", owner, length, err)
		}
		friend, err := users.Field(owner, "Friends").Get(ctx, "friend")
		if err != nil || stringField(t, friend, "Name") != owner+"'s friend" {
			t.Fatalf("%s Get = %s, %v; want its own friend", owner, friend, err)
		}
	}
	for _, owner := range []string{"ada", "bob"} {
		last, err := onlyDocument(users.Field(owner, "Purchases").Pop(ctx))
		if err != nil || stringField(t, last, "Item") != owner+"-ink" {
			t.Fatalf("%s popped %s, %v; want %s-ink", owner, last, err, owner)
		}
		if drained, err := users.Field(owner, "Purchases").Pop(ctx); err != nil || len(drained) != 0 {
			t.Fatalf("%s Pop of its drained queue = %v, %v; want an empty list and a nil error", owner, drained, err)
		}
	}
}

func TestFieldStoreVerbsPerEngine(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")

	t.Run("fifo", func(t *testing.T) {
		store := users.Field("ada", "Purchases")
		receipt, err := store.Push(ctx, hoarddb.Map{"Item": "tea"})
		if err != nil || receipt.ID == "" {
			t.Fatalf("Push = %+v, %v; want the assigned entry id", receipt, err)
		}
		if _, err := store.Push(ctx, hoarddb.Map{"Item": "ink"}); err != nil {
			t.Fatal(err)
		}
		peeked, err := store.Peek(ctx, hoarddb.WithLimit(5))
		if err != nil || len(peeked) != 2 || stringField(t, peeked[0], "Item") != "tea" || stringField(t, peeked[1], "Item") != "ink" {
			t.Fatalf("Peek(5) = %v, %v; want tea then ink, in pop order", peeked, err)
		}
		one, err := store.Peek(ctx)
		if err != nil || len(one) != 1 {
			t.Fatalf("Peek = %v, %v; want one entry by default", one, err)
		}
		if length, err := store.Len(ctx); err != nil || length != 2 {
			t.Fatalf("Len after a peek = %d, %v; a peek removes nothing", length, err)
		}
		popped, err := onlyDocument(store.Pop(ctx))
		if err != nil || stringField(t, popped, "Item") != "tea" {
			t.Fatalf("Pop = %s, %v; want the oldest, tea", popped, err)
		}
	})

	t.Run("lifo", func(t *testing.T) {
		store := users.Field("ada", "Photos")
		for _, title := range []string{"first", "second"} {
			if _, err := store.Push(ctx, hoarddb.Map{"Title": title}); err != nil {
				t.Fatal(err)
			}
		}
		popped, err := onlyDocument(store.Pop(ctx))
		if err != nil || stringField(t, popped, "Title") != "second" {
			t.Fatalf("Pop = %s, %v; want the newest, second", popped, err)
		}
	})

	t.Run("heap", func(t *testing.T) {
		store := users.Field("ada", "Books")
		for title, priority := range map[string]int64{"low": 1, "high": 9, "middle": 5} {
			if _, err := store.Push(ctx, hoarddb.Map{"Title": title}, hoarddb.WithPriority(priority)); err != nil {
				t.Fatal(err)
			}
		}
		popped, err := onlyDocument(store.Pop(ctx))
		if err != nil || stringField(t, popped, "Title") != "high" {
			t.Fatalf("Pop = %s, %v; want the highest priority, high", popped, err)
		}
	})

	for _, name := range []string{"Friends", "Recipes"} {
		t.Run(name, func(t *testing.T) {
			store := users.Field("ada", name)
			receipt, err := store.Put(ctx, "grace", hoarddb.Map{"Name": "Grace", "Born": int32(1906)})
			if err != nil || receipt.ID != "grace" {
				t.Fatalf("Put = %+v, %v; want the element key as the receipt's ID", receipt, err)
			}
			got, err := store.Get(ctx, "grace")
			if err != nil || stringField(t, got, "Name") != "Grace" || intField(t, got, "Born") != 1906 {
				t.Fatalf("Get = %s, %v", got, err)
			}
			if length, err := store.Len(ctx); err != nil || length != 1 {
				t.Fatalf("Len = %d, %v; want 1", length, err)
			}
			if _, err := store.Get(ctx, "nobody"); !errors.Is(err, hoarddb.ErrNotFound) {
				t.Fatalf("Get of a missing element: %v; want ErrNotFound", err)
			}
			if removed, err := store.Delete(ctx, "grace"); err != nil || !removed {
				t.Fatalf("Delete = %v, %v; want true", removed, err)
			}
			if removed, err := store.Delete(ctx, "grace"); err != nil || removed {
				t.Fatalf("Delete again = %v, %v; want false and no error", removed, err)
			}
		})
	}

	t.Run("blob", func(t *testing.T) {
		store := users.Field("ada", "Data")
		content := []byte("Notes by the translator")
		receipt, err := store.PutBlob(ctx, "notes.txt", content, hoarddb.WithMetadata(hoarddb.Map{"ContentType": "text/plain"}))
		if err != nil || receipt.ID != "notes.txt" || receipt.BlobID == "" || receipt.Size != int64(len(content)) {
			t.Fatalf("PutBlob = %+v, %v", receipt, err)
		}
		blob, err := store.GetBlob(ctx, "notes.txt")
		if err != nil || !bytes.Equal(blob.Data, content) || blob.Key != "notes.txt" || blob.BlobID != receipt.BlobID ||
			blob.Size != int64(len(content)) || stringField(t, blob.Metadata, "ContentType") != "text/plain" {
			t.Fatalf("GetBlob = %+v, %v", blob, err)
		}
		if missing, err := store.GetBlob(ctx, "nope.txt"); !errors.Is(err, hoarddb.ErrNotFound) || missing != nil {
			t.Fatalf("GetBlob of a missing element = %+v, %v; want ErrNotFound and no blob", missing, err)
		}
	})
}

// The wire carries the nesting as path and the inner store's own name as field,
// and the parent document's key stays in key.
func TestNestedStoreThroughAnElement(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")
	if _, err := users.Field("ada", "Recipes").Put(ctx, "pie", hoarddb.Map{"Serves": int32(8)}); err != nil {
		t.Fatal(err)
	}

	steps := users.Field("ada", "Recipes").Field("pie", "Steps")
	for _, step := range []string{"mix", "bake"} {
		if _, err := steps.Push(ctx, hoarddb.Map{"Do": step}); err != nil {
			t.Fatalf("nested Push: %v", err)
		}
	}
	first, err := onlyDocument(steps.Pop(ctx))
	if err != nil || stringField(t, first, "Do") != "mix" {
		t.Fatalf("nested Pop = %s, %v; want mix", first, err)
	}
	if length, err := steps.Len(ctx); err != nil || length != 1 {
		t.Fatalf("nested Len = %d, %v; want 1", length, err)
	}
	// The sibling store of another element is its own store.
	if _, err := users.Field("ada", "Recipes").Put(ctx, "tart", hoarddb.Map{"Serves": int32(4)}); err != nil {
		t.Fatal(err)
	}
	if got, err := users.Field("ada", "Recipes").Field("tart", "Steps").Pop(ctx); err != nil || len(got) != 0 {
		t.Fatalf("Pop of tart's Steps = %v, %v; want an empty list, the steps of pie are not tart's", got, err)
	}

	var pushes []hoarddbwire.FieldRequest
	for _, exchange := range proxy.Exchanges() {
		if exchange.Request.Op != hoarddbwire.OpFieldPush {
			continue
		}
		var request hoarddbwire.FieldRequest
		if err := codec.Unmarshal(exchange.Request.Payload, &request); err != nil {
			t.Fatal(err)
		}
		pushes = append(pushes, request)
	}
	if len(pushes) != 2 {
		t.Fatalf("%d FIELD_PUSH requests, want 2", len(pushes))
	}
	for _, request := range pushes {
		if request.Key != "ada" || request.Field != "Steps" || request.Element != "" || !slices.Equal(request.Path, []any{"Recipes", "pie"}) {
			t.Fatalf("nested push sent key %q field %q element %q path %v; want key ada, field Steps, path [Recipes pie]",
				request.Key, request.Field, request.Element, request.Path)
		}
	}
}

func TestUndeclaredFieldIsNotFound(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")
	_, err := users.Field("ada", "Enemies").Len(ctx)
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrNotFound) || !errors.As(err, &failure) || !strings.Contains(failure.Message, `declares no store field named "Enemies"`) {
		t.Fatalf("Len of an undeclared field: %v; want ErrNotFound with the server's sentence", err)
	}
	if _, err := users.Field("ada", "Enemies").Push(ctx, hoarddb.Map{"a": 1}); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("Push onto an undeclared field: %v; want ErrNotFound", err)
	}
}

func TestFieldOnMissingParentIsNotFound(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")
	_, err := users.Field("nobody", "Purchases").Push(ctx, hoarddb.Map{"Item": "tea"})
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrNotFound) || !errors.As(err, &failure) || !strings.Contains(failure.Message, "parent document not found") {
		t.Fatalf("Push onto a missing parent's field: %v; want ErrNotFound with the server's sentence", err)
	}
	if _, err := users.Field("nobody", "Friends").Get(ctx, "x"); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("Get from a missing parent's field: %v; want ErrNotFound", err)
	}
}

// A miss is a successful frame with found false; read naively it is an empty
// document and no error (failure 2).
func TestFieldMissesAreNeverEmptyDocuments(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")
	for _, name := range []string{"Purchases", "Photos", "Books"} {
		store := users.Field("ada", name)
		if got, err := store.Pop(ctx); err != nil || len(got) != 0 {
			t.Fatalf("Pop of an empty %s = %v, %v; want an empty list and a nil error", name, got, err)
		}
		if got, err := store.Peek(ctx); err != nil || len(got) != 0 {
			t.Fatalf("Peek of an empty %s = %v, %v; want an empty list and a nil error", name, got, err)
		}
	}
	if got, err := users.Field("ada", "Friends").Get(ctx, "nobody"); !errors.Is(err, hoarddb.ErrNotFound) || got != nil {
		t.Fatalf("Get of a missing element = %s, %v; want ErrNotFound and no document", got, err)
	}
}

func TestFieldVerbOnWrongEngineIsPassedThrough(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")
	_, err := users.Field("ada", "Purchases").Put(ctx, "x", hoarddb.Map{"Item": "tea"})
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) || !strings.Contains(failure.Message, "not an operation") {
		t.Fatalf("Put on a fifo field: %v; want ErrInvalidArgument with the server's sentence", err)
	}
	if _, err := users.Field("ada", "Friends").Push(ctx, hoarddb.Map{"Item": "tea"}); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("Push onto a hash field: %v; want ErrInvalidArgument", err)
	}
	if _, err := users.Field("ada", "Friends").Peek(ctx); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("Peek of a hash field: %v; want ErrInvalidArgument, as handbook/operations.md section 5 says", err)
	}
}

func TestFieldPriorityBelongsToPushAlone(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")
	before := len(proxy.Exchanges())
	store := users.Field("ada", "Friends")
	if _, err := store.Put(ctx, "x", hoarddb.Map{"a": 1}, hoarddb.WithPriority(3)); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("Put with a priority: %v; want ErrInvalidArgument", err)
	}
	if _, err := users.Field("ada", "Purchases").Pop(ctx, hoarddb.WithPriority(3)); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("Pop with a priority: %v; want ErrInvalidArgument", err)
	}
	if _, err := store.Delete(ctx, "x", hoarddb.WithPriority(3)); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("Delete with a priority: %v; want ErrInvalidArgument", err)
	}
	if sent := len(proxy.Exchanges()) - before; sent != 0 {
		t.Fatalf("%d requests were sent for refused calls; a local refusal sends nothing", sent)
	}
}

// Pinned like the bucket's TestHeapPushWithoutPriority: the handbook says a heap
// push needs a priority, this server accepts one without and files it at
// priority 0, and the driver, which never looks a store's type up, cannot
// refuse it (S-4; reported in the pull request).
func TestFieldHeapPushWithoutAPriority(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	books := usersWith(t, client, "ada").Field("ada", "Books")
	if _, err := books.Push(ctx, hoarddb.Map{"Title": "five"}, hoarddb.WithPriority(5)); err != nil {
		t.Fatal(err)
	}
	if _, err := books.Push(ctx, hoarddb.Map{"Title": "none"}); err != nil {
		t.Fatalf("a heap field push without a priority: %v; the server accepts it", err)
	}
	for _, want := range []string{"five", "none"} {
		popped, err := onlyDocument(books.Pop(ctx))
		if err != nil || stringField(t, popped, "Title") != want {
			t.Fatalf("Pop = %s, %v; want %s (highest priority first; none given is 0)", popped, err, want)
		}
	}
}

func TestFieldPeekRefusesABadLimitLocally(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	users := usersWith(t, client, "ada")
	before := len(proxy.Exchanges())
	if _, err := users.Field("ada", "Purchases").Peek(testContext(t), hoarddb.WithLimit(1001)); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("Peek(WithLimit(1001)): %v; want ErrInvalidArgument", err)
	}
	if len(proxy.Exchanges()) != before {
		t.Fatal("a refused peek sent a request")
	}
}

// The list pages by the opaque next, passed back unchanged (build-go.md step 9).
func TestFieldListPagesWithNext(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")
	purchases := users.Field("ada", "Purchases")
	const count = 2500
	for number := range count {
		if _, err := purchases.Push(ctx, hoarddb.Map{"N": int64(number)}); err != nil {
			t.Fatalf("Push %d: %v", number, err)
		}
	}
	before := len(proxy.Exchanges())

	cursor, err := purchases.List(ctx, hoarddb.PageSize(1000))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	documents, err := cursor.All(ctx)
	if err != nil || len(documents) != count {
		t.Fatalf("List read %d documents, %v; want %d", len(documents), err, count)
	}
	for position, doc := range documents {
		if intField(t, doc, "N") != int64(position) {
			t.Fatalf("document %d is N=%d; a fifo lists in pop order, once each", position, intField(t, doc, "N"))
		}
	}

	var asked []hoarddbwire.FieldListRequest
	var sentAfter [][]byte
	var answered []hoarddbwire.FieldListResponse
	for _, exchange := range proxy.Exchanges()[before:] {
		if exchange.Request.Op != hoarddbwire.OpFieldList {
			continue
		}
		var request hoarddbwire.FieldListRequest
		var answer hoarddbwire.FieldListResponse
		if err := codec.Unmarshal(exchange.Request.Payload, &request); err != nil {
			t.Fatal(err)
		}
		if err := codec.Unmarshal(exchange.Response.Payload, &answer); err != nil {
			t.Fatal(err)
		}
		var after []byte
		if member, present := codec.Lookup(exchange.Request.Payload, "after"); present {
			if _, bytesSent, isBinary := member.BinaryOK(); isBinary {
				after = bytesSent
			}
		}
		asked, sentAfter, answered = append(asked, request), append(sentAfter, after), append(answered, answer)
	}
	if len(asked) != 3 {
		t.Fatalf("%d FIELD_LIST requests, want 3 pages of at most 1000", len(asked))
	}
	if sentAfter[0] != nil {
		t.Errorf("page 1 asked after %x, want the start", sentAfter[0])
	}
	for position := 1; position < len(asked); position++ {
		if len(answered[position-1].Next) == 0 || !bytes.Equal(sentAfter[position], answered[position-1].Next) {
			t.Errorf("page %d asked after %x, want the previous page's next %x unchanged", position+1, sentAfter[position], answered[position-1].Next)
		}
	}
	for position, request := range asked {
		if request.Limit != 1000 || request.Key != "ada" || request.Field != "Purchases" {
			t.Errorf("page %d asked limit %d key %q field %q", position+1, request.Limit, request.Key, request.Field)
		}
	}
}

func TestFieldListRefusesWhatItCannotCarry(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	users := usersWith(t, client, "ada")
	before := len(proxy.Exchanges())
	store := users.Field("ada", "Purchases")
	for name, option := range map[string]hoarddb.FindOption{
		"Skip": hoarddb.Skip(1), "SortBy": hoarddb.SortBy("N", hoarddb.Ascending), "Projection": hoarddb.Projection("N"),
		"PageSize over 1000": hoarddb.PageSize(1001),
	} {
		if _, err := store.List(testContext(t), option); !errors.Is(err, hoarddb.ErrInvalidArgument) {
			t.Errorf("List with %s: %v; want ErrInvalidArgument and nothing sent", name, err)
		}
	}
	if len(proxy.Exchanges()) != before {
		t.Fatal("a refused List sent a request")
	}
}

func TestFieldListOfAnEmptyStoreIsAnEmptyCursor(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	users := usersWith(t, client, "ada")
	cursor, err := users.Field("ada", "Friends").List(ctx)
	if err != nil {
		t.Fatalf("List of an empty field: %v", err)
	}
	if documents, err := cursor.All(ctx); err != nil || len(documents) != 0 {
		t.Fatalf("All = %v, %v; want nothing and no error", documents, err)
	}
	if _, err := users.Field("ada", "Enemies").List(ctx); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("List of an undeclared field: %v; want ErrNotFound", err)
	}
}

// A field handle is a few strings: no I/O, no goroutine, and nothing kept per
// document, so a million documents' stores cost the client nothing until used.
func TestFieldHandlesDoNoIO(t *testing.T) {
	server := startServer(t)
	client, proxy := connectThroughProxy(t, server)
	users := client.Database("app").Bucket("User")
	before, goroutinesBefore, exchangesBefore := client.PoolStats(), runtime.NumGoroutine(), len(proxy.Exchanges())

	var kept *hoarddb.FieldStore
	for number := range 1_000_000 {
		kept = users.Field(fmt.Sprintf("user-%d", number%4096), "Purchases")
	}
	if kept == nil {
		t.Fatal("no handle")
	}
	if after := client.PoolStats(); after != before || len(proxy.Exchanges()) != exchangesBefore {
		t.Fatalf("PoolStats %+v -> %+v, %d -> %d requests; a handle must do no I/O", before, after, exchangesBefore, len(proxy.Exchanges()))
	}
	if goroutinesAfter := runtime.NumGoroutine(); goroutinesAfter > goroutinesBefore {
		t.Fatalf("%d goroutines before a million handles, %d after", goroutinesBefore, goroutinesAfter)
	}
	if allocations := testing.AllocsPerRun(1000, func() { kept = users.Field("ada", "Purchases") }); allocations > 1 {
		t.Fatalf("a field handle costs %v allocations; one small struct is the bound", allocations)
	}
	if size := reflectSize(kept); size > 64 {
		t.Fatalf("a field handle is %d bytes; a few tens of bytes is the bound", size)
	}
}
