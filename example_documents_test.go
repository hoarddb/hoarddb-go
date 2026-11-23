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
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

// These examples are compiled by go test and are the code the README shows. They
// need a running server, so they have no Output comment;
// TestDocumentExamplesWork runs the same calls against one.

// User is a document as the application sees it.
type User struct {
	Name   string `bson:"Name"`
	Age    int32  `bson:"Age"`
	Visits int64  `bson:"Visits"`
}

// Store a document under a key, read it back, change one field in place, and
// delete it. Get of a key that is not there is ErrNotFound, never an empty
// document.
func ExampleBucket_Put() {
	ctx := context.Background()
	client, err := hoarddb.Connect(ctx, "hoarddb://localhost:7433/app?fingerprint=SHA256:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	users := client.Database("app").Bucket("users")

	if _, err := users.Put(ctx, "ada", User{Name: "Ada", Age: 36, Visits: 1}); err != nil {
		log.Fatal(err)
	}

	doc, err := users.Get(ctx, "ada")
	if err != nil {
		log.Fatal(err)
	}
	var ada User
	if err := doc.Decode(&ada); err != nil {
		log.Fatal(err)
	}
	fmt.Println(ada.Name, ada.Age)

	after, err := users.Update(ctx, "ada", hoarddb.Update{
		Set: hoarddb.Map{"Address.City": "London"},
		Inc: map[string]int64{"Visits": 1},
	})
	if err != nil {
		log.Fatal(err)
	}
	city, _ := after.Lookup("Address", "City")
	text, _ := city.AsString()
	fmt.Println(text)

	if _, err := users.Get(ctx, "nobody"); errors.Is(err, hoarddb.ErrNotFound) {
		fmt.Println("no such user:", err)
	}
	if deleted, err := users.Delete(ctx, "ada"); err != nil || !deleted {
		log.Fatal(deleted, err)
	}
}

// A fifo bucket is a queue: push onto it, take from the front. A heap pops its
// highest priority first and needs WithPriority on every push.
func ExampleBucket_Push() {
	ctx := context.Background()
	client, err := hoarddb.Connect(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	orders := client.Database("app").Bucket("orders")

	if _, err := orders.Push(ctx, hoarddb.Map{"Item": "book", "Qty": 2}); err != nil {
		log.Fatal(err)
	}
	next, err := orders.Peek(ctx) // looks, takes nothing
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(next), "waiting")

	for {
		batch, err := orders.Pop(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if len(batch) == 0 {
			break // the queue is drained
		}
		fmt.Println("shipping", batch[0])
	}

	jobs := client.Database("app").Bucket("jobs") // a heap bucket
	if _, err := jobs.Push(ctx, hoarddb.Map{"Task": "backup"}, hoarddb.WithPriority(9)); err != nil {
		log.Fatal(err)
	}
}

// A btree bucket reads in key order. Range pages through the keys from Gte to
// Lte, both inclusive, a page at a time.
func ExampleBucket_Range() {
	ctx := context.Background()
	client, err := hoarddb.Connect(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	index := client.Database("app").Bucket("index")

	cursor, err := index.Range(ctx, hoarddb.Bounds{Gte: "a", Lte: "m"}, hoarddb.PageSize(50))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cursor.Close() }()
	for doc, err := range cursor.Documents(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(doc)
	}

	first, err := index.First(ctx)
	if errors.Is(err, hoarddb.ErrNotFound) {
		fmt.Println("the index is empty")
	} else if err == nil {
		fmt.Println("smallest key:", first.Key)
	}
}

// Find reads the documents that match a filter, a page at a time. Count,
// Distinct and Search answer in one request; Get with Consistent is the quorum
// read. A cursor is not a snapshot: pages are separate requests.
func ExampleBucket_Find() {
	ctx := context.Background()
	client, err := hoarddb.Connect(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	users := client.Database("app").Bucket("users")

	cursor, err := users.Find(ctx, hoarddb.Map{"Age": 36},
		hoarddb.SortBy("Name", hoarddb.Ascending), hoarddb.Projection("Name"), hoarddb.Limit(10))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cursor.Total(), "users are 36")
	for doc, err := range cursor.Documents(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(doc)
	}

	total, err := users.Count(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	ages, err := users.Distinct(ctx, "Age", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(total, "users of", len(ages), "different ages")

	fresh, err := users.Get(ctx, "ada", hoarddb.Consistent())
	if errors.Is(err, hoarddb.ErrNotFound) {
		fmt.Println("no such user")
	} else if err == nil {
		fmt.Println(fresh)
	}
}

// The calls of the examples above, against a real server, with what each must
// print or return.
func TestDocumentExamplesWork(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	db := client.Database("app")
	for name, kind := range map[string]hoarddb.StoreType{"users": hoarddb.Hash, "orders": hoarddb.FIFO, "jobs": hoarddb.Heap, "index": hoarddb.BTree} {
		if _, err := db.CreateBucket(ctx, name, kind); err != nil {
			t.Fatal(err)
		}
	}

	users := db.Bucket("users")
	if _, err := users.Put(ctx, "ada", User{Name: "Ada", Age: 36, Visits: 1}); err != nil {
		t.Fatal(err)
	}
	doc, err := users.Get(ctx, "ada")
	if err != nil {
		t.Fatal(err)
	}
	var ada User
	if err := doc.Decode(&ada); err != nil || ada != (User{Name: "Ada", Age: 36, Visits: 1}) {
		t.Fatalf("Decode = %+v, %v", ada, err)
	}
	after, err := users.Update(ctx, "ada", hoarddb.Update{Set: hoarddb.Map{"Address.City": "London"}, Inc: map[string]int64{"Visits": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if city, _ := after.Lookup("Address", "City"); city.String() != `"London"` {
		t.Fatalf("City = %s", city)
	}
	if _, err := users.Get(ctx, "nobody"); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("Get of nobody: %v", err)
	}
	if deleted, err := users.Delete(ctx, "ada"); err != nil || !deleted {
		t.Fatalf("Delete = %v, %v", deleted, err)
	}

	orders := db.Bucket("orders")
	if _, err := orders.Push(ctx, hoarddb.Map{"Item": "book", "Qty": 2}); err != nil {
		t.Fatal(err)
	}
	if next, err := orders.Peek(ctx); err != nil || len(next) != 1 {
		t.Fatalf("Peek = %v, %v", next, err)
	}
	shipped := 0
	for {
		batch, err := orders.Pop(ctx)
		if err != nil {
			t.Fatalf("Pop = %v, %v", batch, err)
		}
		if len(batch) == 0 {
			break
		}
		if order := batch[0]; !strings.Contains(order.String(), `"Item":"book"`) {
			t.Fatalf("Pop = %s", order)
		}
		shipped++
	}
	if shipped != 1 {
		t.Fatalf("shipped %d orders, want 1", shipped)
	}
	if _, err := db.Bucket("jobs").Push(ctx, hoarddb.Map{"Task": "backup"}, hoarddb.WithPriority(9)); err != nil {
		t.Fatal(err)
	}

	index := db.Bucket("index")
	if _, err := index.First(ctx); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("First of an empty index: %v", err)
	}
	for _, key := range []string{"apple", "fig", "zebra"} {
		if _, err := index.Put(ctx, key, hoarddb.Map{"k": key}); err != nil {
			t.Fatal(err)
		}
	}
	cursor, err := index.Range(ctx, hoarddb.Bounds{Gte: "a", Lte: "m"}, hoarddb.PageSize(50))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cursor.Close() }()
	count := 0
	for _, err := range cursor.Documents(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("Range a..m read %d documents, want apple and fig", count)
	}
	if first, err := index.First(ctx); err != nil || first.Key != "apple" {
		t.Fatalf("First = %+v, %v", first, err)
	}

	for name, age := range map[string]int32{"ada": 36, "grace": 36, "linus": 28} {
		if _, err := users.Put(ctx, name, User{Name: name, Age: age}); err != nil {
			t.Fatal(err)
		}
	}
	found, err := users.Find(ctx, hoarddb.Map{"Age": 36}, hoarddb.SortBy("Name", hoarddb.Ascending), hoarddb.Projection("Name"), hoarddb.Limit(10))
	if err != nil || found.Total() != 2 {
		t.Fatalf("Find = Total %d, %v; want 2 users aged 36", found.Total(), err)
	}
	var names []string
	for doc, err := range found.Documents(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		var named User
		if err := doc.Decode(&named); err != nil {
			t.Fatal(err)
		}
		names = append(names, named.Name)
	}
	if strings.Join(names, ",") != "ada,grace" {
		t.Fatalf("names = %v, want ada and grace in order", names)
	}
	if total, err := users.Count(ctx, nil); err != nil || total != 3 {
		t.Fatalf("Count = %d, %v", total, err)
	}
	if ages, err := users.Distinct(ctx, "Age", nil); err != nil || len(ages) != 2 {
		t.Fatalf("Distinct = %v, %v; want 2 ages", ages, err)
	}
	if fresh, err := users.Get(ctx, "ada", hoarddb.Consistent()); err != nil || !strings.Contains(fresh.String(), `"Name":"ada"`) {
		t.Fatalf("Get(Consistent) = %s, %v", fresh, err)
	}
}
