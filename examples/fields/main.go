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

// Command fields shows the idea hoardDB is built on: every user document
// carries stores of its own, Purchases a queue and Friends a hash table.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/fields
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := hoarddb.Connect(ctx, "") // an empty string reads HOARDDB_URI
	fail("connecting", err)
	defer func() { _ = client.Close() }()
	database, err := client.DefaultDatabase()
	fail("opening the database", err)
	users, err := database.CreateBucket(ctx, "User", hoarddb.Hash, hoarddb.WithIfNotExists(), hoarddb.WithStores(
		hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO},
		hoarddb.StoreSpec{Name: "Friends", Type: hoarddb.Hash}))
	fail("creating the bucket", err)
	for _, name := range []string{"ada", "bob"} {
		_, err = users.Upsert(ctx, name, hoarddb.Map{"Name": name}) // the parent document comes first; Upsert, so the example can run again
		fail("storing "+name, err)
		queue := users.Field(name, "Purchases") // no I/O: a handle costs a few bytes
		_, err = queue.Push(ctx, hoarddb.Map{"Item": "tea for " + name})
		fail("pushing a purchase", err)
		waiting, err := queue.Len(ctx) // this user's queue, not Bob's or Ada's
		fail("counting purchases", err)
		fmt.Printf("%s's purchases: %d\n", name, waiting)
		bought, err := queue.Pop(ctx) // leaves the queue as found, so the example can run again
		fail("popping", err)
		field, _ := bought[0].Lookup("Item")
		text, _ := field.AsString()
		fmt.Printf("%s bought: %s\n", name, text)
	}
	friends := users.Field("bob", "Friends")
	_, err = friends.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"})
	fail("adding a friend", err)
	friend, err := friends.Get(ctx, "ada")
	fail("reading a friend", err)
	name, _ := friend.Lookup("Name")
	text, _ := name.AsString()
	fmt.Println("bob's friend:", text)
}

func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
