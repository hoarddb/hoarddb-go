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

// Command btree keeps documents in key order and reads them back by range.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/btree
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
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
	fruit, err := database.CreateBucket(ctx, "fruit", hoarddb.BTree, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)

	for _, name := range []string{"pear", "cherry", "apple", "banana"} {
		_, err = fruit.Upsert(ctx, name, hoarddb.Map{"Name": name})
		fail("storing "+name, err)
	}
	first, err := fruit.First(ctx)
	fail("reading the first key", err)
	last, err := fruit.Last(ctx)
	fail("reading the last key", err)
	fmt.Println("smallest key:", first.Key, "largest key:", last.Key)

	cursor, err := fruit.Range(ctx, hoarddb.Bounds{Gte: "a", Lte: "d"}) // both ends inclusive
	fail("ranging", err)
	defer func() { _ = cursor.Close() }()
	var names []string
	for entry, err := range cursor.Documents(ctx) {
		fail("paging", err)
		name, _ := entry.Lookup("Name")
		text, _ := name.AsString()
		names = append(names, text)
	}
	fmt.Println("a to c:", strings.Join(names, ", "))
}

func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
