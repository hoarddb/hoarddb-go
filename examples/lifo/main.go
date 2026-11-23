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

// Command lifo uses a last-in-first-out bucket as a stack, here a browsing
// history: the newest page comes back first.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/lifo
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
	history, err := database.CreateBucket(ctx, "history", hoarddb.LIFO, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)

	for page := 1; page <= 3; page++ {
		_, err = history.Push(ctx, hoarddb.Map{"Page": page})
		fail("pushing a page", err)
	}
	fmt.Println("pushed 3 pages")
	for {
		visited, err := history.Pop(ctx) // one document by default, in a list
		fail("popping", err)
		if len(visited) == 0 { // nothing left is an empty list, not an error
			fmt.Println("stack drained")
			return
		}
		page, _ := visited[0].Lookup("Page")
		number, _ := page.Int64()
		fmt.Println("back to page", number)
	}
}

func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
