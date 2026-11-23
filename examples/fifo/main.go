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

// Command fifo uses a first-in-first-out bucket as a work queue: push orders,
// pop them oldest first until the queue says it is empty.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/fifo
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
	orders, err := database.CreateBucket(ctx, "orders", hoarddb.FIFO, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)

	for number := 1; number <= 3; number++ {
		_, err = orders.Push(ctx, hoarddb.Map{"Number": number})
		fail("pushing an order", err)
	}
	fmt.Println("pushed 3 orders")
	for {
		batch, err := orders.Pop(ctx, hoarddb.WithLimit(2)) // up to two at once; a pop consumes, so it is never retried
		fail("popping", err)
		if len(batch) == 0 { // nothing left is an empty list, not an error
			fmt.Println("queue drained")
			return
		}
		for _, order := range batch { // in pop order
			number, _ := order.Lookup("Number")
			shipped, _ := number.Int64()
			fmt.Println("shipping order", shipped)
		}
	}
}

func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
