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

// Command placement creates a hash bucket whose documents spread over a
// cluster by key, reads the placement back, and shows the server's refusal when
// a queue is asked to spread.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/placement
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

	events, err := database.CreateBucket(ctx, "events", hoarddb.Hash, hoarddb.WithIfNotExists(),
		hoarddb.WithPlacement(hoarddb.PlacementSpread)) // the default is PlacementWhole
	fail("creating the bucket", err)
	stats, err := events.Stats(ctx)
	fail("reading the stats", err)
	fmt.Println("events placement:", stats.Placement)

	// Only hash buckets spread; the server's refusal names the type and the fix.
	_, err = database.CreateBucket(ctx, "jobs", hoarddb.FIFO, hoarddb.WithIfNotExists(),
		hoarddb.WithPlacement(hoarddb.PlacementSpread))
	fmt.Println("fifo refused:", err != nil)
}

// fail ends the example with what it was doing; the driver's own message says
// what to do next.
func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
