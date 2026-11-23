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
// Command consume works a queue without ever losing a job: claim jobs under a
// lease, acknowledge each one when it is done, and let Consume run the same
// loop for you. A job whose handler fails comes back after its lease.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/consume
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
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
	jobs, err := database.CreateBucket(ctx, "emails", hoarddb.FIFO, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)
	for number := 1; number <= 4; number++ {
		_, err = jobs.Push(ctx, hoarddb.Map{"Number": number})
		fail("pushing a job", err)
	}

	claimed, err := jobs.Claim(ctx, hoarddb.WithLimit(2), hoarddb.WithLease(time.Minute)) // still in the queue, hidden
	fail("claiming", err)
	fmt.Println("claimed", len(claimed.Items), "jobs")
	done, err := jobs.Ack(ctx, claimed.LeaseID) // only now are they gone
	fail("acknowledging", err)
	fmt.Println("acknowledged", done.Acked)

	var handled atomic.Int32
	consumeCtx, stop := context.WithCancel(ctx)
	err = jobs.Consume(consumeCtx, func(_ context.Context, job hoarddb.Delivery) error {
		if handled.Add(1) == 2 {
			stop() // the example ends after the last job; a real worker runs until it is told to stop
		}
		return nil // nil acknowledges; an error leaves the job for redelivery
	})
	fmt.Println("consumed", handled.Load(), "jobs; stopped:", errors.Is(err, context.Canceled)) // Consume returns when its context ends
}

func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
