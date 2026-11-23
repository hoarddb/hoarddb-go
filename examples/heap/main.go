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

// Command heap uses a heap bucket as a priority queue: the job with the
// highest priority is popped first, whatever order the jobs went in.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/heap
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

type job struct {
	Task     string `bson:"Task"`
	Priority int64  `bson:"Priority"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := hoarddb.Connect(ctx, "") // an empty string reads HOARDDB_URI
	fail("connecting", err)
	defer func() { _ = client.Close() }()
	database, err := client.DefaultDatabase()
	fail("opening the database", err)
	jobs, err := database.CreateBucket(ctx, "jobs", hoarddb.Heap, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)

	for _, queued := range []job{{"cleanup", 1}, {"backup", 9}, {"report", 5}} {
		_, err = jobs.Push(ctx, queued, hoarddb.WithPriority(queued.Priority)) // a heap push needs a priority
		fail("pushing "+queued.Task, err)
	}
	fmt.Println("pushed 3 jobs")
	for {
		popped, err := jobs.Pop(ctx)
		fail("popping", err)
		if len(popped) == 0 { // nothing left is an empty list, not an error
			fmt.Println("heap drained")
			return
		}
		var next job
		fail("decoding a job", popped[0].Decode(&next))
		fmt.Printf("next job: %s (priority %d)\n", next.Task, next.Priority)
	}
}

func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
