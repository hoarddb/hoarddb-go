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

// Command errors shows the two errors a program most often handles: a missing
// bucket, whose message names the fix, and a duplicate key, which a program
// branches on.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/errors
package main

import (
	"context"
	"errors"
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

	// The message is the server's own sentence: what is missing, then the command that fixes it.
	_, err = database.Bucket("no-such-bucket").Get(ctx, "ada")
	if errors.Is(err, hoarddb.ErrNotFound) {
		fmt.Println("not found:", err)
	}

	members, err := database.CreateBucket(ctx, "members", hoarddb.Hash, hoarddb.WithIfNotExists(),
		hoarddb.WithIndexes(hoarddb.IndexSpec{Fields: []string{"Email"}, Unique: true}))
	fail("creating the bucket", err)
	_, err = members.Upsert(ctx, "ada", hoarddb.Map{"Email": "ada@example.com"})
	fail("storing ada", err)
	_, err = members.Upsert(ctx, "ada2", hoarddb.Map{"Email": "ada@example.com"})
	if errors.Is(err, hoarddb.ErrDuplicateKey) { // matched by class, never by message text
		fmt.Println("duplicate key: ada@example.com is taken")
	} else {
		fail("storing a second ada", err)
	}
}

func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
