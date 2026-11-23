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

// Command hash stores, reads and deletes documents in a hash bucket, the
// store type for lookups by key.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/hash
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

type account struct {
	Name string `bson:"Name"`
	Age  int64  `bson:"Age"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := hoarddb.Connect(ctx, "") // an empty string reads HOARDDB_URI
	fail("connecting", err)
	defer func() { _ = client.Close() }()
	database, err := client.DefaultDatabase() // the database the URI names
	fail("opening the database", err)
	accounts, err := database.CreateBucket(ctx, "accounts", hoarddb.Hash, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)

	_, err = accounts.Put(ctx, "ada", account{Name: "Ada", Age: 36})
	fail("storing ada", err)
	fmt.Println("stored ada")
	ada, err := hoarddb.GetAs[account](ctx, accounts, "ada")
	fail("reading ada", err)
	fmt.Printf("read back: %s, %d\n", ada.Name, ada.Age)
	deleted, err := accounts.Delete(ctx, "ada")
	fail("deleting ada", err)
	fmt.Println("deleted ada:", deleted)
	if _, err = hoarddb.GetAs[account](ctx, accounts, "ada"); errors.Is(err, hoarddb.ErrNotFound) {
		fmt.Println("gone: not found") // a miss is an error, never an empty document
	}
}

// fail ends the example with what it was doing; the driver's own message says
// what to do next.
func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
