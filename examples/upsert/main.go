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

// Command upsert shows the three ways to write a document under a key: Put
// inserts and refuses a key that is taken, Upsert replaces it, and Update
// changes some of its fields.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/upsert
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
	people, err := database.CreateBucket(ctx, "people", hoarddb.Hash, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)

	_, err = people.Delete(ctx, "ada") // start clean, so the example can run again
	fail("clearing ada", err)
	_, err = people.Put(ctx, "ada", hoarddb.Map{"Name": "Ada", "Age": 36})
	fail("inserting ada", err)
	if _, err = people.Put(ctx, "ada", hoarddb.Map{"Name": "Ada Lovelace"}); errors.Is(err, hoarddb.ErrDocumentExists) {
		fmt.Println("put refused: ada is taken") // nothing was overwritten; safe to retry
	}
	_, err = people.Upsert(ctx, "ada", hoarddb.Map{"Name": "Ada Lovelace"})
	fail("replacing ada", err)
	_, err = people.Update(ctx, "ada", hoarddb.Update{Set: hoarddb.Map{"Age": 37}})
	fail("updating ada", err)
	ada, err := people.Get(ctx, "ada")
	fail("reading ada", err)
	name, _ := ada.Lookup("Name")
	text, _ := name.AsString()
	fmt.Println("ada is now:", text)
}

// fail ends the example with what it was doing; the driver's own message says
// what to do next.
func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
