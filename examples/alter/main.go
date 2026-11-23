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

// Command alter changes how many copies of a bucket a cluster keeps.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/alter
//
// On a single node there is no second node to copy to: the server refuses, and
// its sentence says how to form a cluster.
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
	database, err := client.DefaultDatabase() // the database the URI names
	fail("opening the database", err)
	_, err = database.CreateBucket(ctx, "invoices", hoarddb.Hash, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)

	result, err := database.AlterBucket(ctx, "invoices", hoarddb.AlterBucketOptions{ReplicationFactor: 3})
	switch {
	case errors.Is(err, hoarddb.ErrInvalidArgument):
		// An operator must act (add members, lower the factor): the sentence names the fix.
		fmt.Println("refused:", err)
	case errors.Is(err, hoarddb.ErrUnavailable):
		// The driver retried until the deadline; nothing changed, so try again later.
		fmt.Println("try again later:", err)
	default:
		fail("altering the bucket", err)
		fmt.Printf("invoices keeps %d copies (was %d); copying to %v\n",
			result.ReplicationFactor, result.PreviousFactor, result.Pending)
	}
}

// fail ends the example with what it was doing; the driver's own message says
// what to do next.
func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
