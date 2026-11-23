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

package hoarddb_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

// These examples are compiled by go test and are the code the README shows.
// They have no Output comment because they need a running server; the
// integration tests run the same calls against one.

// Connect with the fingerprint the server printed at startup. The password comes
// from HOARDDB_ROOT_PASSWORD, the user from HOARDDB_ROOT_USER (default "admin").
func ExampleConnect() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := hoarddb.Connect(ctx,
		"hoarddb://localhost:7433/?fingerprint=SHA256:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68")
	if err != nil {
		log.Fatal(err) // the message says what to do next
	}
	defer func() { _ = client.Close() }()

	if err := client.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("server version:", client.ServerInfo().Version)
}

// Branch on the class of an error with errors.Is.
func ExampleError() {
	ctx := context.Background()
	_, err := hoarddb.Connect(ctx, "hoarddb://localhost:7433/")
	switch {
	case errors.Is(err, hoarddb.ErrUntrustedServer):
		fmt.Println("pin the server with ?fingerprint=, or see WithTrustPrompt")
	case errors.Is(err, hoarddb.ErrAuthFailed):
		fmt.Println("wrong user or password")
	case errors.Is(err, hoarddb.ErrUnreachable):
		fmt.Println("nothing is listening there")
	}
}

// Several hosts: the first that answers is used, the others are a fallback. One
// fingerprint pins every host; or give one per host in the order written.
func ExampleWithFingerprint() {
	client, err := hoarddb.Connect(context.Background(),
		"hoarddb://root@db1:7433,db2:7433/app",
		hoarddb.WithFingerprint(
			"SHA256:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68",
			"SHA256:0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"),
		hoarddb.WithCredentials("root", "s3cr3t"))
	if err != nil {
		log.Fatal(err)
	}
	_ = client.Close()
}

// Create a bucket that carries its own stores: every user has a purchase queue
// and a friends table of their own. WithIfNotExists makes the setup re-runnable.
func ExampleDatabase_CreateBucket() {
	ctx := context.Background()
	client, err := hoarddb.Connect(ctx, "hoarddb://localhost:7433/app?fingerprint=SHA256:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	users, err := client.Database("app").CreateBucket(ctx, "users", hoarddb.Hash,
		hoarddb.WithIfNotExists(),
		hoarddb.WithStores(
			hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO},
			hoarddb.StoreSpec{Name: "Friends", Type: hoarddb.Hash}),
		hoarddb.WithFieldTypes(hoarddb.Strict,
			hoarddb.FieldSpec{Name: "Name", Type: "string", Required: true},
			hoarddb.FieldSpec{Name: "Age", Type: "int"}))
	if err != nil {
		log.Fatal(err)
	}
	stats, err := users.Stats(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(stats.Type, stats.Entries, len(stats.Stores))
}

// Branch on what went wrong with errors.Is: a bucket that is already there, or
// one that is not.
func ExampleDatabase_DropBucket() {
	ctx := context.Background()
	client, err := hoarddb.Connect(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	err = client.Database("app").DropBucket(ctx, "users")
	switch {
	case errors.Is(err, hoarddb.ErrNotFound):
		fmt.Println("nothing to drop:", err) // the message names the command that lists what exists
	case err != nil:
		log.Fatal(err)
	}
}

// Info asks what a bucket is declared as, in one round trip, without counting
// the elements of its store fields (Stats does that).
func ExampleBucket_Info() {
	ctx := context.Background()
	client, err := hoarddb.Connect(ctx, "hoarddb://localhost:7433/?fingerprint="+
		"SHA256:3b7c1f0a9d2e4c6b8a0f5d3e7c1b9a4f6d2e8c0b5a7f9d1e3c5b7a9f0d2e4c68")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	declaration, err := client.Database("app").Bucket("users").Info(ctx)
	if errors.Is(err, hoarddb.ErrNotFound) {
		log.Fatal("no such bucket: create it first") // the error's own message carries the exact command
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s is a %s bucket with %d entries\n", declaration.Name, declaration.Type, declaration.Entries)
	for _, field := range declaration.StoreFields { // label each store field of a document
		fmt.Printf("  %s: %s\n", field.Name, field.Engine)
	}
}

// Open confirms a bucket exists and reads its store type; Drop removes it.
func ExampleDatabase_Open() {
	ctx := context.Background()
	client, err := hoarddb.Connect(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	jobs, err := client.Database("app").Open(ctx, "jobs")
	if errors.Is(err, hoarddb.ErrNotFound) {
		log.Fatal(err) // the message names the command that creates it
	}
	fmt.Println(jobs.Type())
	if err := jobs.Drop(ctx); err != nil {
		log.Fatal(err)
	}
}
