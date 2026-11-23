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

// Command blob stores a file's bytes, with metadata, in a blob bucket and reads
// them back. A blob travels in one frame, so keep it under the server's limit.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run ./examples/blob
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
	files, err := database.CreateBucket(ctx, "files", hoarddb.Blob, hoarddb.WithIfNotExists())
	fail("creating the bucket", err)

	receipt, err := files.PutBlob(ctx, "greeting.txt", []byte("hello world"),
		hoarddb.WithMetadata(hoarddb.Map{"Type": "text/plain"}))
	fail("storing the blob", err)
	fmt.Println("stored", receipt.Size, "bytes")
	stored, err := files.GetBlob(ctx, "greeting.txt")
	fail("reading the blob", err)
	fmt.Printf("read back: %s\n", stored.Data)
	kind, _ := stored.Metadata.Lookup("Type")
	text, _ := kind.AsString()
	fmt.Println("type:", text)
}

func fail(doing string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", doing, err)
	}
}
