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
	"bytes"
	"errors"
	"strings"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// Real-server tests of the blob verbs (handbook/build-go.md step 10). Each
// starts its own server and needs HOARDDB_SERVER_BIN.

// filesBucket creates database app with a blob bucket files.
func filesBucket(t *testing.T, client *hoarddb.Client) *hoarddb.Bucket {
	t.Helper()
	files, err := client.Database("app").CreateBucket(testContext(t), "files", hoarddb.Blob)
	if err != nil {
		t.Fatalf("CreateBucket files: %v", err)
	}
	return files
}

type fileMetadata struct {
	ContentType string `bson:"ContentType"`
	Pages       int32  `bson:"Pages"`
}

// A blob and its metadata go in and come out unchanged, and a delete takes it
// away: the verbs of one blob's life, on a real server.
func TestBlobPutGetDeleteRoundTrip(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	files := filesBucket(t, client)

	content := []byte("hello, hoardDB\x00\xff binary")
	receipt, err := files.PutBlob(ctx, "hello.bin", content, hoarddb.WithMetadata(fileMetadata{ContentType: "application/octet-stream", Pages: 3}))
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	if receipt.ID != "hello.bin" || receipt.BlobID == "" || receipt.Size != int64(len(content)) {
		t.Fatalf("receipt %+v: want the key, a blob id and the size %d", receipt, len(content))
	}

	got, err := files.GetBlob(ctx, "hello.bin")
	if err != nil || got == nil {
		t.Fatalf("GetBlob = %v, %v; want the blob", got, err)
	}
	if got.Key != "hello.bin" || got.BlobID != receipt.BlobID || got.Size != int64(len(content)) || !bytes.Equal(got.Data, content) {
		t.Fatalf("GetBlob = key %q, id %q, size %d, %d bytes; want what PutBlob stored", got.Key, got.BlobID, got.Size, len(got.Data))
	}
	var metadata fileMetadata
	if err := got.Metadata.Decode(&metadata); err != nil || metadata != (fileMetadata{ContentType: "application/octet-stream", Pages: 3}) {
		t.Fatalf("metadata = %+v, %v; want what WithMetadata stored", metadata, err)
	}

	removed, err := files.Delete(ctx, "hello.bin")
	if err != nil || !removed {
		t.Fatalf("Delete = %v, %v; want true", removed, err)
	}
	if _, err := files.GetBlob(ctx, "hello.bin"); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("GetBlob after Delete = %v, want ErrNotFound", err)
	}
	if removed, err := files.Delete(ctx, "hello.bin"); err != nil || removed {
		t.Fatalf("a second Delete = %v, %v; want false and no error", removed, err)
	}
}

// A miss is a successful frame that says found false; read naively it is an
// empty blob and no error (failure 2 of handbook/build-go.md section 2).
func TestGetBlobOfAMissingKeyIsNotFound(t *testing.T) {
	client := connectTo(t, startServer(t))
	files := filesBucket(t, client)

	got, err := files.GetBlob(testContext(t), "nothing.txt")
	if !errors.Is(err, hoarddb.ErrNotFound) || got != nil {
		t.Fatalf("GetBlob = %v, %v; want ErrNotFound and no blob", got, err)
	}
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || !strings.Contains(failure.Message, "nothing.txt") {
		t.Fatalf("the error should carry the server's sentence naming the key: %v", err)
	}
}

// A second PutBlob under the same key replaces the first, and a blob written
// without metadata reads back with none.
func TestPutBlobReplacesAndMetadataIsOptional(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	files := filesBucket(t, client)

	if _, err := files.PutBlob(ctx, "note", []byte("first"), hoarddb.WithMetadata(hoarddb.Map{"Draft": true})); err != nil {
		t.Fatal(err)
	}
	if _, err := files.PutBlob(ctx, "note", []byte("second")); err != nil {
		t.Fatal(err)
	}
	got, err := files.GetBlob(ctx, "note")
	if err != nil || got == nil {
		t.Fatalf("GetBlob = %v, %v; want the blob", got, err)
	}
	if string(got.Data) != "second" || got.Size != 6 {
		t.Fatalf("GetBlob = %q (size %d), want the second write", got.Data, got.Size)
	}
	if _, found := got.Metadata.Lookup("Draft"); found {
		t.Fatalf("metadata %s: the second write had none, so none should remain", got.Metadata)
	}
}

// The default blob limit is 16 MiB and a blob of exactly that goes in and
// comes back byte for byte, over one bulk connection.
func TestBlobRoundTripAtTheDefaultLimit(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	files := filesBucket(t, client)

	content := make([]byte, 16<<20)
	for position := range content {
		content[position] = byte(position*31 + position>>8)
	}
	receipt, err := files.PutBlob(ctx, "big.bin", content)
	if err != nil {
		t.Fatalf("PutBlob of 16 MiB: %v", err)
	}
	if receipt.Size != int64(len(content)) {
		t.Fatalf("receipt size %d, want %d", receipt.Size, len(content))
	}
	got, err := files.GetBlob(ctx, "big.bin")
	if err != nil || got == nil {
		t.Fatalf("GetBlob of 16 MiB = %v, %v; want the blob", got, err)
	}
	if got.Size != int64(len(content)) || !bytes.Equal(got.Data, content) {
		t.Fatalf("GetBlob returned %d bytes (size %d) that differ from the %d written", len(got.Data), got.Size, len(content))
	}
}

// A blob over the server's limit is the server's refusal, passed through with
// its sentence: the driver cannot know the configured limit, so it does not
// guess one (handbook/wire.md section 10). The key is not stored.
func TestBlobOverTheLimitIsInvalidArgument(t *testing.T) {
	server := servertest.Start(t, servertest.Options{Env: map[string]string{
		"HOARDDB_MAX_BLOB_BYTES": "1024", "HOARDDB_MAX_DOCUMENT_BYTES": "4096",
	}})
	client := connectTo(t, server)
	ctx := testContext(t)
	files := filesBucket(t, client)

	_, err := files.PutBlob(ctx, "big.bin", bytes.Repeat([]byte("b"), 2048))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("PutBlob over the limit = %v, want ErrInvalidArgument", err)
	}
	var failure *hoarddb.Error
	if !errors.As(err, &failure) {
		t.Fatalf("not a *hoarddb.Error: %v", err)
	}
	for _, want := range []string{"Blob \"big.bin\" is", "this server's limit is"} {
		if !strings.Contains(failure.Message, want) {
			t.Errorf("the server's message is passed through verbatim; %q lacks %q", failure.Message, want)
		}
	}
	if _, err := files.GetBlob(ctx, "big.bin"); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("the refused blob was stored: GetBlob = %v", err)
	}
	if _, err := files.PutBlob(ctx, "small.bin", []byte("fits")); err != nil {
		t.Fatalf("a blob under the limit after the refusal: %v", err)
	}
}
