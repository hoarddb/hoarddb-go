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
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Handbook/wire.md section 10: the frame is the one limit the driver enforces
// itself. A blob that cannot fit one frame is refused before any byte is sent,
// with the arithmetic, and it costs neither the connection nor a frame.
func TestFrameOverTheCapIsRefusedBeforeSending(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.send(request, hoarddbwire.OpPong, nil)
		return true
	}))
	client := mustConnect(t, server.uri())
	files := client.Database("app").Bucket("files")

	_, err := files.PutBlob(testContext(t), "big.bin", make([]byte, 33<<20))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("PutBlob of 33 MiB = %v, want ErrInvalidArgument", err)
	}
	for _, want := range []string{"33554432", "put blob", "app/files/big.bin", "Nothing was sent"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message must contain %q (the arithmetic, and what was and was not done):\n%v", want, err)
		}
	}
	if strings.Contains(err.Error(), "store a large value as a blob") {
		t.Errorf("the remedy for a blob must not be to store it as a blob:\n%v", err)
	}
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatalf("the call after the refusal failed: %v", err)
	}
	if frames.Load() != 1 || client.PoolStats().Dials != 1 {
		t.Fatalf("%d frames over %d dials; the oversize blob must not reach the server and must not cost the connection",
			frames.Load(), client.PoolStats().Dials)
	}
}

// Blobs are bulk traffic (handbook/wire.md section 7): a blob write that the
// server is slow to answer holds a bulk connection and nothing else, so a point
// read on the same client is answered at once. Both lanes have one connection,
// so a blob that borrowed the point lane would make the read wait.
func TestBlobTrafficLeavesPointReadsAlone(t *testing.T) {
	release := make(chan struct{})
	var blobFrames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		switch request.Op {
		case hoarddbwire.OpBlobWrite:
			blobFrames.Add(1)
			<-release
			c.send(request, hoarddbwire.OpBlobWrite, hoarddbwire.BlobWriteResponse{Success: true, Key: "a.bin", BlobID: "id", Size: 1})
		case hoarddbwire.OpGet:
			c.send(request, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: []byte{5, 0, 0, 0, 0}})
		default:
			c.send(request, hoarddbwire.OpPong, nil)
		}
		return true
	}))
	client := mustConnect(t, server.uri(), hoarddb.WithPoolSize(1), hoarddb.WithBulkPoolSize(1))
	files := client.Database("app").Bucket("files")
	// Registered after mustConnect's Close, so it runs first: a failed assertion
	// frees the blocked write instead of hanging Close behind it.
	var closeRelease sync.Once
	releaseBlob := func() { closeRelease.Do(func() { close(release) }) }
	t.Cleanup(releaseBlob)

	written := make(chan error, 1)
	go func() {
		_, err := files.PutBlob(testContext(t), "a.bin", []byte{1})
		written <- err
	}()
	waitUntil(t, func() bool { return blobFrames.Load() == 1 })

	started := time.Now()
	if _, err := files.Get(testContext(t), "a"); err != nil {
		t.Fatalf("a point read behind a blob write: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("the point read took %v behind a blob write", elapsed)
	}
	if stats := client.PoolStats(); stats.Open != 2 || stats.InUse != 1 {
		t.Fatalf("PoolStats = %+v; want the blob on its own connection (2 open, 1 in use)", stats)
	}
	releaseBlob()
	if err := <-written; err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
}
