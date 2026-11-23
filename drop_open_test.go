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
	"sync/atomic"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// Bucket.Drop sends the same DELETE_BUCKET as Database.DropBucket.
func TestBucketDropSendsDeleteBucket(t *testing.T) {
	var sent hoarddbwire.DropBucketRequest
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		if request.Op != hoarddbwire.OpDeleteBucket {
			t.Errorf("opcode %v; want DELETE_BUCKET", request.Op)
		}
		if err := codec.Unmarshal(request.Payload, &sent); err != nil {
			t.Error(err)
		}
		c.send(request, hoarddbwire.OpDeleteBucket, map[string]any{"success": true})
		return true
	}))
	client := mustConnect(t, server.uri())
	if err := client.Database("app").Bucket("users").Drop(testContext(t)); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if sent.Database != "app" || sent.Name != "users" {
		t.Fatalf("request = %+v; want database app, name users", sent)
	}
}

// A refusal of Drop is the typed error DropBucket gives.
func TestBucketDropOfMissingBucketIsNotFound(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.errorFrame(request, 0x01, "Bucket \"users\" not found in database \"app\". Run 'show buckets in app' to list what exists")
		return true
	}))
	client := mustConnect(t, server.uri())
	if err := client.Database("app").Bucket("users").Drop(testContext(t)); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("Drop of a missing bucket: %v; want ErrNotFound", err)
	}
}

// Open reads the store type from STATS and exposes it as Type.
func TestOpenReadsTheStoreType(t *testing.T) {
	var asked hoarddbwire.BucketRequest
	var statsFrames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		statsFrames.Add(1)
		if request.Op != hoarddbwire.OpBucketStats {
			t.Errorf("opcode %v; want BUCKET_STATS", request.Op)
		}
		if err := codec.Unmarshal(request.Payload, &asked); err != nil {
			t.Error(err)
		}
		c.send(request, hoarddbwire.OpBucketStats, map[string]any{"found": true, "database": "app", "bucket": "jobs", "type": "fifo"})
		return true
	}))
	client := mustConnect(t, server.uri())
	bucket, err := client.Database("app").Open(testContext(t), "jobs")
	if err != nil || bucket == nil {
		t.Fatalf("Open = %v, %v", bucket, err)
	}
	if bucket.Type() != hoarddb.FIFO {
		t.Fatalf("Type = %q; want fifo", bucket.Type())
	}
	if frames := statsFrames.Load(); frames != 1 {
		t.Fatalf("Open sent %d requests; want exactly one BUCKET_STATS", frames)
	}
	if asked.Database != "app" || asked.Bucket != "jobs" {
		t.Fatalf("asked %+v; want app/jobs", asked)
	}
}

// A handle from Bucket does no lookup, so its Type is empty.
func TestBucketHandleHasNoType(t *testing.T) {
	server := startFake(t, serveFrames(nil))
	client := mustConnect(t, server.uri())
	if got := client.Database("app").Bucket("jobs").Type(); got != "" {
		t.Fatalf("Type of an unopened handle = %q; want empty", got)
	}
}

// A missing bucket is ErrNotFound whether the server answers found false or an
// ERROR frame.
func TestOpenOfMissingBucketIsNotFound(t *testing.T) {
	for name, answer := range map[string]func(c *fakeConn, request hoarddbwire.Frame){
		"found false": func(c *fakeConn, request hoarddbwire.Frame) {
			c.send(request, hoarddbwire.OpBucketStats, map[string]any{"found": false, "message": "no such bucket"})
		},
		"error frame": func(c *fakeConn, request hoarddbwire.Frame) {
			c.errorFrame(request, 0x01, "Bucket \"jobs\" not found in database \"app\"")
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				answer(c, request)
				return true
			}))
			client := mustConnect(t, server.uri())
			bucket, err := client.Database("app").Open(testContext(t), "jobs")
			if !errors.Is(err, hoarddb.ErrNotFound) || bucket != nil {
				t.Fatalf("Open of a missing bucket = %v, %v; want nil and ErrNotFound", bucket, err)
			}
		})
	}
}

// The handles of Open and CreateBucket name their bucket and carry its type:
// a Drop on each sends DELETE_BUCKET for that name.
func TestOpenAndCreateBucketHandlesNameTheirBucketAndType(t *testing.T) {
	dropped := make(chan string, 2)
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		switch request.Op {
		case hoarddbwire.OpBucketStats:
			c.send(request, hoarddbwire.OpBucketStats, map[string]any{"found": true, "database": "app", "bucket": "opened", "type": "heap"})
		case hoarddbwire.OpCreateBucket:
			c.send(request, hoarddbwire.OpCreateBucket, map[string]any{"success": true})
		case hoarddbwire.OpDeleteBucket:
			var sent hoarddbwire.DropBucketRequest
			if err := codec.Unmarshal(request.Payload, &sent); err != nil {
				t.Error(err)
			}
			dropped <- sent.Database + "/" + sent.Name
			c.send(request, hoarddbwire.OpDeleteBucket, map[string]any{"success": true})
		}
		return true
	}))
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	database := client.Database("app")
	opened, err := database.Open(ctx, "opened")
	if err != nil {
		t.Fatal(err)
	}
	created, err := database.CreateBucket(ctx, "created", hoarddb.LIFO)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Type() != hoarddb.Heap || created.Type() != hoarddb.LIFO {
		t.Fatalf("types = %q, %q; want heap, lifo", opened.Type(), created.Type())
	}
	for handle, want := range map[*hoarddb.Bucket]string{opened: "app/opened", created: "app/created"} {
		if err := handle.Drop(ctx); err != nil {
			t.Fatal(err)
		}
		if got := <-dropped; got != want {
			t.Fatalf("Drop sent %q; want %q", got, want)
		}
	}
}
