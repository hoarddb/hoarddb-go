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
	"reflect"
	"strings"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// TestInfoDecodesTheDeclaration answers BUCKET_INFO the way the server does
// (handbook/operations.md section 4) and checks what Info makes of each shape:
// every member, an absent replication factor (0, not reported), a bucket with
// no store fields, and an engine word this driver has no constant for.
func TestInfoDecodesTheDeclaration(t *testing.T) {
	cases := []struct {
		name   string
		answer map[string]any
		want   hoarddb.BucketDeclaration
	}{
		{
			name: "store fields of several engines",
			answer: map[string]any{"found": true, "database": "app", "bucket": map[string]any{
				"name": "User", "type": "hash", "entries": int64(7), "stores": []any{"Friends", "Purchases"},
				"replication_factor": int32(3), "inherited": true,
			}, "store_fields": []any{
				map[string]any{"name": "Friends", "engine": "hash"},
				map[string]any{"name": "Purchases", "engine": "fifo"},
			}},
			want: hoarddb.BucketDeclaration{Name: "User", Type: hoarddb.Hash, Entries: 7, Stores: []string{"Friends", "Purchases"},
				StoreFields:       []hoarddb.StoreFieldInfo{{Name: "Friends", Engine: hoarddb.Hash}, {Name: "Purchases", Engine: hoarddb.FIFO}},
				ReplicationFactor: 3, Inherited: true},
		},
		{
			name: "no store fields and no replication factor",
			answer: map[string]any{"found": true, "database": "app", "bucket": map[string]any{
				"name": "plain", "type": "btree", "entries": int64(0),
			}},
			want: hoarddb.BucketDeclaration{Name: "plain", Type: hoarddb.BTree},
		},
		{
			name: "an engine word the driver does not know passes through",
			answer: map[string]any{"found": true, "database": "app", "bucket": map[string]any{
				"name": "odd", "type": "hash", "entries": int64(1), "stores": []any{"Ring"},
			}, "store_fields": []any{map[string]any{"name": "Ring", "engine": "ringbuffer"}}},
			want: hoarddb.BucketDeclaration{Name: "odd", Type: hoarddb.Hash, Entries: 1, Stores: []string{"Ring"},
				StoreFields: []hoarddb.StoreFieldInfo{{Name: "Ring", Engine: hoarddb.StoreType("ringbuffer")}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var asked hoarddbwire.BucketInfoRequest
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				if request.Op != hoarddbwire.OpBucketInfo {
					c.errorFrame(request, 0x05, "unexpected opcode "+request.Op.String())
					return false
				}
				if err := codec.Unmarshal(request.Payload, &asked); err != nil {
					t.Error(err)
				}
				c.send(request, hoarddbwire.OpBucketInfo, tc.answer)
				return true
			}))
			client := mustConnect(t, server.uri())
			got, err := client.Database("app").Bucket("User").Info(testContext(t))
			if err != nil {
				t.Fatalf("Info: %v", err)
			}
			if asked.Database != "app" || asked.Bucket != "User" {
				t.Errorf("request = %+v, want app/User", asked)
			}
			if !reflect.DeepEqual(*got, tc.want) {
				t.Fatalf("Info = %+v, want %+v", *got, tc.want)
			}
		})
	}
}

// A missing bucket or database is the server's NOT_FOUND, and a caller with no
// read grant is the ordinary permission error, each with the server's sentence.
func TestInfoRefusalsAreTheOrdinaryClasses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int32
		message string
		want    *hoarddb.Error
	}{
		{"missing bucket", 0x01, `bucket not found: bucket "b" not found in database "app". Run 'show buckets in app'`, hoarddb.ErrNotFound},
		{"missing database", 0x01, `database "app" not found on this node. Run 'show databases'`, hoarddb.ErrNotFound},
		{"no read grant", 0x03, `permission denied: user "u" lacks "reader" on "app"`, hoarddb.ErrPermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				c.errorFrame(request, tc.code, tc.message)
				return true
			}))
			client := mustConnect(t, server.uri())
			got, err := client.Database("app").Bucket("b").Info(testContext(t))
			var failure *hoarddb.Error
			if got != nil || !errors.Is(err, tc.want) || !errors.As(err, &failure) || failure.Message != tc.message {
				t.Fatalf("Info = %+v, %v; want %v with the server's sentence %q", got, err, tc.want.Code, tc.message)
			}
		})
	}
}

// Info is a read: after a connection failure it is sent once more on a fresh
// connection, and only once.
func TestInfoIsRetriedOnceAfterAConnectionLoss(t *testing.T) {
	var log frameLog
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		if log.add(request.Op) == 1 {
			return false
		}
		c.send(request, hoarddbwire.OpBucketInfo, map[string]any{"found": true, "bucket": map[string]any{"name": "User", "type": "hash", "entries": int64(1)}})
		return true
	}))
	client := mustConnect(t, server.uri())
	if _, err := client.Database("app").Bucket("User").Info(testContext(t)); err != nil {
		t.Fatalf("Info: %v; a read is resent once on a fresh connection", err)
	}
	if sent := log.count(hoarddbwire.OpBucketInfo); sent != 2 {
		t.Errorf("BUCKET_INFO sent %d times, want 2", sent)
	}
	if connections := server.connections.Load(); connections != 2 {
		t.Errorf("%d connections, want 2", connections)
	}
}

// The server answers a missing bucket with an ERROR frame, but a reply that
// says found:false must not become an empty declaration: it is ErrNotFound,
// and carries the bucket it was asked about.
func TestInfoFoundFalseIsNotFound(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.send(request, hoarddbwire.OpBucketInfo, map[string]any{"found": false})
		return true
	}))
	client := mustConnect(t, server.uri())
	got, err := client.Database("app").Bucket("ghost").Info(testContext(t))
	var failure *hoarddb.Error
	if got != nil || !errors.Is(err, hoarddb.ErrNotFound) || !errors.As(err, &failure) || !strings.Contains(failure.Message, "app/ghost") {
		t.Fatalf("Info = %+v, %v; want ErrNotFound naming app/ghost", got, err)
	}
}
