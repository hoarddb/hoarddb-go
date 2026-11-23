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
	"reflect"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// The owner's view of a bucket's copies reaches the public results of
// ListBuckets, Info and Stats under the same names (handbook/operations.md,
// "Copies"); a server that sends none leaves the zero value.
func TestCopiesViewReachesListInfoAndStats(t *testing.T) {
	row := map[string]any{
		"name": "users", "type": "hash", "entries": int64(2),
		"in_sync": int32(2), "state": "under-replicated", "not_in_sync": []any{"node3"},
		"pending_copies": []any{map[string]any{"node": "node3", "since": int64(1767225600)}},
	}
	want := hoarddb.BucketCopies{InSync: 2, State: "under-replicated", NotInSync: []string{"node3"},
		PendingCopies: []hoarddb.PendingCopy{{Node: "node3", Since: 1767225600}}}
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		switch request.Op {
		case hoarddbwire.OpListBuckets:
			c.send(request, hoarddbwire.OpListBuckets, map[string]any{"found": true, "database": "app", "buckets": []any{row}})
		case hoarddbwire.OpBucketInfo:
			c.send(request, hoarddbwire.OpBucketInfo, map[string]any{"found": true, "database": "app", "bucket": row})
		default:
			c.send(request, hoarddbwire.OpBucketStats, map[string]any{"found": true, "database": "app", "bucket": "users", "type": "hash",
				"in_sync": int32(2), "state": "under-replicated", "not_in_sync": []any{"node3"}, "nodes": []any{"node1", "node2", "node3"},
				"pending_copies": row["pending_copies"]})
		}
		return true
	}))
	client := mustConnect(t, server.uri())
	database := client.Database("app")

	listed, err := database.ListBuckets(testContext(t))
	if err != nil || len(listed) != 1 || !reflect.DeepEqual(listed[0].BucketCopies, want) {
		t.Fatalf("ListBuckets = %+v, %v; want copies %+v", listed, err, want)
	}
	if listed[0].InSync != 2 || listed[0].State != "under-replicated" {
		t.Fatalf("the members are not promoted to the row: %+v", listed[0])
	}
	info, err := database.Bucket("users").Info(testContext(t))
	if err != nil || !reflect.DeepEqual(info.BucketCopies, want) {
		t.Fatalf("Info = %+v, %v; want copies %+v", info, err, want)
	}
	stats, err := database.Bucket("users").Stats(testContext(t))
	if err != nil || !reflect.DeepEqual(stats.BucketCopies, want) {
		t.Fatalf("Stats = %+v, %v; want copies %+v", stats, err, want)
	}
}
