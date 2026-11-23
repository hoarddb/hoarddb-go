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
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Stats reports the replication factor and whether the bucket inherits it from
// its database, as Info does (handbook/operations.md section 4).
func TestStatsReportsTheReplicationFactorAndWhereItCameFrom(t *testing.T) {
	for _, tc := range []struct {
		name          string
		answer        map[string]any
		wantFactor    int
		wantInherited bool
	}{
		{"inherited from the database", map[string]any{"found": true, "database": "app", "bucket": "users", "type": "hash",
			"entries": int64(2), "replication_factor": int32(3), "inherited": true}, 3, true},
		{"declared by the bucket", map[string]any{"found": true, "database": "app", "bucket": "users", "type": "hash",
			"replication_factor": int32(2)}, 2, false},
		{"not reported", map[string]any{"found": true, "database": "app", "bucket": "users", "type": "hash"}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				c.send(request, hoarddbwire.OpBucketStats, tc.answer)
				return true
			}))
			client := mustConnect(t, server.uri())
			stats, err := client.Database("app").Bucket("users").Stats(testContext(t))
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if stats.ReplicationFactor != tc.wantFactor || stats.Inherited != tc.wantInherited {
				t.Fatalf("Stats = %+v; want replication factor %d, inherited %t", stats, tc.wantFactor, tc.wantInherited)
			}
		})
	}
}

// Real server: a bucket created without a replication factor inherits its
// database's, and Stats says so.
func TestStatsAgainstTheServerSaysInherited(t *testing.T) {
	client := connectTo(t, startServer(t))
	bucket := bucketOf(t, client, hoarddb.Hash)
	stats, err := bucket.Stats(testContext(t))
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Inherited || stats.ReplicationFactor < 1 {
		t.Fatalf("Stats = %+v; want an inherited replication factor of at least 1", stats)
	}
}
