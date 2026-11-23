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
	"slices"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// The alter against three real nodes: a raise names the members that will
// copy, a repeat changes nothing, and a factor over the cluster's size is an
// invalid-argument refusal with the server's sentence. Needs
// HOARDDB_SERVER_BIN (scripts/fetch-server.sh).
func TestAlterBucketAgainstAThreeNodeCluster(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	client := connectTo(t, cluster.Node(0))
	ctx := testContext(t)
	database := client.Database("app")

	if _, err := database.CreateBucket(ctx, "users", hoarddb.Hash, hoarddb.WithReplicationFactor(1)); err != nil {
		t.Fatal(err)
	}
	raised, err := database.AlterBucket(ctx, "users", hoarddb.AlterBucketOptions{ReplicationFactor: 3})
	if err != nil {
		t.Fatal(err)
	}
	if raised.ReplicationFactor != 3 || raised.PreviousFactor != 1 || len(raised.Pending) != 2 || len(raised.Releasing) != 0 || raised.Message == "" {
		t.Fatalf("raise = %+v, want 3 (was 1) with two pending members", *raised)
	}
	repeated, err := database.AlterBucket(ctx, "users", hoarddb.AlterBucketOptions{ReplicationFactor: 3})
	if err != nil {
		t.Fatal(err)
	}
	if repeated.ReplicationFactor != 3 || repeated.PreviousFactor != 3 || len(repeated.Pending) != 0 {
		t.Fatalf("repeat = %+v, want 3 (was 3) and nothing pending", *repeated)
	}
	listed, err := database.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(listed, func(info hoarddb.BucketInfo) bool { return info.Name == "users" })
	if index < 0 || listed[index].ReplicationFactor != 3 || listed[index].Inherited {
		t.Fatalf("ListBuckets = %+v, want users declaring 3 copies", listed)
	}

	_, err = database.AlterBucket(ctx, "users", hoarddb.AlterBucketOptions{ReplicationFactor: 4})
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) || failure.Message == "" {
		t.Fatalf("a factor over the cluster's size: err = %v, want an invalid-argument refusal with the server's sentence", err)
	}
}
