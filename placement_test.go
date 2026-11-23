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
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

func TestWithPlacementSendsThePlacementOptionUnderItsWireName(t *testing.T) {
	server, requests, _ := captureCreates(t, []string{"auth_token"})
	client := mustConnect(t, server.uri())
	database := client.Database("app")
	for _, placement := range []hoarddb.Placement{hoarddb.PlacementSpread, hoarddb.PlacementWhole} {
		if _, err := database.CreateBucket(context.Background(), "b", hoarddb.Hash, hoarddb.WithPlacement(placement)); err != nil {
			t.Fatalf("WithPlacement(%q): %v", placement, err)
		}
		options := canonical(t, nextCreate(t, requests).Options)
		if options["placement"] != string(placement) {
			t.Errorf("WithPlacement(%q) sent options %v, want placement %q", placement, options, placement)
		}
	}
	if _, err := database.CreateBucket(context.Background(), "b", hoarddb.Hash); err != nil {
		t.Fatal(err)
	}
	if options := canonical(t, nextCreate(t, requests).Options); options != nil {
		t.Errorf("a create without WithPlacement sent options %v, want none", options)
	}
}

func TestWithPlacementRefusesAWordTheServerDoesNotKnowBeforeSending(t *testing.T) {
	server, _, frames := captureCreates(t, []string{"auth_token"})
	client := mustConnect(t, server.uri())
	before := frames.Load()
	_, err := client.Database("app").CreateBucket(context.Background(), "b", hoarddb.Hash, hoarddb.WithPlacement("everywhere"))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
	if frames.Load() != before {
		t.Error("the driver sent a CREATE_BUCKET for a placement it knows is wrong")
	}
}

func TestWithPlacementIsABucketOptionNotADatabaseOption(t *testing.T) {
	server, _, _ := captureCreates(t, []string{"auth_token"})
	client := mustConnect(t, server.uri())
	err := client.CreateDatabase(context.Background(), "app", hoarddb.WithPlacement(hoarddb.PlacementSpread))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestStatsReportsThePlacement(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.send(request, hoarddbwire.OpBucketStats, hoarddbwire.BucketStatsResponse{
			Found: true, Database: "app", Bucket: "b", Type: "hash", Placement: "spread",
		})
		return true
	}))
	client := mustConnect(t, server.uri())
	stats, err := client.Database("app").Bucket("b").Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Placement != hoarddb.PlacementSpread {
		t.Errorf("Placement = %q, want %q", stats.Placement, hoarddb.PlacementSpread)
	}
}
