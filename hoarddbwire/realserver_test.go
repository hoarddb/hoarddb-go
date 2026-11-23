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

package hoarddbwire_test

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// exchange sends one request on conn and decodes the answer into response,
// failing the test when the answer is an error frame.
func exchange(t *testing.T, conn *hoarddbwire.Conn, op hoarddbwire.Opcode, request, response any) hoarddbwire.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	reply, err := conn.Do(ctx, op, request, response)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if reply.Op == hoarddbwire.OpError {
		t.Fatalf("%s: the server answered %s", op, hoarddbwire.DecodeError(reply.Payload).Message)
	}
	return reply
}

func document(t *testing.T, fields map[string]any) hoarddbwire.Document {
	t.Helper()
	encoded, err := codec.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// TestTypesAgainstARealServer sends the wire types of every family to a real
// server and decodes what comes back with the response types: the vectors prove
// the types against captured bytes, this proves them against a live answer.
func TestTypesAgainstARealServer(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	conn, err := server.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// PING answers PONG, and the answer may carry no payload.
	var pong hoarddbwire.PongResponse
	if reply := exchange(t, conn, hoarddbwire.OpPing, nil, &pong); reply.Op != hoarddbwire.OpPong {
		t.Fatalf("PING answered %s", reply.Op)
	}

	var ack hoarddbwire.AckResponse
	exchange(t, conn, hoarddbwire.OpCreateDB, hoarddbwire.NameRequest{Name: "live"}, &ack)
	if !ack.Success {
		t.Fatalf("create database: %+v", ack)
	}
	options := document(t, map[string]any{"stores": []map[string]any{{"name": "Purchases", "engine": "fifo"}}})
	exchange(t, conn, hoarddbwire.OpCreateBucket,
		hoarddbwire.CreateBucketRequest{Database: "live", Name: "User", Type: "hash", Options: options}, &ack)
	exchange(t, conn, hoarddbwire.OpCreateBucket, hoarddbwire.CreateBucketRequest{Database: "live", Name: "events", Type: "fifo"}, &ack)
	if !ack.Success {
		t.Fatalf("create bucket: %+v", ack)
	}

	t.Run("a document is written and read back, and a miss is found false", func(t *testing.T) {
		var put hoarddbwire.InsertResponse
		exchange(t, conn, hoarddbwire.OpPut, hoarddbwire.PutRequest{
			Database: "live", Bucket: "User", Key: "ada", Data: document(t, map[string]any{"Name": "Ada", "Born": int32(1815)}),
		}, &put)
		if !put.Success || put.ID != "ada" || put.Seq == 0 || put.CopiesConfirmed != nil {
			t.Fatalf("put answered %+v; a single node reports no copy counts", put)
		}
		var got hoarddbwire.GetResponse
		exchange(t, conn, hoarddbwire.OpGet, hoarddbwire.KeyRequest{Database: "live", Bucket: "User", Key: "ada"}, &got)
		var fields struct {
			Name string `bson:"Name"`
			Born int32  `bson:"Born"`
		}
		if err := codec.Unmarshal(got.Data, &fields); err != nil || !got.Found || fields.Name != "Ada" || fields.Born != 1815 {
			t.Fatalf("get answered %+v, document %+v (%v)", got, fields, err)
		}
		// A fresh value per answer: decoding into one that was used keeps its old members.
		var missed hoarddbwire.GetResponse
		exchange(t, conn, hoarddbwire.OpGet, hoarddbwire.KeyRequest{Database: "live", Bucket: "User", Key: "nobody"}, &missed)
		if missed.Found || missed.Message == "" || len(missed.Data) != 0 {
			t.Fatalf("a miss answered %+v; it is found false with a message and no document", missed)
		}
		var deleted hoarddbwire.DeleteResponse
		exchange(t, conn, hoarddbwire.OpDelete, hoarddbwire.DeleteRequest{Database: "live", Bucket: "User", Key: "nobody"}, &deleted)
		if deleted.Success || deleted.WriteOutcome != hoarddbwire.WriteNotApplied {
			t.Fatalf("deleting a missing key answered %+v", deleted)
		}
	})

	t.Run("a bucket peek honours its limit and a store-field peek lists entries", func(t *testing.T) {
		for _, item := range []string{"a", "b", "c"} {
			var pushed hoarddbwire.InsertResponse
			exchange(t, conn, hoarddbwire.OpFIFOPush, hoarddbwire.PushRequest{
				Database: "live", Bucket: "events", Data: document(t, map[string]any{"Item": item}),
			}, &pushed)
			var fieldPushed hoarddbwire.FieldResponse
			exchange(t, conn, hoarddbwire.OpFieldPush, hoarddbwire.FieldRequest{
				Database: "live", Bucket: "User", Key: "ada", Field: "Purchases", Data: document(t, map[string]any{"Item": item}),
			}, &fieldPushed)
		}
		var peek hoarddbwire.PeekResponse
		exchange(t, conn, hoarddbwire.OpFIFOPeek, hoarddbwire.PeekRequest{Database: "live", Bucket: "events", Limit: 2}, &peek)
		if !peek.Found || len(peek.Items) != 2 {
			t.Fatalf("peek with limit 2 answered %+v", peek)
		}
		var fieldPeek hoarddbwire.FieldResponse
		exchange(t, conn, hoarddbwire.OpFieldPeek, hoarddbwire.FieldRequest{Database: "live", Bucket: "User", Key: "ada", Field: "Purchases"}, &fieldPeek)
		if !fieldPeek.Found || len(fieldPeek.Items) != 1 || len(fieldPeek.Items[0].Element) == 0 {
			t.Fatalf("store-field peek answered %+v", fieldPeek)
		}
		var length hoarddbwire.FieldResponse
		exchange(t, conn, hoarddbwire.OpFieldLen, hoarddbwire.FieldRequest{Database: "live", Bucket: "User", Key: "ada", Field: "Purchases"}, &length)
		if length.Len != 3 {
			t.Fatalf("store field length answered %+v, want 3", length)
		}
		var empty hoarddbwire.PopResponse
		for range 3 {
			exchange(t, conn, hoarddbwire.OpFIFOPop, hoarddbwire.BucketRequest{Database: "live", Bucket: "events"}, &empty)
		}
		exchange(t, conn, hoarddbwire.OpFIFOPop, hoarddbwire.BucketRequest{Database: "live", Bucket: "events"}, &empty)
		if empty.Found || empty.Message != "queue is empty" {
			t.Fatalf("popping an empty queue answered %+v", empty)
		}
	})

	t.Run("queries, listings and health decode", func(t *testing.T) {
		var scan hoarddbwire.ScanResponse
		exchange(t, conn, hoarddbwire.OpScan, hoarddbwire.ScanRequest{Database: "live", Bucket: "User"}, &scan)
		if !scan.Found || scan.Total != 1 || len(scan.Documents) != 1 || scan.BucketID == 0 {
			t.Fatalf("scan answered %+v", scan)
		}
		var count hoarddbwire.CountResponse
		exchange(t, conn, hoarddbwire.OpCount, hoarddbwire.CountRequest{Database: "live", Bucket: "User"}, &count)
		if count.Count != 1 {
			t.Fatalf("count answered %+v", count)
		}
		var listed hoarddbwire.ListBucketsResponse
		exchange(t, conn, hoarddbwire.OpListBuckets, hoarddbwire.DatabaseRequest{Database: "live"}, &listed)
		if len(listed.Buckets) != 2 {
			t.Fatalf("list buckets answered %+v", listed)
		}
		var stats hoarddbwire.BucketStatsResponse
		exchange(t, conn, hoarddbwire.OpBucketStats, hoarddbwire.BucketRequest{Database: "live", Bucket: "User"}, &stats)
		if stats.Type != "hash" || len(stats.Stores) != 1 || stats.Stores[0].Name != "Purchases" || stats.Stores[0].Elements != 3 {
			t.Fatalf("bucket stats answered %+v", stats)
		}
		var health hoarddbwire.HealthResponse
		if reply := exchange(t, conn, hoarddbwire.OpHealthCheck, nil, &health); reply.Op != hoarddbwire.OpGetResponse {
			t.Fatalf("HEALTH_CHECK answered %s, want GET_RESPONSE", reply.Op)
		}
		// Healthy is not asserted: the server reports a host whose disk is nearly full as unhealthy.
		if health.NodeID == "" || health.BucketCount != 2 || health.Encryption == nil {
			t.Fatalf("health answered %+v", health)
		}
	})
}
