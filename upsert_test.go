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

func TestUpsertSendsUPSERTWithTheKeyAndDocument(t *testing.T) {
	seen := make(chan hoarddbwire.UpsertRequest, 1)
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var decoded hoarddbwire.UpsertRequest
		if request.Op != hoarddbwire.OpUpsert || unmarshal(request.Payload, &decoded) != nil {
			c.errorFrame(request, 0x05, "expected UPSERT")
			return true
		}
		seen <- decoded
		c.send(request, hoarddbwire.OpUpsertOK, hoarddbwire.InsertResponse{ID: decoded.Key, Success: true, Seq: 7})
		return true
	}))
	client := mustConnect(t, server.uri())
	receipt, err := client.Database("app").Bucket("users").Upsert(context.Background(), "ada", hoarddb.Map{"Name": "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ID != "ada" {
		t.Errorf("receipt ID = %q, want ada", receipt.ID)
	}
	request := <-seen
	if request.Database != "app" || request.Bucket != "users" || request.Key != "ada" || len(request.Data) == 0 {
		t.Errorf("the server saw %+v", request)
	}
}

func TestUpsertRefusesAnEmptyKeyBeforeSending(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		t.Errorf("the driver sent %v for an empty key", request.Op)
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("users").Upsert(context.Background(), "", hoarddb.Map{"Name": "Ada"})
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestUpsertOnAQueueIsTheServersRefusalVerbatim(t *testing.T) {
	const sentence = `Bucket "target" is a fifo bucket: a queue has no key to replace; use push, db.target.push({...}).`
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.errorWith(request, map[string]any{"code": int32(hoarddbwire.CodeInvalidArgument), "message": sentence, "write_outcome": "not_applied"})
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("target").Upsert(context.Background(), "ada", hoarddb.Map{"Name": "Ada"})
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Code != hoarddb.CodeInvalidArgument || failure.Message != sentence || failure.Outcome != hoarddb.OutcomeNotApplied {
		t.Fatalf("err = %v, want the server's INVALID_ARGUMENT sentence, not applied", err)
	}
}

func TestUpsertManySendsBATCH_UPSERTAndReturnsTheKeys(t *testing.T) {
	seen := make(chan hoarddbwire.BatchUpsertRequest, 1)
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var decoded hoarddbwire.BatchUpsertRequest
		if request.Op != hoarddbwire.OpBatchUpsert || unmarshal(request.Payload, &decoded) != nil {
			c.errorFrame(request, 0x05, "expected BATCH_UPSERT")
			return true
		}
		seen <- decoded
		c.send(request, hoarddbwire.OpBatchUpsertOK, hoarddbwire.BatchPutResponse{
			Success: true, IDs: []string{"ada", "grace"}, Count: 2,
			Results: []hoarddbwire.WriteResult{{ID: "ada", WriteOutcome: hoarddbwire.WriteApplied}, {ID: "grace", WriteOutcome: hoarddbwire.WriteApplied}},
		})
		return true
	}))
	client := mustConnect(t, server.uri())
	receipt, err := client.Database("app").Bucket("users").UpsertMany(context.Background(),
		[]any{hoarddb.Map{"_id_": "ada", "Name": "Ada"}, hoarddb.Map{"_id_": "grace", "Name": "Grace"}})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Count != 2 || len(receipt.IDs) != 2 || receipt.IDs[1] != "grace" || len(receipt.Results) != 2 {
		t.Errorf("receipt = %+v", receipt)
	}
	if request := <-seen; len(request.Data) != 2 {
		t.Errorf("the server saw %d documents, want 2", len(request.Data))
	}
}

func TestUpsertManyRefusesNothingToSend(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		t.Errorf("the driver sent %v for no documents", request.Op)
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("users").UpsertMany(context.Background(), nil)
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}
