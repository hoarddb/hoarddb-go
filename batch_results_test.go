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

// The two ERROR payloads below are the member shapes of the server vectors
// "insert many naming an existing id is refused whole" and "insert many on a
// fifo bucket reports what it pushed".

func TestInsertManyRefusedWholeReportsEveryPosition(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.errorWith(request, map[string]any{
			"code":          int32(hoarddbwire.CodeDocumentExists),
			"message":       `This batch for bucket "users" in database "app" names documents that already exist: "ada" at position 2. Nothing was stored.`,
			"write_outcome": "not_applied",
			"results": []map[string]any{
				{"id": "grace", "write_outcome": "not_applied", "message": "not applied: another document in this batch already exists"},
				{"id": "ada", "write_outcome": "not_applied", "code": int32(hoarddbwire.CodeDocumentExists), "message": `document "ada" already exists in this bucket`},
			},
		})
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("users").InsertMany(context.Background(), []any{hoarddb.Map{"_id_": "grace"}, hoarddb.Map{"_id_": "ada"}})
	if !errors.Is(err, hoarddb.ErrDocumentExists) {
		t.Fatalf("err = %v, want ErrDocumentExists", err)
	}
	var failure *hoarddb.Error
	if !errors.As(err, &failure) {
		t.Fatalf("err = %v is not a *hoarddb.Error", err)
	}
	if len(failure.Results) != 2 {
		t.Fatalf("Results = %+v, want one per position", failure.Results)
	}
	first, second := failure.Results[0], failure.Results[1]
	if first.ID != "grace" || first.Outcome != hoarddb.OutcomeNotApplied || first.Code != 0 || first.Message == "" {
		t.Errorf("position 0 = %+v", first)
	}
	if second.ID != "ada" || second.Outcome != hoarddb.OutcomeNotApplied || second.Code != hoarddb.CodeDocumentExists {
		t.Errorf("position 1 = %+v, want ada refused with DOCUMENT_EXISTS", second)
	}
}

func TestInsertManyReceiptCarriesAResultPerPosition(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{
			Success: true, IDs: []string{"first", "second"}, Count: 2,
			Results: []hoarddbwire.WriteResult{
				{ID: "first", WriteOutcome: hoarddbwire.WriteApplied},
				{ID: "second", WriteOutcome: hoarddbwire.WriteApplied},
			},
		})
		return true
	}))
	client := mustConnect(t, server.uri())
	receipt, err := client.Database("app").Bucket("q").InsertMany(context.Background(), []any{hoarddb.Map{"n": 1}, hoarddb.Map{"n": 2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.Results) != 2 || receipt.Results[1].ID != "second" || receipt.Results[1].Outcome != hoarddb.OutcomeApplied {
		t.Errorf("Results = %+v", receipt.Results)
	}
}
