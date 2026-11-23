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

package conformance_test

import (
	"context"
	"fmt"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
)

// The batch verb of leg 3 (handbook/operations.md section 2). The vector is in
// the write group.

func verbInsertMany(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	return replayBatch(ctx, client, request, vector, (*hoarddb.Bucket).InsertMany)
}

func verbUpsertMany(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	return replayBatch(ctx, client, request, vector, (*hoarddb.Bucket).UpsertMany)
}

func replayBatch(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector,
	send func(*hoarddb.Bucket, context.Context, []any, ...hoarddb.WriteOption) (hoarddb.BatchReceipt, error)) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	members, _ := request.tree["data"].([]any)
	docs := make([]any, 0, len(members))
	for position := range members {
		doc, docErr := (requestOf{tree: map[string]any{"data": members[position]}}).document("data")
		if docErr != nil {
			return fmt.Errorf("document %d of the request: %w", position+1, docErr)
		}
		docs = append(docs, doc)
	}
	receipt, err := send(bucketFor(client, request), ctx, docs)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	wantIDs, _ := answer["ids"].([]any)
	if receipt.Count != len(docs) || receipt.Count != int((requestOf{tree: answer}).number("count")) || len(receipt.IDs) != len(wantIDs) {
		return fmt.Errorf("receipt %+v: want Count %d and %d ids, as the vector reports", receipt, len(docs), len(wantIDs))
	}
	for position, want := range wantIDs {
		if receipt.IDs[position] != want {
			return fmt.Errorf("receipt id %d is %q, the vector has %q", position, receipt.IDs[position], want)
		}
	}
	return nil
}
