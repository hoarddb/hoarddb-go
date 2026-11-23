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
	"errors"
	"fmt"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
)

// The blob verbs of leg 3 (handbook/operations.md sections 2 and 3). Each calls
// the public verb the vector's opcode stands for and holds what it returns to
// the vector's answer.

func verbBlobWrite(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	content, err := request.bytes("data")
	if err != nil {
		return err
	}
	var options []hoarddb.BlobOption
	if !request.empty("metadata") {
		metadata, metadataErr := request.document("metadata")
		if metadataErr != nil {
			return metadataErr
		}
		options = append(options, hoarddb.WithMetadata(metadata))
	}
	key := request.text("key")
	receipt, err := bucketFor(client, request).PutBlob(ctx, key, content, options...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	if receipt.ID != key || receipt.BlobID == "" || receipt.Size != int64(len(content)) || receipt.Size != (requestOf{tree: answer}).number("size") {
		return fmt.Errorf("receipt %+v: want the key %q, a blob id and the size %d the vector reports", receipt, key, len(content))
	}
	return nil
}

func verbBlobRead(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).GetBlob(ctx, request.text("key"))
	if refused, problem := refusedAs(vector, err); refused {
		if got != nil {
			return errors.New("GetBlob returned a blob with an error")
		}
		return problem
	}
	if found, _ := answer["found"].(bool); !found {
		if !errors.Is(err, hoarddb.ErrNotFound) || got != nil {
			return fmt.Errorf("GetBlob = %v, %v; the vector is a miss, which is ErrNotFound and no blob", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if got == nil {
		return errors.New("GetBlob returned no blob and no error; a verb that finds the blob returns it")
	}
	want, err := (requestOf{tree: answer}).bytes("data")
	if err != nil {
		return err
	}
	if string(got.Data) != string(want) || got.Size != (requestOf{tree: answer}).number("size") || got.BlobID == "" || got.Key != request.text("key") {
		return fmt.Errorf("GetBlob = key %q, id %q, size %d, content %q; the vector has %q", got.Key, got.BlobID, got.Size, got.Data, want)
	}
	return documentMatches(vector, "metadata", got.Metadata)
}
