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
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
)

// The verbs of the store-fields group of leg 3 (handbook/operations.md
// section 5). Each calls the public verb the vector's opcode stands for and
// holds what it returns to the vector's answer.

// fieldFor is the store field the request addresses: its key and field, and
// the path of outer field, element, inner field, element... that reaches a
// store nested inside an element.
func fieldFor(client *hoarddb.Client, request requestOf) *hoarddb.FieldStore {
	users := bucketFor(client, request)
	key, field := request.text("key"), request.text("field")
	path, _ := request.tree["path"].([]any)
	if len(path) == 0 {
		return users.Field(key, field)
	}
	store := users.Field(key, fmt.Sprint(path[0]))
	for position := 1; position < len(path); position += 2 {
		inner := field
		if position+1 < len(path) {
			inner = fmt.Sprint(path[position+1])
		}
		store = store.Field(fmt.Sprint(path[position]), inner)
	}
	return store
}

// refusedAs reports whether the vector's answer is an error frame and, if so,
// whether err is the driver's error for it: the same code, the server's
// sentence passed through, and the write outcome the vector records. A vector
// that records an outcome is a refusal the server stamped, so the driver must
// report that outcome and must not append its unknown-outcome advice (the
// advice is for an answer that says nothing); an unstamped refusal of a data
// write is unknown with the advice, and one of a read or a DDL request is not
// reported.
func refusedAs(vector conformance.Vector, err error) (refused bool, problem error) {
	answer, parseErr := answerOf(vector)
	if parseErr != nil {
		return false, parseErr
	}
	code := (requestOf{tree: answer}).number("code")
	if code == 0 {
		return false, nil
	}
	var failure *hoarddb.Error
	// The driver may refine a wire class into a code of its own that still
	// matches the class (every error is matched by its sentinel), so the class decides, not the integer.
	sameClass := false
	if vector.Error != nil {
		if sentinel, known := sentinels[vector.Error.Name]; known {
			sameClass = errors.Is(err, sentinel)
		}
	}
	if !errors.As(err, &failure) || (int64(failure.Code) != code && !sameClass) {
		return true, fmt.Errorf("the verb returned %v; the vector's answer is the error code %d", err, code)
	}
	if vector.Error != nil {
		if want := vector.Error.WriteOutcome; want != "" && failure.Outcome.String() != want {
			return true, fmt.Errorf("the error's outcome is %q; the vector's server stamped %q", failure.Outcome, want)
		}
		if vector.Error.WriteOutcome == "" && slices.Contains(writeOpcodes, vector.Request.OpcodeName) &&
			(failure.Outcome != hoarddb.OutcomeUnknown || !strings.Contains(failure.Message, "Whether the write was applied is unknown")) {
			return true, fmt.Errorf("an unstamped refusal of %s is %q %q; want unknown with the advice", vector.Request.OpcodeName, failure.Outcome, failure.Message)
		}
		if vector.Error.WriteOutcome != "" && strings.Contains(failure.Message, "Whether the write was applied is unknown") {
			return true, fmt.Errorf("the error carries the unknown-outcome advice (%q) although the server stamped %q", failure.Message, vector.Error.WriteOutcome)
		}
	}
	if message, _ := answer["message"].(string); !strings.Contains(failure.Message, message) {
		return true, fmt.Errorf("the error's message %q lacks the server's sentence %q", failure.Message, message)
	}
	return true, nil
}

func verbFieldPush(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	doc, err := request.document("data")
	if err != nil {
		return err
	}
	var options []hoarddb.WriteOption
	if priority := request.number("priority"); priority != 0 {
		options = append(options, hoarddb.WithPriority(priority))
	}
	receipt, err := fieldFor(client, request).Push(ctx, doc, options...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	if receipt.ID == "" {
		return fmt.Errorf("receipt %+v: want the entry id as the key", receipt)
	}
	return nil
}

func verbFieldPop(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	options := []hoarddb.PopOption{}
	if limit := request.number("limit"); limit > 0 {
		options = append(options, hoarddb.WithLimit(int(limit)))
	}
	got, err := fieldFor(client, request).Pop(ctx, options...)
	if found, _ := answer["found"].(bool); !found {
		if err != nil || len(got) != 0 {
			return fmt.Errorf("Pop = %s, %v; the vector is an empty store, which is an empty list and a nil error", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return elementsMatch(vector, "items", answer["items"], got)
}

func verbFieldPeek(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	var options []hoarddb.PeekOption
	if limit := request.number("limit"); limit > 0 {
		options = append(options, hoarddb.WithLimit(int(limit)))
	}
	got, err := fieldFor(client, request).Peek(ctx, options...)
	if found, _ := answer["found"].(bool); !found {
		if err != nil || len(got) != 0 {
			return fmt.Errorf("Peek = %v, %v; the vector is an empty store, which is an empty list and a nil error", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return elementsMatch(vector, "items", answer["items"], got)
}

func verbFieldPut(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	doc, err := request.document("data")
	if err != nil {
		return err
	}
	store := fieldFor(client, request)
	element := request.text("element")
	var (
		receiptID string
		putErr    error
	)
	if request.empty("content") {
		var receipt hoarddb.Receipt
		receipt, putErr = store.Put(ctx, element, doc)
		receiptID = receipt.ID
	} else {
		content, contentErr := request.bytes("content")
		if contentErr != nil {
			return contentErr
		}
		var receipt hoarddb.BlobReceipt
		receipt, putErr = store.PutBlob(ctx, element, content, hoarddb.WithMetadata(doc))
		receiptID = receipt.ID
	}
	err = putErr
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	if receiptID != element {
		return fmt.Errorf("receipt ID %q, want the element key %q", receiptID, element)
	}
	return nil
}

func verbFieldGet(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	store, element := fieldFor(client, request), request.text("element")
	_, isBlob := answer["blob_id"]
	var got hoarddb.Document
	var blob *hoarddb.StoredBlob
	if isBlob {
		blob, err = store.GetBlob(ctx, element)
		if blob != nil {
			got = blob.Metadata
		}
	} else {
		got, err = store.Get(ctx, element)
	}
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if found, _ := answer["found"].(bool); !found {
		if !errors.Is(err, hoarddb.ErrNotFound) || got != nil {
			return fmt.Errorf("Get = %s, %v; the vector is a missing element, which is ErrNotFound and no document", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if isBlob {
		if blob == nil {
			return errors.New("GetBlob returned no blob and no error; a verb that finds the blob returns it")
		}
		want, wantErr := (requestOf{tree: answer}).bytes("content")
		if wantErr != nil || string(blob.Data) != string(want) {
			return fmt.Errorf("blob content %q, the vector has %q (%v)", blob.Data, want, wantErr)
		}
	}
	return documentMatches(vector, "element", got)
}

func verbFieldDelete(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	got, err := fieldFor(client, request).Delete(ctx, request.text("element"))
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if want, _ := answer["found"].(bool); err != nil || got != want {
		return fmt.Errorf("Delete = %v, %v; the vector says found %v", got, err, want)
	}
	return nil
}

func verbFieldLen(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	got, err := fieldFor(client, request).Len(ctx)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if want := (requestOf{tree: answer}).number("len"); err != nil || got != want {
		return fmt.Errorf("Len = %d, %v; the vector says %d", got, err, want)
	}
	return nil
}

func verbFieldList(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	var options []hoarddb.FindOption
	if limit := request.number("limit"); limit > 0 {
		options = append(options, hoarddb.PageSize(int(limit)), hoarddb.Limit(int(limit)))
	}
	cursor, err := fieldFor(client, request).List(ctx, options...)
	if err != nil {
		return err
	}
	if cursor == nil {
		return errors.New("List returned no cursor and no error; a verb that lists returns the cursor")
	}
	documents, err := cursor.All(ctx)
	if err != nil {
		return err
	}
	return elementsMatch(vector, "entries", answer["entries"], documents)
}

// elementsMatch holds the documents a peek or a list returned to the element
// of each of the vector's items or entries. The members that say where an
// element is (its position, entry id, key) are not part of what the verbs
// return, so only the element is compared; the vary paths still apply.
func elementsMatch(vector conformance.Vector, member string, want any, got []hoarddb.Document) error {
	wantEntries := []any{}
	listed, _ := want.([]any)
	for _, entry := range listed {
		if fields, isMap := entry.(map[string]any); isMap {
			wantEntries = append(wantEntries, map[string]any{"element": fields["element"]})
		}
	}
	gotEntries := make([]any, 0, len(got))
	for _, document := range got {
		tree, err := conformance.WireTree(document)
		if err != nil {
			return err
		}
		gotEntries = append(gotEntries, map[string]any{"element": tree})
	}
	return treesMatch(vector, map[string]any{member: wantEntries}, map[string]any{member: gotEntries})
}

// bytes is a binary member of the request, written in Extended JSON as
// {"$binary": {"base64": ..., "subType": "00"}}.
func (request requestOf) bytes(name string) ([]byte, error) {
	wrapper, _ := request.tree[name].(map[string]any)
	inner, _ := wrapper["$binary"].(map[string]any)
	encoded, isText := inner["base64"].(string)
	if !isText {
		return nil, fmt.Errorf("the member %q is not an Extended JSON binary", name)
	}
	return base64.StdEncoding.DecodeString(encoded)
}
