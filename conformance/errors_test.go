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
	"slices"
	"strings"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// sentinels are the public error classes by the vectors' code names.
var sentinels = map[string]*hoarddb.Error{
	"NOT_FOUND": hoarddb.ErrNotFound, "ALREADY_EXISTS": hoarddb.ErrAlreadyExists,
	"PERMISSION_DENIED": hoarddb.ErrPermissionDenied, "INVALID_ARGUMENT": hoarddb.ErrInvalidArgument,
	"INTERNAL": hoarddb.ErrInternal, "UNAVAILABLE": hoarddb.ErrUnavailable, "TIMEOUT": hoarddb.ErrTimeout,
	"CONFLICT": hoarddb.ErrConflict, "UNSUPPORTED_VERSION": hoarddb.ErrUnsupportedVersion,
	"NOT_AUTHENTICATED": hoarddb.ErrNotAuthenticated, "OVERLOADED": hoarddb.ErrOverloaded,
	"RESULT_TOO_LARGE": hoarddb.ErrResultTooLarge, "DUPLICATE_KEY": hoarddb.ErrDuplicateKey,
	"FIELD_TYPE": hoarddb.ErrFieldType,
}

// writeOpcodes are the data mutations whose refusals say whether the write
// happened (handbook/errors.md section 4).
var writeOpcodes = []string{
	"PUT", "BATCH_PUT", "UPSERT", "BATCH_UPSERT", "DELETE", "UPDATE", "BLOB_WRITE", "BTREE_INSERT",
	"FIFO_PUSH", "FIFO_POP", "LIFO_PUSH", "LIFO_POP", "HEAP_PUSH", "HEAP_POP",
	"FIELD_PUSH", "FIELD_POP", "FIELD_PUT", "FIELD_DELETE",
	"QUEUE_CLAIM", "QUEUE_ACK", "QUEUE_RELEASE", "QUEUE_EXTEND", "DEAD_REQUEUE", "DEAD_DISCARD",
}

// TestEveryWriteRefusalCarriesItsOutcome holds the vectors to the rule that
// lets the driver be strict: a refused write says whether it happened, on a
// single node as on a cluster, so the driver appends its unknown-outcome advice
// only to an answer that says nothing. A vector that carries no stamp would
// bring the old allowance back (leg 5 and the replay compare the message
// verbatim, and refusedAs rejects the advice where the stamp exists).
func TestEveryWriteRefusalCarriesItsOutcome(t *testing.T) {
	file := loadVectors(t)
	checked := 0
	for _, vector := range file.Vectors {
		if vector.Kind != "exchange" || vector.Error == nil || !slices.Contains(writeOpcodes, vector.Request.OpcodeName) {
			continue
		}
		checked++
		if vector.Error.WriteOutcome == "" {
			t.Errorf("%s: a refused %s with no write_outcome", vector.Name, vector.Request.OpcodeName)
		}
	}
	if checked < 15 {
		t.Fatalf("only %d write refusals found; the vectors changed or the opcode list is stale", checked)
	}
}

// leg5Case is one bad request of handbook/testing.md section 2, leg 5: the
// vector that captured the server's answer, and the public call that sends the
// same request.
type leg5Case struct {
	vector string
	// forgeAuth makes the proxy answer the client's first login itself, so the
	// server sees an unauthenticated connection.
	forgeAuth bool
	// write marks a case whose call is a data write although its vector's request
	// is another opcode: the vector carries no write_outcome, so the driver must
	// call the outcome unknown and add its advice after the server's sentence.
	write bool
	call  func(ctx context.Context, client *hoarddb.Client, request requestOf) error
}

var leg5Cases = []leg5Case{
	{vector: "get of a bucket that does not exist", call: publicGet},
	{vector: "get of a database that does not exist", call: publicGet},
	{vector: "create database that exists", call: func(ctx context.Context, client *hoarddb.Client, request requestOf) error {
		return client.CreateDatabase(ctx, request.text("name"))
	}},
	{vector: "create bucket that exists with another type", call: func(ctx context.Context, client *hoarddb.Client, request requestOf) error {
		_, err := client.Database(request.text("database")).CreateBucket(ctx, request.text("name"), hoarddb.StoreType(request.text("type")))
		return err
	}},
	// The vector's request is a GET; through the public API a read is logged in
	// again and resent (handbook/errors.md section 6), so the refusal reaches the
	// caller on a write to the same address.
	{vector: "request before authentication", forgeAuth: true, write: true, call: func(ctx context.Context, client *hoarddb.Client, request requestOf) error {
		_, err := bucketFor(client, request).Put(ctx, request.text("key"), hoarddb.Map{"Name": "Ada"})
		return err
	}},
	{vector: "a document over the document limit", call: publicPut},
	{vector: "a mistyped document on a typed bucket", call: publicPut},
	{vector: "update with a bad operand", call: publicUpdate},
	{vector: "update with mul on a string is refused", call: publicUpdate},
	{vector: "update with inc past the 64-bit range is refused", call: publicUpdate},
	{vector: "update with unset of an array position is refused", call: publicUpdate},
}

func publicUpdate(ctx context.Context, client *hoarddb.Client, request requestOf) error {
	update, err := updateOf(request)
	if err != nil {
		return err
	}
	_, err = bucketFor(client, request).Update(ctx, request.text("key"), update)
	return err
}

func publicGet(ctx context.Context, client *hoarddb.Client, request requestOf) error {
	_, err := bucketFor(client, request).Get(ctx, request.text("key"))
	return err
}

func publicPut(ctx context.Context, client *hoarddb.Client, request requestOf) error {
	document, err := request.document("data")
	if err != nil {
		return fmt.Errorf("the vector's document: %w", err)
	}
	_, err = bucketFor(client, request).Put(ctx, request.text("key"), document)
	return err
}

func vectorNamed(t *testing.T, file *conformance.File, name string) conformance.Vector {
	t.Helper()
	for _, vector := range file.Vectors {
		if vector.Name == name {
			return vector
		}
	}
	t.Fatalf("no vector named %q", name)
	return conformance.Vector{}
}

// TestLeg5Errors is conformance leg 5 (handbook/testing.md section 2): each bad
// request, sent through the public API to a fresh server set up as its vector
// was, is the vector's code and error class with the server's message
// verbatim.
func TestLeg5Errors(t *testing.T) {
	if testing.Short() {
		t.Skip("real-server tests are skipped with -short")
	}
	file := loadVectors(t)
	report := conformance.LegReport{Leg: 5, Name: "errors"}
	for _, tc := range leg5Cases {
		vector := vectorNamed(t, file, tc.vector)
		report.Vectors++
		if vector.Provisional() {
			report.Provisional++
		}
		t.Run(tc.vector, func(t *testing.T) {
			err := replayError(t, file, vector, tc)
			switch {
			case err == nil && !vector.Provisional():
				report.Passed++
			case err == nil:
				t.Logf("provisional vector (%s) passes", vector.Status)
			case vector.Provisional():
				t.Logf("provisional vector (%s) does not pass, and that is not a failure: %v", vector.Status, err)
			default:
				t.Error(err)
			}
		})
	}
	t.Log(report)
	if report.Passed+report.Provisional != report.Vectors {
		t.Fatalf("%s: every bad request must be answered as its vector was", report)
	}
}

func replayError(t *testing.T, file *conformance.File, vector conformance.Vector, tc leg5Case) error {
	t.Helper()
	if vector.Error == nil {
		return errors.New("the vector has no error expectation")
	}
	sentinel, known := sentinels[vector.Error.Name]
	if !known {
		return fmt.Errorf("no public class for %s", vector.Error.Name)
	}
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	server, vector := replayServer(t, file, vector)
	request, err := conformance.ParseExtended(vector.Request.PayloadJSON)
	if err != nil {
		return err
	}
	proxy := servertest.StartProxy(t, server)
	if tc.forgeAuth {
		proxy.ForgeNextAuth()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := hoarddb.Connect(ctx, fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s",
		server.User, server.Password, proxy.Address, proxy.Fingerprint))
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer func() { _ = client.Close() }()

	got := tc.call(ctx, client, requestOf{tree: request})
	var failure *hoarddb.Error
	if !errors.As(got, &failure) {
		return fmt.Errorf("the call returned %v (%T); want an *hoarddb.Error", got, got)
	}
	if int(failure.Code) != vector.Error.Code || !errors.Is(got, sentinel) {
		return fmt.Errorf("the call returned code %#x (%v), the vector has %s (%d)", uint16(failure.Code), failure.Code, vector.Error.Name, vector.Error.Code)
	}
	if want := vector.Error.WriteOutcome; want != "" && failure.Outcome.String() != want {
		return fmt.Errorf("the call's outcome is %q, the server stamped %q", failure.Outcome, want)
	}
	want := fmt.Sprint(answer["message"])
	if tc.write && vector.Error.WriteOutcome == "" {
		// A data write refused with no stamp: unknown, the server's sentence
		// first and the driver's advice after it (handbook/errors.md section 4).
		if failure.Outcome != hoarddb.OutcomeUnknown || !strings.HasPrefix(failure.Message, want) ||
			!strings.Contains(failure.Message, "Whether the write was applied is unknown") {
			return fmt.Errorf("an unstamped write refusal is %q %q; want unknown, the server's sentence %q first, then the advice", failure.Outcome, failure.Message, want)
		}
	} else if failure.Message != want {
		return fmt.Errorf("message %q; want the server's sentence verbatim: %q", failure.Message, want)
	}
	if tc.forgeAuth {
		// The server refused the first request on the connection the proxy
		// logged in; the driver logged that same connection in again, once, and
		// did not resend the write.
		seen := proxy.Connections()
		if len(seen) != 1 || len(seen[0].Auths) != 2 || seen[0].Auths[1].Answer != hoarddbwire.OpAuthOK || seen[0].Auths[1].OnFreshConnection {
			return fmt.Errorf("want one connection with the forged login and one real login after the refusal: %s", servertest.Describe(seen))
		}
		if len(proxy.Exchanges()) != 1 {
			return fmt.Errorf("the write crossed the wire %d times, want once", len(proxy.Exchanges()))
		}
	}
	t.Logf("%s: %v", vector.Error.Name, got)
	return nil
}

// After NOT_AUTHENTICATED a read is resent, once, on the connection the driver
// logged in again (handbook/errors.md section 6), against a real server.
func TestReadAfterNotAuthenticatedIsResentOnce(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	proxy := servertest.StartProxy(t, server)
	proxy.ForgeNextAuth()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := hoarddb.Connect(ctx, fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s",
		server.User, server.Password, proxy.Address, proxy.Fingerprint))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	_, err = client.Database("app").Bucket("users").Get(ctx, "ada")
	if !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("Get = %v; want the resent read's own answer, NOT_FOUND for a database that does not exist", err)
	}
	exchanges := proxy.Exchanges()
	if len(exchanges) != 2 || hoarddbwire.DecodeError(exchanges[0].Response.Payload).Code != hoarddbwire.CodeNotAuthenticated {
		t.Fatalf("want the GET refused NOT_AUTHENTICATED, then sent once more: %d exchanges", len(exchanges))
	}
	if seen := proxy.Connections(); len(seen) != 1 || len(seen[0].Auths) != 2 {
		t.Fatalf("want both GETs and the second login on one connection: %s", servertest.Describe(seen))
	}
}
