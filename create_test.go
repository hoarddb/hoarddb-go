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
	"encoding/json"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// captureCreates serves a fake that answers every CREATE_BUCKET with success and
// hands the decoded request to the test.
func captureCreates(t *testing.T, capabilities []string) (*fakeServer, chan hoarddbwire.CreateBucketRequest, *atomic.Int32) {
	t.Helper()
	requests := make(chan hoarddbwire.CreateBucketRequest, 8)
	var frames atomic.Int32
	server := startFake(t, serveFramesWith(capabilities, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		var decoded hoarddbwire.CreateBucketRequest
		if request.Op != hoarddbwire.OpCreateBucket || unmarshal(request.Payload, &decoded) != nil {
			c.errorFrame(request, 0x05, "unexpected request")
			return true
		}
		requests <- decoded
		c.send(request, hoarddbwire.OpCreateBucket, hoarddbwire.AckResponse{Success: true, Message: `bucket "b" created (type: ` + decoded.Type + ")"})
		return true
	}))
	return server, requests, &frames
}

// nextCreate waits for the CREATE_BUCKET the fake saw, failing instead of
// hanging when the driver sent none.
func nextCreate(t *testing.T, requests <-chan hoarddbwire.CreateBucketRequest) hoarddbwire.CreateBucketRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("the server saw no CREATE_BUCKET within 5s")
		return hoarddbwire.CreateBucketRequest{}
	}
}

// canonical renders a BSON document as Extended JSON and parses it back, so a
// test can compare declarations without naming the BSON library.
func canonical(t *testing.T, document []byte) map[string]any {
	t.Helper()
	if len(document) == 0 {
		return nil
	}
	rendered, err := codec.ToExtendedJSON(document)
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(rendered, &parsed); err != nil {
		t.Fatal(err)
	}
	return parsed
}

// Every name below is written out from handbook/operations.md section 4, not
// taken from the driver's own tags: a misspelt option key is accepted by the
// server and silently ignored (build-go.md section 2, failure 15).
func TestCreateBucketSendsTheDeclarationsUnderTheirWireNames(t *testing.T) {
	server, requests, _ := captureCreates(t, []string{"auth_token", "bucket_rf"})
	client := mustConnect(t, server.uri())
	database := client.Database("app")

	_, err := database.CreateBucket(testContext(t), "User", hoarddb.Hash,
		hoarddb.WithEncrypted(),
		hoarddb.WithReplicationFactor(3),
		hoarddb.WithStores(
			hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO},
			hoarddb.StoreSpec{Name: "Recipes", Type: hoarddb.BTree, Stores: []hoarddb.StoreSpec{{Name: "Steps", Type: hoarddb.LIFO}}}),
		hoarddb.WithIndexes(
			hoarddb.IndexSpec{Fields: []string{"Name"}},
			hoarddb.IndexSpec{Fields: []string{"Email"}, Unique: true, Name: "by_email"},
			hoarddb.IndexSpec{Fields: []string{"Seen"}, ExpireAfter: time.Hour}),
		hoarddb.WithFieldTypes(hoarddb.Warn,
			hoarddb.FieldSpec{Name: "Name", Type: "string", Required: true},
			hoarddb.FieldSpec{Name: "Address", Type: "object", Members: []hoarddb.FieldSpec{{Name: "City", Type: "string"}}},
			hoarddb.FieldSpec{Name: "Tags", Type: "array", Elem: &hoarddb.FieldSpec{Type: "string"}}))
	if err != nil {
		t.Fatal(err)
	}
	request := nextCreate(t, requests)
	if request.Database != "app" || request.Name != "User" || request.Type != "hash" || !request.Encrypted || request.ReplicationFactor != 3 {
		t.Fatalf("request = %+v", request)
	}
	wantOptions := map[string]any{
		"stores": []any{
			map[string]any{"name": "Purchases", "engine": "fifo"},
			map[string]any{"name": "Recipes", "engine": "btree", "fields": []any{map[string]any{"name": "Steps", "engine": "lifo"}}},
		},
		"indices": []any{
			map[string]any{"fields": []any{"Name"}},
			map[string]any{"fields": []any{"Email"}, "name": "by_email", "unique": true},
			map[string]any{"fields": []any{"Seen"}, "expire_after_seconds": map[string]any{"$numberLong": "3600"}},
		},
		"field_types": "warn",
		"fields": []any{
			map[string]any{"name": "Name", "type": "string", "required": true},
			map[string]any{"name": "Address", "type": "object", "members": []any{map[string]any{"name": "City", "type": "string"}}},
			map[string]any{"name": "Tags", "type": "array", "elem": map[string]any{"type": "string"}},
		},
	}
	if got := canonical(t, request.Options); !reflect.DeepEqual(got, wantOptions) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		wantJSON, _ := json.MarshalIndent(wantOptions, "", "  ")
		t.Fatalf("options on the wire:\n%s\nwant:\n%s", gotJSON, wantJSON)
	}
}

func TestCreateBucketWithoutOptionsSendsNoDeclarations(t *testing.T) {
	server, requests, _ := captureCreates(t, []string{"auth_token"})
	client := mustConnect(t, server.uri())
	if _, err := client.Database("app").CreateBucket(testContext(t), "b", hoarddb.Blob); err != nil {
		t.Fatal(err)
	}
	request := nextCreate(t, requests)
	if len(request.Options) != 0 || request.Encrypted || request.ReplicationFactor != 0 || request.Type != "blob" {
		t.Fatalf("request = %+v; a plain create carries a name, a type and nothing else", request)
	}
}

func TestReplicationFactorNeedsCapability(t *testing.T) {
	server, _, frames := captureCreates(t, []string{"auth_token"}) // no bucket_rf
	client := mustConnect(t, server.uri())
	before := frames.Load()
	_, err := client.Database("app").CreateBucket(testContext(t), "b", hoarddb.Hash, hoarddb.WithReplicationFactor(3))
	if !errors.Is(err, hoarddb.ErrUnsupportedByServer) {
		t.Fatalf("err = %v, want ErrUnsupportedByServer", err)
	}
	for _, want := range []string{"bucket_rf", "fake-1.0", "nothing was sent"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message must name %q (the capability, the server's version, and that nothing was sent):\n%v", want, err)
		}
	}
	if frames.Load() != before {
		t.Fatalf("%d frames reached the server; an option the server did not advertise must send nothing", frames.Load()-before)
	}
	// Zero is "the node's default" and needs no capability.
	if _, err := client.Database("app").CreateBucket(testContext(t), "b", hoarddb.Hash, hoarddb.WithReplicationFactor(0)); err != nil {
		t.Fatalf("WithReplicationFactor(0): %v", err)
	}
}

func TestCreateOptionsAreCheckedBeforeAnythingIsSent(t *testing.T) {
	server, _, frames := captureCreates(t, []string{"auth_token", "bucket_rf"})
	client := mustConnect(t, server.uri())
	tests := []struct {
		name    string
		option  hoarddb.CreateOption
		contain string
	}{
		{"negative replication factor", hoarddb.WithReplicationFactor(-1), "WithReplicationFactor(-1)"},
		{"unknown type mode", hoarddb.WithFieldTypes("loose"), "hoarddb.Strict"},
		{"negative expiry", hoarddb.WithIndexes(hoarddb.IndexSpec{Fields: []string{"Seen"}, ExpireAfter: -time.Second}), "ExpireAfter"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.Database("app").CreateBucket(testContext(t), "b", hoarddb.Hash, test.option)
			if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), test.contain) {
				t.Fatalf("err = %v; want ErrInvalidArgument mentioning %q", err, test.contain)
			}
		})
	}
	if frames.Load() != 0 {
		t.Fatalf("%d frames were sent for options the driver itself refuses", frames.Load())
	}
}

func TestBucketOptionsOnADatabaseAreRefusedByName(t *testing.T) {
	server, _, frames := captureCreates(t, []string{"auth_token", "bucket_rf"})
	client := mustConnect(t, server.uri())
	for name, option := range map[string]hoarddb.CreateOption{
		"WithEncrypted":         hoarddb.WithEncrypted(),
		"WithReplicationFactor": hoarddb.WithReplicationFactor(2),
		"WithStores":            hoarddb.WithStores(hoarddb.StoreSpec{Name: "Q", Type: hoarddb.FIFO}),
		"WithIndexes":           hoarddb.WithIndexes(hoarddb.IndexSpec{Fields: []string{"a"}}),
		"WithFieldTypes":        hoarddb.WithFieldTypes(hoarddb.Strict),
	} {
		err := client.CreateDatabase(testContext(t), "app", option)
		if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "bucket") {
			t.Errorf("CreateDatabase with %s: %v; want ErrInvalidArgument naming the option and saying it is for a bucket", name, err)
		}
	}
	if frames.Load() != 0 {
		t.Fatalf("%d frames were sent", frames.Load())
	}
}

// A handle is a name and a pointer: no I/O, no connection, no goroutine, a few
// bytes. This is handbook/README.md "Words" and api-go.md section 1 as a test.
func TestHandlesDoNoIO(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		return true
	}))
	client := mustConnect(t, server.uri())
	before := client.PoolStats()
	goroutinesBefore := runtime.NumGoroutine()

	var kept *hoarddb.Bucket
	for index := range 1_000_000 {
		// A million distinct names, as a program with a bucket per entity would hold.
		kept = client.Database("app").Bucket(handleNames[index%len(handleNames)])
	}
	if kept == nil {
		t.Fatal("no handle")
	}
	if frames.Load() != 0 {
		t.Fatalf("%d frames after a million handles; a handle must do no I/O", frames.Load())
	}
	if after := client.PoolStats(); after != before {
		t.Fatalf("PoolStats %+v -> %+v; a handle must not touch the pool", before, after)
	}
	if goroutinesAfter := runtime.NumGoroutine(); goroutinesAfter > goroutinesBefore {
		t.Fatalf("%d goroutines before a million handles, %d after; a handle must start none", goroutinesBefore, goroutinesAfter)
	}
	if server.connections.Load() != 1 {
		t.Fatalf("%d connections; a handle must not dial", server.connections.Load())
	}

	allocations := testing.AllocsPerRun(1000, func() { kept = client.Database("app").Bucket("users") })
	if allocations > 2 {
		t.Fatalf("a database handle and a bucket handle cost %v allocations; two small structs is the bound", allocations)
	}
	if size := reflectSize(client.Database("app")) + reflectSize(kept); size > 64 {
		t.Fatalf("a database handle and a bucket handle are %d bytes; a few tens of bytes is the bound", size)
	}
}

var handleNames = func() []string {
	names := make([]string, 4096)
	for index := range names {
		names[index] = "bucket-" + strings.Repeat("x", index%7)
	}
	return names
}()

func TestDefaultDatabaseAndBucketComeFromTheConnectionString(t *testing.T) {
	server := startFake(t, serveFrames(nil))
	base := "hoarddb://admin:fake-password@" + server.address
	pin := "?fingerprint=" + server.fingerprint

	both := mustConnect(t, base+"/app/users"+pin)
	database, err := both.DefaultDatabase()
	if err != nil || database.Name() != "app" {
		t.Fatalf("DefaultDatabase = %v, %v", database, err)
	}
	if bucket, err := both.DefaultBucket(); err != nil || bucket == nil {
		t.Fatalf("DefaultBucket = %v, %v", bucket, err)
	}

	onlyDatabase := mustConnect(t, base+"/app"+pin)
	if _, err := onlyDatabase.DefaultDatabase(); err != nil {
		t.Fatal(err)
	}
	_, err = onlyDatabase.DefaultBucket()
	if !errors.Is(err, hoarddb.ErrInvalidURI) || !strings.Contains(err.Error(), "bucket") || !strings.Contains(err.Error(), "/app/<bucket>") {
		t.Fatalf("DefaultBucket without a bucket segment: %v; want ErrInvalidURI naming the missing segment and its place", err)
	}

	neither := mustConnect(t, base+"/"+pin)
	_, err = neither.DefaultDatabase()
	if !errors.Is(err, hoarddb.ErrInvalidURI) || !strings.Contains(err.Error(), "database") {
		t.Fatalf("DefaultDatabase without a path: %v; want ErrInvalidURI naming the missing segment", err)
	}
	if _, err := neither.DefaultBucket(); !errors.Is(err, hoarddb.ErrInvalidURI) || !strings.Contains(err.Error(), "database") {
		t.Fatalf("DefaultBucket without a path: %v", err)
	}
}

// reflectSize is the size in bytes of what a handle points at.
func reflectSize[T any](handle *T) uintptr { return reflect.TypeOf(*handle).Size() }
