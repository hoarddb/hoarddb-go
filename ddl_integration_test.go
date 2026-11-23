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
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// Real-server tests of databases and buckets. Each starts its own server, so a
// test never sees another's databases. They need HOARDDB_SERVER_BIN
// (scripts/fetch-server.sh).

func connectTo(t *testing.T, server *servertest.Server, opts ...hoarddb.Option) *hoarddb.Client {
	t.Helper()
	return mustConnect(t, pinnedFor(server, server.Password), opts...)
}

func bucketTypes(t *testing.T, database *hoarddb.Database) map[string]hoarddb.StoreType {
	t.Helper()
	listed, err := database.ListBuckets(testContext(t))
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	types := make(map[string]hoarddb.StoreType, len(listed))
	for _, info := range listed {
		types[info.Name] = info.Type
	}
	return types
}

func TestCreateDatabaseTwiceIsAlreadyExists(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	if err := client.CreateDatabase(ctx, "app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	err := client.CreateDatabase(ctx, "app")
	if !errors.Is(err, hoarddb.ErrAlreadyExists) {
		t.Fatalf("second CreateDatabase: %v, want ErrAlreadyExists", err)
	}
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Op != "create database" || failure.Target != "app" ||
		!strings.Contains(failure.Message, `database "app" already exists`) {
		t.Fatalf("error = %#v; want Op, Target and the server's sentence verbatim", failure)
	}
	// The option makes a setup script re-runnable.
	if err := client.CreateDatabase(ctx, "app", hoarddb.WithIfNotExists()); err != nil {
		t.Fatalf("CreateDatabase with WithIfNotExists on an existing database: %v", err)
	}

	listed, err := client.ListDatabases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(listed, func(info hoarddb.DatabaseInfo) bool { return info.Name == "app" && info.BucketCount == 0 }) {
		t.Fatalf("ListDatabases = %+v; want app with no buckets", listed)
	}

	if err := client.DropDatabase(ctx, "app"); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	err = client.DropDatabase(ctx, "app")
	if !errors.Is(err, hoarddb.ErrNotFound) || !strings.Contains(err.Error(), "create database app") {
		t.Fatalf("second DropDatabase: %v; want ErrNotFound carrying the server's fix", err)
	}
}

func TestCreateBucketSameTypeWithIfNotExists(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	if _, err := database.CreateBucket(ctx, "users", hoarddb.Hash); err != nil {
		t.Fatal(err)
	}
	bucket, err := database.CreateBucket(ctx, "users", hoarddb.Hash, hoarddb.WithIfNotExists())
	if err != nil || bucket == nil {
		t.Fatalf("CreateBucket of an existing bucket of the same type with WithIfNotExists: %v, %v", bucket, err)
	}
	if got := bucketTypes(t, database); len(got) != 1 || got["users"] != hoarddb.Hash {
		t.Fatalf("buckets = %v", got)
	}
}

func TestCreateBucketSameTypeWithoutIfNotExistsIsAlreadyExists(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	if _, err := database.CreateBucket(ctx, "users", hoarddb.Hash); err != nil {
		t.Fatal(err)
	}
	bucket, err := database.CreateBucket(ctx, "users", hoarddb.Hash)
	if !errors.Is(err, hoarddb.ErrAlreadyExists) || bucket != nil {
		t.Fatalf("CreateBucket of an existing bucket without WithIfNotExists: %v, %v; want nil and ErrAlreadyExists "+
			"(create means create, on every store type)", bucket, err)
	}
	for _, want := range []string{`"users"`, "already exists", "WithIfNotExists"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message must contain %q (the server's words and the way to make a create idempotent):\n%v", want, err)
		}
	}
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Op != "create bucket" || failure.Target != "app/users" {
		t.Fatalf("error = %#v; want Op create bucket and Target app/users", failure)
	}
}

func TestCreateBucketOtherTypeIsAlreadyExistsEvenWithIfNotExists(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	if _, err := database.CreateBucket(ctx, "users", hoarddb.Hash); err != nil {
		t.Fatal(err)
	}
	for _, opts := range [][]hoarddb.CreateOption{nil, {hoarddb.WithIfNotExists()}} {
		_, err := database.CreateBucket(ctx, "users", hoarddb.BTree, opts...)
		if !errors.Is(err, hoarddb.ErrAlreadyExists) || !strings.Contains(err.Error(), "type cannot be changed in place") {
			t.Fatalf("CreateBucket of another type (options %d): %v; want ErrAlreadyExists with the server's sentence", len(opts), err)
		}
	}
	if got := bucketTypes(t, database); got["users"] != hoarddb.Hash {
		t.Fatalf("the bucket is now %v; a refused create must change nothing", got)
	}
}

func TestCreateBucketOverLongNameRefused(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	_, err := database.CreateBucket(ctx, strings.Repeat("b", 256), hoarddb.Hash)
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("a 256-byte name: %v, want ErrInvalidArgument (the server refuses in-band)", err)
	}
	if !strings.Contains(err.Error(), "too long") || !strings.Contains(err.Error(), "255") {
		t.Fatalf("the server's sentence, which holds the fix, was lost: %v", err)
	}
	if _, err := database.CreateBucket(ctx, strings.Repeat("b", 255), hoarddb.Hash); err != nil {
		t.Fatalf("a 255-byte name is the longest allowed: %v", err)
	}
}

func TestCreateBucketOfAnUnsupportedTypeIsRefusedWithTheServersSentence(t *testing.T) {
	client := connectTo(t, startServer(t))
	_, err := client.Database("app").CreateBucket(testContext(t), "q", hoarddb.StoreType("queue"))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "unsupported bucket type: queue") {
		t.Fatalf("err = %v; want ErrInvalidArgument with the server's sentence", err)
	}
}

func TestCreateBucketInMissingDatabaseCreatesIt(t *testing.T) {
	// Documents the server's behaviour (handbook/operations.md section 4): a
	// driver does not hide it, and CreateBucket's godoc says so.
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	if _, err := client.Database("ghost").CreateBucket(ctx, "b", hoarddb.FIFO); err != nil {
		t.Fatal(err)
	}
	listed, err := client.ListDatabases(ctx)
	if err != nil || !slices.ContainsFunc(listed, func(info hoarddb.DatabaseInfo) bool { return info.Name == "ghost" && info.BucketCount == 1 }) {
		t.Fatalf("ListDatabases = %+v, %v; want ghost with one bucket", listed, err)
	}
}

func TestEveryStoreTypeCanBeCreatedListedAndDropped(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	kinds := []hoarddb.StoreType{hoarddb.FIFO, hoarddb.LIFO, hoarddb.Hash, hoarddb.BTree, hoarddb.Heap, hoarddb.Blob}
	for _, kind := range kinds {
		if _, err := database.CreateBucket(ctx, string(kind), kind); err != nil {
			t.Fatalf("CreateBucket %s: %v", kind, err)
		}
	}
	got := bucketTypes(t, database)
	for _, kind := range kinds {
		if got[string(kind)] != kind {
			t.Errorf("ListBuckets reports %q as %q", kind, got[string(kind)])
		}
		stats, err := database.Bucket(string(kind)).Stats(ctx)
		if err != nil || stats == nil || stats.Type != kind || stats.Entries != 0 || stats.Database != "app" || stats.Bucket != string(kind) {
			t.Errorf("Stats of %s = %+v, %v", kind, stats, err)
		}
	}
	for _, kind := range kinds {
		if err := database.DropBucket(ctx, string(kind)); err != nil {
			t.Fatalf("DropBucket %s: %v", kind, err)
		}
	}
	if got := bucketTypes(t, database); len(got) != 0 {
		t.Fatalf("buckets after dropping all: %v", got)
	}
}

func TestMissingNamesAreNotFoundWithTheServersFix(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	if err := client.CreateDatabase(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	database := client.Database("app")
	_, listErr := client.Database("nope").ListBuckets(ctx)
	_, statsErr := database.Bucket("nope").Stats(ctx)
	dropErr := database.DropBucket(ctx, "nope")
	for name, err := range map[string]error{"ListBuckets of a missing database": listErr, "Stats of a missing bucket": statsErr, "DropBucket of a missing bucket": dropErr} {
		var failure *hoarddb.Error
		if !errors.Is(err, hoarddb.ErrNotFound) || !errors.As(err, &failure) || !strings.Contains(failure.Message, "Run '") {
			t.Errorf("%s: %v; want ErrNotFound carrying the server's sentence (it names the command that fixes it)", name, err)
		}
	}
}

// Failure 15: a declaration that is sent and silently not enforced. A misspelt
// key is ignored by the server, so the proof is a mistyped document refused.
func TestTypedCreateEnforces(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	if _, err := database.CreateBucket(ctx, "people", hoarddb.Hash,
		hoarddb.WithFieldTypes(hoarddb.Strict, hoarddb.FieldSpec{Name: "Age", Type: "int"})); err != nil {
		t.Fatal(err)
	}
	put := func(document map[string]any) error {
		payload, err := marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		return client.DoWire(ctx, "put", hoarddbwire.OpPut, hoarddbwire.OpPut,
			hoarddbwire.PutRequest{Database: "app", Bucket: "people", Key: "ada", Data: payload}, &hoarddbwire.InsertResponse{})
	}
	if err := put(map[string]any{"Age": 36}); err != nil {
		t.Fatalf("a document of the declared type: %v", err)
	}
	err := put(map[string]any{"Age": "x"})
	if !errors.Is(err, hoarddb.ErrFieldType) || !strings.Contains(err.Error(), "Age") {
		t.Fatalf("a mistyped document: %v; want ErrFieldType naming the field", err)
	}
}

func TestStoresAndIndexesRoundTripThroughStats(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	bucket, err := database.CreateBucket(ctx, "User", hoarddb.Hash,
		hoarddb.WithStores(
			hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO},
			hoarddb.StoreSpec{Name: "Friends", Type: hoarddb.Hash},
			hoarddb.StoreSpec{Name: "Recipes", Type: hoarddb.BTree, Stores: []hoarddb.StoreSpec{{Name: "Steps", Type: hoarddb.LIFO}}}),
		hoarddb.WithIndexes(
			hoarddb.IndexSpec{Fields: []string{"Name"}},
			hoarddb.IndexSpec{Fields: []string{"Email"}, Unique: true, Name: "by_email"}),
		hoarddb.WithFieldTypes(hoarddb.Strict,
			hoarddb.FieldSpec{Name: "Name", Type: "string", Required: true},
			hoarddb.FieldSpec{Name: "Age", Type: "int"}))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := bucket.Stats(ctx)
	if err != nil || stats == nil {
		t.Fatalf("Stats = %+v, %v", stats, err)
	}
	if stats.Type != hoarddb.Hash || stats.FieldTypes != hoarddb.Strict {
		t.Errorf("Type=%v FieldTypes=%v", stats.Type, stats.FieldTypes)
	}
	wantStores := map[string]hoarddb.StoreType{"Purchases": hoarddb.FIFO, "Friends": hoarddb.Hash, "Recipes": hoarddb.BTree}
	for _, store := range stats.Stores {
		if want, declared := wantStores[store.Name]; declared && store.Type != want {
			t.Errorf("store field %s is %s, declared %s", store.Name, store.Type, want)
		}
		delete(wantStores, store.Name)
	}
	if len(wantStores) != 0 {
		t.Errorf("store fields missing from Stats: %v (reported %+v)", wantStores, stats.Stores)
	}
	if len(stats.Indexes) != 2 || !slices.ContainsFunc(stats.Indexes, func(index string) bool { return strings.HasPrefix(index, "by_email") }) {
		t.Errorf("Indexes = %v; want two, one named by_email", stats.Indexes)
	}
	wantFields := []hoarddb.FieldInfo{{Name: "Name", Type: "string", Required: true}, {Name: "Age", Type: "int"}}
	if !slices.Equal(stats.Fields, wantFields) {
		t.Errorf("Fields = %+v, want %+v", stats.Fields, wantFields)
	}

	listed, err := database.ListBuckets(ctx)
	if err != nil || len(listed) != 1 || listed[0].Name != "User" || listed[0].Type != hoarddb.Hash {
		t.Fatalf("ListBuckets = %+v, %v", listed, err)
	}
	for _, name := range []string{"Purchases", "Friends", "Recipes"} {
		if !slices.Contains(listed[0].Stores, name) {
			t.Errorf("ListBuckets does not list store field %s: %v", name, listed[0].Stores)
		}
	}
}

func TestDeclareStoreFieldsAddsToAnExistingBucket(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	bucket, err := database.CreateBucket(ctx, "Post", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.DeclareStoreFields(ctx, "Post",
		hoarddb.StoreSpec{Name: "Tags", Type: hoarddb.Hash}, hoarddb.StoreSpec{Name: "Comments", Type: hoarddb.FIFO}); err != nil {
		t.Fatal(err)
	}
	stats, err := bucket.Stats(ctx)
	if err != nil || stats == nil {
		t.Fatalf("Stats = %+v, %v", stats, err)
	}
	names := make([]string, 0, len(stats.Stores))
	for _, store := range stats.Stores {
		names = append(names, store.Name)
	}
	if !slices.Contains(names, "Tags") || !slices.Contains(names, "Comments") {
		t.Fatalf("store fields after DeclareStoreFields = %v", names)
	}

	err = database.DeclareStoreFields(ctx, "Post", hoarddb.StoreSpec{Name: "Bad", Type: hoarddb.StoreType("queue")})
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("an undeclarable store field: %v, want ErrInvalidArgument", err)
	}
}

func TestBadDeclarationIsRefusedWithTheServersList(t *testing.T) {
	client := connectTo(t, startServer(t))
	_, err := client.Database("app").CreateBucket(testContext(t), "b", hoarddb.Hash,
		hoarddb.WithFieldTypes(hoarddb.Strict, hoarddb.FieldSpec{Name: "Name", Type: "colour"}))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "colour") || !strings.Contains(err.Error(), "string, int32") {
		t.Fatalf("err = %v; want ErrInvalidArgument with the server's list of field types", err)
	}
}

func TestEncryptionAndReplicationOnASingleNode(t *testing.T) {
	server := startServer(t)
	client := connectTo(t, server)
	ctx := testContext(t)
	database := client.Database("app")

	_, err := database.CreateBucket(ctx, "secret", hoarddb.Hash, hoarddb.WithEncrypted())
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "encrypt") {
		t.Fatalf("WithEncrypted on a node that is not encrypted: %v; want the server's refusal, verbatim", err)
	}

	if !client.ServerInfo().Has("bucket_rf") {
		_, err := database.CreateBucket(ctx, "rf", hoarddb.Hash, hoarddb.WithReplicationFactor(3))
		if !errors.Is(err, hoarddb.ErrUnsupportedByServer) {
			t.Fatalf("the server does not advertise bucket_rf: %v, want ErrUnsupportedByServer", err)
		}
		return
	}
	_, err = database.CreateBucket(ctx, "rf", hoarddb.Hash, hoarddb.WithReplicationFactor(3))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "standalone") {
		t.Fatalf("a replication factor of 3 on one node: %v; want the server's refusal, verbatim", err)
	}
	if _, err := database.CreateBucket(ctx, "rf", hoarddb.Hash, hoarddb.WithReplicationFactor(1)); err != nil {
		t.Fatalf("a replication factor of 1 on one node: %v", err)
	}
}

func TestHealthAndListNodesOnOneNode(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	health, err := client.Health(ctx)
	if err != nil || health == nil || health.NodeID == "" || health.Version == "" || !health.Healthy {
		t.Fatalf("Health = %+v, %v", health, err)
	}
	if nodes, err := client.ListNodes(ctx); err != nil || len(nodes) != 0 {
		t.Fatalf("ListNodes on one node = %+v, %v; a single node answers with no members", nodes, err)
	}
}

func TestClusterNodesHealthAndReplicationFactor(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	node := cluster.Node(0)
	client := connectTo(t, node)
	ctx := testContext(t)

	health, err := client.Health(ctx)
	if err != nil || health == nil || health.NodeID != "node1" || health.Membership != "confirmed" || !health.Healthy {
		t.Fatalf("Health = %+v, %v; want node1, confirmed", health, err)
	}
	nodes, err := client.ListNodes(ctx)
	if err != nil || len(nodes) != 3 {
		t.Fatalf("ListNodes = %+v, %v; want three", nodes, err)
	}
	for index, member := range nodes {
		if member.ID != "node"+string(rune('1'+index)) || member.Status != "active" || member.Address == "" ||
			!strings.HasPrefix(member.Fingerprint, "SHA256:") || member.JoinedAt.IsZero() {
			t.Errorf("node %d = %+v", index, member)
		}
	}

	if !client.ServerInfo().Has("bucket_rf") {
		t.Fatalf("a cluster node does not advertise bucket_rf: %v", client.ServerInfo().Capabilities)
	}
	if _, err := client.Database("app").CreateBucket(ctx, "pairs", hoarddb.Hash, hoarddb.WithReplicationFactor(2)); err != nil {
		t.Fatal(err)
	}
	// ListBuckets is cluster-wide (see its godoc): every node lists a bucket
	// of factor 2, though only two of the three hold a copy. The catalog
	// arrives asynchronously, so wait for all three to list it.
	nodeClients := []*hoarddb.Client{client, connectTo(t, cluster.Node(1)), connectTo(t, cluster.Node(2))}
	holders := func() (count int) {
		for index, nodeClient := range nodeClients {
			listed, err := nodeClient.Database("app").ListBuckets(ctx)
			if errors.Is(err, hoarddb.ErrNotFound) {
				continue // the catalog has not reached this node yet
			}
			if err != nil {
				t.Fatalf("ListBuckets on node%d: %v", index+1, err)
			}
			for _, info := range listed {
				if info.Name != "pairs" {
					continue
				}
				count++
				if info.ReplicationFactor != 2 || info.Inherited {
					t.Errorf("node%d lists %+v; want a replication factor of 2 that is not inherited", index+1, info)
				}
			}
		}
		return count
	}
	waitUntil(t, func() bool { return holders() == len(nodeClients) })
}

func TestConcurrentCreatesOnOneClientAllLand(t *testing.T) {
	client := connectTo(t, startServer(t), hoarddb.WithPoolSize(4))
	ctx := testContext(t)
	database := client.Database("app")
	var group sync.WaitGroup
	for worker := range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range 5 {
				name := fmt.Sprintf("w%d-b%d", worker, index)
				if _, err := database.CreateBucket(ctx, name, hoarddb.Hash); err != nil {
					t.Errorf("CreateBucket %s: %v", name, err)
				}
			}
		}()
	}
	group.Wait()
	if got := bucketTypes(t, database); len(got) != 40 {
		t.Fatalf("%d buckets after 8 workers created 5 each, want 40", len(got))
	}
	if stats := client.PoolStats(); stats.Open > 4 || stats.InUse != 0 || stats.Waiting != 0 {
		t.Fatalf("PoolStats = %+v; at most the point lane's 4 connections, all idle", stats)
	}
}

// Every goroutine the client starts is a connection's reader, and Close, a
// discard and an abandoned call each stop theirs.
func TestClientStartsNoGoroutineItDoesNotStop(t *testing.T) {
	server := startServer(t)
	before := runtime.NumGoroutine()
	func() {
		ctx := testContext(t)
		client, err := hoarddb.Connect(ctx, pinnedFor(server, server.Password), hoarddb.WithPoolSize(3), hoarddb.WithBulkPoolSize(3))
		if err != nil {
			t.Fatal(err)
		}
		var group sync.WaitGroup
		for range 6 {
			group.Add(2)
			go func() {
				defer group.Done()
				if err := client.Ping(ctx); err != nil {
					t.Errorf("Ping: %v", err)
				}
			}()
			go func() {
				defer group.Done()
				if err := client.DoOnBulkLane(ctx, hoarddbwire.OpPing, hoarddbwire.OpPong); err != nil {
					t.Errorf("bulk call: %v", err)
				}
			}()
		}
		group.Wait()
		// An abandoned call closes its connection and the reader with it.
		abandoned, cancel := context.WithCancel(ctx)
		cancel()
		if err := client.CreateDatabase(abandoned, "app"); !errors.Is(err, context.Canceled) {
			t.Errorf("a cancelled call: %v, want context.Canceled", err)
		}
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	waitUntil(t, func() bool { return runtime.NumGoroutine() <= before })
}

// The README's bucket example, line for line, against a real server.
func TestREADMEBucketExampleWorks(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	db := client.Database("app")
	for range 2 { // the second pass is the re-run WithIfNotExists is for
		users, err := db.CreateBucket(ctx, "users", hoarddb.Hash,
			hoarddb.WithIfNotExists(),
			hoarddb.WithStores(
				hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO},
				hoarddb.StoreSpec{Name: "Friends", Type: hoarddb.Hash}),
			hoarddb.WithIndexes(hoarddb.IndexSpec{Fields: []string{"Email"}, Unique: true}),
			hoarddb.WithFieldTypes(hoarddb.Strict,
				hoarddb.FieldSpec{Name: "Name", Type: "string", Required: true},
				hoarddb.FieldSpec{Name: "Age", Type: "int"}))
		if err != nil {
			t.Fatal(err)
		}
		stats, err := users.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.TrimSpace(fmt.Sprintln(stats.Type, stats.Entries, len(stats.Stores))); got != "hash 0 2" {
			t.Fatalf("the README says this prints %q, got %q", "hash 0 2", got)
		}
	}
}

// Open returns a handle with the stored type for every store type, and Drop on
// the handle removes the bucket; both name a missing bucket as ErrNotFound.
func TestOpenAndBucketDropAgainstTheServer(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	kinds := []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree, hoarddb.FIFO, hoarddb.LIFO, hoarddb.Heap, hoarddb.Blob}
	for _, kind := range kinds {
		if _, err := database.CreateBucket(ctx, string(kind), kind); err != nil {
			t.Fatalf("CreateBucket %s: %v", kind, err)
		}
	}
	for _, kind := range kinds {
		bucket, err := database.Open(ctx, string(kind))
		if err != nil || bucket.Type() != kind {
			t.Fatalf("Open %s = %v, %v; want type %s", kind, bucket, err, kind)
		}
		if err := bucket.Drop(ctx); err != nil {
			t.Fatalf("Drop %s: %v", kind, err)
		}
		if _, err := database.Open(ctx, string(kind)); !errors.Is(err, hoarddb.ErrNotFound) {
			t.Fatalf("Open of dropped %s: %v; want ErrNotFound", kind, err)
		}
		if err := bucket.Drop(ctx); !errors.Is(err, hoarddb.ErrNotFound) {
			t.Fatalf("second Drop of %s: %v; want ErrNotFound", kind, err)
		}
	}
	if _, err := client.Database("nope").Open(ctx, "x"); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("Open in a missing database: %v; want ErrNotFound", err)
	}
	created, err := database.CreateBucket(ctx, "made", hoarddb.LIFO)
	if err != nil || created.Type() != hoarddb.LIFO {
		t.Fatalf("CreateBucket handle type = %v, %v; want lifo", created, err)
	}
}
