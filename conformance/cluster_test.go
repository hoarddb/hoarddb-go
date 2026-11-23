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
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// Conformance leg 9, the cluster (handbook/testing.md section 2), and the
// write-concern tests of handbook/build-go.md step 13. A write concern is
// proved on three nodes: one node has no replicas, so every level looks the
// same there (handbook/build-go.md section 2, failure 11).

// clusterVectors are the cluster vectors this leg replays through the public
// API on three nodes. The others: "health", "list nodes" and "list nodes with
// membership history" are replayed by TestLeg9ClusterIdentity and the three
// "not yet added" vectors by TestLeg9NodeNotYetAdded, both with the vectors'
// fixed ports and fingerprints swapped for the live ones; "put with write
// concern all and one node stopped" is replay false (a node is stopped after
// the setup) and the write-concern tests below exercise it.
var clusterVectors = []string{
	"put on a cluster with write concern all",
	"consistent get on a cluster",
	"put on a cluster with a write concern the server does not know",
}

// TestLeg9Cluster replays the cluster vectors on a three-node cluster: by the
// public API where a verb sends the vector (with its write concern), by the
// wire layer otherwise. Each vector has a database of its own, so one cluster
// serves them all.
func TestLeg9Cluster(t *testing.T) {
	file := loadVectors(t)
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	node := cluster.Node(0)
	report := conformance.LegReport{Leg: 9, Name: "cluster"}
	for _, name := range clusterVectors {
		vector := vectorNamed(t, file, name)
		if vector.Topology != "cluster3" || !vector.Replay {
			t.Fatalf("vector %q is topology %q, replay %v; leg 9 replays cluster3 vectors", name, vector.Topology, vector.Replay)
		}
		report.Vectors++
		t.Run(name, func(t *testing.T) {
			runSetup(t, node, vector)
			var err error
			if verb, usesPublicAPI := publicVerb(vector); usesPublicAPI {
				err = replayThroughPublicAPI(t, node, vector, verb)
			} else {
				err = replayThroughWire(t, node, vector)
			}
			if err != nil {
				t.Fatal(err)
			}
			report.Passed++
		})
	}
	t.Log(report)
}

// TestLeg9NodeNotYetAdded replays the three "not yet added" vectors on the
// fourth node of the vectors' cluster, started alone so that no member
// confirms it. Its sentences name its own cluster address, which the vectors
// record as 127.0.0.1:4439 (testdata normalisation: loopback ports are
// rewritten), so that address is the live one in the vector's copy.
func TestLeg9NodeNotYetAdded(t *testing.T) {
	file := loadVectors(t)
	names := []string{"health of a node not yet added", "get at a node not yet added", "put at a node not yet added"}
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	report := conformance.LegReport{Leg: 9, Name: "node not yet added"}
	for _, name := range names {
		vector := vectorNamed(t, file, name)
		report.Vectors++
		t.Run(name, func(t *testing.T) {
			node := cluster.StartNewcomer(t, servertest.Options{})
			local := localised(vector, "127.0.0.1:4439", node.RingAddress)
			if err := replayThroughWire(t, node, local); err != nil {
				t.Fatal(err)
			}
			report.Passed++
		})
	}
	t.Log(report)
}

// localised is a copy of the vector with what the vectors fixed (a loopback
// address, a key fingerprint) replaced by this run's: each pair is a fixed
// string and its live one, swapped in one pass in the answer and in the error
// expectation, so a live value equal to another fixed one is not swapped twice.
func localised(vector conformance.Vector, pairs ...string) conformance.Vector {
	swap := strings.NewReplacer(pairs...)
	response := *vector.Response
	response.PayloadJSON = []byte(swap.Replace(string(response.PayloadJSON)))
	vector.Response = &response
	if vector.Error != nil {
		expect := *vector.Error
		expect.MessageContains = nil
		for _, part := range vector.Error.MessageContains {
			expect.MessageContains = append(expect.MessageContains, swap.Replace(part))
		}
		vector.Error = &expect
	}
	return vector
}

// The vectors' cluster has fixed identities: node1 to node3 with the ring
// addresses below and fingerprints of fixed keys, and a fourth node, newcomer,
// on 4439 (testdata normalisation). A live cluster has other ports and keys.
var (
	fixedRingAddresses = []string{"127.0.0.1:4433", "127.0.0.1:4435", "127.0.0.1:4437"}
	fixedFingerprints  = []string{
		"SHA256:fdf72a088f18f7399e8c52bce448441501f759a595a86980e5d9a422a01e5d55",
		"SHA256:ed23d853125a3acff2782dddf84e018ebc85942ebf328ff1287202a479964207",
		"SHA256:defe6330f78fcc11efd0fb28614f0d29d7b60d6e66c25c1c5b0f98fe505fc8bb",
	}
)

// clusterIdentity is the pairs that turn the vectors' three nodes into the live
// cluster's, and newcomer's address into the live one when there is one.
func clusterIdentity(cluster *servertest.Cluster, newcomer *servertest.Server) []string {
	var pairs []string
	for i, ring := range cluster.RingAddresses() {
		pairs = append(pairs, fixedRingAddresses[i], ring, fixedFingerprints[i], cluster.Fingerprints()[i])
	}
	if newcomer != nil {
		pairs = append(pairs, "127.0.0.1:4439", newcomer.RingAddress)
	}
	return pairs
}

// TestLeg9ClusterIdentity is the cluster part of leg 3 (replay): it replays "health", "list nodes" and "list nodes with
// membership history" on node1 of a live three-node cluster, with the vectors'
// fixed ring addresses and fingerprints swapped for the live ones. "health"
// reports no bucket, so the replication probe bucket is dropped first; the
// history vector lists newcomer, so a fourth node is started beside the
// cluster before it (and after the other two, whose answers have three nodes).
func TestLeg9ClusterIdentity(t *testing.T) {
	file := loadVectors(t)
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	cluster.DropProbe(t)
	node := cluster.Node(0)
	report := conformance.LegReport{Leg: 3, Name: "replay, cluster identity"}
	var newcomer *servertest.Server
	for _, name := range []string{"health", "list nodes", "list nodes with membership history"} {
		vector := vectorNamed(t, file, name)
		report.Vectors++
		t.Run(name, func(t *testing.T) {
			if name == "list nodes with membership history" {
				newcomer = cluster.StartNewcomer(t, servertest.Options{})
			}
			local := localised(vector, clusterIdentity(cluster, newcomer)...)
			if err := replayThroughWire(t, node, local); err != nil {
				t.Fatal(err)
			}
			report.Passed++
		})
	}
	t.Log(report)
}

func clusterContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// writeTargets are the buckets every write verb of TestServerReadsTheConcernOnEveryWriteVerb
// aims at, each holding one element so that the probe would succeed but for
// its write concern.
func createWriteTargets(ctx context.Context, t *testing.T, client *hoarddb.Client, database string) {
	t.Helper()
	db := client.Database(database)
	buckets := []struct {
		name string
		kind hoarddb.StoreType
		opts []hoarddb.CreateOption
	}{
		{"users", hoarddb.Hash, nil}, {"ranks", hoarddb.BTree, nil}, {"jobs", hoarddb.FIFO, nil},
		{"stack", hoarddb.LIFO, nil}, {"tasks", hoarddb.Heap, nil}, {"files", hoarddb.Blob, nil},
		{"User", hoarddb.Hash, []hoarddb.CreateOption{hoarddb.WithStores(
			hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO},
			hoarddb.StoreSpec{Name: "Friends", Type: hoarddb.Hash},
			hoarddb.StoreSpec{Name: "Data", Type: hoarddb.Blob})}},
	}
	for _, bucket := range buckets {
		if _, err := db.CreateBucket(ctx, bucket.name, bucket.kind, bucket.opts...); err != nil {
			t.Fatalf("CreateBucket %s: %v", bucket.name, err)
		}
	}
	steps := []func() error{
		func() error { _, err := db.Bucket("users").Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); return err },
		func() error { _, err := db.Bucket("ranks").Put(ctx, "ada", hoarddb.Map{"Rank": 1}); return err },
		func() error { _, err := db.Bucket("jobs").Push(ctx, hoarddb.Map{"Job": 1}); return err },
		func() error { _, err := db.Bucket("stack").Push(ctx, hoarddb.Map{"Plate": 1}); return err },
		func() error {
			_, err := db.Bucket("tasks").Push(ctx, hoarddb.Map{"Task": 1}, hoarddb.WithPriority(5))
			return err
		},
		func() error { _, err := db.Bucket("files").PutBlob(ctx, "hello.txt", []byte("hello")); return err },
		func() error { _, err := db.Bucket("User").Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); return err },
		func() error {
			_, err := db.Bucket("User").Field("ada", "Purchases").Push(ctx, hoarddb.Map{"Item": "pen"})
			return err
		},
		func() error {
			_, err := db.Bucket("User").Field("ada", "Friends").Put(ctx, "grace", hoarddb.Map{"Since": 1952})
			return err
		},
	}
	for index, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("setting up the write targets, step %d: %v", index+1, err)
		}
	}
}

// wireProbe is one write opcode and its request, carrying a write concern.
type wireProbe struct {
	name    string
	opcode  hoarddbwire.Opcode
	request any
}

// probesWith is every write opcode a driver sends, each with the write concern
// word, aimed at createWriteTargets' buckets.
func probesWith(t *testing.T, database, word string) []wireProbe {
	t.Helper()
	document := func(fields map[string]any) hoarddbwire.Document {
		encoded, err := codec.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	priority := int64(9)
	field := func(name, element string) hoarddbwire.FieldRequest {
		return hoarddbwire.FieldRequest{Database: database, Bucket: "User", Key: "ada", Field: name, Element: element, WriteConcern: word}
	}
	fieldPush, fieldPut, fieldBlob := field("Purchases", ""), field("Friends", "alan"), field("Data", "avatar")
	fieldPush.Data = document(map[string]any{"Item": "ink"})
	fieldPut.Data = document(map[string]any{"Since": 1936})
	fieldBlob.Content = []byte("png")
	return []wireProbe{
		{"put", hoarddbwire.OpPut, hoarddbwire.PutRequest{Database: database, Bucket: "users", Key: "grace", Data: document(map[string]any{"Name": "Grace"}), WriteConcern: word}},
		{"insert", hoarddbwire.OpPut, hoarddbwire.PutRequest{Database: database, Bucket: "users", Data: document(map[string]any{"Name": "Alan"}), WriteConcern: word}},
		{"btree insert", hoarddbwire.OpBTreeInsert, hoarddbwire.PutRequest{Database: database, Bucket: "ranks", Key: "grace", Data: document(map[string]any{"Rank": 2}), WriteConcern: word}},
		{"insert many", hoarddbwire.OpBatchPut, hoarddbwire.BatchPutRequest{Database: database, Bucket: "users", Data: []hoarddbwire.Document{document(map[string]any{"Name": "Edsger"})}, WriteConcern: word}},
		{"upsert", hoarddbwire.OpUpsert, hoarddbwire.UpsertRequest{Database: database, Bucket: "users", Key: "grace", Data: document(map[string]any{"Name": "Grace H"}), WriteConcern: word}},
		{"upsert many", hoarddbwire.OpBatchUpsert, hoarddbwire.BatchUpsertRequest{Database: database, Bucket: "users", Data: []hoarddbwire.Document{document(map[string]any{"_id_": "edsger", "Name": "Edsger"})}, WriteConcern: word}},
		{"delete", hoarddbwire.OpDelete, hoarddbwire.DeleteRequest{Database: database, Bucket: "users", Key: "ada", WriteConcern: word}},
		{"update", hoarddbwire.OpUpdate, hoarddbwire.UpdateRequest{Database: database, Bucket: "users", Key: "ada", Set: document(map[string]any{"Name": "Augusta"}), WriteConcern: word}},
		{"fifo push", hoarddbwire.OpFIFOPush, hoarddbwire.PushRequest{Database: database, Bucket: "jobs", Data: document(map[string]any{"Job": 2}), WriteConcern: word}},
		{"fifo pop", hoarddbwire.OpFIFOPop, hoarddbwire.PopRequest{Database: database, Bucket: "jobs", WriteConcern: word}},
		{"lifo push", hoarddbwire.OpLIFOPush, hoarddbwire.PushRequest{Database: database, Bucket: "stack", Data: document(map[string]any{"Plate": 2}), WriteConcern: word}},
		{"lifo pop", hoarddbwire.OpLIFOPop, hoarddbwire.PopRequest{Database: database, Bucket: "stack", WriteConcern: word}},
		{"heap push", hoarddbwire.OpHeapPush, hoarddbwire.PushRequest{Database: database, Bucket: "tasks", Data: document(map[string]any{"Task": 2}), Priority: &priority, WriteConcern: word}},
		{"heap pop", hoarddbwire.OpHeapPop, hoarddbwire.PopRequest{Database: database, Bucket: "tasks", WriteConcern: word}},
		{"blob write", hoarddbwire.OpBlobWrite, hoarddbwire.BlobWriteRequest{Database: database, Bucket: "files", Key: "bye.txt", Data: []byte("bye"), WriteConcern: word}},
		{"field push", hoarddbwire.OpFieldPush, fieldPush},
		{"field pop", hoarddbwire.OpFieldPop, field("Purchases", "")},
		{"field put", hoarddbwire.OpFieldPut, fieldPut},
		{"field delete", hoarddbwire.OpFieldDelete, field("Friends", "grace")},
		{"field put of a blob", hoarddbwire.OpFieldPut, fieldBlob},
	}
}

// Failure 11 (handbook/build-go.md section 2), the server's half: every write
// opcode the driver sends, with the word "bogus", is refused INVALID_ARGUMENT
// and not_applied, which proves the server reads writeconcern on that opcode,
// and nothing is written. It runs on three nodes, and on one: the vector "put on
// one node with a write concern the server does not know" shows a standalone
// node reads the word too, where handbook/errors.md section 2 says it does not.
func TestServerReadsTheConcernOnEveryWriteVerb(t *testing.T) {
	topologies := []struct {
		name  string
		start func(t *testing.T) *servertest.Server
	}{
		{"cluster3", func(t *testing.T) *servertest.Server {
			return servertest.StartCluster(t, 3, servertest.Options{}).Node(0)
		}},
		{"single", func(t *testing.T) *servertest.Server { return servertest.Start(t, servertest.Options{}) }},
	}
	for _, topology := range topologies {
		t.Run(topology.name, func(t *testing.T) {
			server := topology.start(t)
			ctx := clusterContext(t)
			client := connectDirect(t, server)
			createWriteTargets(ctx, t, client, "wc")
			for _, probe := range probesWith(t, "wc", "bogus") {
				conn, err := server.Dial(ctx)
				if err != nil {
					t.Fatal(err)
				}
				reply, err := conn.Do(ctx, probe.opcode, probe.request, nil)
				_ = conn.Close()
				if err != nil {
					t.Fatalf("%s: %v", probe.name, err)
				}
				if reply.Op != hoarddbwire.OpError {
					t.Errorf("%s with write concern \"bogus\" was answered %s: the server did not read writeconcern on %s", probe.name, reply.Op, probe.opcode)
					continue
				}
				refusal := hoarddbwire.DecodeError(reply.Payload)
				if refusal.Code != hoarddbwire.CodeInvalidArgument || refusal.WriteOutcome != hoarddbwire.WriteNotApplied ||
					!strings.Contains(refusal.Message, `"bogus"`) {
					t.Errorf("%s: %+v; want INVALID_ARGUMENT, not_applied, naming the word", probe.name, refusal)
				}
			}
			requireUntouched(ctx, t, client.Database("wc"))
		})
	}
}

// requireUntouched shows that no refused probe wrote anything: every target
// still holds exactly what createWriteTargets put there.
func requireUntouched(ctx context.Context, t *testing.T, db *hoarddb.Database) {
	t.Helper()
	counts := map[string]int64{"users": 1, "ranks": 1, "jobs": 1, "stack": 1, "tasks": 1, "User": 1}
	for bucket, want := range counts {
		if held, err := db.Bucket(bucket).Count(ctx, nil); err != nil || held != want {
			t.Errorf("%s holds %d (%v) after the refused probes; want %d", bucket, held, err, want)
		}
	}
	ada, err := db.Bucket("users").Get(ctx, "ada")
	if name, _ := ada.Lookup("Name"); err != nil || name.String() != `"Ada"` {
		t.Errorf("users/ada = %s, %v after the refused probes; want it unchanged", ada, err)
	}
	if _, err := db.Bucket("files").GetBlob(ctx, "bye.txt"); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Errorf("files/bye.txt after a refused blob write: %v; want ErrNotFound", err)
	}
	purchases, err := db.Bucket("User").Field("ada", "Purchases").Len(ctx)
	if err != nil || purchases != 1 {
		t.Errorf("ada's Purchases holds %d (%v); want 1", purchases, err)
	}
	friends, err := db.Bucket("User").Field("ada", "Friends").Len(ctx)
	if err != nil || friends != 1 {
		t.Errorf("ada's Friends holds %d (%v); want 1", friends, err)
	}
	if _, err := db.Bucket("User").Field("ada", "Data").GetBlob(ctx, "avatar"); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Errorf("ada's Data/avatar after a refused field put: %v; want ErrNotFound", err)
	}
}

// receiptPuts numbers the keys Bucket.Put writes in receiptVerbs, which a test
// builds again for each write concern.
var receiptPuts atomic.Int64

// receiptVerbs are the public writes that return a receipt with copy counts.
func receiptVerbs(db *hoarddb.Database) []struct {
	name string
	call func(ctx context.Context, opts ...hoarddb.WriteOption) (confirmed, factor int, err error)
} {
	return []struct {
		name string
		call func(ctx context.Context, opts ...hoarddb.WriteOption) (confirmed, factor int, err error)
	}{
		{"Bucket.Put", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			// Put inserts and refuses a held key, so each call names a new one.
			receipt, err := db.Bucket("users").Put(ctx, fmt.Sprintf("grace-%d", receiptPuts.Add(1)), hoarddb.Map{"Name": "Grace"}, opts...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"Bucket.Insert", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("users").Insert(ctx, hoarddb.Map{"Name": "Alan"}, opts...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"Bucket.InsertMany", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("users").InsertMany(ctx, []any{hoarddb.Map{"Name": "Edsger"}, hoarddb.Map{"Name": "Barbara"}}, opts...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"Bucket.Upsert", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("users").Upsert(ctx, "grace", hoarddb.Map{"Name": "Grace"}, opts...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"Bucket.UpsertMany", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("users").UpsertMany(ctx, []any{hoarddb.Map{"_id_": "edsger", "Name": "Edsger"}}, opts...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"Bucket.Push", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("jobs").Push(ctx, hoarddb.Map{"Job": 2}, opts...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"Bucket.PutBlob", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("files").PutBlob(ctx, "bye.txt", []byte("bye"), blobOptions(opts)...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"FieldStore.Push", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("User").Field("ada", "Purchases").Push(ctx, hoarddb.Map{"Item": "ink"}, opts...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"FieldStore.Put", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("User").Field("ada", "Friends").Put(ctx, "alan", hoarddb.Map{"Since": 1936}, opts...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
		{"FieldStore.PutBlob", func(ctx context.Context, opts ...hoarddb.WriteOption) (int, int, error) {
			receipt, err := db.Bucket("User").Field("ada", "Data").PutBlob(ctx, "avatar", []byte("png"), blobOptions(opts)...)
			return receipt.CopiesConfirmed, receipt.ReplicationFactor, err
		}},
	}
}

func blobOptions(opts []hoarddb.WriteOption) []hoarddb.BlobOption {
	converted := make([]hoarddb.BlobOption, len(opts))
	for position, option := range opts {
		converted[position] = option
	}
	return converted
}

// An all write with every node up is held by all three copies, and every
// receipt says so: copies confirmed and the replication factor, from the
// answer.
func TestAllWithEveryNodeUpReportsThreeCopies(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	ctx := clusterContext(t)
	client := connectDirect(t, cluster.Node(0))
	createWriteTargets(ctx, t, client, "copies")
	for _, verb := range receiptVerbs(client.Database("copies")) {
		confirmed, factor, err := verb.call(ctx, hoarddb.WithWriteConcern(hoarddb.All))
		if err != nil {
			t.Errorf("%s with write concern all: %v", verb.name, err)
			continue
		}
		if confirmed != 3 || factor != 3 {
			t.Errorf("%s: %d copies confirmed of a replication factor of %d; want 3 of 3", verb.name, confirmed, factor)
		}
	}
}

// handbook/errors.md section 4: a write concern counts the copies in sync, so
// an all write with a replica down is answered success with CopiesConfirmed
// below ReplicationFactor. Durability is a number to read, not a refusal.
func TestAllWithOneNodeStoppedIsAnsweredWithFewerCopies(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	ctx := clusterContext(t)
	client := connectDirect(t, cluster.Node(0))
	users, err := client.Database("v157").CreateBucket(ctx, "users", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Stop(2)
	receipt, err := users.Put(ctx, "ada", hoarddb.Map{"Name": "Ada", "Born": 1815}, hoarddb.WithWriteConcern(hoarddb.All))
	if err != nil {
		t.Fatalf("Put with write concern all and node3 stopped: %v; want success with fewer copies", err)
	}
	// The vector "put with write concern all and one node stopped" pins 2 of 3:
	// the two copies still in sync.
	if receipt.CopiesConfirmed != 2 || receipt.ReplicationFactor != 3 {
		t.Fatalf("receipt %+v; want 2 copies confirmed of a replication factor of 3, as the vector", receipt)
	}
	t.Logf("all with node3 stopped: %d of %d copies confirmed", receipt.CopiesConfirmed, receipt.ReplicationFactor)
}

// One node has no replicas: the server reports no copies, and every receipt
// says 0, "not reported", whatever level was asked for (handbook/operations.md
// section 6).
func TestSingleNodeReceiptsReportZeroCopies(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	ctx := clusterContext(t)
	client := connectDirect(t, server)
	createWriteTargets(ctx, t, client, "single")
	for _, concern := range []hoarddb.WriteConcern{hoarddb.One, hoarddb.Majority, hoarddb.All} {
		for _, verb := range receiptVerbs(client.Database("single")) {
			confirmed, factor, err := verb.call(ctx, hoarddb.WithWriteConcern(concern))
			if err != nil {
				t.Errorf("%s with write concern %s on one node: %v", verb.name, concern, err)
				continue
			}
			if confirmed != 0 || factor != 0 {
				t.Errorf("%s with %s on one node: %d of %d copies; want 0 of 0, not reported", verb.name, concern, confirmed, factor)
			}
		}
	}
}

// Consistent() is the read-your-writes verb on a cluster: a write acknowledged
// by one node is read back at once through another (handbook/operations.md
// section 6).
func TestConsistentGetAfterWrite(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	ctx := clusterContext(t)
	writer := connectDirect(t, cluster.Node(0))
	users, err := writer.Database("app").CreateBucket(ctx, "users", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	readers := []*hoarddb.Client{connectDirect(t, cluster.Node(1)), connectDirect(t, cluster.Node(2))}
	for round := range 10 {
		name := fmt.Sprintf("Ada %d", round)
		if _, err := users.Upsert(ctx, "ada", hoarddb.Map{"Name": name}, hoarddb.WithWriteConcern(hoarddb.One)); err != nil {
			t.Fatalf("round %d: Upsert: %v", round, err)
		}
		for index, reader := range readers {
			document, err := reader.Database("app").Bucket("users").Get(ctx, "ada", hoarddb.Consistent())
			if err != nil {
				t.Fatalf("round %d: consistent Get through node%d: %v", round, index+2, err)
			}
			if got, _ := document.Lookup("Name"); got.String() != fmt.Sprintf("%q", name) {
				t.Fatalf("round %d: consistent Get through node%d = %s; want the write just acknowledged, %q", round, index+2, got, name)
			}
		}
	}
}

// A node that leaves does not break the pool: the client was using it, and its
// next calls go to the nodes that are still up.
func TestPoolSurvivesANodeLeaving(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	ctx := clusterContext(t)
	first := cluster.Node(0)
	client, err := hoarddb.Connect(ctx, fmt.Sprintf("hoarddb://%s:%s@%s/", first.User, first.Password, strings.Join(cluster.Addresses(), ",")),
		hoarddb.WithFingerprint(cluster.Fingerprints()...), hoarddb.WithDefaultWriteConcern(hoarddb.Majority))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	users, err := client.Database("app").CreateBucket(ctx, "users", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 8 { // fill the pool with connections to node1, the host written first
		if _, err := users.Put(ctx, fmt.Sprintf("before-%d", index), hoarddb.Map{"Index": index}); err != nil {
			t.Fatal(err)
		}
	}
	if open := client.PoolStats().Open; open == 0 {
		t.Fatalf("pool %+v; want open connections before the node leaves", client.PoolStats())
	}

	cluster.Stop(0)
	health, err := client.Health(ctx)
	if err != nil || health.NodeID == "node1" {
		t.Fatalf("Health after node1 left = %+v, %v; want an answer from another node", health, err)
	}
	for index := range 8 {
		key := fmt.Sprintf("after-%d", index)
		receipt, err := users.Put(ctx, key, hoarddb.Map{"Index": index})
		if err != nil {
			t.Fatalf("Put %s after node1 left: %v", key, err)
		}
		if receipt.ReplicationFactor != 3 || receipt.CopiesConfirmed < 2 {
			t.Errorf("Put %s: %+v; want a majority of 3 copies confirmed", key, receipt)
		}
		if _, err := users.Get(ctx, key, hoarddb.Consistent()); err != nil {
			t.Fatalf("consistent Get %s after node1 left: %v", key, err)
		}
	}
	held, err := users.Count(ctx, nil)
	if err != nil || held != 16 {
		t.Fatalf("Count after node1 left = %d, %v; want the 16 documents", held, err)
	}
	if !slices.Contains([]string{"node2", "node3"}, health.NodeID) {
		t.Fatalf("Health answered by %q", health.NodeID)
	}
}
