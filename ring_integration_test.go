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
	"encoding/binary"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/internal/servertest"
	"github.com/hoarddb/hoarddb-go/ring"
)

// ringScene is what the steps of TestRingEveryMethodAgainstAServer share: the
// server, two connections to it and what earlier steps left for later ones.
type ringScene struct {
	ctx    context.Context
	server *servertest.Server
	client *ring.Client
	// stale is a second connection. It caches the header of app.User before the
	// alter step and writes the added field after it, with its old header.
	stale *ring.Client

	users, posts, rooms, sessions *ring.Bucket
	userInfo, roomInfo            ring.BucketInfo
	inserted                      ring.WriteResult
	lease                         ring.Lease
	batch                         *ring.Batch
	queued                        *ring.Bucket
	topology                      *ring.Topology

	// clusterServer is a second server, in cluster mode, for the membership steps; member is
	// the connection to it and only its one member.
	clusterServer *servertest.Server
	member        *ring.Client
	only          ring.Member

	eve         *ring.Entity
	evePurchase *ring.Store
	eveLease    ring.Lease
}

// ringStep is one row of the table: an exported function or method of package
// ring, named as Type.Method, and the call that exercises it against the
// server. The rows run in order and build on each other.
type ringStep struct {
	method string
	run    func(t *testing.T, scene *ringScene)
}

func ringSteps() []ringStep {
	return []ringStep{
		{"Dial", stepDial},
		{"Client.NextRequestID", stepNextRequestID},
		{"Client.CreateDatabase", stepCreateDatabase},
		{"Client.CreateDatabaseWithStorage", stepCreateDatabaseWithStorage},
		{"Client.ShowDatabases", stepShowDatabases},
		{"Client.CreateBucket", stepCreateBucket},
		{"Client.CreateBucketExpiring", stepCreateBucketExpiring},
		{"Client.DescribeBucket", stepDescribeBucket},
		{"Client.ShowBuckets", stepShowBuckets},
		{"Client.DescribeDatabase", stepDescribeDatabase},
		{"Client.DescribeNode", stepDescribeNode},
		{"Client.Bucket", stepBucket},
		{"Bucket.Info", stepInfo},
		{"Bucket.Insert", stepInsert},
		{"WriteResult.Applied", stepApplied},
		{"Bucket.Get", stepGet},
		{"Bucket.GetFields", stepGetFields},
		{"Bucket.Upsert", stepUpsert},
		{"Bucket.Update", stepUpdate},
		{"If", stepIf},
		{"Return", stepReturn},
		{"WithRequestID", stepWithRequestID},
		{"WithWriteDeadline", stepWithWriteDeadline},
		{"Error.Is", stepErrorIs},
		{"Error.Error", stepErrorError},
		{"Bucket.Push", stepPush},
		{"Bucket.StoreCount", stepStoreCount},
		{"Bucket.Peek", stepPeek},
		{"Bucket.PeekKeyed", stepPeekKeyed},
		{"Bucket.StoreList", stepStoreList},
		{"Bucket.Claim", stepClaim},
		{"Bucket.LeaseGet", stepLeaseGet},
		{"Bucket.Extend", stepExtend},
		{"Bucket.Release", stepRelease},
		{"Bucket.Ack", stepAck},
		{"Bucket.Pop", stepPop},
		{"Bucket.PopN", stepPopN},
		{"Bucket.Put", stepPut},
		{"Bucket.StoreGet", stepStoreGet},
		{"Bucket.Take", stepTake},
		{"Bucket.Inc", stepInc},
		{"Bucket.IncAfter", stepIncAfter},
		{"Bucket.Nearest", stepNearest},
		{"Bucket.Sum", stepSum},
		{"Bucket.SumBy", stepSumBy},
		{"Bucket.CountBy", stepCountBy},
		{"Bucket.Aggregate", stepAggregate},
		{"Bucket.DescribeStore", stepDescribeStore},
		{"Bucket.GetExpanded", stepGetExpanded},
		{"Bucket.StoreListExpanded", stepStoreListExpanded},
		{"Bucket.GetMany", stepBucketGetMany},
		{"Client.GetMany", stepClientGetMany},
		{"Bucket.List", stepList},
		{"Bucket.Count", stepCount},
		{"Client.NewBatch", stepNewBatch},
		{"Bucket.In", stepIn},
		{"Batch.Len", stepBatchLen},
		{"Batch.Send", stepBatchSend},
		{"Client.Topology", stepTopology},
		{"Topology.Owner", stepOwner},
		{"Client.SeedingStatus", stepSeedingStatus},
		{"Client.SeedingRetry", stepSeedingRetry},
		{"Client.SeedingForget", stepSeedingForget},
		{"Client.Join", stepJoin},
		{"Client.Promote", stepPromote},
		{"Client.Leave", stepLeave},
		{"Client.RemoveNode", stepRemoveNode},
		{"Client.ReplaceNode", stepReplaceNode},
		{"Client.AlterBucket", stepAlterBucket},
		{"ExpiresIn", stepExpiresIn},
		{"IsExpired", stepIsExpired},
		{"Bucket.Entity", stepEntity},
		{"Bucket.Field", stepField},
		{"Entity.Store", stepEntityStore},
		{"Entity.Insert", stepEntityInsert},
		{"Entity.Get", stepEntityGet},
		{"Entity.GetFields", stepEntityGetFields},
		{"Entity.GetExpanded", stepEntityGetExpanded},
		{"Entity.Upsert", stepEntityUpsert},
		{"Entity.Update", stepEntityUpdate},
		{"Store.Push", stepStorePush},
		{"Store.Count", stepStoreHandleCount},
		{"Store.Peek", stepStorePeek},
		{"Store.PeekKeyed", stepStorePeekKeyed},
		{"Store.List", stepStoreHandleList},
		{"Store.ListExpanded", stepStoreHandleListExpanded},
		{"Store.Claim", stepStoreClaim},
		{"Store.LeaseGet", stepStoreLeaseGet},
		{"Store.Extend", stepStoreExtend},
		{"Store.Release", stepStoreRelease},
		{"Store.Ack", stepStoreAck},
		{"Store.Pop", stepStorePop},
		{"Store.PopN", stepStorePopN},
		{"Store.Put", stepStorePut},
		{"Store.Get", stepStoreHandleGet},
		{"Store.Take", stepStoreTake},
		{"Store.Inc", stepStoreInc},
		{"Store.IncAfter", stepStoreIncAfter},
		{"Store.Nearest", stepStoreNearest},
		{"Store.Sum", stepStoreSum},
		{"Store.SumBy", stepStoreSumBy},
		{"Store.CountBy", stepStoreCountBy},
		{"Store.Aggregate", stepStoreAggregate},
		{"Store.Describe", stepStoreDescribe},
		{"Entity.Delete", stepEntityDelete},
		{"Bucket.Delete", stepDelete},
		{"Client.DropBucketExpecting", stepDropBucketExpecting},
		{"Client.DropBucket", stepDropBucket},
		{"Client.Close", stepClose},
	}
}

// TestRingEveryMethodAgainstAServer calls every exported function and method of
// package ring against a real server, the one HOARDDB_SERVER_BIN names (the
// gate builds it from this tree). It is one scenario on a one-node cluster, in
// table order: a step that fails stops the run, because the later ones read
// what it wrote.
func TestRingEveryMethodAgainstAServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	// Both servers belong to the test, not to a step: a server started inside a step would stop
	// when that step returns.
	scene := &ringScene{ctx: ctx, server: servertest.Start(t, servertest.Options{})}
	scene.clusterServer = servertest.Start(t, servertest.Options{Env: map[string]string{
		"HOARDDB_CLUSTER_ENABLED": "1", "HOARDDB_NODE_ID": "ring-one",
	}})
	for _, step := range ringSteps() {
		if step.run == nil {
			t.Fatalf("step %s is not implemented", step.method)
		}
		if !t.Run(step.method, func(t *testing.T) { step.run(t, scene) }) {
			t.Fatalf("stopped at %s: the steps after it build on what it leaves on the server", step.method)
		}
	}
}

// TestRingTableNamesEveryExportedMethod reads the source of package ring and
// fails for each exported function, or exported method of an exported type,
// that the table of TestRingEveryMethodAgainstAServer does not name, and for
// each row that names nothing. It needs no server.
func TestRingTableNamesEveryExportedMethod(t *testing.T) {
	packages, err := parser.ParseDir(token.NewFileSet(), "ring", func(file fs.FileInfo) bool {
		return !strings.HasSuffix(file.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	exported := map[string]bool{}
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || !fn.Name.IsExported() {
					continue
				}
				if fn.Recv == nil {
					exported[fn.Name.Name] = true
					continue
				}
				receiver := fn.Recv.List[0].Type
				if star, ok := receiver.(*ast.StarExpr); ok {
					receiver = star.X
				}
				if ident, ok := receiver.(*ast.Ident); ok && ident.IsExported() {
					exported[ident.Name+"."+fn.Name.Name] = true
				}
			}
		}
	}
	if len(exported) == 0 {
		t.Fatal("found no exported function in ./ring; the test reads the wrong directory")
	}
	named := map[string]bool{}
	for _, step := range ringSteps() {
		if named[step.method] {
			t.Errorf("the table names %s twice", step.method)
		}
		named[step.method] = true
		if !exported[step.method] {
			t.Errorf("the table names %s, which package ring does not export", step.method)
		}
	}
	var missing []string
	for name := range exported {
		if !named[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("the table of TestRingEveryMethodAgainstAServer names no step for: %s", strings.Join(missing, ", "))
	}
}

// --- helpers ---

func ringLabelled(label string) bson.D { return bson.D{{Key: "item", Value: label}} }

// ringText is the string at keys of raw, or "" when it is absent or no string.
func ringText(raw bson.Raw, keys ...string) string {
	found, err := raw.LookupErr(keys...)
	if err != nil {
		return ""
	}
	text, _ := found.StringValueOK()
	return text
}

// ringWhole is the integer at keys of raw, whichever integer width it has.
func ringWhole(t *testing.T, raw bson.Raw, keys ...string) int64 {
	t.Helper()
	found, err := raw.LookupErr(keys...)
	if err != nil {
		t.Fatalf("%v has no %v: %v", raw, keys, err)
	}
	whole, ok := found.AsInt64OK()
	if !ok {
		t.Fatalf("%v of %v is a %s, not an integer", keys, raw, found.Type)
	}
	return whole
}

// ringListLen is how many members the array at key of raw has.
func ringListLen(t *testing.T, raw bson.Raw, key string) int {
	t.Helper()
	found, err := raw.LookupErr(key)
	if err != nil {
		t.Fatalf("%v has no %s: %v", raw, key, err)
	}
	array, ok := found.ArrayOK()
	if !ok {
		t.Fatalf("%s of %v is a %s, not an array", key, raw, found.Type)
	}
	members, err := array.Values()
	if err != nil {
		t.Fatal(err)
	}
	return len(members)
}

func ringLabels(elements []bson.Raw) string {
	labels := make([]string, len(elements))
	for i, element := range elements {
		labels[i] = ringText(element, "item")
	}
	return strings.Join(labels, ",")
}

// ringVector is a little-endian float32 vector, the form Nearest reads.
func ringVector(components ...float32) primitive.Binary {
	encoded := make([]byte, 4*len(components))
	for i, component := range components {
		binary.LittleEndian.PutUint32(encoded[4*i:], math.Float32bits(component))
	}
	return primitive.Binary{Data: encoded}
}

// ringApplied returns a check that a write was acknowledged as applied.
func ringApplied(t *testing.T, what string) func(ring.WriteResult, error) {
	return func(written ring.WriteResult, err error) {
		t.Helper()
		if err != nil || !written.Applied() {
			t.Fatalf("%s: %+v, %v; want an applied write", what, written, err)
		}
	}
}

// ringRefusal is the server's refusal inside err; anything else fails the step:
// a refusal with a ring code means the server decoded the request and answered.
func ringRefusal(t *testing.T, what string, err error) *ring.Error {
	t.Helper()
	var refusal *ring.Error
	if !errors.As(err, &refusal) {
		t.Fatalf("%s: %v (%T); want a refusal from the server with a ring code", what, err, err)
	}
	return refusal
}

func ringDial(t *testing.T, scene *ringScene, password string) (*ring.Client, error) {
	t.Helper()
	return ring.Dial(scene.ctx, ring.Options{
		Addr: scene.server.Address, User: scene.server.User, Password: password, Fingerprint: scene.server.Fingerprint,
	})
}

func ringOrder(amount int64, kind string) bson.D {
	return bson.D{{Key: "Amount", Value: amount}, {Key: "Kind", Value: kind}}
}

var ringAuthor = []ring.ExpandRequest{{Field: "Author", Fields: []string{"Name"}}}

// --- connection and declarations ---

func stepDial(t *testing.T, scene *ringScene) {
	if _, err := ringDial(t, scene, "not-the-password"); err == nil {
		t.Fatal("Dial with a wrong password succeeded")
	}
	client, err := ringDial(t, scene, scene.server.Password)
	if err != nil {
		t.Fatalf("Dial pinned to the fingerprint the server printed: %v", err)
	}
	stale, err := ringDial(t, scene, scene.server.Password)
	if err != nil {
		t.Fatalf("a second Dial: %v", err)
	}
	scene.client, scene.stale = client, stale
}

func stepNextRequestID(t *testing.T, scene *ringScene) {
	first, second := scene.client.NextRequestID(), scene.client.NextRequestID()
	if first == ([16]byte{}) || first == second {
		t.Fatalf("NextRequestID gave %x then %x; want two different ids, neither zero", first, second)
	}
}

func stepCreateDatabase(t *testing.T, scene *ringScene) {
	created, err := scene.client.CreateDatabase(scene.ctx, "app", 0)
	if err != nil || created.RF < 1 {
		t.Fatalf("CreateDatabase app: %+v, %v", created, err)
	}
}

func stepCreateDatabaseWithStorage(t *testing.T, scene *ringScene) {
	created, err := scene.client.CreateDatabaseWithStorage(scene.ctx, "cache", 0, "memory")
	if err != nil || created.Storage != "memory" {
		t.Fatalf("CreateDatabaseWithStorage cache memory: %+v, %v", created, err)
	}
}

func stepShowDatabases(t *testing.T, scene *ringScene) {
	page, err := scene.client.ShowDatabases(scene.ctx, ring.PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for _, database := range page.Databases {
		listed[database.Name] = true
	}
	if !listed["app"] || !listed["cache"] {
		t.Fatalf("ShowDatabases = %+v; want app and cache in it", page.Databases)
	}
}

func stepCreateBucket(t *testing.T, scene *ringScene) {
	created, err := scene.client.CreateBucket(scene.ctx, "app", "User", []ring.FieldDecl{
		{Name: "Name", Type: "string"}, {Name: "Age", Type: "int"},
		{Name: "Purchases", Type: "fifoStore"}, {Name: "Photos", Type: "lifoStore"},
		{Name: "Friends", Type: "hashStore"}, {Name: "Recipes", Type: "btreeStore"},
		{Name: "Counters", Type: "hashStore"}, {Name: "Places", Type: "hashStore"},
		{Name: "Orders", Type: "hashStore", Aggregates: []ring.AggregateDecl{
			{Kind: "sum", Field: "Amount"}, {Kind: "sum by", Field: "Amount", By: "Kind"}, {Kind: "count by", By: "Kind"},
		}},
	})
	if err != nil || created.ID == 0 || len(created.Fields) != 10 {
		t.Fatalf("CreateBucket app.User: %+v, %v; want an id and ten fields, _id_ first", created, err)
	}
	scene.userInfo = created
	if _, err := scene.client.CreateBucket(scene.ctx, "app", "Post", []ring.FieldDecl{
		{Name: "Author", Type: "string", Of: "User"}, {Name: "Text", Type: "string"},
	}); err != nil {
		t.Fatalf("CreateBucket app.Post: %v", err)
	}
	scene.roomInfo, err = scene.client.CreateBucket(scene.ctx, "app", "Room", []ring.FieldDecl{
		{Name: "Name", Type: "string"}, {Name: "Members", Type: "hashStore", Of: "User"},
	})
	if err != nil {
		t.Fatalf("CreateBucket app.Room: %v", err)
	}
}

func stepCreateBucketExpiring(t *testing.T, scene *ringScene) {
	created, err := scene.client.CreateBucketExpiring(scene.ctx, "app", "Session", "ExpiresAt", []ring.FieldDecl{
		{Name: "Token", Type: "string"}, {Name: "ExpiresAt", Type: "datetime"},
	})
	if err != nil || created.ExpireField != "ExpiresAt" {
		t.Fatalf("CreateBucketExpiring app.Session: %+v, %v; want it to expire at ExpiresAt", created, err)
	}
}

func stepDescribeBucket(t *testing.T, scene *ringScene) {
	described, err := scene.client.DescribeBucket(scene.ctx, "app", "User")
	if err != nil || described.ID != scene.userInfo.ID || described.State != "active" || len(described.Aggregates) != 3 {
		t.Fatalf("DescribeBucket app.User: %+v, %v; want bucket %d, active, with three aggregates", described, err, scene.userInfo.ID)
	}
	_, err = scene.client.DescribeBucket(scene.ctx, "app", "Nobody")
	if refusal := ringRefusal(t, "DescribeBucket of a bucket nobody declared", err); !errors.Is(refusal, ring.ErrNotFound) {
		t.Fatalf("DescribeBucket of a bucket nobody declared: %v; want NOT_FOUND", refusal)
	}
}

func stepShowBuckets(t *testing.T, scene *ringScene) {
	listed := map[string]bool{}
	request := ring.PageRequest{Limit: 2}
	for pages := 0; ; pages++ {
		page, err := scene.client.ShowBuckets(scene.ctx, "app", request)
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range page.Buckets {
			listed[bucket.Name] = true
		}
		if !page.More {
			break
		}
		if pages > 10 {
			t.Fatal("ShowBuckets with limit 2 never answered a last page of four buckets")
		}
		request.Cursor = page.Cursor
	}
	if len(listed) != 4 || !listed["User"] || !listed["Post"] || !listed["Room"] || !listed["Session"] {
		t.Fatalf("ShowBuckets paged by two = %v; want User, Post, Room and Session", listed)
	}
}

func stepDescribeDatabase(t *testing.T, scene *ringScene) {
	described, err := scene.client.DescribeDatabase(scene.ctx, "app", ring.PageRequest{})
	if err != nil || described.Name != "app" || len(described.Buckets) != 4 {
		t.Fatalf("DescribeDatabase app: %+v, %v; want its four buckets", described, err)
	}
	memory, err := scene.client.DescribeDatabase(scene.ctx, "cache", ring.PageRequest{})
	if err != nil || memory.Storage != "memory" {
		t.Fatalf("DescribeDatabase cache: %+v, %v; want storage memory", memory, err)
	}
}

func stepDescribeNode(t *testing.T, scene *ringScene) {
	node, err := scene.client.DescribeNode(scene.ctx)
	if err != nil || node.NodeID == "" || node.Version == "" || node.Members != 1 {
		t.Fatalf("DescribeNode: %+v, %v; want a node id, a version and one member", node, err)
	}
}

func stepBucket(t *testing.T, scene *ringScene) {
	scene.users, scene.posts = scene.client.Bucket("app", "User"), scene.client.Bucket("app", "Post")
	scene.rooms, scene.sessions = scene.client.Bucket("app", "Room"), scene.client.Bucket("app", "Session")
	// A handle sends nothing, so one on a bucket nobody declared is made without an error and
	// the first call through it is what the server refuses.
	_, err := scene.client.Bucket("app", "Nobody").Get(scene.ctx, "ada")
	if refusal := ringRefusal(t, "a Get through a handle on an undeclared bucket", err); !errors.Is(refusal, ring.ErrNotFound) {
		t.Fatalf("a Get through a handle on an undeclared bucket: %v; want NOT_FOUND", refusal)
	}
}

func stepInfo(t *testing.T, scene *ringScene) {
	cached, err := scene.users.Info(scene.ctx)
	if err != nil || cached.ID != scene.userInfo.ID || cached.DeclVersion != scene.userInfo.DeclVersion {
		t.Fatalf("Info on the creating client: %+v, %v; want the header CreateBucket cached", cached, err)
	}
	described, err := scene.stale.Bucket("app", "User").Info(scene.ctx)
	if err != nil || described.ID != scene.userInfo.ID || len(described.Fields) != 10 {
		t.Fatalf("Info on a client that has not seen the bucket: %+v, %v; want it described", described, err)
	}
}

// --- documents ---

func stepInsert(t *testing.T, scene *ringScene) {
	written, err := scene.users.Insert(scene.ctx, "ada", ring.Doc{"Name": "Ada", "Age": int64(36)})
	if err != nil {
		t.Fatal(err)
	}
	scene.inserted = written
	if _, err := scene.users.Insert(scene.ctx, "ada", ring.Doc{"Name": "Again"}); err == nil {
		t.Fatal("a second insert of the same id was accepted")
	}
	ringApplied(t, "insert of a post")(scene.posts.Insert(scene.ctx, "p1", ring.Doc{"Author": "ada", "Text": "hello"}))
	ringApplied(t, "insert of a room")(scene.rooms.Insert(scene.ctx, "r1", ring.Doc{"Name": "general"}))
}

func stepApplied(t *testing.T, scene *ringScene) {
	if !scene.inserted.Applied() || scene.inserted.Outcome != "applied" {
		t.Fatalf("the insert answered %+v; want Applied on a one-node cluster", scene.inserted)
	}
	if (ring.WriteResult{Outcome: "applied_degraded"}).Applied() || (ring.WriteResult{}).Applied() {
		t.Fatal("Applied is true for a write that was not acknowledged as applied")
	}
}

func stepGet(t *testing.T, scene *ringScene) {
	got, err := scene.users.Get(scene.ctx, "ada")
	if err != nil || ringText(got, "Name") != "Ada" || ringWhole(t, got, "Age") != 36 {
		t.Fatalf("Get ada: %v, %v", got, err)
	}
}

func stepGetFields(t *testing.T, scene *ringScene) {
	got, err := scene.users.GetFields(scene.ctx, "ada", []string{"Name"})
	if err != nil || ringText(got, "Name") != "Ada" {
		t.Fatalf("GetFields ada Name: %v, %v", got, err)
	}
	if _, err := got.LookupErr("Age"); err == nil {
		t.Fatalf("GetFields ada Name returned Age too: %v", got)
	}
}

func stepUpsert(t *testing.T, scene *ringScene) {
	ringApplied(t, "upsert over ada")(scene.users.Upsert(scene.ctx, "ada", ring.Doc{"Name": "Ada L", "Age": int64(37)}))
	ringApplied(t, "upsert of a new id")(scene.users.Upsert(scene.ctx, "bob", ring.Doc{"Name": "Bob", "Age": int64(20)}))
	got, err := scene.users.Get(scene.ctx, "ada")
	if err != nil || ringText(got, "Name") != "Ada L" || ringWhole(t, got, "Age") != 37 {
		t.Fatalf("after the upsert: %v, %v", got, err)
	}
}

func stepUpdate(t *testing.T, scene *ringScene) {
	ringApplied(t, "update set")(scene.users.Update(scene.ctx, "ada", ring.Update{Set: bson.D{{Key: "Age", Value: int64(40)}}}))
	got, err := scene.users.Get(scene.ctx, "ada")
	if err != nil || ringWhole(t, got, "Age") != 40 || ringText(got, "Name") != "Ada L" {
		t.Fatalf("after the update: %v, %v", got, err)
	}
	_, err = scene.users.Update(scene.ctx, "nobody", ring.Update{Set: bson.D{{Key: "Age", Value: int64(1)}}})
	if !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("update of a missing document: %v; want NOT_FOUND", err)
	}
}

func stepIf(t *testing.T, scene *ringScene) {
	rename := ring.Update{Set: bson.D{{Key: "Name", Value: "Countess"}}}
	refused, err := scene.users.Update(scene.ctx, "ada", rename, ring.If(ring.Guard{{Key: "Name", Value: "Somebody"}}))
	if err != nil || refused.Applied() || !refused.NotApplied || refused.Reason != "guard" || refused.GuardField != "Name" {
		t.Fatalf("an update whose guard does not hold: %+v, %v; want not_applied naming Name", refused, err)
	}
	if ringText(refused.Document, "Name") != "Ada L" {
		t.Fatalf("a refused guard returned %v; want the current document", refused.Document)
	}
	ringApplied(t, "an update whose guard holds")(scene.users.Update(scene.ctx, "ada", rename, ring.If(ring.Guard{{Key: "Name", Value: "Ada L"}})))
}

func stepReturn(t *testing.T, scene *ringScene) {
	after, err := scene.users.Update(scene.ctx, "ada", ring.Update{Set: bson.D{{Key: "Age", Value: int64(41)}}}, ring.Return(ring.ReturnAfter))
	if err != nil || !after.Applied() || ringWhole(t, after.Document, "Age") != 41 {
		t.Fatalf("update returning after: %+v, %v", after, err)
	}
	before, err := scene.users.Update(scene.ctx, "ada", ring.Update{Set: bson.D{{Key: "Age", Value: int64(42)}}}, ring.Return(ring.ReturnBefore))
	if err != nil || !before.Applied() || ringWhole(t, before.Document, "Age") != 41 {
		t.Fatalf("update returning before: %+v, %v", before, err)
	}
}

func stepWithRequestID(t *testing.T, scene *ringScene) {
	id := scene.client.NextRequestID()
	add := ring.Update{Inc: bson.D{{Key: "Age", Value: int64(1)}}}
	first, err := scene.users.Update(scene.ctx, "ada", add, ring.WithRequestID(id))
	if err != nil || !first.Applied() || first.Deduped {
		t.Fatalf("the first send: %+v, %v", first, err)
	}
	retry, err := scene.users.Update(scene.ctx, "ada", add, ring.WithRequestID(id))
	if err != nil || !retry.Deduped {
		t.Fatalf("the retry under the same id: %+v, %v; want it answered from the dedupe window", retry, err)
	}
	got, err := scene.users.Get(scene.ctx, "ada")
	if err != nil || ringWhole(t, got, "Age") != 43 {
		t.Fatalf("after an inc sent twice under one id: %v, %v; want Age 43, added once", got, err)
	}
}

func stepWithWriteDeadline(t *testing.T, scene *ringScene) {
	ringApplied(t, "a write with a five second deadline")(
		scene.users.Upsert(scene.ctx, "bob", ring.Doc{"Name": "Bob", "Age": int64(21)}, ring.WithWriteDeadline(5000)))
}

// A Get that finds nothing is ErrNotFound made by the client from the reply; a refusal is an
// *Error, and its Is makes the server's NOT_FOUND match ErrNotFound too.
func stepErrorIs(t *testing.T, scene *ringScene) {
	if _, err := scene.users.Get(scene.ctx, "nobody"); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("Get of a missing document: %v; want ErrNotFound", err)
	}
	_, err := scene.client.DescribeBucket(scene.ctx, "app", "Nobody")
	refusal := ringRefusal(t, "DescribeBucket of a bucket nobody declared", err)
	if !refusal.Is(ring.ErrNotFound) || refusal.Is(context.Canceled) || !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("Is of %v: want true for ErrNotFound and false for another error", refusal)
	}
	_, err = scene.users.Insert(scene.ctx, "ada", ring.Doc{"Name": "Again"})
	if exists := ringRefusal(t, "a second insert", err); exists.Is(ring.ErrNotFound) {
		t.Fatalf("Is(ErrNotFound) is true for %v, which is not NOT_FOUND", exists)
	}
}

func stepErrorError(t *testing.T, scene *ringScene) {
	_, err := scene.client.DescribeBucket(scene.ctx, "app", "Nobody")
	refusal := ringRefusal(t, "DescribeBucket of a bucket nobody declared", err)
	if refusal.Message == "" || refusal.Error() != fmt.Sprintf("%s: %s", refusal.Code, refusal.Message) {
		t.Fatalf("Error() = %q; want the code, a colon and the server's message %q", refusal.Error(), refusal.Message)
	}
}

// --- stores: queue, lease, keyed ---

func stepPush(t *testing.T, scene *ringScene) {
	for _, label := range []string{"coffee", "tea", "milk"} {
		ringApplied(t, "push "+label)(scene.users.Push(scene.ctx, "ada", "Purchases", ringLabelled(label)))
	}
	for _, label := range []string{"first", "second"} {
		ringApplied(t, "push "+label)(scene.users.Push(scene.ctx, "ada", "Photos", ringLabelled(label)))
	}
}

func stepStoreCount(t *testing.T, scene *ringScene) {
	if count, err := scene.users.StoreCount(scene.ctx, "ada", "Purchases"); err != nil || count != 3 {
		t.Fatalf("StoreCount of Purchases: %d, %v; want 3", count, err)
	}
	if count, err := scene.users.StoreCount(scene.ctx, "ada", "Friends"); err != nil || count != 0 {
		t.Fatalf("StoreCount of an empty store: %d, %v; want 0", count, err)
	}
}

func stepPeek(t *testing.T, scene *ringScene) {
	peeked, err := scene.users.Peek(scene.ctx, "ada", "Purchases", 2)
	if err != nil || ringLabels(peeked) != "coffee,tea" {
		t.Fatalf("Peek 2 of the fifo: %v, %v; want coffee then tea", peeked, err)
	}
	newest, err := scene.users.Peek(scene.ctx, "ada", "Photos", 1)
	if err != nil || ringLabels(newest) != "second" {
		t.Fatalf("Peek 1 of the lifo: %v, %v; want the newest", newest, err)
	}
}

func stepPeekKeyed(t *testing.T, scene *ringScene) {
	page, err := scene.users.PeekKeyed(scene.ctx, "ada", "Purchases", 1)
	if err != nil || ringLabels(page.Elements) != "coffee" || len(page.Keys) != 0 {
		t.Fatalf("PeekKeyed of a queue: %+v, %v; want coffee and no keys", page, err)
	}
}

func stepStoreList(t *testing.T, scene *ringScene) {
	first, err := scene.users.StoreList(scene.ctx, "ada", "Purchases", 2, nil)
	if err != nil || ringLabels(first.Elements) != "coffee,tea" || !first.More || len(first.Cursor) == 0 {
		t.Fatalf("StoreList page one: %+v, %v; want two elements, more and a cursor", first, err)
	}
	second, err := scene.users.StoreList(scene.ctx, "ada", "Purchases", 2, first.Cursor)
	if err != nil || ringLabels(second.Elements) != "milk" || second.More {
		t.Fatalf("StoreList after the cursor: %+v, %v; want milk and no more", second, err)
	}
}

func stepClaim(t *testing.T, scene *ringScene) {
	lease, err := scene.users.Claim(scene.ctx, "ada", "Purchases", 1)
	if err != nil || lease.ID == ([16]byte{}) || ringLabels(lease.Elements) != "coffee" {
		t.Fatalf("Claim 1: %+v, %v; want a lease on coffee", lease, err)
	}
	scene.lease = lease
}

func stepLeaseGet(t *testing.T, scene *ringScene) {
	read, err := scene.users.LeaseGet(scene.ctx, "ada", "Purchases", scene.lease.ID)
	if err != nil || !read.Found || ringLabels(read.Elements) != "coffee" {
		t.Fatalf("LeaseGet of a live lease: %+v, %v", read, err)
	}
	gone, err := scene.users.LeaseGet(scene.ctx, "ada", "Purchases", scene.client.NextRequestID())
	if err != nil || gone.Found {
		t.Fatalf("LeaseGet of a lease nobody holds: %+v, %v; want found false", gone, err)
	}
}

func stepExtend(t *testing.T, scene *ringScene) {
	change, err := scene.users.Extend(scene.ctx, "ada", "Purchases", scene.lease.ID, 60000)
	if err != nil || !change.Live {
		t.Fatalf("Extend: %+v, %v; want the lease live", change, err)
	}
}

func stepRelease(t *testing.T, scene *ringScene) {
	change, err := scene.users.Release(scene.ctx, "ada", "Purchases", scene.lease.ID)
	if err != nil || !change.Live {
		t.Fatalf("Release: %+v, %v; want the lease live when it was released", change, err)
	}
	if read, err := scene.users.LeaseGet(scene.ctx, "ada", "Purchases", scene.lease.ID); err != nil || read.Found {
		t.Fatalf("a released lease: %+v, %v; want it gone", read, err)
	}
}

func stepAck(t *testing.T, scene *ringScene) {
	again, err := scene.users.Claim(scene.ctx, "ada", "Purchases", 1)
	if err != nil || ringLabels(again.Elements) != "coffee" {
		t.Fatalf("Claim after the release: %+v, %v; want coffee back at the head", again, err)
	}
	if _, err := scene.users.Ack(scene.ctx, "ada", "Purchases", again.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if read, err := scene.users.LeaseGet(scene.ctx, "ada", "Purchases", again.ID); err != nil || read.Found {
		t.Fatalf("an acked lease: %+v, %v; want it gone", read, err)
	}
	if count, err := scene.users.StoreCount(scene.ctx, "ada", "Purchases"); err != nil || count != 2 {
		t.Fatalf("after the ack the store holds %d, %v; want 2: the acked message is removed", count, err)
	}
}

func stepPop(t *testing.T, scene *ringScene) {
	popped, err := scene.users.Pop(scene.ctx, "ada", "Purchases")
	if err != nil || ringText(popped, "item") != "tea" {
		t.Fatalf("Pop of the fifo: %v, %v; want tea", popped, err)
	}
	top, err := scene.users.Pop(scene.ctx, "ada", "Photos")
	if err != nil || ringText(top, "item") != "second" {
		t.Fatalf("Pop of the lifo: %v, %v; want the newest", top, err)
	}
}

func stepPopN(t *testing.T, scene *ringScene) {
	for _, label := range []string{"sugar", "salt"} {
		ringApplied(t, "push "+label)(scene.users.Push(scene.ctx, "ada", "Purchases", ringLabelled(label)))
	}
	popped, err := scene.users.PopN(scene.ctx, "ada", "Purchases", 2)
	if err != nil || ringLabels(popped) != "milk,sugar" {
		t.Fatalf("PopN 2: %v, %v; want milk then sugar", popped, err)
	}
	rest, err := scene.users.PopN(scene.ctx, "ada", "Purchases", 10)
	if err != nil || ringLabels(rest) != "salt" {
		t.Fatalf("PopN 10 of one left: %v, %v", rest, err)
	}
	none, err := scene.users.PopN(scene.ctx, "ada", "Purchases", 3)
	if err != nil || len(none) != 0 {
		t.Fatalf("PopN of an empty store: %v, %v; want an empty list and no error", none, err)
	}
}

func stepPut(t *testing.T, scene *ringScene) {
	for _, store := range []string{"Friends", "Recipes"} {
		for _, key := range []string{"grace", "bob"} {
			ringApplied(t, "put "+key+" in "+store)(scene.users.Put(scene.ctx, "ada", store, key, ringLabelled("about "+key)))
		}
	}
	ringApplied(t, "put over a key")(scene.users.Put(scene.ctx, "ada", "Friends", "grace", ringLabelled("pal")))
	if count, err := scene.users.StoreCount(scene.ctx, "ada", "Friends"); err != nil || count != 2 {
		t.Fatalf("after three puts on two keys the store holds %d, %v; want 2", count, err)
	}
	for _, member := range []string{"ada", "ghost"} {
		ringApplied(t, "put a room member")(scene.rooms.Put(scene.ctx, "r1", "Members", member, ring.Doc{"since": int64(1)}))
	}
}

func stepStoreGet(t *testing.T, scene *ringScene) {
	got, err := scene.users.StoreGet(scene.ctx, "ada", "Friends", "grace")
	if err != nil || ringText(got, "item") != "pal" {
		t.Fatalf("StoreGet grace: %v, %v; want the element the last put wrote", got, err)
	}
	if _, err := scene.users.StoreGet(scene.ctx, "ada", "Recipes", "linus"); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("StoreGet of an absent key: %v; want ErrNotFound", err)
	}
}

func stepTake(t *testing.T, scene *ringScene) {
	for _, store := range []string{"Friends", "Recipes"} {
		taken, err := scene.users.Take(scene.ctx, "ada", store, "bob")
		if err != nil || ringText(taken, "item") != "about bob" {
			t.Fatalf("Take bob from %s: %v, %v", store, taken, err)
		}
		if _, err := scene.users.Take(scene.ctx, "ada", store, "bob"); !errors.Is(err, ring.ErrNotFound) {
			t.Fatalf("a second Take of bob from %s: %v; want ErrNotFound", store, err)
		}
	}
	page, err := scene.users.StoreList(scene.ctx, "ada", "Friends", 0, nil)
	if err != nil || fmt.Sprint(page.Keys) != "[grace]" {
		t.Fatalf("Friends after the take: %+v, %v; want only grace", page, err)
	}
}

func stepInc(t *testing.T, scene *ringScene) {
	if _, err := scene.users.Inc(scene.ctx, "ada", "Counters", "visits", map[string]any{"N": int64(2)}); err != nil {
		t.Fatalf("Inc of a key nobody wrote: %v", err)
	}
	got, err := scene.users.StoreGet(scene.ctx, "ada", "Counters", "visits")
	if err != nil || ringWhole(t, got, "N") != 2 {
		t.Fatalf("after the first Inc: %v, %v; want N 2", got, err)
	}
}

func stepIncAfter(t *testing.T, scene *ringScene) {
	element, written, err := scene.users.IncAfter(scene.ctx, "ada", "Counters", "visits", map[string]any{"N": int64(3)})
	if err != nil || !written.Applied() || ringWhole(t, element, "N") != 5 {
		t.Fatalf("IncAfter: %v, %+v, %v; want the element with N 5", element, written, err)
	}
}

func stepNearest(t *testing.T, scene *ringScene) {
	for key, vector := range map[string]primitive.Binary{"near": ringVector(1, 0, 0), "mid": ringVector(1, 1, 0), "far": ringVector(0, 1, 0)} {
		ringApplied(t, "put "+key)(scene.users.Put(scene.ctx, "ada", "Places", key, bson.D{{Key: "Vector", Value: vector}}))
	}
	nearest, err := scene.users.Nearest(scene.ctx, "ada", "Places", ring.NearestQuery{Vector: ringVector(1, 0, 0).Data, K: 2})
	if err != nil || fmt.Sprint(nearest.Keys) != "[near mid]" || len(nearest.Elements) != 2 || nearest.Scanned != 3 {
		t.Fatalf("Nearest k 2: %+v, %v; want near then mid of three scanned", nearest, err)
	}
	if len(nearest.Scores) != 2 || math.Abs(nearest.Scores[0]-1) > 1e-6 || nearest.Scores[1] >= nearest.Scores[0] {
		t.Fatalf("scores %v; want two, best first, the exact match at 1", nearest.Scores)
	}
}

// --- aggregates ---

func stepSum(t *testing.T, scene *ringScene) {
	for key, order := range map[string]bson.D{"o1": ringOrder(5, "book"), "o2": ringOrder(7, "book"), "o3": ringOrder(1, "pen")} {
		ringApplied(t, "put "+key)(scene.users.Put(scene.ctx, "ada", "Orders", key, order))
	}
	sum, err := scene.users.Sum(scene.ctx, "ada", "Orders", "Amount")
	if err != nil || sum.IsFloat || sum.Int != 13 {
		t.Fatalf("Sum of Amount: %+v, %v; want the integer 13", sum, err)
	}
	if _, err := scene.users.Sum(scene.ctx, "ada", "Orders", "Weight"); err == nil {
		t.Fatal("Sum of an aggregate the field does not declare was answered")
	}
}

func stepSumBy(t *testing.T, scene *ringScene) {
	page, err := scene.users.SumBy(scene.ctx, "ada", "Orders", "Amount", "Kind", 0, nil)
	if err != nil || ringListLen(t, page.Document, "values") != 2 || page.More {
		t.Fatalf("SumBy Kind: %v, %v; want one value per kind, book and pen", page.Document, err)
	}
	first, err := scene.users.SumBy(scene.ctx, "ada", "Orders", "Amount", "Kind", 1, nil)
	if err != nil || ringListLen(t, first.Document, "values") != 1 || !first.More {
		t.Fatalf("SumBy Kind limit 1: %v more %v, %v; want one value and more", first.Document, first.More, err)
	}
	second, err := scene.users.SumBy(scene.ctx, "ada", "Orders", "Amount", "Kind", 1, first.Cursor)
	if err != nil || ringListLen(t, second.Document, "values") != 1 || second.More {
		t.Fatalf("SumBy Kind after the cursor: %v more %v, %v; want the last value", second.Document, second.More, err)
	}
}

func stepCountBy(t *testing.T, scene *ringScene) {
	page, err := scene.users.CountBy(scene.ctx, "ada", "Orders", "Kind", 0, nil)
	if err != nil || ringListLen(t, page.Document, "values") != 2 {
		t.Fatalf("CountBy Kind: %v, %v; want one value per kind", page.Document, err)
	}
}

func stepAggregate(t *testing.T, scene *ringScene) {
	page, err := scene.users.Aggregate(scene.ctx, "ada", "Orders", ring.AggregateQuery{Kind: "sum", Field: "Amount"})
	if err != nil || ringWhole(t, page.Document, "sum") != 13 {
		t.Fatalf("Aggregate sum of Amount: %v, %v; want {sum: 13}", page.Document, err)
	}
	if _, err := scene.users.Aggregate(scene.ctx, "ada", "Orders", ring.AggregateQuery{Kind: "average"}); err == nil {
		t.Fatal("Aggregate of a kind that does not exist was sent")
	}
}

func stepDescribeStore(t *testing.T, scene *ringScene) {
	described, err := scene.users.DescribeStore(scene.ctx, "ada", "Orders")
	if err != nil || ringListLen(t, described, "aggregates") != 3 {
		t.Fatalf("DescribeStore Orders: %v, %v; want its three aggregates", described, err)
	}
}

// --- expansion, many keys, the bucket-wide reads ---

func stepGetExpanded(t *testing.T, scene *ringScene) {
	document, expanded, err := scene.posts.GetExpanded(scene.ctx, "p1", nil, ringAuthor, true)
	if err != nil || ringText(document, "Text") != "hello" {
		t.Fatalf("GetExpanded p1: %v, %v", document, err)
	}
	if name := ringText(expanded, "Author", "doc", "Name"); name != "Countess" {
		t.Fatalf("the expanded author is %q in %v; want Countess, ada's name", name, expanded)
	}
}

func stepStoreListExpanded(t *testing.T, scene *ringScene) {
	members, err := scene.rooms.StoreListExpanded(scene.ctx, "r1", "Members", 0, nil, &ring.ExpandRequest{Fields: []string{"Name"}}, false)
	if err != nil || fmt.Sprint(members.Keys) != "[ada ghost]" || len(members.Expanded) != 2 {
		t.Fatalf("StoreListExpanded of the members: %+v, %v", members, err)
	}
	if name := ringText(members.Expanded[0], "Members", "doc", "Name"); name != "Countess" {
		t.Fatalf("member ada expanded to %v; want her name", members.Expanded[0])
	}
	if found, err := members.Expanded[1].LookupErr("Members", "expanded"); err != nil || found.Boolean() {
		t.Fatalf("member ghost expanded to %v; want expanded false: there is no such user", members.Expanded[1])
	}
}

func stepBucketGetMany(t *testing.T, scene *ringScene) {
	answers, err := scene.posts.GetMany(scene.ctx, []string{"p1", "gone"}, nil)
	if err != nil || len(answers) != 2 {
		t.Fatalf("GetMany of two ids: %+v, %v", answers, err)
	}
	if answers[0].Err != nil || !answers[0].Found || ringText(answers[0].Doc, "Text") != "hello" {
		t.Fatalf("the answer for p1: %+v", answers[0])
	}
	if answers[1].Err != nil || answers[1].Found {
		t.Fatalf("the answer for a post nobody wrote: %+v; want found false and no error", answers[1])
	}
	named, err := scene.users.GetMany(scene.ctx, []string{"bob", "ada"}, []string{"Name"})
	if err != nil || len(named) != 2 || ringText(named[0].Doc, "Name") != "Bob" || ringText(named[1].Doc, "Name") != "Countess" {
		t.Fatalf("GetMany with fields, in the order asked: %+v, %v", named, err)
	}
}

func stepClientGetMany(t *testing.T, scene *ringScene) {
	answers, err := scene.client.GetMany(scene.ctx, []ring.Key{
		{Bucket: scene.users, ID: "ada", Fields: []string{"Name"}},
		{Bucket: scene.posts, ID: "p1"},
		{Bucket: scene.users, ID: "nobody"},
		{Bucket: scene.client.Bucket("app", "Nobody"), ID: "ada"},
	})
	if err != nil || len(answers) != 4 {
		t.Fatalf("GetMany across buckets: %+v, %v; want four answers", answers, err)
	}
	if !answers[0].Found || ringText(answers[0].Doc, "Name") != "Countess" {
		t.Fatalf("the user key: %+v", answers[0])
	}
	if _, err := answers[0].Doc.LookupErr("Age"); err == nil {
		t.Fatalf("a key that asked for Name got Age too: %v", answers[0].Doc)
	}
	if !answers[1].Found || ringText(answers[1].Doc, "Text") != "hello" {
		t.Fatalf("the post key: %+v", answers[1])
	}
	if answers[2].Found || answers[2].Err != nil {
		t.Fatalf("a missing document: %+v; want found false and no error", answers[2])
	}
	if answers[3].Err == nil {
		t.Fatalf("a key of an undeclared bucket: %+v; want that key's own error", answers[3])
	}
}

func stepList(t *testing.T, scene *ringScene) {
	var ids []string
	request := ring.PageRequest{Limit: 1}
	for pages := 0; ; pages++ {
		page, err := scene.users.List(scene.ctx, nil, request)
		if err != nil || len(page.Entries) > 1 {
			t.Fatalf("List page %d by one: %+v, %v", pages, page, err)
		}
		for _, entry := range page.Entries {
			ids = append(ids, entry.ID)
		}
		if !page.More {
			break
		}
		if pages > 10 {
			t.Fatal("List with limit 1 never answered a last page of two documents")
		}
		request.Cursor = page.Cursor
	}
	sort.Strings(ids)
	if fmt.Sprint(ids) != "[ada bob]" {
		t.Fatalf("List paged by one saw %v; want ada and bob once each", ids)
	}
	filtered, err := scene.users.List(scene.ctx, bson.D{{Key: "Name", Value: "Bob"}}, ring.PageRequest{Fields: []string{"Name"}})
	if err != nil || len(filtered.Entries) != 1 || filtered.Entries[0].ID != "bob" || ringText(filtered.Entries[0].Body, "Name") != "Bob" {
		t.Fatalf("List where Name is Bob: %+v, %v", filtered, err)
	}
	expanded, err := scene.posts.List(scene.ctx, nil, ring.PageRequest{Expand: ringAuthor})
	if err != nil || len(expanded.Entries) != 1 || len(expanded.Expanded) != 1 || ringText(expanded.Expanded[0], "Author", "doc", "Name") != "Countess" {
		t.Fatalf("List of posts with their authors: %+v, %v", expanded, err)
	}
}

func stepCount(t *testing.T, scene *ringScene) {
	if count, err := scene.users.Count(scene.ctx, nil); err != nil || count != 2 {
		t.Fatalf("Count: %d, %v; want 2", count, err)
	}
	if count, err := scene.users.Count(scene.ctx, bson.D{{Key: "Name", Value: "Bob"}}); err != nil || count != 1 {
		t.Fatalf("Count where Name is Bob: %d, %v; want 1", count, err)
	}
}

// --- batch ---

func stepNewBatch(t *testing.T, scene *ringScene) {
	scene.batch = scene.client.NewBatch()
	if scene.batch.Len() != 0 {
		t.Fatalf("a new batch holds %d operations", scene.batch.Len())
	}
}

// stepIn queues the mixed batch the vectors leave to this test (design section
// 6): an upsert, a put and a push in one frame, the last two on the document
// the first one writes.
func stepIn(t *testing.T, scene *ringScene) {
	scene.queued = scene.users.In(scene.batch)
	if written, err := scene.queued.Upsert(scene.ctx, "carol", ring.Doc{"Name": "Carol", "Age": int64(50)}); err != nil || written.Outcome != "" {
		t.Fatalf("queueing an upsert: %+v, %v; want a zero result, the answer comes with Send", written, err)
	}
	if _, err := scene.queued.Put(scene.ctx, "carol", "Friends", "dave", ringLabelled("pal")); err != nil {
		t.Fatalf("queueing a put: %v", err)
	}
	if _, err := scene.queued.Push(scene.ctx, "carol", "Purchases", ringLabelled("bread")); err != nil {
		t.Fatalf("queueing a push: %v", err)
	}
	if _, err := scene.queued.Get(scene.ctx, "ada"); err == nil {
		t.Fatal("a read through a batch handle was accepted; a batch carries writes only")
	}
	if _, err := scene.users.Get(scene.ctx, "carol"); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("carol before Send: %v; want nothing sent yet", err)
	}
}

func stepBatchLen(t *testing.T, scene *ringScene) {
	if scene.batch.Len() != 3 {
		t.Fatalf("the batch holds %d operations; want the three queued", scene.batch.Len())
	}
}

func stepBatchSend(t *testing.T, scene *ringScene) {
	sent, err := scene.batch.Send(scene.ctx)
	if err != nil || sent.Outcome != "ok" || len(sent.Answers) != 3 {
		t.Fatalf("Send: %+v, %v; want ok and three answers", sent, err)
	}
	for i, answer := range sent.Answers {
		if answer.Err != nil || !answer.Write.Applied() {
			t.Fatalf("answer %d: %+v; want applied", i, answer)
		}
	}
	got, err := scene.users.Get(scene.ctx, "carol")
	if err != nil || ringText(got, "Name") != "Carol" {
		t.Fatalf("carol after Send: %v, %v", got, err)
	}
	for store, want := range map[string]int64{"Friends": 1, "Purchases": 1} {
		if count, err := scene.users.StoreCount(scene.ctx, "carol", store); err != nil || count != want {
			t.Fatalf("carol's %s after Send: %d, %v; want %d", store, count, err, want)
		}
	}
	// Send again is the retry after a lost answer: the same request id, so nothing is applied twice.
	retried, err := scene.batch.Send(scene.ctx)
	if err != nil || len(retried.Answers) != 3 {
		t.Fatalf("the retry: %+v, %v", retried, err)
	}
	for i, answer := range retried.Answers {
		if answer.Err != nil || !answer.Write.Deduped {
			t.Fatalf("retried answer %d: %+v; want it deduped", i, answer)
		}
	}
	if count, err := scene.users.StoreCount(scene.ctx, "carol", "Purchases"); err != nil || count != 1 {
		t.Fatalf("carol's Purchases after the retry: %d, %v; want 1, the push not applied twice", count, err)
	}
	ringApplied(t, "delete carol")(scene.users.Delete(scene.ctx, "carol"))
}

// --- the cluster: topology, seeding, membership on one node ---

func stepTopology(t *testing.T, scene *ringScene) {
	topology, err := scene.client.Topology(scene.ctx, nil)
	if err != nil || len(topology.Members) != 1 || topology.Members[0].State != "active" || len(topology.Witnesses) != 0 {
		t.Fatalf("Topology: %+v, %v; want one active member and no witness", topology, err)
	}
	listed := map[string]bool{}
	for _, database := range topology.Databases {
		listed[database.Name] = true
	}
	if !listed["app"] || !listed["cache"] {
		t.Fatalf("Topology lists the databases %+v; want app and cache", topology.Databases)
	}
	same, err := scene.client.Topology(scene.ctx, topology)
	if err != nil || same != topology {
		t.Fatalf("Topology with the view held: %p, %v; want the same view back (%p), nothing changed", same, err, topology)
	}
	scene.topology = topology
}

func stepOwner(t *testing.T, scene *ringScene) {
	only := scene.topology.Members[0]
	for _, slot := range []uint16{0, 4096, 8191} {
		if owner, ok := scene.topology.Owner("app", slot); !ok || owner.ID != only.ID {
			t.Fatalf("Owner of app slot %d: %+v, %v; want the only member, %s", slot, owner, ok, only.ID)
		}
	}
	if owner, ok := scene.topology.Owner("nowhere", 0); ok {
		t.Fatalf("Owner of a database the view does not list: %+v", owner)
	}
	if owner, ok := scene.topology.Owner("app", 8192); ok {
		t.Fatalf("Owner of slot 8192, one past the last: %+v", owner)
	}
}

func stepSeedingStatus(t *testing.T, scene *ringScene) {
	page, err := scene.client.SeedingStatus(scene.ctx, ring.SeedingQuery{})
	if err != nil {
		t.Fatalf("SeedingStatus on one node: %v", err)
	}
	// A node with no seeding engine at work has nothing to report: an empty page is no rows,
	// no next page and no cursor, not a nil answer or a row of zeroes.
	if len(page.Rows) != 0 || page.More || len(page.Cursor) != 0 {
		t.Fatalf("SeedingStatus on one node = %d rows, more=%v, cursor=%x; want an empty page: no rows, no more, no cursor", len(page.Rows), page.More, page.Cursor)
	}
}

func stepSeedingRetry(t *testing.T, scene *ringScene) {
	refusal := ringRefusal(t, "SeedingRetry of a slot with no seed", scene.client.SeedingRetry(scene.ctx, "app", 5))
	if !strings.Contains(refusal.Message, "status seeding") {
		t.Fatalf("SeedingRetry of a slot with no seed: %v; want the refusal that names 'status seeding'", refusal)
	}
}

func stepSeedingForget(t *testing.T, scene *ringScene) {
	refusal := ringRefusal(t, "SeedingForget of a slot with no seed", scene.client.SeedingForget(scene.ctx, "app", 5, false))
	if !strings.Contains(refusal.Message, "status seeding") {
		t.Fatalf("SeedingForget of a slot with no seed: %v; want the refusal that names 'status seeding'", refusal)
	}
}

// The five membership statements need a node in cluster mode: the zero-configuration node of
// the other steps has no cluster to change and refuses all five alike. They run on the
// second server, a one-node cluster. Join and Promote name that node itself, which is a
// member and a voter already, so both answer the committed facts and change nothing. Leave,
// RemoveNode and ReplaceNode are the next membership slice: each is refused on this build, so
// the steps assert the one code and the text that names what is available instead.
const ringGhostNode = "ghost-node"

func stepJoin(t *testing.T, scene *ringScene) {
	_, err := scene.client.Join(scene.ctx, scene.topology.Members[0].ID, scene.topology.Members[0].DataAddr, false)
	if refusal := ringRefusal(t, "Join on a node that is not in cluster mode", err); !strings.Contains(refusal.Message, "cluster.enabled") {
		t.Fatalf("Join on a node that is not in cluster mode: %v; want the refusal that names cluster.enabled", refusal)
	}
	member, err := ring.Dial(scene.ctx, ring.Options{
		Addr: scene.clusterServer.Address, User: scene.clusterServer.User, Password: scene.clusterServer.Password,
		Fingerprint: scene.clusterServer.Fingerprint,
	})
	if err != nil {
		t.Fatalf("Dial of the one-node cluster: %v", err)
	}
	scene.member = member
	topology, err := member.Topology(scene.ctx, nil)
	if err != nil || len(topology.Members) != 1 || topology.Members[0].ID != "ring-one" {
		t.Fatalf("the topology of the one-node cluster: %+v, %v", topology, err)
	}
	scene.only = topology.Members[0]
	joined, err := member.Join(scene.ctx, scene.only.ID, scene.only.DataAddr, false)
	if err != nil || joined.Result == "" || joined.Epoch < 1 {
		t.Fatalf("Join of the member itself at its own address: %+v, %v; want the result of its registration again", joined, err)
	}
}

func stepPromote(t *testing.T, scene *ringScene) {
	promoted, err := scene.member.Promote(scene.ctx, scene.only.ID)
	if err != nil || !strings.Contains(promoted.Result, "already votes") {
		t.Fatalf("Promote of a member that already votes: %+v, %v; want the facts and no change", promoted, err)
	}
}

func stepLeave(t *testing.T, scene *ringScene) {
	_, err := scene.member.Leave(scene.ctx, ringGhostNode)
	refusal := ringRefusal(t, "Leave of a node that is no member", err)
	if refusal.Code != ringwire.CodeInvalidArgument ||
		!strings.Contains(refusal.Message, "leave is not available in this build") || !strings.Contains(refusal.Message, "Run 'nodes'") {
		t.Fatalf("Leave on a one-node cluster = %v; want INVALID_ARGUMENT with the text that says leave is not built and names 'nodes'", refusal)
	}
}

func stepRemoveNode(t *testing.T, scene *ringScene) {
	_, err := scene.member.RemoveNode(scene.ctx, ringGhostNode)
	refusal := ringRefusal(t, "RemoveNode of a node that is no member", err)
	if refusal.Code != ringwire.CodeInvalidArgument ||
		!strings.Contains(refusal.Message, "remove-node is not available in this build") || !strings.Contains(refusal.Message, "Run 'nodes'") {
		t.Fatalf("RemoveNode on a one-node cluster = %v; want INVALID_ARGUMENT with the text that says remove-node is not built and names 'nodes'", refusal)
	}
}

func stepReplaceNode(t *testing.T, scene *ringScene) {
	_, err := scene.member.ReplaceNode(scene.ctx, ringGhostNode, "127.0.0.1:1")
	refusal := ringRefusal(t, "ReplaceNode of a node that is no member", err)
	if refusal.Code != ringwire.CodeInvalidArgument ||
		!strings.Contains(refusal.Message, "replace-node is not available in this build") || !strings.Contains(refusal.Message, "Run 'nodes'") {
		t.Fatalf("ReplaceNode on a one-node cluster = %v; want INVALID_ARGUMENT with the text that says replace-node is not built and names 'nodes'", refusal)
	}
	node, err := scene.member.DescribeNode(scene.ctx)
	if err != nil || node.Members != 1 || node.Voters != 1 {
		t.Fatalf("after the five membership statements: %+v, %v; want the same one member, a voter", node, err)
	}
	if err := scene.member.Close(); err != nil {
		t.Fatal(err)
	}
	scene.clusterServer.Stop()
}

// --- alter and the header cache, expiry ---

// stepAlterBucket adds a field on one connection and writes it on the other,
// whose cached header is from before the alter: the client learns the new
// declaration by itself and sends once more.
func stepAlterBucket(t *testing.T, scene *ringScene) {
	altered, err := scene.client.AlterBucket(scene.ctx, "app", "User", ring.AlterChange{Add: []ring.FieldDecl{{Name: "Email", Type: "string"}}})
	if err != nil || altered.DeclVersion != scene.userInfo.DeclVersion+1 || len(altered.Fields) != 11 {
		t.Fatalf("AlterBucket add Email: %+v, %v; want the next declaration version and eleven fields", altered, err)
	}
	again, err := scene.client.AlterBucket(scene.ctx, "app", "User", ring.AlterChange{Add: []ring.FieldDecl{{Name: "Email", Type: "string"}}})
	if err != nil || again.DeclVersion != altered.DeclVersion {
		t.Fatalf("the same alter twice: %+v, %v; want the same declaration", again, err)
	}
	staleUsers := scene.stale.Bucket("app", "User")
	ringApplied(t, "a write of the new field with a header from before the alter")(
		staleUsers.Upsert(scene.ctx, "bob", ring.Doc{"Name": "Bob", "Age": int64(21), "Email": "bob@example.org"}))
	got, err := scene.users.Get(scene.ctx, "bob")
	if err != nil || ringText(got, "Email") != "bob@example.org" {
		t.Fatalf("bob after the write: %v, %v", got, err)
	}
	if refreshed, err := staleUsers.Info(scene.ctx); err != nil || refreshed.DeclVersion != altered.DeclVersion {
		t.Fatalf("the header the other connection holds now: %+v, %v; want version %d", refreshed, err, altered.DeclVersion)
	}
}

func stepExpiresIn(t *testing.T, scene *ringScene) {
	before := time.Now()
	at := ring.ExpiresIn(time.Hour)
	if at.Before(before.Add(time.Hour)) || at.After(time.Now().Add(time.Hour)) {
		t.Fatalf("ExpiresIn(1h) = %v; want this clock plus an hour", at)
	}
	ringApplied(t, "insert of a session that expires in an hour")(scene.sessions.Insert(scene.ctx, "live", ring.Doc{"Token": "t1", "ExpiresAt": at}))
	if got, err := scene.sessions.Get(scene.ctx, "live"); err != nil || ringText(got, "Token") != "t1" {
		t.Fatalf("a session that expires in an hour: %v, %v", got, err)
	}
}

// stepIsExpired reads a session past its expire time. The server of this tree answers that
// read as a plain miss (found false, which the client turns into ErrNotFound), so no call
// made here carries the expired detail IsExpired reads; the step holds what is true on both
// sides of that: the document is gone for a reader the moment it expires, and IsExpired says
// no for a miss that is not an expiry and for a refusal that is NOT_FOUND for another reason.
func stepIsExpired(t *testing.T, scene *ringScene) {
	if ring.IsExpired(nil) {
		t.Fatal("IsExpired(nil) is true")
	}
	if _, err := scene.sessions.Get(scene.ctx, "nobody"); !errors.Is(err, ring.ErrNotFound) || ring.IsExpired(err) {
		t.Fatalf("Get of a session nobody wrote: %v; want a NOT_FOUND that is not an expiry", err)
	}
	if _, err := scene.client.DescribeBucket(scene.ctx, "app", "Nobody"); !errors.Is(err, ring.ErrNotFound) || ring.IsExpired(err) {
		t.Fatalf("DescribeBucket of an undeclared bucket: %v; want a NOT_FOUND refusal that is not an expiry", err)
	}
	ringApplied(t, "insert of a session that expires in a second")(
		scene.sessions.Insert(scene.ctx, "brief", ring.Doc{"Token": "t2", "ExpiresAt": ring.ExpiresIn(time.Second)}))
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := scene.sessions.Get(scene.ctx, "brief")
		if errors.Is(err, ring.ErrNotFound) {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("a session that expired a second after its insert: %v; want ErrNotFound within 20 seconds", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if count, err := scene.sessions.Count(scene.ctx, nil); err != nil || count != 1 {
		t.Fatalf("Count of the sessions: %d, %v; want 1, the one that has not expired", count, err)
	}
}

// --- layer 2: the entity and store handles, on one more user ---

func stepEntity(t *testing.T, scene *ringScene) {
	scene.eve = scene.users.Entity("eve")
	if _, err := scene.eve.Get(scene.ctx); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("Get through a handle on a user nobody wrote: %v; want ErrNotFound", err)
	}
}

func stepField(t *testing.T, scene *ringScene) {
	scene.evePurchase = scene.users.Field("eve", "Purchases")
	if _, err := scene.users.Field("eve", "Nothing").Count(scene.ctx); err == nil {
		t.Fatal("a call through a handle on an undeclared field was accepted")
	}
}

func stepEntityInsert(t *testing.T, scene *ringScene) {
	ringApplied(t, "Entity.Insert")(scene.eve.Insert(scene.ctx, ring.Doc{"Name": "Eve", "Age": int64(30)}))
	if _, err := scene.eve.Insert(scene.ctx, ring.Doc{"Name": "Again"}); err == nil {
		t.Fatal("a second insert through the handle was accepted")
	}
}

func stepEntityStore(t *testing.T, scene *ringScene) {
	count, err := scene.eve.Store("Purchases").Count(scene.ctx)
	if !errors.Is(err, ring.ErrNotFound) && (err != nil || count != 0) {
		t.Fatalf("a store of a user nobody wrote: %d, %v; want 0 or NOT_FOUND", count, err)
	}
}

func stepEntityGet(t *testing.T, scene *ringScene) {
	got, err := scene.eve.Get(scene.ctx)
	flat, flatErr := scene.users.Get(scene.ctx, "eve")
	if err != nil || flatErr != nil || ringText(got, "Name") != "Eve" || string(got) != string(flat) {
		t.Fatalf("Entity.Get: %v, %v; the flat Get: %v, %v; want the same document", got, err, flat, flatErr)
	}
}

func stepEntityGetFields(t *testing.T, scene *ringScene) {
	got, err := scene.eve.GetFields(scene.ctx, []string{"Age"})
	if err != nil || ringWhole(t, got, "Age") != 30 {
		t.Fatalf("Entity.GetFields Age: %v, %v", got, err)
	}
	if _, err := got.LookupErr("Name"); err == nil {
		t.Fatalf("Entity.GetFields Age returned Name too: %v", got)
	}
}

func stepEntityGetExpanded(t *testing.T, scene *ringScene) {
	document, expanded, err := scene.posts.Entity("p1").GetExpanded(scene.ctx, []string{"Author", "Text"}, ringAuthor, true)
	if err != nil || ringText(document, "Text") != "hello" || ringText(expanded, "Author", "doc", "Name") != "Countess" {
		t.Fatalf("Entity.GetExpanded: %v, %v, %v", document, expanded, err)
	}
}

func stepEntityUpsert(t *testing.T, scene *ringScene) {
	ringApplied(t, "Entity.Upsert")(scene.eve.Upsert(scene.ctx, ring.Doc{"Name": "Eve M", "Age": int64(30)}))
	if got, err := scene.users.Get(scene.ctx, "eve"); err != nil || ringText(got, "Name") != "Eve M" {
		t.Fatalf("after Entity.Upsert: %v, %v", got, err)
	}
}

func stepEntityUpdate(t *testing.T, scene *ringScene) {
	updated, err := scene.eve.Update(scene.ctx, ring.Update{Inc: bson.D{{Key: "Age", Value: int64(1)}}}, ring.Return(ring.ReturnAfter))
	if err != nil || !updated.Applied() || ringWhole(t, updated.Document, "Age") != 31 {
		t.Fatalf("Entity.Update: %+v, %v; want Age 31 back", updated, err)
	}
}

func stepStorePush(t *testing.T, scene *ringScene) {
	for _, label := range []string{"a", "b", "c"} {
		ringApplied(t, "Store.Push "+label)(scene.evePurchase.Push(scene.ctx, ringLabelled(label)))
	}
}

func stepStoreHandleCount(t *testing.T, scene *ringScene) {
	count, err := scene.evePurchase.Count(scene.ctx)
	flat, flatErr := scene.users.StoreCount(scene.ctx, "eve", "Purchases")
	if err != nil || flatErr != nil || count != 3 || flat != 3 {
		t.Fatalf("Store.Count: %d, %v; StoreCount: %d, %v; want 3 from both", count, err, flat, flatErr)
	}
}

func stepStorePeek(t *testing.T, scene *ringScene) {
	if peeked, err := scene.evePurchase.Peek(scene.ctx, 2); err != nil || ringLabels(peeked) != "a,b" {
		t.Fatalf("Store.Peek 2: %v, %v", peeked, err)
	}
}

func stepStorePeekKeyed(t *testing.T, scene *ringScene) {
	if page, err := scene.evePurchase.PeekKeyed(scene.ctx, 1); err != nil || ringLabels(page.Elements) != "a" || len(page.Keys) != 0 {
		t.Fatalf("Store.PeekKeyed 1: %+v, %v", page, err)
	}
}

func stepStoreHandleList(t *testing.T, scene *ringScene) {
	first, err := scene.evePurchase.List(scene.ctx, 2, nil)
	if err != nil || ringLabels(first.Elements) != "a,b" || !first.More {
		t.Fatalf("Store.List 2: %+v, %v", first, err)
	}
	if rest, err := scene.evePurchase.List(scene.ctx, 2, first.Cursor); err != nil || ringLabels(rest.Elements) != "c" || rest.More {
		t.Fatalf("Store.List after the cursor: %+v, %v", rest, err)
	}
}

func stepStoreHandleListExpanded(t *testing.T, scene *ringScene) {
	members, err := scene.rooms.Entity("r1").Store("Members").ListExpanded(scene.ctx, 1, nil, &ring.ExpandRequest{Fields: []string{"Name"}}, false)
	if err != nil || fmt.Sprint(members.Keys) != "[ada]" || len(members.Expanded) != 1 || !members.More ||
		ringText(members.Expanded[0], "Members", "doc", "Name") != "Countess" {
		t.Fatalf("Store.ListExpanded 1: %+v, %v", members, err)
	}
}

func stepStoreClaim(t *testing.T, scene *ringScene) {
	lease, err := scene.evePurchase.Claim(scene.ctx, 2)
	if err != nil || lease.ID == ([16]byte{}) || ringLabels(lease.Elements) != "a,b" {
		t.Fatalf("Store.Claim 2: %+v, %v", lease, err)
	}
	scene.eveLease = lease
}

func stepStoreLeaseGet(t *testing.T, scene *ringScene) {
	if read, err := scene.evePurchase.LeaseGet(scene.ctx, scene.eveLease.ID); err != nil || !read.Found || ringLabels(read.Elements) != "a,b" {
		t.Fatalf("Store.LeaseGet: %+v, %v", read, err)
	}
}

func stepStoreExtend(t *testing.T, scene *ringScene) {
	if change, err := scene.evePurchase.Extend(scene.ctx, scene.eveLease.ID, 30000); err != nil || !change.Live {
		t.Fatalf("Store.Extend: %+v, %v", change, err)
	}
}

func stepStoreRelease(t *testing.T, scene *ringScene) {
	if change, err := scene.evePurchase.Release(scene.ctx, scene.eveLease.ID); err != nil || !change.Live {
		t.Fatalf("Store.Release: %+v, %v", change, err)
	}
}

func stepStoreAck(t *testing.T, scene *ringScene) {
	lease, err := scene.evePurchase.Claim(scene.ctx, 1)
	if err != nil || ringLabels(lease.Elements) != "a" {
		t.Fatalf("a claim after the release: %+v, %v", lease, err)
	}
	if _, err := scene.evePurchase.Ack(scene.ctx, lease.ID); err != nil {
		t.Fatalf("Store.Ack: %v", err)
	}
	if count, err := scene.evePurchase.Count(scene.ctx); err != nil || count != 2 {
		t.Fatalf("after Store.Ack the store holds %d, %v; want 2", count, err)
	}
}

func stepStorePop(t *testing.T, scene *ringScene) {
	if popped, err := scene.evePurchase.Pop(scene.ctx); err != nil || ringText(popped, "item") != "b" {
		t.Fatalf("Store.Pop: %v, %v; want b", popped, err)
	}
}

func stepStorePopN(t *testing.T, scene *ringScene) {
	if popped, err := scene.evePurchase.PopN(scene.ctx, 5); err != nil || ringLabels(popped) != "c" {
		t.Fatalf("Store.PopN 5 of one left: %v, %v", popped, err)
	}
	if _, err := scene.evePurchase.Pop(scene.ctx); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("Store.Pop of an empty store: %v; want ErrNotFound", err)
	}
}

func stepStorePut(t *testing.T, scene *ringScene) {
	friends := scene.eve.Store("Friends")
	ringApplied(t, "Store.Put")(friends.Put(scene.ctx, "zed", ringLabelled("pal")))
	if flat, err := scene.users.StoreGet(scene.ctx, "eve", "Friends", "zed"); err != nil || ringText(flat, "item") != "pal" {
		t.Fatalf("the flat StoreGet of what Store.Put wrote: %v, %v", flat, err)
	}
}

func stepStoreHandleGet(t *testing.T, scene *ringScene) {
	friends := scene.eve.Store("Friends")
	if got, err := friends.Get(scene.ctx, "zed"); err != nil || ringText(got, "item") != "pal" {
		t.Fatalf("Store.Get: %v, %v", got, err)
	}
	if _, err := friends.Get(scene.ctx, "nobody"); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("Store.Get of an absent key: %v; want ErrNotFound", err)
	}
}

func stepStoreTake(t *testing.T, scene *ringScene) {
	friends := scene.eve.Store("Friends")
	if taken, err := friends.Take(scene.ctx, "zed"); err != nil || ringText(taken, "item") != "pal" {
		t.Fatalf("Store.Take: %v, %v", taken, err)
	}
	if count, err := friends.Count(scene.ctx); err != nil || count != 0 {
		t.Fatalf("after Store.Take the store holds %d, %v; want 0", count, err)
	}
}

func stepStoreInc(t *testing.T, scene *ringScene) {
	if _, err := scene.eve.Store("Counters").Inc(scene.ctx, "hits", map[string]any{"N": int64(1)}); err != nil {
		t.Fatalf("Store.Inc: %v", err)
	}
}

func stepStoreIncAfter(t *testing.T, scene *ringScene) {
	element, written, err := scene.eve.Store("Counters").IncAfter(scene.ctx, "hits", map[string]any{"N": int64(1)})
	if err != nil || !written.Applied() || ringWhole(t, element, "N") != 2 {
		t.Fatalf("Store.IncAfter: %v, %+v, %v; want N 2", element, written, err)
	}
}

func stepStoreNearest(t *testing.T, scene *ringScene) {
	places := scene.eve.Store("Places")
	ringApplied(t, "put a place")(places.Put(scene.ctx, "here", bson.D{{Key: "Vector", Value: ringVector(0, 0, 1)}}))
	nearest, err := places.Nearest(scene.ctx, ring.NearestQuery{Vector: ringVector(0, 0, 1).Data})
	if err != nil || fmt.Sprint(nearest.Keys) != "[here]" || nearest.Scanned != 1 {
		t.Fatalf("Store.Nearest: %+v, %v", nearest, err)
	}
}

func stepStoreSum(t *testing.T, scene *ringScene) {
	orders := scene.eve.Store("Orders")
	ringApplied(t, "put an order")(orders.Put(scene.ctx, "o1", ringOrder(2, "ink")))
	ringApplied(t, "put an order")(orders.Put(scene.ctx, "o2", ringOrder(3, "ink")))
	if sum, err := orders.Sum(scene.ctx, "Amount"); err != nil || sum.IsFloat || sum.Int != 5 {
		t.Fatalf("Store.Sum: %+v, %v; want 5", sum, err)
	}
}

func stepStoreSumBy(t *testing.T, scene *ringScene) {
	page, err := scene.eve.Store("Orders").SumBy(scene.ctx, "Amount", "Kind", 0, nil)
	if err != nil || ringListLen(t, page.Document, "values") != 1 {
		t.Fatalf("Store.SumBy: %v, %v; want the one kind", page.Document, err)
	}
}

func stepStoreCountBy(t *testing.T, scene *ringScene) {
	page, err := scene.eve.Store("Orders").CountBy(scene.ctx, "Kind", 0, nil)
	if err != nil || ringListLen(t, page.Document, "values") != 1 {
		t.Fatalf("Store.CountBy: %v, %v; want the one kind", page.Document, err)
	}
}

func stepStoreAggregate(t *testing.T, scene *ringScene) {
	page, err := scene.eve.Store("Orders").Aggregate(scene.ctx, ring.AggregateQuery{Kind: "sum", Field: "Amount"})
	if err != nil || ringWhole(t, page.Document, "sum") != 5 {
		t.Fatalf("Store.Aggregate: %v, %v; want {sum: 5}", page.Document, err)
	}
}

func stepStoreDescribe(t *testing.T, scene *ringScene) {
	described, err := scene.eve.Store("Orders").Describe(scene.ctx)
	flat, flatErr := scene.users.DescribeStore(scene.ctx, "eve", "Orders")
	if err != nil || flatErr != nil || ringListLen(t, described, "aggregates") != 3 || string(described) != string(flat) {
		t.Fatalf("Store.Describe: %v, %v; DescribeStore: %v, %v; want the same three aggregates", described, err, flat, flatErr)
	}
}

func stepEntityDelete(t *testing.T, scene *ringScene) {
	ringApplied(t, "Entity.Delete")(scene.eve.Delete(scene.ctx))
	if _, err := scene.users.Get(scene.ctx, "eve"); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("eve after Entity.Delete: %v; want ErrNotFound", err)
	}
}

// --- deletes, drops, close ---

func stepDelete(t *testing.T, scene *ringScene) {
	kept, err := scene.users.Delete(scene.ctx, "bob", ring.If(ring.Guard{{Key: "Name", Value: "Somebody"}}))
	if err != nil || !kept.NotApplied {
		t.Fatalf("a delete whose guard does not hold: %+v, %v; want not_applied", kept, err)
	}
	ringApplied(t, "delete bob")(scene.users.Delete(scene.ctx, "bob"))
	if _, err := scene.users.Get(scene.ctx, "bob"); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("bob after the delete: %v; want ErrNotFound", err)
	}
	if count, err := scene.users.Count(scene.ctx, nil); err != nil || count != 1 {
		t.Fatalf("Count after the deletes: %d, %v; want 1, ada", count, err)
	}
}

func stepDropBucketExpecting(t *testing.T, scene *ringScene) {
	err := scene.client.DropBucketExpecting(scene.ctx, "app", "Room", scene.roomInfo.ID+1000)
	if refusal := ringRefusal(t, "a drop expecting another bucket id", err); !errors.Is(refusal, ring.ErrNotFound) {
		t.Fatalf("a drop expecting another bucket id: %v; want NOT_FOUND", refusal)
	}
	if described, err := scene.client.DescribeBucket(scene.ctx, "app", "Room"); err != nil || described.State != "active" {
		t.Fatalf("the bucket after a refused drop: %+v, %v; want it active", described, err)
	}
	if err := scene.client.DropBucketExpecting(scene.ctx, "app", "Room", scene.roomInfo.ID); err != nil {
		t.Fatalf("a drop expecting the right id: %v", err)
	}
}

// stepDropBucket drops once and then watches with describe: the documents go in the
// background, on the server's sweep, and the bucket is gone when describe is NOT_FOUND. It
// does not repeat the drop while it waits, because a drop of a dropping bucket queues the
// sweep again.
func stepDropBucket(t *testing.T, scene *ringScene) {
	if err := scene.client.DropBucket(scene.ctx, "app", "Session"); err != nil {
		t.Fatalf("DropBucket: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		described, err := scene.client.DescribeBucket(scene.ctx, "app", "Session")
		if errors.Is(err, ring.ErrNotFound) {
			break
		}
		if err != nil || described.State != "dropping" || time.Now().After(deadline) {
			t.Fatalf("the bucket after its drop: %+v, %v; want it dropping and then NOT_FOUND within 90 seconds", described, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := scene.client.DropBucket(scene.ctx, "app", "Session"); !errors.Is(err, ring.ErrNotFound) {
		t.Fatalf("a drop of a bucket that is gone: %v; want NOT_FOUND", err)
	}
	page, err := scene.client.ShowBuckets(scene.ctx, "app", ring.PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, bucket := range page.Buckets {
		if bucket.Name == "Session" || bucket.Name == "Room" {
			t.Fatalf("a dropped bucket is still listed: %+v", bucket)
		}
	}
	if _, err := scene.sessions.Get(scene.ctx, "live"); err == nil {
		t.Fatal("a document of the dropped bucket was read")
	}
}

func stepClose(t *testing.T, scene *ringScene) {
	if err := scene.stale.Close(); err != nil {
		t.Fatalf("Close of the second connection: %v", err)
	}
	if err := scene.client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := scene.client.DescribeNode(scene.ctx); err == nil {
		t.Fatal("a call on a closed client was answered")
	}
}
