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
	"errors"
	"reflect"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Info against a real server: a bucket with store fields of all six engines
// reports each in declaration order, in exactly one BUCKET_INFO request, and
// the request costs the same when a store field holds many elements: it is
// never a BUCKET_STATS, which walks them.
func TestInfoReportsStoreFieldsInOneRequest(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	users, err := client.Database("app").CreateBucket(ctx, "User", hoarddb.Hash, hoarddb.WithStores(
		hoarddb.StoreSpec{Name: "Friends", Type: hoarddb.Hash},
		hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO},
		hoarddb.StoreSpec{Name: "Photos", Type: hoarddb.LIFO},
		hoarddb.StoreSpec{Name: "Recipes", Type: hoarddb.BTree},
		hoarddb.StoreSpec{Name: "Books", Type: hoarddb.Heap},
		hoarddb.StoreSpec{Name: "Data", Type: hoarddb.Blob},
	))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := users.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	purchases := users.Field("ada", "Purchases")
	for item := range 300 {
		if _, err := purchases.Push(ctx, hoarddb.Map{"Item": int64(item)}); err != nil {
			t.Fatal(err)
		}
	}

	before := len(proxy.Exchanges())
	got, err := users.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	exchanges := proxy.Exchanges()[before:]
	if len(exchanges) != 1 || exchanges[0].Request.Op != hoarddbwire.OpBucketInfo {
		t.Fatalf("Info sent %d requests (first %v), want exactly one BUCKET_INFO", len(exchanges), exchanges)
	}

	wantFields := []hoarddb.StoreFieldInfo{
		{Name: "Friends", Engine: hoarddb.Hash}, {Name: "Purchases", Engine: hoarddb.FIFO}, {Name: "Photos", Engine: hoarddb.LIFO},
		{Name: "Recipes", Engine: hoarddb.BTree}, {Name: "Books", Engine: hoarddb.Heap}, {Name: "Data", Engine: hoarddb.Blob},
	}
	if got.Name != "User" || got.Type != hoarddb.Hash || got.Entries != 1 || !reflect.DeepEqual(got.StoreFields, wantFields) {
		t.Fatalf("Info = %+v; want User, hash, 1 entry (the stores' elements are not counted) and %v", got, wantFields)
	}
	if want := []string{"Friends", "Purchases", "Photos", "Recipes", "Books", "Data"}; !reflect.DeepEqual(got.Stores, want) {
		t.Errorf("Stores = %v, want %v", got.Stores, want)
	}
}

// A bucket with no store fields has none, and a bucket or database that does
// not exist is ErrNotFound with the server's own sentence.
func TestInfoOfPlainAndMissingBuckets(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	plain, err := client.Database("app").CreateBucket(ctx, "plain", hoarddb.BTree)
	if err != nil {
		t.Fatal(err)
	}
	got, err := plain.Info(ctx)
	if err != nil || got.Type != hoarddb.BTree || len(got.StoreFields) != 0 || len(got.Stores) != 0 {
		t.Fatalf("Info of a plain bucket = %+v, %v", got, err)
	}
	for name, bucket := range map[string]*hoarddb.Bucket{
		"missing bucket":   client.Database("app").Bucket("nope"),
		"missing database": client.Database("nodb").Bucket("nope"),
	} {
		got, err := bucket.Info(ctx)
		var failure *hoarddb.Error
		if got != nil || !errors.Is(err, hoarddb.ErrNotFound) || !errors.As(err, &failure) || failure.Message == "" {
			t.Errorf("%s: Info = %+v, %v; want ErrNotFound with the server's sentence", name, got, err)
		}
	}
}
