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
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// Real-server tests of queries (handbook/build-go.md step 8). Each starts its
// own server and needs HOARDDB_SERVER_BIN.

// connectThroughProxy connects a client to server through a relay that records
// the frames the driver sends, so a test can say how many requests a query
// took and what each asked for.
func connectThroughProxy(t *testing.T, server *servertest.Server, opts ...hoarddb.Option) (*hoarddb.Client, *servertest.Proxy) {
	t.Helper()
	proxy := servertest.StartProxy(t, server)
	uri := fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s", server.User, server.Password, proxy.Address, proxy.Fingerprint)
	return mustConnect(t, uri, opts...), proxy
}

// scansSent returns the SCAN requests the proxy saw, in order, and whether the
// server answered each with an error frame.
func scansSent(t *testing.T, proxy *servertest.Proxy) (requests []hoarddbwire.ScanRequest, refused []bool) {
	t.Helper()
	for _, exchange := range proxy.Exchanges() {
		if exchange.Request.Op != hoarddbwire.OpScan {
			continue
		}
		var request hoarddbwire.ScanRequest
		if err := codec.Unmarshal(exchange.Request.Payload, &request); err != nil {
			t.Fatalf("decoding a SCAN the driver sent: %v", err)
		}
		requests = append(requests, request)
		refused = append(refused, exchange.Response.Op == hoarddbwire.OpError)
	}
	return requests, refused
}

// seedNumbered puts n documents {N: 0..n-1, Name, Field} into a hash bucket.
func seedNumbered(t *testing.T, bucket *hoarddb.Bucket, count int) {
	t.Helper()
	ctx := testContext(t)
	for number := range count {
		field := "maths"
		if number%2 == 1 {
			field = "computing"
		}
		doc := hoarddb.Map{"N": int64(number), "Name": fmt.Sprintf("person-%03d", number), "Field": field, "Extra": "left out by a projection"}
		if _, err := bucket.Put(ctx, fmt.Sprintf("p%03d", number), doc); err != nil {
			t.Fatalf("seeding document %d: %v", number, err)
		}
	}
}

func numbersOf(t *testing.T, documents []hoarddb.Document) []int64 {
	t.Helper()
	numbers := make([]int64, len(documents))
	for position, doc := range documents {
		numbers[position] = intField(t, doc, "N")
	}
	return numbers
}

func TestFindPagesThroughAll(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 250)

	cursor, err := bucket.Find(ctx, nil, hoarddb.PageSize(100))
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if cursor.Total() != 250 {
		t.Fatalf("Total = %d before reading a document, want 250 from the first page", cursor.Total())
	}
	documents, err := cursor.All(ctx)
	if err != nil || len(documents) != 250 {
		t.Fatalf("All = %d documents, %v; want 250", len(documents), err)
	}
	seen := map[string]bool{}
	for _, doc := range documents {
		seen[stringField(t, doc, "_id_")] = true
	}
	if len(seen) != 250 {
		t.Fatalf("%d distinct keys in 250 documents: a page was read twice or skipped", len(seen))
	}
	requests, _ := scansSent(t, proxy)
	if len(requests) != 3 {
		t.Fatalf("%d SCAN requests, want 3 pages of at most 100", len(requests))
	}
	for position, wantSkip := range []int64{0, 100, 200} {
		if requests[position].Limit != 100 || requests[position].Skip != wantSkip {
			t.Errorf("page %d asked skip %d limit %d, want skip %d limit 100", position+1, requests[position].Skip, requests[position].Limit, wantSkip)
		}
	}
}

func TestFindSendsALimitEvenWithoutOne(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 3)
	if _, err := bucket.Find(testContext(t), nil); err != nil {
		t.Fatal(err)
	}
	requests, _ := scansSent(t, proxy)
	if len(requests) != 1 || requests[0].Limit != 100 {
		t.Fatalf("SCANs %+v: the driver must ask for its own page size (100) rather than lean on a server default", requests)
	}
}

func TestFindMissingBucketIsNotFound(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	if err := client.CreateDatabase(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	missing := client.Database("app").Bucket("nothing")

	cursor, err := missing.Find(ctx, nil)
	if !errors.Is(err, hoarddb.ErrNotFound) || cursor != nil {
		t.Fatalf("Find of a missing bucket = %v, %v; want ErrNotFound and no cursor (found:false in-band, like Get)", cursor, err)
	}
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || !strings.Contains(failure.Message, "nothing") {
		t.Fatalf("the error should carry the server's sentence naming the bucket: %v", err)
	}
	if _, err := missing.Count(ctx, nil); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Errorf("Count of a missing bucket = %v, want ErrNotFound", err)
	}
	if _, err := missing.Distinct(ctx, "N", nil); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Errorf("Distinct of a missing bucket = %v, want ErrNotFound", err)
	}
	if _, err := missing.Search(ctx, hoarddb.Map{"N": 1}); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Errorf("Search of a missing bucket = %v, want ErrNotFound", err)
	}
}

func TestFindOnAnEmptyBucketIsAnEmptyCursor(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	cursor, err := bucket.Find(ctx, nil)
	if err != nil {
		t.Fatalf("Find of an empty bucket: %v; an empty result is a cursor with nothing in it", err)
	}
	documents, err := cursor.All(ctx)
	if err != nil || len(documents) != 0 || cursor.Total() != 0 {
		t.Fatalf("All = %d documents, %v, Total %d; want none, no error, 0", len(documents), err, cursor.Total())
	}
	if count, err := bucket.Count(ctx, nil); err != nil || count != 0 {
		t.Fatalf("Count = %d, %v; want 0", count, err)
	}
}

func TestSortProjectionSkipLimit(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 10)

	cursor, err := bucket.Find(ctx, nil, hoarddb.SortBy("N", hoarddb.Descending), hoarddb.Skip(2), hoarddb.Limit(3), hoarddb.Projection("N", "Name"))
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := numbersOf(t, documents); !slices.Equal(got, []int64{7, 6, 5}) {
		t.Fatalf("N = %v, want [7 6 5]: descending, the first two skipped, three kept", got)
	}
	if _, left := documents[0].Lookup("Extra"); left {
		t.Fatalf("the projection kept Extra: %s", documents[0])
	}
	if cursor.Total() != 10 {
		t.Fatalf("Total = %d, want the 10 documents that match, whatever Skip and Limit leave", cursor.Total())
	}

	ascending, err := bucket.Find(ctx, hoarddb.Map{"Field": "computing"}, hoarddb.SortBy("N", hoarddb.Ascending))
	if err != nil {
		t.Fatal(err)
	}
	all, err := ascending.All(ctx)
	if err != nil || !slices.Equal(numbersOf(t, all), []int64{1, 3, 5, 7, 9}) {
		t.Fatalf("filtered ascending = %v, %v; want the odd numbers in order", numbersOf(t, all), err)
	}

	// Skip and Limit span pages: the request of each page carries the offset.
	paged, err := bucket.Find(ctx, nil, hoarddb.SortBy("N", hoarddb.Ascending), hoarddb.Skip(3), hoarddb.Limit(5), hoarddb.PageSize(2))
	if err != nil {
		t.Fatal(err)
	}
	pagedDocs, err := paged.All(ctx)
	if err != nil || !slices.Equal(numbersOf(t, pagedDocs), []int64{3, 4, 5, 6, 7}) {
		t.Fatalf("paged = %v, %v; want [3 4 5 6 7]", numbersOf(t, pagedDocs), err)
	}
	requests, _ := scansSent(t, proxy)
	tail := requests[len(requests)-3:]
	for position, want := range [][2]int64{{3, 2}, {5, 2}, {7, 1}} {
		if tail[position].Skip != want[0] || tail[position].Limit != want[1] {
			t.Errorf("paged request %d asked skip %d limit %d, want skip %d limit %d", position+1, tail[position].Skip, tail[position].Limit, want[0], want[1])
		}
	}
}

// A cursor is not a snapshot, but it must not read a different bucket as more
// of the same one.
func TestCursorRefusesABucketRecreatedBetweenPages(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 5)

	cursor, err := bucket.Find(ctx, nil, hoarddb.PageSize(2), hoarddb.SortBy("N", hoarddb.Ascending))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.DropBucket(ctx, "hash"); err != nil {
		t.Fatal(err)
	}
	replacement, err := database.CreateBucket(ctx, "hash", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	seedNumbered(t, replacement, 5)

	documents, err := cursor.All(ctx)
	if len(documents) != 2 || err == nil || !strings.Contains(err.Error(), "dropped and re-created") {
		t.Fatalf("All = %d documents, %v; want the 2 of the first page, then the server's refusal that the bucket was re-created", len(documents), err)
	}
	requests, _ := scansSent(t, proxy)
	if last := requests[len(requests)-1]; last.ExpectBucketID == 0 {
		t.Fatalf("the later page named no bucket id (%+v), so the server could not tell", last)
	}
}

func TestFindWithAFilterOfEachKind(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 6)
	encoded, err := codec.Marshal(hoarddb.Map{"Field": "computing"})
	if err != nil {
		t.Fatal(err)
	}
	type byField struct {
		Field string `bson:"Field"`
	}
	filters := map[string]any{
		"a Map":      hoarddb.Map{"Field": "computing"},
		"a struct":   byField{Field: "computing"},
		"a Document": hoarddb.Document(encoded),
	}
	for name, filter := range filters {
		cursor, err := bucket.Find(ctx, filter)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if documents, err := cursor.All(ctx); err != nil || len(documents) != 3 {
			t.Errorf("%s: %d documents, %v; want the 3 odd ones", name, len(documents), err)
		}
	}
	for name, everything := range map[string]any{"nil": nil, "an empty Map": hoarddb.Map{}} {
		cursor, err := bucket.Find(ctx, everything)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if documents, err := cursor.All(ctx); err != nil || len(documents) != 6 {
			t.Errorf("%s matches %d documents, %v; want all 6", name, len(documents), err)
		}
	}
}

func TestCountAndDistinct(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 10)

	if count, err := bucket.Count(ctx, nil); err != nil || count != 10 {
		t.Fatalf("Count(nil) = %d, %v; want 10", count, err)
	}
	if count, err := bucket.Count(ctx, hoarddb.Map{"Field": "computing"}); err != nil || count != 5 {
		t.Fatalf("Count(computing) = %d, %v; want 5", count, err)
	}
	if count, err := bucket.Count(ctx, hoarddb.Map{"Field": "nothing"}); err != nil || count != 0 {
		t.Fatalf("Count(nothing) = %d, %v; want 0", count, err)
	}

	values, err := bucket.Distinct(ctx, "Field", nil)
	if err != nil {
		t.Fatal(err)
	}
	words := make([]string, len(values))
	for position, distinct := range values {
		text, isText := distinct.AsString()
		if !isText {
			t.Fatalf("a distinct value %s is not a string", distinct)
		}
		words[position] = text
	}
	slices.Sort(words)
	if !slices.Equal(words, []string{"computing", "maths"}) {
		t.Fatalf("Distinct(Field) = %v", words)
	}
	numbers, err := bucket.Distinct(ctx, "N", hoarddb.Map{"Field": "computing"})
	if err != nil || len(numbers) != 5 {
		t.Fatalf("Distinct(N | computing) = %d values, %v; want 5", len(numbers), err)
	}
	for _, distinct := range numbers {
		if number, isNumber := distinct.Int64(); !isNumber || number%2 != 1 {
			t.Errorf("distinct N %s: want an odd integer", distinct)
		}
	}
}

// Every store type that answers find and count (handbook/operations.md
// section 1) answers them the same way.
func TestFindAndCountOnEveryQueryableStoreType(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree, hoarddb.FIFO, hoarddb.LIFO, hoarddb.Heap} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			for number := range 3 {
				doc := hoarddb.Map{"N": int64(number), "Name": fmt.Sprintf("person-%d", number)}
				var err error
				switch kind {
				case hoarddb.Hash, hoarddb.BTree:
					_, err = bucket.Put(ctx, fmt.Sprintf("k%d", number), doc)
				case hoarddb.Heap:
					_, err = bucket.Push(ctx, doc, hoarddb.WithPriority(int64(number)))
				default:
					_, err = bucket.Push(ctx, doc)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			cursor, err := bucket.Find(ctx, nil)
			if err != nil {
				t.Fatalf("Find: %v", err)
			}
			documents, err := cursor.All(ctx)
			if err != nil || len(documents) != 3 || cursor.Total() != 3 {
				t.Fatalf("Find = %d documents, Total %d, %v; want 3", len(documents), cursor.Total(), err)
			}
			numbers := numbersOf(t, documents)
			slices.Sort(numbers)
			if !slices.Equal(numbers, []int64{0, 1, 2}) {
				t.Fatalf("Find returned N = %v", numbers)
			}
			if kind == hoarddb.FIFO && !slices.Equal(numbersOf(t, documents), []int64{0, 1, 2}) {
				t.Fatalf("a fifo's natural order is the order pushed, got %v", numbersOf(t, documents))
			}
			if count, err := bucket.Count(ctx, nil); err != nil || count != 3 {
				t.Fatalf("Count = %d, %v; want 3", count, err)
			}
		})
	}
}

// A blob bucket's value is content, not a document. The "-" cells of
// handbook/operations.md section 1 are the intended contract; what this server
// answers for each is recorded here (build doc S-4), and the driver passes the
// answer through.
func TestQueryVerbsOnABlobBucketPassTheAnswerThrough(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	blobs := bucketOf(t, client, hoarddb.Blob)

	cursor, err := blobs.Find(ctx, nil)
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || cursor != nil {
		t.Fatalf("Find of a blob bucket = %v, %v; the server says found:true with a message and no documents, which is a refusal", cursor, err)
	}
	if !strings.Contains(err.Error(), "blob") {
		t.Errorf("the refusal should carry the server's sentence about blob buckets: %v", err)
	}
	// Known deviation: COUNT of a blob bucket is accepted and answers 0.
	if count, err := blobs.Count(ctx, nil); err != nil || count != 0 {
		t.Errorf("Count of a blob bucket = %d, %v; this server answers 0 with no error", count, err)
	}
	if _, err := blobs.Distinct(ctx, "N", nil); err == nil {
		t.Error("Distinct of a blob bucket succeeded; this server refuses it")
	}
	if _, err := blobs.Search(ctx, hoarddb.Map{"N": 1}); err == nil {
		t.Error("Search of a blob bucket succeeded; this server refuses it")
	}
}

func TestSearchUsesTheIndex(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	database := client.Database("app")
	indexed, err := database.CreateBucket(ctx, "indexed", hoarddb.Hash, hoarddb.WithIndexes(hoarddb.IndexSpec{Fields: []string{"Field"}}))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := database.CreateBucket(ctx, "plain", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	seedNumbered(t, indexed, 10)
	seedNumbered(t, plain, 10)

	searchIndexUsed := func(bucket *hoarddb.Bucket) (string, []hoarddb.Document, int64) {
		t.Helper()
		before := len(proxy.Exchanges())
		cursor, err := bucket.Search(ctx, hoarddb.Map{"Field": "computing"})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		documents, err := cursor.All(ctx)
		if err != nil {
			t.Fatal(err)
		}
		exchanges := proxy.Exchanges()[before:]
		if len(exchanges) != 1 || exchanges[0].Request.Op != hoarddbwire.OpSearch {
			t.Fatalf("a Search sent %d requests, want one SEARCH", len(exchanges))
		}
		var answer hoarddbwire.SearchResponse
		if err := codec.Unmarshal(exchanges[0].Response.Payload, &answer); err != nil {
			t.Fatal(err)
		}
		return answer.IndexUsed, documents, cursor.Total()
	}
	used, documents, total := searchIndexUsed(indexed)
	if used != "Field_1" || len(documents) != 5 || total != 5 {
		t.Fatalf("indexed Search used %q and found %d documents (Total %d); want the index Field_1 and 5", used, len(documents), total)
	}
	used, documents, _ = searchIndexUsed(plain)
	if used == "Field_1" || !strings.Contains(used, "full scan") || len(documents) != 5 {
		t.Fatalf("Search without an index used %q and found %d documents; want a full scan and the same 5", used, len(documents))
	}
}

func TestSearchLimit(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 10)
	cursor, err := bucket.Search(ctx, hoarddb.Map{"Field": "computing"}, hoarddb.Limit(2))
	if err != nil {
		t.Fatal(err)
	}
	documents, err := cursor.All(ctx)
	if err != nil || len(documents) != 2 || cursor.Total() != 2 {
		t.Fatalf("Search with Limit(2) = %d documents, Total %d, %v; want 2, and Total 2 (SEARCH counts what it returns)", len(documents), cursor.Total(), err)
	}
	if !cursor.HasMore() {
		t.Error("HasMore = false after Limit(2) of 5 matches; a further match exists")
	}
}

// HasMore is true only when a match exists past the limit: a search that holds
// every match, with or without a limit, says false, and so does a Find, whose
// Next pages by itself.
func TestSearchHasMore(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 10) // five of the ten documents are "computing"
	for _, tc := range []struct {
		name string
		opts []hoarddb.FindOption
		want int
		more bool
	}{
		{"cut below the matches", []hoarddb.FindOption{hoarddb.Limit(2)}, 2, true},
		{"limit equal to the matches", []hoarddb.FindOption{hoarddb.Limit(5)}, 5, false},
		{"limit above the matches", []hoarddb.FindOption{hoarddb.Limit(50)}, 5, false},
		{"no limit", nil, 5, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursor, err := bucket.Search(ctx, hoarddb.Map{"Field": "computing"}, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			documents, err := cursor.All(ctx)
			if err != nil || len(documents) != tc.want || cursor.Total() != int64(tc.want) || cursor.HasMore() != tc.more {
				t.Fatalf("%d documents, Total %d, HasMore %t, %v; want %d, %d, %t", len(documents), cursor.Total(), cursor.HasMore(), err,
					tc.want, tc.want, tc.more)
			}
		})
	}
	found, err := bucket.Find(ctx, hoarddb.Map{"Field": "computing"}, hoarddb.Limit(2))
	if err != nil {
		t.Fatal(err)
	}
	if found.HasMore() {
		t.Error("a Find cursor reports HasMore; Next pages by itself, so only Search says it was cut")
	}
}

// A query option the request has no member for is refused locally. Sending
// the request without it would answer a different question than the caller
// asked.
func TestOptionsARequestCannotCarryAreRefusedBeforeSending(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	hash := bucketOf(t, client, hoarddb.Hash)
	tree := bucketOf(t, client, hoarddb.BTree)
	seedNumbered(t, hash, 3)
	before := len(proxy.Exchanges())

	for name, attempt := range map[string]func() error{
		"Search with Skip": func() error { _, err := hash.Search(ctx, hoarddb.Map{}, hoarddb.Skip(1)); return err },
		"Search with SortBy": func() error {
			_, err := hash.Search(ctx, hoarddb.Map{}, hoarddb.SortBy("N", hoarddb.Ascending))
			return err
		},
		"Search with Projection": func() error { _, err := hash.Search(ctx, hoarddb.Map{}, hoarddb.Projection("N")); return err },
		"Search with PageSize":   func() error { _, err := hash.Search(ctx, hoarddb.Map{}, hoarddb.PageSize(5)); return err },
		"Range with Skip":        func() error { _, err := tree.Range(ctx, hoarddb.Bounds{}, hoarddb.Skip(1)); return err },
		"Range with SortBy": func() error {
			_, err := tree.Range(ctx, hoarddb.Bounds{}, hoarddb.SortBy("N", hoarddb.Ascending))
			return err
		},
		"Range with Projection": func() error { _, err := tree.Range(ctx, hoarddb.Bounds{}, hoarddb.Projection("N")); return err },
	} {
		err := attempt()
		if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "nothing was sent") {
			t.Errorf("%s = %v; want a local ErrInvalidArgument that says nothing was sent", name, err)
		}
	}
	if sent := len(proxy.Exchanges()) - before; sent != 0 {
		t.Fatalf("%d requests reached the server; a refused call sends none", sent)
	}
}

func TestInvalidFindOptionsAreRefusedBeforeSending(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	before := len(proxy.Exchanges())
	for name, option := range map[string]hoarddb.FindOption{
		"Skip(-1)":                hoarddb.Skip(-1),
		"SortBy with no field":    hoarddb.SortBy("", hoarddb.Ascending),
		"SortBy with a bad order": hoarddb.SortBy("N", 0),
		"Projection of nothing":   hoarddb.Projection(),
		"Projection of an empty":  hoarddb.Projection("N", ""),
		"Limit(0)":                hoarddb.Limit(0),
		"PageSize(0)":             hoarddb.PageSize(0),
	} {
		if _, err := bucket.Find(ctx, nil, option); !errors.Is(err, hoarddb.ErrInvalidArgument) {
			t.Errorf("Find with %s = %v, want ErrInvalidArgument", name, err)
		}
	}
	for name, filter := range map[string]any{"a string": "Field", "a number": 7, "a slice": []string{"a"}} {
		if _, err := bucket.Find(ctx, filter); !errors.Is(err, hoarddb.ErrInvalidArgument) {
			t.Errorf("Find with a filter that is %s = %v, want ErrInvalidArgument", name, err)
		}
		if _, err := bucket.Count(ctx, filter); !errors.Is(err, hoarddb.ErrInvalidArgument) {
			t.Errorf("Count with a filter that is %s = %v, want ErrInvalidArgument", name, err)
		}
	}
	if _, err := bucket.Distinct(ctx, "", nil); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Errorf("Distinct of no field = %v, want ErrInvalidArgument", err)
	}
	if sent := len(proxy.Exchanges()) - before; sent != 0 {
		t.Fatalf("%d requests reached the server; a refused call sends none", sent)
	}
}

func TestConsistentGetReadsBack(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	for _, kind := range []hoarddb.StoreType{hoarddb.Hash, hoarddb.BTree} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketOf(t, client, kind)
			if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
				t.Fatal(err)
			}
			before := len(proxy.Exchanges())
			doc, err := bucket.Get(ctx, "ada", hoarddb.Consistent())
			if err != nil || stringField(t, doc, "Name") != "Ada" || stringField(t, doc, "_id_") != "ada" {
				t.Fatalf("Get(Consistent) = %s, %v", doc, err)
			}
			exchanges := proxy.Exchanges()[before:]
			if len(exchanges) != 1 || exchanges[0].Request.Op != hoarddbwire.OpConsistentGet {
				t.Fatalf("Get(Consistent) sent %d requests, want one CONSISTENT_GET", len(exchanges))
			}
			miss, err := bucket.Get(ctx, "nobody", hoarddb.Consistent())
			if !errors.Is(err, hoarddb.ErrNotFound) || miss != nil {
				t.Fatalf("Get(Consistent) of a missing key = %s, %v; want ErrNotFound and no document (failure 2)", miss, err)
			}
		})
	}
	missing := client.Database("app").Bucket("nothing")
	if _, err := missing.Get(ctx, "ada", hoarddb.Consistent()); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("Get(Consistent) of a missing bucket = %v, want ErrNotFound", err)
	}
}

// A break in a range-over-func loop must leave nothing running and send
// nothing more.
func TestDocumentsIteratorStopsOnBreak(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 10)

	// The first query dials the bulk lane, whose reader goroutine is the
	// pool's; it is counted before the iterators.
	warm, err := bucket.Find(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = warm.Close()
	before := runtime.NumGoroutine()
	exchangesBefore := len(proxy.Exchanges())

	for range 25 {
		cursor, err := bucket.Find(ctx, nil, hoarddb.PageSize(2))
		if err != nil {
			t.Fatal(err)
		}
		taken := 0
		for doc, err := range cursor.Documents(ctx) {
			if err != nil || doc == nil {
				t.Fatalf("document %d: %v", taken, err)
			}
			taken++
			if taken == 3 {
				break
			}
		}
		if taken != 3 {
			t.Fatalf("took %d documents, want 3", taken)
		}
	}
	// Three documents at two a page is two requests per cursor, and the break
	// sends no third.
	if sent := len(proxy.Exchanges()) - exchangesBefore; sent != 50 {
		t.Fatalf("25 cursors broken after 3 documents sent %d requests, want 50 (two pages each)", sent)
	}
	time.Sleep(10 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("%d goroutines before 25 abandoned iterators, %d after; an iterator must leave none", before, after)
	}
}

// bigDocument is a document of about size bytes.
func bigDocument(number int, size int) hoarddb.Map {
	return hoarddb.Map{"N": int64(number), "Padding": strings.Repeat("x", size)}
}

// startCeilingServer starts a server whose query result ceiling is its minimum,
// 1 MiB (handbook/testing.md section 3.1).
func startCeilingServer(t *testing.T) *servertest.Server {
	t.Helper()
	return servertest.Start(t, servertest.Options{Env: map[string]string{"HOARDDB_QUERY_MAX_RESULT_BYTES": "1048576"}})
}

// limitsAsked is the page size of each SCAN the driver sent, and whether the
// server refused it.
func limitsAsked(t *testing.T, proxy *servertest.Proxy) (limits []int64, refused []bool) {
	t.Helper()
	requests, answers := scansSent(t, proxy)
	for _, request := range requests {
		limits = append(limits, request.Limit)
	}
	return limits, answers
}

func TestCursorHalvesAsOftenAsNeeded(t *testing.T) {
	t.Run("once is enough", func(t *testing.T) {
		client, proxy := connectThroughProxy(t, startCeilingServer(t))
		ctx := testContext(t)
		bucket := bucketOf(t, client, hoarddb.Hash)
		for number := range 8 {
			if _, err := bucket.Put(ctx, fmt.Sprintf("big%d", number), bigDocument(number, 200_000)); err != nil {
				t.Fatal(err)
			}
		}
		// Eight 200 KB documents are over the 1 MiB ceiling; four are under it.
		cursor, err := bucket.Find(ctx, nil, hoarddb.PageSize(8))
		if err != nil {
			t.Fatalf("Find: %v; the cursor should have halved its page and retried", err)
		}
		documents, err := cursor.All(ctx)
		if err != nil || len(documents) != 8 {
			t.Fatalf("All = %d documents, %v; want all 8", len(documents), err)
		}
		limits, refused := limitsAsked(t, proxy)
		if !slices.Equal(limits, []int64{8, 4, 4}) || !slices.Equal(refused, []bool{true, false, false}) {
			t.Fatalf("page sizes asked %v (refused %v); want 8 refused, then 4 and 4 at the halved size", limits, refused)
		}
		// The connection survived the refusal.
		if err := client.Ping(ctx); err != nil {
			t.Fatalf("Ping after RESULT_TOO_LARGE: %v", err)
		}
	})
	// Owner ruling 2026-10-10: 400 documents of 100 KiB are over the 16 MiB
	// ceiling at 400 and at 200 (about 163 fit), so one halving is not enough.
	t.Run("uniform 100 KiB documents at PageSize 400", func(t *testing.T) {
		client, proxy := connectThroughProxy(t, startServer(t))
		ctx := testContext(t)
		bucket := bucketOf(t, client, hoarddb.Hash)
		for number := range 400 {
			if _, err := bucket.Put(ctx, fmt.Sprintf("doc%03d", number), bigDocument(number, 100*1024)); err != nil {
				t.Fatal(err)
			}
		}
		cursor, err := bucket.Find(ctx, nil, hoarddb.PageSize(400))
		if err != nil {
			t.Fatalf("Find: %v; the cursor should have halved to a size that fits", err)
		}
		documents, err := cursor.All(ctx)
		if err != nil || len(documents) != 400 {
			t.Fatalf("All = %d documents, %v; want all 400", len(documents), err)
		}
		limits, refused := limitsAsked(t, proxy)
		wantLimits := []int64{400, 200, 100, 100, 100, 100}
		wantRefused := []bool{true, true, false, false, false, false}
		if !slices.Equal(limits, wantLimits) || !slices.Equal(refused, wantRefused) {
			t.Fatalf("page sizes asked %v (refused %v); want %v (refused %v): halve twice, then keep the size that worked", limits, refused, wantLimits, wantRefused)
		}
	})
}

// Page 1 succeeds after one halving; page 2 holds larger documents, so it is
// refused at the kept size and halves again, twice. The size that worked on
// page 2 is then kept.
func TestCursorHalvesAgainOnALaterPage(t *testing.T) {
	client, proxy := connectThroughProxy(t, startCeilingServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	for number := range 8 {
		size := 100_000
		if number >= 4 {
			size = 600_000
		}
		if _, err := bucket.Put(ctx, fmt.Sprintf("doc%d", number), bigDocument(number, size)); err != nil {
			t.Fatal(err)
		}
	}
	cursor, err := bucket.Find(ctx, nil, hoarddb.PageSize(8), hoarddb.SortBy("N", hoarddb.Ascending))
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	documents, err := cursor.All(ctx)
	if err != nil || len(documents) != 8 {
		t.Fatalf("All = %d documents, %v; want all 8 (page 2 must halve again)", len(documents), err)
	}
	limits, refused := limitsAsked(t, proxy)
	wantLimits := []int64{8, 4, 4, 2, 1, 1, 1, 1}
	wantRefused := []bool{true, false, true, true, false, false, false, false}
	if !slices.Equal(limits, wantLimits) || !slices.Equal(refused, wantRefused) {
		t.Fatalf("page sizes asked %v (refused %v); want %v (refused %v)", limits, refused, wantLimits, wantRefused)
	}
}

// The attempts per page are capped at log2(PageSize)+1: a server that refuses
// every size ends the cursor after exactly that many requests. The fake turns a
// runaway into a different error after 100 requests so the test fails instead
// of hanging.
func TestCursorHalvingIsCappedPerPage(t *testing.T) {
	var asked []int64
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var scan hoarddbwire.ScanRequest
		if err := codec.Unmarshal(request.Payload, &scan); err != nil {
			t.Error(err)
			return false
		}
		asked = append(asked, scan.Limit)
		if len(asked) > 100 {
			c.errorFrame(request, int32(hoarddbwire.CodeInternal), "runaway: the cursor kept asking")
			return true
		}
		c.errorWith(request, map[string]any{"code": int32(hoarddbwire.CodeResultTooLarge), "message": "too big", "max_result_bytes": int64(1048576)})
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("people").Find(testContext(t), nil, hoarddb.PageSize(100))
	if !errors.Is(err, hoarddb.ErrResultTooLarge) {
		t.Fatalf("Find = %v, want ErrResultTooLarge", err)
	}
	if want := []int64{100, 50, 25, 12, 6, 3, 1}; !slices.Equal(asked, want) {
		t.Fatalf("page sizes asked %v, want %v: log2(100)+1 = 7 attempts, down to 1 document", asked, want)
	}
	if !strings.Contains(err.Error(), "100") || !strings.Contains(err.Error(), "1 document") {
		t.Fatalf("the message should name the first size and the last: %v", err)
	}
}

// Failure 12 of handbook/build-go.md section 2: a page that can never fit must
// not make the cursor loop.
func TestCursorReportsADocumentThatCannotFit(t *testing.T) {
	client, proxy := connectThroughProxy(t, startCeilingServer(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bucket := bucketOf(t, client, hoarddb.Hash)
	if _, err := bucket.Put(ctx, "huge", bigDocument(1, 1_200_000)); err != nil {
		t.Fatal(err)
	}
	if _, err := bucket.Put(ctx, "small", bigDocument(2, 10)); err != nil {
		t.Fatal(err)
	}
	alone, err := client.Database("app").CreateBucket(ctx, "alone", hoarddb.BTree)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alone.Put(ctx, "huge", bigDocument(1, 1_200_000)); err != nil {
		t.Fatal(err)
	}
	pair := bucketOf(t, client, hoarddb.BTree)
	for number := range 2 {
		if _, err := pair.Put(ctx, fmt.Sprintf("half%d", number), bigDocument(number, 700_000)); err != nil {
			t.Fatal(err)
		}
	}

	// retained_bytes is a lower bound the server fills in as far as it got, so
	// only what it held before stopping can be asserted.
	assertTooLarge := func(t *testing.T, err error, atLeastRetained int64) *hoarddb.Error {
		t.Helper()
		var failure *hoarddb.Error
		if !errors.Is(err, hoarddb.ErrResultTooLarge) || !errors.As(err, &failure) {
			t.Fatalf("Find = %v, want ErrResultTooLarge", err)
		}
		if failure.MaxResultBytes != 1048576 || failure.RetainedBytes < atLeastRetained {
			t.Fatalf("the error carries ceiling %d and retained %d, want 1048576 and at least %d", failure.MaxResultBytes, failure.RetainedBytes, atLeastRetained)
		}
		for _, number := range []string{"1048576", fmt.Sprint(failure.RetainedBytes)} {
			if !strings.Contains(failure.Message, number) {
				t.Fatalf("the message should carry both numbers, %s is missing: %s", number, failure.Message)
			}
		}
		return failure
	}

	t.Run("a page of one cannot be halved", func(t *testing.T) {
		before, _ := scansSent(t, proxy)
		cursor, err := bucket.Find(ctx, nil, hoarddb.PageSize(1), hoarddb.SortBy("N", hoarddb.Ascending))
		if cursor != nil {
			t.Fatalf("Find returned a cursor over a document that cannot fit")
		}
		_ = assertTooLarge(t, err, 0)
		after, _ := scansSent(t, proxy)
		if len(after)-len(before) != 1 {
			t.Fatalf("%d SCANs for a page of one that cannot fit, want exactly 1", len(after)-len(before))
		}
	})
	t.Run("a larger page is halved down to one document, and reads on when that fits", func(t *testing.T) {
		before, _ := scansSent(t, proxy)
		cursor, err := pair.Find(ctx, nil)
		if err != nil {
			t.Fatalf("Find: %v; one 700 KB document fits under the 1 MiB ceiling", err)
		}
		documents, err := cursor.All(ctx)
		if err != nil || len(documents) != 2 {
			t.Fatalf("All = %d documents, %v; want both", len(documents), err)
		}
		after, _ := scansSent(t, proxy)
		var limits []int64
		for _, request := range after[len(before):] {
			limits = append(limits, request.Limit)
		}
		if want := []int64{100, 50, 25, 12, 6, 3, 1, 1}; !slices.Equal(limits, want) {
			t.Fatalf("page sizes asked %v, want %v", limits, want)
		}
	})
	t.Run("a single document over the ceiling is reported at one document, after 7 tries", func(t *testing.T) {
		before, _ := scansSent(t, proxy)
		_, err := alone.Find(ctx, nil)
		failure := assertTooLarge(t, err, 0)
		if !strings.Contains(failure.Message, "100") || !strings.Contains(failure.Message, "1 document") {
			t.Errorf("the message should say the page was halved from 100 to 1 document: %s", failure.Message)
		}
		after, _ := scansSent(t, proxy)
		if len(after)-len(before) != 7 {
			t.Fatalf("%d SCANs, want 7: 100, 50, 25, 12, 6, 3, 1", len(after)-len(before))
		}
	})
	t.Run("a later page that cannot fit is reported through the cursor", func(t *testing.T) {
		cursor, err := bucket.Find(ctx, nil, hoarddb.PageSize(1), hoarddb.SortBy("N", hoarddb.Descending))
		if err != nil {
			t.Fatalf("Find: %v; the first page holds the small document", err)
		}
		documents, err := cursor.All(ctx)
		if len(documents) != 1 {
			t.Fatalf("read %d documents before the failure, want the 1 small one", len(documents))
		}
		_ = assertTooLarge(t, err, 0)
		if !errors.Is(cursor.Err(), hoarddb.ErrResultTooLarge) || cursor.Next(ctx) {
			t.Fatalf("the cursor must stay failed: Err=%v", cursor.Err())
		}
	})
}

func TestFindOneReturnsTheFirstMatchAndAMissIsErrNotFound(t *testing.T) {
	client, proxy := connectThroughProxy(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	seedNumbered(t, bucket, 10)

	doc, err := bucket.FindOne(ctx, hoarddb.Map{"Field": "computing"}, hoarddb.SortBy("N", hoarddb.Descending))
	if err != nil || intField(t, doc, "N") != 9 {
		t.Fatalf("FindOne = %s, %v; want the computing person with the highest N (9)", doc, err)
	}
	requests, _ := scansSent(t, proxy)
	if len(requests) != 1 || requests[0].Limit != 1 {
		t.Fatalf("SCANs %+v, want one with limit 1", requests)
	}
	doc, err = bucket.FindOne(ctx, hoarddb.Map{"Field": "no such field"})
	if !errors.Is(err, hoarddb.ErrNotFound) || doc != nil {
		t.Fatalf("FindOne with no match = %s, %v; want ErrNotFound", doc, err)
	}
	if _, err := bucket.FindOne(ctx, nil, hoarddb.Projection("Name")); err != nil {
		t.Fatalf("FindOne with a nil filter and a projection: %v", err)
	}
	missing := client.Database("app").Bucket("nothing")
	if _, err := missing.FindOne(ctx, nil); !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("FindOne of a missing bucket = %v, want ErrNotFound", err)
	}
}

// A field stored as null is a document that was found: only no document at
// all is a miss.
func TestFindOneOfAStoredNullFieldIsAFoundDocument(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	bucket := bucketOf(t, client, hoarddb.Hash)
	if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada", "Nickname": nil}); err != nil {
		t.Fatal(err)
	}
	doc, err := bucket.FindOne(ctx, hoarddb.Map{"Name": "Ada"})
	if err != nil {
		t.Fatalf("FindOne of a document with a null field: %v; it is found", err)
	}
	if value, ok := doc.Lookup("Nickname"); !ok || value.String() != "null" {
		t.Fatalf("Nickname = %v, %v; want a present null", value, ok)
	}
}
