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

package ring

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	ringwire "github.com/hoarddb/hoarddb-wire"
)

// fillHeaders stores the declarations numbered from to until and returns how long it took.
// Every declaration has its own name and its own BucketInfo; the field table is shared, so
// a million of them fit in a test.
func fillHeaders(cache *headerCache, from, until int, fields []FieldInfo) time.Duration {
	start := time.Now()
	for number := from; number < until; number++ {
		cache.store(BucketInfo{Database: "app", Name: "bucket-" + strconv.Itoa(number), ID: uint64(number), State: "active", DeclVersion: 1, Fields: fields})
	}
	return time.Since(start)
}

// TestMillionDescribesStayLinear is the measurement behind the statement that the client
// does no work that grows with the number of buckets: it stores 10^6 declarations in the
// header cache alone and fails when the second half takes more than twice the first. A
// cache that copies itself on every describe takes three times as long for the second
// half and many minutes in all; one locked map write per describe takes about as long for
// both. The timing is tried up to three times, because the test VMs are shared and one slow
// half must not fail a linear cache; a quadratic one fails all three.
func TestMillionDescribesStayLinear(t *testing.T) {
	if testing.Short() {
		t.Skip("stores 10^6 declarations; run without -short")
	}
	const total = 1_000_000
	fields := []FieldInfo{{ID: 1, Name: "_id_", Type: "string"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 3, Name: "Purchases", Type: "fifoStore"}}
	for attempt := 1; ; attempt++ {
		cache := new(headerCache)
		runtime.GC()
		first := fillHeaders(cache, 0, total/2, fields)
		second := fillHeaders(cache, total/2, total, fields)

		counts := cache.perShard()
		if len(counts) != 16 {
			t.Fatalf("the header cache has %d shard(s), want 16 shard locks", len(counts))
		}
		held := 0
		for shard, count := range counts {
			held += count
			if count < total/32 || count > total/8 {
				t.Fatalf("shard %d holds %d of %d declarations, want about one sixteenth (between %d and %d): the shards are not balanced: %v",
					shard, count, total, total/32, total/8, counts)
			}
		}
		if held != total {
			t.Fatalf("the cache holds %d declarations after %d were stored", held, total)
		}
		for number := 0; number < total; number += 997 {
			info, ok := cache.load("app", "bucket-"+strconv.Itoa(number))
			if !ok || info.ID != uint64(number) {
				t.Fatalf("declaration %d read back as id %d, found %v", number, info.ID, ok)
			}
		}

		ratio := float64(second) / float64(first)
		t.Logf("attempt %d: %d declarations stored; first half %v, second half %v, ratio %.2f (%.0f ns per store)",
			attempt, total, first, second, ratio, float64(first+second)/total)
		if ratio <= 2 {
			return
		}
		if attempt == 3 {
			t.Fatalf("the second %d stores took %.2f times as long as the first %d in three attempts; a store must not get slower as the cache grows", total/2, ratio, total/2)
		}
	}
}

// TestConcurrentDescribesShareTheCache stores, reads and forgets headers from many
// goroutines at once, on names of their own and on one name they all share. Run under the
// race detector it is the check that every access holds its shard's lock.
func TestConcurrentDescribesShareTheCache(t *testing.T) {
	const workers, each = 16, 2000
	cache := new(headerCache)
	var group sync.WaitGroup
	failures := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			database := "db" + strconv.Itoa(worker)
			for number := 0; number < each; number++ {
				name := "bucket-" + strconv.Itoa(number)
				cache.store(BucketInfo{Database: database, Name: name, ID: uint64(number)})
				cache.store(BucketInfo{Database: "shared", Name: "hot", ID: uint64(worker)})
				if info, ok := cache.load(database, name); !ok || info.ID != uint64(number) {
					failures <- database + "." + name + " was not read back after it was stored"
					return
				}
				cache.load("shared", "hot")
				if number%2 == 1 {
					cache.forget(database, name)
				}
				cache.forget("shared", "hot")
			}
		}()
	}
	group.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	held := 0
	for _, count := range cache.perShard() {
		held += count
	}
	if _, hot := cache.load("shared", "hot"); hot {
		held--
	}
	if held != workers*each/2 {
		t.Fatalf("the cache holds %d headers, want the %d that were stored and not forgotten", held, workers*each/2)
	}
}

// describeReplyWith is a describe_bucket reply for a bucket of the given number of fields:
// _id_, then scalar and store fields in turn, named as a developer would name them.
func describeReplyWith(t *testing.T, fields int) []byte {
	t.Helper()
	declared := []ringwire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "string"}, {ID: 2, Name: "Name", Type: "string"},
		{ID: 3, Name: "Purchases", Type: "fifoStore"}, {ID: 4, Name: "Photos", Type: "lifoStore"}, {ID: 5, Name: "Friends", Type: "hashStore"},
		{ID: 6, Name: "Recipes", Type: "btreeStore"}, {ID: 7, Name: "Books", Type: "heapStore"}, {ID: 8, Name: "Data", Type: "blobStore"},
		{ID: 9, Name: "Email", Type: "string"}}
	payload, err := ringwire.EncodeRingAdminReply(ringwire.RingAdminReply{Cmd: ringwire.AdminDescribeBucket, BucketID: 77, State: "active",
		DeclVersion: 1, SchemaVersionSeen: 1, Fields: declared[:fields]})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// heapInUse is the bytes of live heap objects after a full collection.
func heapInUse() uint64 {
	runtime.GC()
	runtime.GC() // the second pass frees what the first one's finalizers released
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// TestCachedDescribesCostBytesPerBucket measures what one cached bucket costs the process:
// it decodes a describe_bucket reply 100 000 times, as 100 000 describes of different
// buckets would, stores each declaration and divides the growth of the live heap by the
// count. The figure includes the bucket's name, its field table and the cache's own slot.
// The numbers it logs are the ones written down as the client's per-bucket cost; the bounds
// it asserts are loose and only catch a cost that doubled.
func TestCachedDescribesCostBytesPerBucket(t *testing.T) {
	const buckets = 100_000
	measure := func(fields int) float64 {
		payload := describeReplyWith(t, fields)
		cache := new(headerCache)
		before := heapInUse()
		for number := 0; number < buckets; number++ {
			reply, err := ringwire.DecodeRingAdminReply(payload)
			if err != nil {
				t.Fatal(err)
			}
			cache.store(bucketInfoOf("app", "bucket-"+strconv.Itoa(number), reply))
		}
		after := heapInUse()
		runtime.KeepAlive(cache)
		return float64(after-before) / buckets
	}
	one, nine := measure(1), measure(9)
	t.Logf("live heap per cached bucket over %d buckets: %.0f B with one field (_id_ only), %.0f B with nine fields, so about %.0f B per declared field",
		buckets, one, nine, (nine-one)/8)
	if one <= 0 || one > 800 {
		t.Errorf("a cached bucket of one field costs %.0f B, want under 800 B", one)
	}
	if nine <= one || nine > 2400 {
		t.Errorf("a cached bucket of nine fields costs %.0f B, want more than one field's %.0f B and under 2400 B", nine, one)
	}

	// A lookup is on the path of every keyed call: it must not allocate.
	cache := new(headerCache)
	cache.store(BucketInfo{Database: "app", Name: "User", ID: 77})
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, ok := cache.load("app", "User"); !ok {
			t.Fatal("the stored header was not found")
		}
	}); allocs != 0 {
		t.Errorf("a header lookup allocates %.0f times, want 0", allocs)
	}
	// A store of a new bucket allocates the cached copy and, now and then, a larger map.
	next := 0
	names := make([]string, 1001)
	for i := range names {
		names[i] = "fresh-" + strconv.Itoa(i)
	}
	storeAllocs := testing.AllocsPerRun(1000, func() {
		cache.store(BucketInfo{Database: "app", Name: names[next], ID: uint64(next)})
		next++
	})
	t.Logf("allocations per store of a new bucket: %.0f", storeAllocs)
	if storeAllocs > 2 {
		t.Errorf("a store of a new bucket allocates %.0f times, want at most 2", storeAllocs)
	}
}
