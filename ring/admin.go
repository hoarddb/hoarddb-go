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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

func (c *Client) admin(ctx context.Context, request ringwire.RingAdminRequest) (ringwire.RingAdminReply, error) {
	body, err := ringwire.EncodeRingAdmin(request)
	if err != nil {
		return ringwire.RingAdminReply{}, fmt.Errorf("ring: encoding %s: %w", request.Cmd, err)
	}
	payload, err := c.call(ctx, hoarddbwire.OpRingAdmin, c.NextRequestID(), body)
	if err != nil {
		return ringwire.RingAdminReply{}, fmt.Errorf("%s: %w", request.Cmd, err)
	}
	reply, err := ringwire.DecodeRingAdminReply(payload)
	if err != nil {
		return ringwire.RingAdminReply{}, fmt.Errorf("ring: decoding the %s reply: %w", request.Cmd, err)
	}
	if reply.Cmd != request.Cmd {
		return ringwire.RingAdminReply{}, fmt.Errorf("ring: asked %s and the server answered %s. Check that the server is a hoarddb server of this version", request.Cmd, reply.Cmd)
	}
	return reply, nil
}

// CreateDatabase creates database name on disk. rf is the replication factor; 0 leaves it to the
// server, which means 1. A database that exists is a CONFLICT *Error.
func (c *Client) CreateDatabase(ctx context.Context, name string, rf int32) (DatabaseInfo, error) {
	return c.CreateDatabaseWithStorage(ctx, name, rf, "")
}

// CreateDatabaseWithStorage creates database name with the storage word "disk" or "memory"; ""
// means disk. rf is as CreateDatabase's. The returned Storage is the word the server answers,
// which is the one the database keeps.
func (c *Client) CreateDatabaseWithStorage(ctx context.Context, name string, rf int32, storage string) (DatabaseInfo, error) {
	request := ringwire.RingAdminRequest{Cmd: ringwire.AdminCreateDatabase, Name: name, Storage: storage}
	if rf != 0 {
		request.RF = &rf
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return DatabaseInfo{}, err
	}
	return DatabaseInfo{ID: reply.DatabaseID, RF: reply.RF, Storage: reply.Storage}, nil
}

// CreateBucket declares bucket name in database with the given fields and caches its header.
// Repeating a create with the same fields returns the existing bucket; the same name with
// other fields is BUCKET_EXISTS.
func (c *Client) CreateBucket(ctx context.Context, database, name string, fields []FieldDecl) (BucketInfo, error) {
	return c.CreateBucketExpiring(ctx, database, name, "", fields)
}

// CreateBucketExpiring is CreateBucket for a bucket that expires: expireField names the datetime
// field its documents expire at (empty for none) and a field's ExpirePath makes a fifoStore or
// lifoStore expire its elements at that path.
func (c *Client) CreateBucketExpiring(ctx context.Context, database, name, expireField string, fields []FieldDecl) (BucketInfo, error) {
	declared := make([]ringwire.RingAdminField, len(fields))
	var paths map[string]string
	var aggregates []ringwire.AggregateDecl
	for i, field := range fields {
		declared[i] = ringwire.RingAdminField{Name: field.Name, Type: field.Type, Of: field.Of}
		for _, aggregate := range field.Aggregates {
			aggregates = append(aggregates, ringwire.AggregateDecl{Store: field.Name, Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By})
		}
		if field.ExpirePath != "" {
			if paths == nil {
				paths = map[string]string{}
			}
			paths[field.Name] = field.ExpirePath
		}
	}
	reply, err := c.admin(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminCreateBucket, Database: database, Name: name, Fields: declared, Aggregates: aggregates, ExpireField: expireField, ExpirePaths: paths})
	if err != nil {
		return BucketInfo{}, err
	}
	return c.remember(bucketInfoOf(database, name, reply)), nil
}

// AlterChange is what AlterBucket changes: fields to add and names of fields to drop. A name
// appears once in the whole change.
type AlterChange struct {
	Add  []FieldDecl
	Drop []string
	// SetExpire names the datetime field to expire documents at; DropExpire removes the bucket's
	// expiry. At most one of them.
	SetExpire  string
	DropExpire bool
}

// AlterBucket adds and drops fields of bucket name in database and returns the declaration
// in force afterwards, which it also caches as the bucket's header. Adding a field rewrites no
// document; a dropped field's values are deleted in the background and read as unset at once.
// A field's type never changes (INVALID_ARGUMENT). It is safe to repeat: the same alter run
// again changes nothing, and run after an interrupted one it finishes that one.
func (c *Client) AlterBucket(ctx context.Context, database, name string, change AlterChange) (BucketInfo, error) {
	added := make([]ringwire.RingAdminField, len(change.Add))
	for i, field := range change.Add {
		added[i] = ringwire.RingAdminField{Name: field.Name, Type: field.Type, Of: field.Of}
	}
	dropped := change.Drop
	if dropped == nil {
		dropped = []string{}
	}
	reply, err := c.admin(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminAlterBucket, Database: database, Bucket: name, Add: added, Drop: dropped, ExpireField: change.SetExpire, DropExpire: change.DropExpire})
	if err != nil {
		return BucketInfo{}, err
	}
	return c.remember(bucketInfoOf(database, name, reply)), nil
}

// DropBucket drops bucket name of database and its documents, and forgets the bucket's cached
// header. It returns as soon as the bucket is dropping: it stops answering (BUCKET_DROPPING), its
// documents are deleted in the background by the node, and the name is free to create again when
// that is done (show buckets lists the bucket as dropping until then, and a create of the name
// is BUCKET_EXISTS). Nothing the caller does after this returns, closing the connection
// included, stops the deletion. A bucket that is not there is a NOT_FOUND *Error, which is also
// what a drop that finished answers when it is repeated. It is safe to repeat while the bucket is
// dropping. DropBucket drops whatever bucket the name carries, and its reply's id is not returned
// here, so a drop RETRIED after the first one completed and the name was created again drops the
// NEW bucket: a caller that cannot tell must use DropBucketExpecting with the id it holds.
func (c *Client) DropBucket(ctx context.Context, database, name string) error {
	return c.dropBucket(ctx, database, name, 0)
}

func (c *Client) dropBucket(ctx context.Context, database, name string, expect uint64) error {
	if _, err := c.admin(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminDropBucket, Database: database, Bucket: name, ExpectBucketID: expect}); err != nil {
		return err
	}
	c.headers.forget(database, name)
	return nil
}

// DropBucketExpecting is DropBucket for a caller that holds the id of the bucket it means, from
// a describe or a create: if the name now carries another id, because the bucket was dropped and
// created again, nothing is dropped and the call is refused.
func (c *Client) DropBucketExpecting(ctx context.Context, database, name string, expect uint64) error {
	return c.dropBucket(ctx, database, name, expect)
}

// DescribeBucket reads the declaration in force and refreshes the cached header.
func (c *Client) DescribeBucket(ctx context.Context, database, name string) (BucketInfo, error) {
	reply, err := c.admin(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminDescribeBucket, Database: database, Bucket: name})
	if err != nil {
		return BucketInfo{}, err
	}
	return c.remember(bucketInfoOf(database, name, reply)), nil
}

// ShowBuckets lists one page of the buckets of database in name-slot order. A page can hold
// fewer buckets than the limit; keep asking with the returned Cursor while More is true.
func (c *Client) ShowBuckets(ctx context.Context, database string, page PageRequest) (BucketPage, error) {
	request := ringwire.RingAdminRequest{Cmd: ringwire.AdminShowBuckets, Database: database, Cursor: page.Cursor}
	if page.Limit != 0 {
		request.Limit = &page.Limit
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return BucketPage{}, err
	}
	out := BucketPage{Buckets: make([]BucketSummary, len(reply.Buckets)), More: reply.More, Cursor: reply.Cursor}
	for i, listed := range reply.Buckets {
		out.Buckets[i] = BucketSummary{Name: listed.Name, ID: listed.BucketID, State: listed.State}
	}
	return out, nil
}

// ShowDatabases lists one page of the databases this session may see, by name. A session with
// no role on any database gets an empty page and no error. Keep asking with the returned
// Cursor while More is true.
func (c *Client) ShowDatabases(ctx context.Context, page PageRequest) (DatabasePage, error) {
	request := ringwire.RingAdminRequest{Cmd: ringwire.AdminShowDatabases, Cursor: page.Cursor}
	if page.Limit != 0 {
		request.Limit = &page.Limit
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return DatabasePage{}, err
	}
	out := DatabasePage{Databases: make([]DatabaseSummary, len(reply.Databases)), More: reply.More, Cursor: reply.Cursor}
	for i, listed := range reply.Databases {
		out.Databases[i] = DatabaseSummary{Name: listed.Name, ID: listed.DatabaseID, RF: listed.RF}
	}
	return out, nil
}

// DescribeDatabase reads a database's id and replication factor and one page of its buckets,
// paged as ShowBuckets is. A database that does not exist and one the session has no role on
// are the same PERMISSION_DENIED *Error.
func (c *Client) DescribeDatabase(ctx context.Context, database string, page PageRequest) (DatabaseDescription, error) {
	request := ringwire.RingAdminRequest{Cmd: ringwire.AdminDescribeDatabase, Database: database, Cursor: page.Cursor}
	if page.Limit != 0 {
		request.Limit = &page.Limit
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return DatabaseDescription{}, err
	}
	out := DatabaseDescription{Name: reply.Name, ID: reply.DatabaseID, RF: reply.RF, Storage: reply.Storage,
		MemoryBytes: reply.MemoryBytes, MemoryCapBytes: reply.MemoryCapBytes, Buckets: make([]BucketSummary, len(reply.Buckets)),
		More: reply.More, Cursor: reply.Cursor}
	for i, listed := range reply.Buckets {
		out.Buckets[i] = BucketSummary{Name: listed.Name, ID: listed.BucketID, State: listed.State}
	}
	return out, nil
}

func bucketInfoOf(database, name string, reply ringwire.RingAdminReply) BucketInfo {
	info := BucketInfo{Database: database, Name: name, ID: reply.BucketID, State: reply.State,
		DeclVersion: reply.DeclVersion, SchemaVersionSeen: reply.SchemaVersionSeen, Fields: make([]FieldInfo, len(reply.Fields))}
	for i, field := range reply.Fields {
		info.Fields[i] = FieldInfo{ID: field.ID, Name: field.Name, Type: field.Type, Of: field.Of}
	}
	info.ExpireField = reply.ExpireField
	info.ExpirePending, info.ExpirePendingMore = reply.ExpirePending, reply.ExpirePendingMore
	for _, store := range reply.ExpireStores {
		info.ExpireStores = append(info.ExpireStores, ExpireStoreInfo{Field: store.Field, Path: store.Path})
	}
	for _, aggregate := range reply.Aggregates {
		info.Aggregates = append(info.Aggregates, AggregateInfo{Store: aggregate.Store, ID: aggregate.ID, Kind: aggregate.Kind, Field: aggregate.Field, By: aggregate.By, State: aggregate.State})
	}
	return info
}

// headerShardBits sets the number of locks the header cache is split over: 16. A process
// that names many buckets from many goroutines then waits on a sixteenth of the lookups
// instead of all of them, and the cost is fixed per client (16 locks and 16 maps), never
// per bucket.
const headerShardBits = 4

const headerShards = 1 << headerShardBits

// headerName is the key of a cached header. It is a struct of the two names rather than
// one joined string so that a lookup, which every keyed call makes, allocates nothing.
type headerName struct{ database, bucket string }

type headerShard struct {
	lock    sync.RWMutex
	headers map[headerName]BucketInfo
}

// headerCache holds the declaration of every bucket the client has described, by name.
// A stored BucketInfo is never changed, only replaced, so a reader may keep the copy it
// got. Storing, reading and forgetting one header each take one shard lock and one map
// operation whatever the number of buckets cached: nothing here walks or copies the cache.
type headerCache struct {
	shards [headerShards]headerShard
}

// shard picks the lock of name with FNV-1a over both names (a zero byte between them, as
// no name holds one). The top bits choose, because FNV-1a mixes them best.
func (cache *headerCache) shard(name headerName) *headerShard {
	const offset, prime = 14695981039346656037, 1099511628211
	hash := uint64(offset)
	for i := 0; i < len(name.database); i++ {
		hash = (hash ^ uint64(name.database[i])) * prime
	}
	hash *= prime // the zero byte: x ^ 0 is x
	for i := 0; i < len(name.bucket); i++ {
		hash = (hash ^ uint64(name.bucket[i])) * prime
	}
	return &cache.shards[hash>>(64-headerShardBits)]
}

func (cache *headerCache) store(info BucketInfo) {
	name := headerName{database: info.Database, bucket: info.Name}
	shard := cache.shard(name)
	shard.lock.Lock()
	if shard.headers == nil {
		shard.headers = map[headerName]BucketInfo{}
	}
	shard.headers[name] = info
	shard.lock.Unlock()
}

func (cache *headerCache) load(database, bucket string) (BucketInfo, bool) {
	name := headerName{database: database, bucket: bucket}
	shard := cache.shard(name)
	shard.lock.RLock()
	info, ok := shard.headers[name]
	shard.lock.RUnlock()
	return info, ok
}

func (cache *headerCache) forget(database, bucket string) {
	name := headerName{database: database, bucket: bucket}
	shard := cache.shard(name)
	shard.lock.Lock()
	delete(shard.headers, name)
	shard.lock.Unlock()
}

// perShard is how many headers each shard holds. It walks the cache, so it is for tests.
func (cache *headerCache) perShard() [headerShards]int {
	var counts [headerShards]int
	for i := range cache.shards {
		shard := &cache.shards[i]
		shard.lock.RLock()
		counts[i] = len(shard.headers)
		shard.lock.RUnlock()
	}
	return counts
}

func (c *Client) remember(info BucketInfo) BucketInfo {
	c.headers.store(info)
	return info
}

func (c *Client) cachedHeader(database, name string) (BucketInfo, bool) {
	return c.headers.load(database, name)
}

// DescribeNode reads the facts of the node this connection is on: id, build, uptime, its
// state in the cluster, the cluster's size, epoch and leader, the default replication factor,
// whether it is serving and, when the server can read it, the machine. Any authenticated
// session may ask. A server older than the command refuses it with an INVALID_ARGUMENT *Error.
func (c *Client) DescribeNode(ctx context.Context) (NodeDescription, error) {
	reply, err := c.admin(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminDescribeNode})
	if err != nil {
		return NodeDescription{}, err
	}
	node := reply.Node
	if node == nil {
		return NodeDescription{}, errors.New("ring: the server answered describe_node without the node. Check that the server is a hoarddb server of this version")
	}
	out := NodeDescription{NodeID: node.NodeID, Version: node.Version, UptimeMS: node.UptimeMS, MemberState: node.MemberState, Members: node.Members,
		Voters: node.Voters, Epoch: node.Epoch, Leader: node.Leader, DefaultRF: node.DefaultRF, Serving: node.Serving,
		StateRange: node.StateRange, RefusedPeers: node.RefusedPeers, ClusterVersion: node.ClusterVersion}
	if host := node.Host; host != nil {
		out.Host = &HostDescription{CPUs: host.CPUs, HostCPUs: host.HostCPUs, MemUsed: host.MemUsed, MemTotal: host.MemTotal, MemSource: host.MemSource}
		if host.Load != nil {
			out.Host.Load = &Load{One: host.Load.One, Five: host.Load.Five, Fifteen: host.Load.Fifteen}
		}
		if host.Process != nil {
			out.Host.Process = &ProcessMemory{Used: host.Process.Used, Limit: host.Process.Limit}
		}
	}
	return out, nil
}

// SeedingStatus reads the slot pulls of the node this connection is on: the ones placed,
// seeding, stalled and retired, ordered by database then slot. It needs the cluster-manager
// role; any other session gets the one PERMISSION_DENIED *Error of the admin commands.
func (c *Client) SeedingStatus(ctx context.Context, query SeedingQuery) (SeedingPage, error) {
	request := ringwire.RingAdminRequest{Cmd: ringwire.AdminSeedingStatus, Database: query.Database, Cursor: query.Page.Cursor}
	if query.Slot != nil {
		slot, err := seedingSlot(*query.Slot)
		if err != nil {
			return SeedingPage{}, err
		}
		request.Slot = &slot
	}
	if query.Page.Limit != 0 {
		request.Limit = &query.Page.Limit
	}
	reply, err := c.admin(ctx, request)
	if err != nil {
		return SeedingPage{}, err
	}
	if reply.Seeding == nil {
		return SeedingPage{}, errors.New("ring: the server answered seeding_status without rows. Check that the server is a hoarddb server of this version")
	}
	page := SeedingPage{Rows: make([]SeedingRow, len(reply.Seeding.Rows)), More: reply.Seeding.More, Cursor: reply.Seeding.Cursor}
	for i, row := range reply.Seeding.Rows {
		page.Rows[i] = SeedingRow{Database: row.Database, Slot: int(row.Slot), State: row.State, Source: row.Source, BytesDone: row.BytesDone,
			BytesKnown: row.BytesKnown, Entries: row.Entries, Idle: time.Duration(row.IdleMS) * time.Millisecond, Refusal: row.Refusal}
	}
	return page, nil
}

// SeedingRetry restarts the pull of one slot of a database on the node this connection is on,
// from a fresh cut. A slot with no seeding record is a SEED_NOT_HELD *Error that names the fix.
func (c *Client) SeedingRetry(ctx context.Context, database string, slot int) error {
	return c.seedingAction(ctx, ringwire.AdminSeedingRetry, database, slot, false)
}

// SeedingForget removes the retired record of one slot and keeps its range, so the slot seeds
// from scratch the next time it is in the node's copy set. A record that is not retired is a
// SEED_HELD *Error and nothing changes; no record is SEED_NOT_HELD. A retired record that no
// other member has confirmed a copy of is also SEED_HELD, because this node may hold the only copy of
// the slot's data; anyway goes ahead with that forget.
func (c *Client) SeedingForget(ctx context.Context, database string, slot int, anyway bool) error {
	return c.seedingAction(ctx, ringwire.AdminSeedingForget, database, slot, anyway)
}

func (c *Client) seedingAction(ctx context.Context, cmd, database string, slot int, anyway bool) error {
	wireSlot, err := seedingSlot(slot)
	if err != nil {
		return err
	}
	reply, err := c.admin(ctx, ringwire.RingAdminRequest{Cmd: cmd, Database: database, Slot: &wireSlot, Anyway: anyway})
	if err != nil {
		return err
	}
	if reply.Seeding == nil {
		return fmt.Errorf("ring: the server answered %s without the slot. Check that the server is a hoarddb server of this version", cmd)
	}
	return nil
}

// seedingSlot checks a slot against the 8192 a database has, so a slot that does not fit the
// wire's int32 is refused instead of being cut down to another slot.
func seedingSlot(slot int) (int32, error) {
	if slot < 0 || slot > 8191 {
		return 0, fmt.Errorf("ring: slot %d is outside 0 to 8191. Send a slot 'status seeding' lists", slot)
	}
	return int32(slot), nil
}

// MembershipResult is what the five membership calls answer: the committed position of the
// change and the server's one-line account of it, which a front end prints as it is.
type MembershipResult struct {
	// Epoch and StateSeq are the committed position; a repeated join answers the position its
	// registration committed at.
	Epoch, StateSeq int64
	// Result is the server's line.
	Result string
}

// Join registers the running node id at the internode address addr as a member of the cluster
// this connection is on. witness asks for a witness and must agree with the node's own
// configuration, which is the truth. A join of a known id at the same address answers the same
// result again.
func (c *Client) Join(ctx context.Context, id, addr string, witness bool) (MembershipResult, error) {
	return c.membership(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminJoin, NodeID: id, DataAddr: addr, Witness: witness})
}

// Promote makes member id a voter, if the cluster has a free voter slot. A member that already
// votes is answered with the facts and nothing changes.
func (c *Client) Promote(ctx context.Context, id string) (MembershipResult, error) {
	return c.membership(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminPromote, NodeID: id})
}

// Leave starts the orderly departure of member id from the cluster this connection is on. The
// result is the position the start committed at, not the end of the departure.
func (c *Client) Leave(ctx context.Context, id string) (MembershipResult, error) {
	return c.membership(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminLeave, NodeID: id})
}

// RemoveNode removes member id, which must be down or dead; a member that is up leaves with
// Leave.
func (c *Client) RemoveNode(ctx context.Context, id string) (MembershipResult, error) {
	return c.membership(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminRemoveNode, NodeID: id})
}

// ReplaceNode puts the machine at the internode address addr in the place of member id, which
// must be down or dead.
func (c *Client) ReplaceNode(ctx context.Context, id, addr string) (MembershipResult, error) {
	return c.membership(ctx, ringwire.RingAdminRequest{Cmd: ringwire.AdminReplaceNode, NodeID: id, DataAddr: addr})
}

func (c *Client) membership(ctx context.Context, request ringwire.RingAdminRequest) (MembershipResult, error) {
	reply, err := c.admin(ctx, request)
	if err != nil {
		return MembershipResult{}, err
	}
	if reply.Membership == nil {
		return MembershipResult{}, fmt.Errorf("ring: the server answered %s without a result. Check that the server is a hoarddb server of this version", request.Cmd)
	}
	return MembershipResult{Epoch: reply.Membership.Epoch, StateSeq: reply.Membership.StateSeq, Result: reply.Membership.Result}, nil
}
