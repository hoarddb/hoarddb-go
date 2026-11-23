# The Go driver's API

Binds the Go driver only. Every name here comes from a hoardDB concept (the bucket, the document, its
store fields) or from a Go standard-library convention, never from another database's driver.

**Where it is today.** The Go ring client is the package `ring` of the module
`github.com/hoarddb/hoarddb-go`, version v0.1.0, pre-release: the API can still change. It speaks the ring
opcodes of `wire.md` section 11 through the codec module `github.com/hoarddb/hoarddb-wire`, imports
nothing from the server, and is Apache-2.0.

    go get github.com/hoarddb/hoarddb-go@v0.1.0

    import "github.com/hoarddb/hoarddb-go/ring"

The root package of the same module (`hoarddb.Connect`) is a different client: it sends the earlier
engine's opcodes (`wire.md` section 4) and is described in the module's README, not here.

## 1. Shape

```
Client                one authenticated connection; safe for many goroutines
 ├─ admin             CreateDatabase, CreateBucket, DescribeBucket, ShowBuckets, ...
 └─ Bucket            a name; no I/O, no resources; every verb of every store field
     └─ Entity        one document id of the bucket; Store(field) is that document's own store
```

A **bucket holds documents (entities); a document carries its own stores.** `Bucket` methods take the
document id, then the field, then the arguments: `users.Push(ctx, "ada", "Purchases", item)` pushes onto
`ada`'s own `Purchases` queue. The bucket has no store type; each field has one.
`users.Entity("ada")` and `users.Field("ada", "Purchases")` are handles that fix the id, or the id and
the field, so the same verbs read `ada.Store("Purchases").Push(ctx, item)`; they send what the `Bucket`
methods send.

**A handle costs nothing.** `client.Bucket("app", "User")` allocates one small struct, does no I/O, and
holds no connection, goroutine, timer or metric of its own. The bucket's id, field ids and declaration
versions are fetched on the first call that needs them and cached per bucket in the client; a
`SCHEMA_NOT_CURRENT` refusal refreshes them and sends the request once more under the same request ID.

## 2. Connecting

```go
func Dial(ctx context.Context, opts Options) (*Client, error)
func (c *Client) Close() error                       // waits for the reader goroutine
func (c *Client) NextRequestID() [16]byte

type Options struct {
    Addr           string        // host:port of the node's TCP listener
    User, Password string        // AUTH
    Fingerprint    string        // pins the server certificate: the value the server prints when it starts
    KnownServers   string        // path of a known-servers file, used when Fingerprint is empty
    Insecure       bool          // local development only: skips certificate checks
    DialTimeout    time.Duration // connect and AUTH; 0 means 10 seconds
    RequestTimeout time.Duration // when the call's context has no deadline; 0 means 30 seconds
    NoResend       bool          // never resend after a stale-header refusal; for benchmarks
}
```

`Dial` refuses options that set none of `Fingerprint`, `KnownServers` and `Insecure`, and the error says
how to get the fingerprint (the server prints it when it starts). The strictest one that is set decides:
`Fingerprint`, else `KnownServers`, else `Insecure`. With `KnownServers` (the file the `hoarddb` command
line writes at `~/.hoarddb/known_servers`), `Dial` connects only when the file records `Addr` with the key
the server presents; it never asks and never adds a line. There is no option that turns TLS off. A
`Client` keeps **one connection** and pipelines requests on it: each carries a 16-byte request ID and
its reply is matched by that ID, so replies may arrive in any order. Tokens, a URI and a connection
pool are not part of this client yet (`auth.md`, `trust.md`).

## 3. Databases and buckets

```go
func (c *Client) CreateDatabase(ctx context.Context, name string, rf int32) (DatabaseInfo, error)   // rf 0 means the default of 1
func (c *Client) CreateDatabaseWithStorage(ctx context.Context, name string, rf int32, storage string) (DatabaseInfo, error)
func (c *Client) ShowDatabases(ctx context.Context, page PageRequest) (DatabasePage, error)
func (c *Client) DescribeDatabase(ctx context.Context, database string, page PageRequest) (DatabaseDescription, error)

func (c *Client) CreateBucket(ctx context.Context, database, name string, fields []FieldDecl) (BucketInfo, error)
func (c *Client) CreateBucketExpiring(ctx context.Context, database, name, expireField string, fields []FieldDecl) (BucketInfo, error)
func (c *Client) AlterBucket(ctx context.Context, database, name string, change AlterChange) (BucketInfo, error)
func (c *Client) DropBucket(ctx context.Context, database, name string) error
func (c *Client) DropBucketExpecting(ctx context.Context, database, name string, expect uint64) error
func (c *Client) DescribeBucket(ctx context.Context, database, name string) (BucketInfo, error)
func (c *Client) ShowBuckets(ctx context.Context, database string, page PageRequest) (BucketPage, error)
func (c *Client) DescribeNode(ctx context.Context) (NodeDescription, error)
func (c *Client) Bucket(database, name string) *Bucket
func (b *Bucket) Info(ctx context.Context) (BucketInfo, error)

type FieldDecl struct {
    Name, Type string      // Type: a scalar word (string, int, datetime, ...) or a store word (fifoStore, ...)
    ExpirePath string      // a fifoStore or lifoStore field that expires its elements, by this datetime path
    Of         string      // "<Bucket>" or "<Database>.<Bucket>" when the field holds that bucket's ids
    Aggregates []AggregateDecl
}
```

```go
_, err = client.CreateDatabase(ctx, "app", 0)
_, err = client.CreateBucket(ctx, "app", "User", []ring.FieldDecl{
    {Name: "Name", Type: "string"},
    {Name: "Purchases", Type: "fifoStore"},
    {Name: "Friends", Type: "hashStore"},
})
```

- A type word is exact (`operations.md` section 2); a synonym is refused.
- `CreateDatabase` of an existing name is `CONFLICT`; `CreateBucket` of an identical declaration answers
  the existing bucket, and the same name with other fields is `BUCKET_EXISTS`.
- `DropBucket` asks for no confirmation. There is no `DropDatabase` and no `AlterDatabase`: they are not
  available yet.
- `ShowBuckets` and `ShowDatabases` return one page; loop while `More` is true, passing `Cursor` back.
  There is no bucket count per database.
- `BucketInfo` is the declaration in force: `ID`, `State`, `DeclVersion`, `SchemaVersionSeen`, and
  `Fields` (id, name and type, `_id_` first).

## 4. Documents

```go
type Doc map[string]any            // scalar fields by declared name; store fields are written with the store methods

func (b *Bucket) Insert(ctx context.Context, id string, doc Doc, options ...CallOption) (WriteResult, error)
func (b *Bucket) Upsert(ctx context.Context, id string, doc Doc, options ...CallOption) (WriteResult, error)
func (b *Bucket) Update(ctx context.Context, id string, change Update, options ...CallOption) (WriteResult, error)
func (b *Bucket) Delete(ctx context.Context, id string, options ...CallOption) (WriteResult, error)
func (b *Bucket) Get(ctx context.Context, id string) (bson.Raw, error)                          // ErrNotFound on a miss
func (b *Bucket) GetFields(ctx context.Context, id string, fields []string) (bson.Raw, error)
func (b *Bucket) GetExpanded(ctx context.Context, id string, fields []string, expand []ExpandRequest, local bool) (document, expanded bson.Raw, err error)
func (b *Bucket) List(ctx context.Context, filter any, page PageRequest) (EntryPage, error)      // reads every document: O(documents)
func (b *Bucket) Count(ctx context.Context, filter any) (int64, error)
```

`Insert` refuses an `_id_` the bucket holds (`DOCUMENT_EXISTS`). `Upsert` replaces the scalar fields and
keeps the stores. `Update` takes an `Update{Set, Unset, Inc, Mul, Min, Max, Push, AddToSet, Rename}`;
the modifiers of `Push` and `AddToSet` (such as `$each`) are refused by name. A document is raw BSON
(`bson.Raw` of `github.com/hoarddb/hoarddb-bson/bson`), decoded only on request. A `Doc` member the
bucket does not declare is refused (`FIELD_TYPE`), and the message lists the declared fields.

```go
type WriteResult struct {
    Outcome    string  // applied, applied_degraded, unknown (not_applied with NotApplied set)
    Copies     int     // copies that confirmed
    Position   string  // epoch:seq of the applied record
    Deduped    bool    // answered from the dedupe window
    NotApplied bool    // a guard did not hold; nothing was written
    Reason, GuardField string
    Document   bson.Raw
    DocumentTruncated bool  // a deduped retry's document was too large to repeat; read it with Get
}
func (w WriteResult) Applied() bool
```

**Call options** (`CallOption`): `If(guard)` makes an update or delete apply only when the stored
document still holds every equality (`Guard` is a `bson.D`); `Return(ReturnAfter | ReturnBefore)` asks
for the document back; `WithRequestID(id)` sends under a given request ID, which is how a retry of a
write whose outcome is unknown is made safe (section 7); `WithWriteDeadline(ms)` bounds the owner's wait
for copies. Insert and upsert refuse `If` and `Return`.

## 5. Stores

Every store verb takes the document id and the field name.

```go
// fifoStore, lifoStore
func (b *Bucket) Push(ctx context.Context, id, field string, element any, options ...CallOption) (WriteResult, error)
func (b *Bucket) Pop(ctx context.Context, id, field string, options ...CallOption) (bson.Raw, error)               // ErrNotFound when empty
func (b *Bucket) PopN(ctx context.Context, id, field string, n int, options ...CallOption) ([]bson.Raw, error)
func (b *Bucket) Peek(ctx context.Context, id, field string, count int) ([]bson.Raw, error)                        // 1 to 100; nothing removed
func (b *Bucket) Claim(ctx context.Context, id, field string, count int, options ...CallOption) (Lease, error)
func (b *Bucket) Ack(ctx context.Context, id, field string, lease [16]byte, options ...CallOption) (LeaseChange, error)
func (b *Bucket) Release(ctx context.Context, id, field string, lease [16]byte, options ...CallOption) (LeaseChange, error)
func (b *Bucket) Extend(ctx context.Context, id, field string, lease [16]byte, leaseMs int64, options ...CallOption) (LeaseChange, error)
func (b *Bucket) LeaseGet(ctx context.Context, id, field string, lease [16]byte) (LeaseRead, error)

// hashStore, btreeStore
func (b *Bucket) Put(ctx context.Context, id, field, key string, element any, options ...CallOption) (WriteResult, error)
func (b *Bucket) Take(ctx context.Context, id, field, key string, options ...CallOption) (bson.Raw, error)
func (b *Bucket) StoreGet(ctx context.Context, id, field, key string) (bson.Raw, error)
func (b *Bucket) Inc(ctx context.Context, id, field, key string, increments map[string]any, options ...CallOption) (WriteResult, error)
func (b *Bucket) IncAfter(ctx context.Context, id, field, key string, increments map[string]any, options ...CallOption) (bson.Raw, WriteResult, error)

// every store
func (b *Bucket) StoreList(ctx context.Context, id, field string, limit int, after []byte) (ElementPage, error)   // limit 0 means 100; 1 to 1000
func (b *Bucket) StoreCount(ctx context.Context, id, field string) (int64, error)
func (b *Bucket) PeekKeyed(ctx context.Context, id, field string, count int) (ElementPage, error)
func (b *Bucket) StoreListExpanded(ctx context.Context, id, field string, limit int, after []byte, expand *ExpandRequest, local bool) (ElementPage, error)
```

A single-value read that finds nothing (`Get`, `Pop`, `Take`, `StoreGet`) is `ErrNotFound`; a list-shaped
read (`Peek`, `PopN`, `StoreList`, `List`) answers an empty result. `PopN` takes 1 to 100 elements in one
write. `Claim` of an empty store answers an
all-zero lease ID and no elements. `Claim` asks for 1 to 65535 messages and the server leases at most 100,
for 30 seconds; `Extend` names a new length, 1000 to 3600000 milliseconds from now. For a release or an extend, `LeaseChange.Live` is false for an expired or
unknown lease: an answer, not an error. An ack leaves it false. `Sum`, `SumBy`, `CountBy`, `Aggregate`, `DescribeStore` and `Nearest` read
maintained sums and nearest elements of a store field.

A `blobStore` answers `StoreCount` only; a chunked blob upload is not available yet. This client has
no heap push: `Push` on a `heapStore` field is refused by the server (`INVALID_ARGUMENT`), because a heap
push carries an entry id and a priority that `Push` does not send. A heap field can be read (`Peek`,
`PeekKeyed`, `StoreGet`, `StoreList`, `StoreCount`) and popped.

## 6. Many documents

```go
func (c *Client) NewBatch() *Batch
func (b *Bucket) In(batch *Batch) *Bucket                       // write methods of the returned handle queue on the batch
func (batch *Batch) Send(ctx context.Context, options ...CallOption) (BatchResult, error)

func (c *Client) GetMany(ctx context.Context, keys []Key, options ...CallOption) ([]GetAnswer, error)
func (b *Bucket) GetMany(ctx context.Context, ids []string, fields []string, options ...CallOption) ([]GetAnswer, error)
```

A batch sends up to **255** writes, to any buckets, in one request. It is **not a transaction**: each
operation is applied or refused on its own, a refused one does not stop the others, and operations on one
document apply in the order queued. `Send` returns one answer per operation in queue order. To retry
after a lost answer, call `Send` again on the same `Batch`: it reuses the request ID, the applied
operations answer `Deduped`, and the rest run. A subset retry is not offered. `WithRequestID` and
`WithWriteDeadline` go to `Send`, not to the queued methods. `GetMany` returns one `GetAnswer` per key in
order (`Found` false for a miss, `Err` for a key the server refused); more than 255 keys are sent as
concurrent frames and rejoined in order, and the keys are not one snapshot.

## 7. Errors and retries

```go
var ErrNotFound error
type Error struct { Code ringwire.Code; Message string; Detail *ringwire.Detail }   // ringwire is hoarddb-wire
func IsExpired(err error) bool                  // NOT_FOUND of an expired document (detail.expired)
func ExpiresIn(d time.Duration) time.Time       // the client's clock plus d, for an expire field
```

`*Error` carries the ring code, the server's message (which names the fix; pass it through) and the
structured `Detail` for the codes that define one (`NOT_OWNER`, `STALE_EPOCH`, `OWNER_DOWN`,
`NO_QUORUM`, `LEASE_LAPSED`, `RF_UNSUPPORTED`, and `NOT_FOUND` of an expired document). `errors.Is(err,
ErrNotFound)` is true for `NOT_FOUND`, an expired one included. The codes and what to retry are
`errors.md`.

**To retry a write whose outcome you do not know** (a timeout, a dropped connection, `UNAVAILABLE`), take
a request ID once and send every attempt under it:

```go
id := client.NextRequestID()
res, err := users.Push(ctx, "ada", "Purchases", item, ring.WithRequestID(id))
// on a timeout: the same call again with the same id. res.Deduped is true when the first attempt landed.
```

A write sent again under a new request ID is a new write. The server remembers an ID only inside its
dedupe window (`operations.md` section 7).

## 8. Cluster calls

```go
func (c *Client) Topology(ctx context.Context, have *Topology) (*Topology, error)
func (t *Topology) Owner(database string, slot uint16) (Member, bool)
func (c *Client) SeedingStatus(ctx context.Context, query SeedingQuery) (SeedingPage, error)
func (c *Client) SeedingRetry(ctx context.Context, database string, slot int) error
func (c *Client) SeedingForget(ctx context.Context, database string, slot int, anyway bool) error
func (c *Client) Join(ctx context.Context, id, addr string, witness bool) (MembershipResult, error)
func (c *Client) Promote(ctx context.Context, id string) (MembershipResult, error)
func (c *Client) ReplaceNode(ctx context.Context, id, addr string) (MembershipResult, error)
func (c *Client) Leave(ctx context.Context, id string) (MembershipResult, error)
func (c *Client) RemoveNode(ctx context.Context, id string) (MembershipResult, error)
```

`Topology` returns the members, the slot table and the databases the session may see (`wire.md`
section 11.6); pass the view you hold to learn whether it changed. The membership calls are the
operator's and need a node in cluster mode: a node that is not answers `INVALID_ARGUMENT`. A v1.1.0
server refuses `Leave` and `RemoveNode` as not available yet.

## 9. Not in this client yet

- **Routing by the ring.** The client keeps one connection to the node it dialled, which forwards.
  `Topology` and `Owner` give a caller the table; the client does not route with it.
- A connection pool, tokens and a connection URI. `Dial` takes an address, a user and a password.
- **User administration** (`auth.md` sections 6 and 7): the client has no `CreateUser` and its codec has
  no user commands. Use the `hoarddb` command line.
- A heap push (section 5).
- Dropping and altering a database, indexes, and the blob verbs listed under "not available yet" in
  `operations.md` are not available on the server.
