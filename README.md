# hoardDB Go driver

The Go client for hoardDB: six store types, store fields (every document can carry
its own queue, table or heap), typed reads, and errors that say what to run next.
Apache License 2.0.

> **Status: pre-release.** Every store type, store field, query, blob and batch is
> implemented and tested against real servers, one node and three. The API can still
> change between versions.

    go get github.com/hoarddb/hoarddb-go

Go 1.23 or later. The module requires the public `hoarddb-bson` codec and
`hoarddb-wire`; nothing else beyond the standard library.

## Quick start (five minutes)

**1. Start a server.** It needs no configuration:

    hoardDB-server

On first start it prints the root credentials once. At every start it logs the
fingerprint of its key, which is how you know, later, that you are talking to your
server:

    === ROOT CREDENTIALS (shown only on first start) ===
    export HOARDDB_ROOT_USER=admin
    export HOARDDB_ROOT_PASSWORD='IBMrPyxdoXFEjeK7yq0rxEufy2ZA4e5L'
    ...
    {"msg":"TCP driver listener listening","address":"127.0.0.1:7433","fingerprint":"SHA256:8c63...2796", ...}

**2. Trust it.** In another shell, paste the two `export` lines, then name the
server and its fingerprint (copy the whole value from the log line):

    export HOARDDB_URI='hoarddb://localhost:7433/app?fingerprint=SHA256:8c63...2796'

The password stays in the environment, out of the URI.

**3. Connect, create a bucket, put, get.** Make a module:

    mkdir hello && cd hello && go mod init hello

Save this as `main.go` in it:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

type User struct {
	Name string `bson:"Name"`
	Age  int64  `bson:"Age"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := hoarddb.Connect(ctx, "") // "" reads HOARDDB_URI
	if err != nil {
		log.Fatal(err) // the message says what to do next
	}
	defer client.Close()

	app, err := client.DefaultDatabase() // "app", from the URI
	if err != nil {
		log.Fatal(err)
	}
	users, err := app.CreateBucket(ctx, "users", hoarddb.Hash, hoarddb.WithIfNotExists())
	if err != nil {
		log.Fatal(err)
	}

	if _, err := users.Upsert(ctx, "ada", User{Name: "Ada", Age: 36}); err != nil {
		log.Fatal(err)
	}
	ada, err := hoarddb.GetAs[User](ctx, users, "ada") // Get, decoded into a struct
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s is %d\n", ada.Name, ada.Age)
}
```

Add the driver. This comes after `main.go` on purpose: `go mod tidy` keeps only
the modules the code imports, so run before `main.go` exists it would drop the
requirement it was given.

    go get github.com/hoarddb/hoarddb-go

Run it:

    go run .
    Ada is 36

`go test` compiles every Go block in this file (`readme_test.go`), so what you paste
is what builds. The `examples/` directory has a runnable program for each store type;
each reads `HOARDDB_URI` and prints what it did.

## The six store types

A bucket has one store type. The same verb has the same name, argument order and
result shape on every type that answers it. The full table is in the package
documentation (`go doc github.com/hoarddb/hoarddb-go`):

| Store type | For | Verbs | Example |
|---|---|---|---|
| `Hash` | lookups by key, secondary indexes | `Put`, `Insert`, `Upsert`, `UpsertMany`, `Get`, `Exists`, `Delete`, `Update`, `Search` | `examples/hash` |
| `BTree` | keys in order | the keyed verbs, `Upsert`, `UpsertMany`, and `Range`, `First`, `Last` | `examples/btree` |
| `FIFO` | a queue | `Push`, `Pop`, `Peek`, and the lease verbs below | `examples/fifo`, `examples/consume` |
| `LIFO` | a stack | `Push`, `Pop`, `Peek`, and the lease verbs below | `examples/lifo` |
| `Heap` | a priority queue | `Push` with `WithPriority`, `Pop`, `Peek`, and the lease verbs below | `examples/heap` |
| `Blob` | opaque bytes with metadata | `PutBlob`, `GetBlob`, `Delete` | `examples/blob` |

Every type except blob also answers `Find` and `Distinct`; every type answers `Count`,
`Stats` and `Info`. `Info` is what a bucket is declared as (its type, entry count and store
fields with their engines) in one cheap request that never counts the stores' elements;
`Stats` is the one that does. A miss on a one-value read is an error, never an empty document:
`ErrNotFound` for a key, a field element, or `First`/`Last` of an empty btree. A verb that returns a
list (`Pop`, `Peek`, `Claim`, on a bucket or a store field) answers an empty list and a nil error
when nothing is there: check `len`.
A verb the type does not answer is `ErrInvalidArgument` with the server's sentence
(it names the bucket, its type and the type the verb needs), and `Exists` on a
fifo, lifo or heap is that refusal, never `false`.

`Search` answers in one reply and cannot be paged: `Cursor.Total()` is the number
of documents it returned (at most your `Limit`), not the number that match, and
`Cursor.HasMore()` says whether a further match exists beyond the limit. The way on
is a larger `Limit`, or `Find` when you need the count.

```go
db := client.Database("app") // a handle: no I/O, no connection

jobs, err := db.CreateBucket(ctx, "jobs", hoarddb.Heap, hoarddb.WithIfNotExists())
if err != nil {
	log.Fatal(err)
}
if _, err := jobs.Push(ctx, hoarddb.Map{"Task": "backup"}, hoarddb.WithPriority(9)); err != nil {
	log.Fatal(err)
}
next, err := jobs.Pop(ctx) // the highest priority first, in a list of one; an empty list when drained
if err != nil {
	log.Fatal(err)
}
fmt.Println(next[0]) // relaxed Extended JSON, for logs
// hoarddb.WithLimit(n) takes up to n (1 to 100) in one request, in pop order.
```

### Open a bucket you did not create, and drop one

`Database.Bucket(name)` is a handle and asks the server nothing. `Database.Open(ctx, name)`
confirms the bucket exists and reads its store type, which `Type()` then returns; a missing
bucket or database is `ErrNotFound`. `Bucket.Drop` is `Database.DropBucket` on the handle.

```go
jobs, err := client.Database("app").Open(ctx, "jobs")
if errors.Is(err, hoarddb.ErrNotFound) {
	log.Fatal(err) // the message names the command that creates it
}
fmt.Println(jobs.Type()) // fifo
if err := jobs.Drop(ctx); err != nil {
	log.Fatal(err)
}
```

### Change how many copies a bucket keeps

On a cluster, `Database.AlterBucket` raises or lowers a bucket's replication factor. It needs the
`bucket_rf` capability. The answer comes when the bucket's owner has recorded the change; `Pending`
names the members that will copy the bucket by themselves, and `Releasing` the members whose copies
are deleted once the others hold everything they hold. A refusal an operator must act on (a factor
larger than the cluster) is `ErrInvalidArgument` with the server's sentence; one a retry clears is
`ErrUnavailable` with `OutcomeNotApplied`, which the driver retries with backoff until your deadline.
Runnable: `go run ./examples/alter`.

```go
result, err := client.Database("app").AlterBucket(ctx, "orders", hoarddb.AlterBucketOptions{ReplicationFactor: 3})
if err != nil {
	log.Fatal(err) // the message says what to run
}
fmt.Println(result.PreviousFactor, "->", result.ReplicationFactor, "copying to", result.Pending)
```

### Work a queue without losing a job

`Pop` removes an entry, so an entry whose worker dies is gone. `Claim` takes entries
under a lease and leaves them in the store, hidden from `Pop`, `Peek` and `Find`;
`Ack` removes the ones you name. An entry whose lease runs out is delivered again, and
one delivered 5 times without an ack moves to the dead list. `Consume` is the loop:

```go
jobs := client.Database("app").Bucket("jobs") // a fifo, lifo or heap bucket
err := jobs.Consume(ctx, func(ctx context.Context, job hoarddb.Delivery) error {
	fmt.Println(job.Document)
	return nil // nil acknowledges the job; an error leaves it for redelivery
}, hoarddb.WithLimit(10), hoarddb.WithLease(30*time.Second))
// the handlers of a batch run together; ctx ends them at the lease's deadline;
// job.Extend(ctx, time.Minute) asks for longer; Consume returns when ctx ends
dead, err := jobs.DeadList(ctx) // what was given up on; Requeue and Discard move them
if err != nil {
	log.Fatal(err)
}
fmt.Println(len(dead.Items), err)
```

A claim, an ack, a release, an extend, a requeue and a discard are writes and are never
resent: a lost answer costs a redelivery or one lease of delay, never an entry.
`ErrLeaseUnknown` and `ErrLeaseExpired` say an ack came too late; the result returned
beside the error says what the server answered. See `examples/consume`.

`Find` returns a cursor. **A cursor is not a snapshot**: a document written while you
page may or may not appear.

```go
people := client.Database("app").Bucket("users")
cursor, err := people.Find(ctx, hoarddb.Map{"Name": "Ada"}, hoarddb.SortBy("Age", hoarddb.Ascending), hoarddb.Limit(100))
if err != nil {
	log.Fatal(err)
}
for document, err := range cursor.Documents(ctx) {
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(document)
}
```

`FindOne` returns the first matching document, or an error matching `ErrNotFound` when
nothing matches (a document whose field holds null is found). It takes the same options as
`Find`; `Limit` and `PageSize` are overridden because it asks the server for one document.

```go
people := client.Database("app").Bucket("users")
ada, err := people.FindOne(ctx, hoarddb.Map{"Name": "Ada"})
if errors.Is(err, hoarddb.ErrNotFound) {
	fmt.Println("no such person")
} else if err != nil {
	log.Fatal(err)
} else {
	fmt.Println(ada)
}
```

## Store fields

A bucket can declare fields that are themselves stores, **one per document**: Ada's
`Purchases` is Ada's own FIFO, Bob's is Bob's. Declare them when you create the
bucket and address them by the document's key; the handle does no I/O.

```go
users, err := client.Database("app").CreateBucket(ctx, "User", hoarddb.Hash,
	hoarddb.WithIfNotExists(),
	hoarddb.WithStores(
		hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO},
		hoarddb.StoreSpec{Name: "Friends", Type: hoarddb.Hash}),
	hoarddb.WithFieldTypes(hoarddb.Strict,
		hoarddb.FieldSpec{Name: "Name", Type: "string", Required: true}))
if err != nil {
	log.Fatal(err)
}
if _, err := users.Upsert(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
	log.Fatal(err)
}
purchases := users.Field("ada", "Purchases") // Ada's own queue
if _, err := purchases.Push(ctx, hoarddb.Map{"Item": "tea"}); err != nil {
	log.Fatal(err)
}
count, err := purchases.Len(ctx)
if err != nil {
	log.Fatal(err)
}
fmt.Println("Ada has", count, "purchases")
```

`examples/fields` is this with two users, to show the queues are separate.

## In a web framework

`examples/gin` is a users API on Gin: `POST /users` binds and validates a struct
carrying `json`, `binding` and `bson` tags and stores it; `GET /users/:id` reads it
with `GetAs[User]` and answers 404 on `ErrNotFound`; `POST /users/:id/purchases`
pushes onto that user's own `Purchases` queue. Every driver call is given
`c.Request.Context()`, so a client that goes away cancels its query. It is its own
module (`examples/gin/go.mod`), so the driver itself does not depend on Gin. The
example has its own Go floor, Go 1.26 or later, higher than the driver's 1.23.

    cd examples/gin
    go run .
    curl -d '{"name":"Ada","email":"ada@example.com","age":36}' \
         -H 'Content-Type: application/json' localhost:8080/users

The handler shape is the same in any framework:

```go
user, err := hoarddb.GetAs[map[string]any](ctx, client.Database("app").Bucket("User"), "ada")
switch {
case errors.Is(err, hoarddb.ErrNotFound):
	// answer 404; err.Error() already says which bucket and key
case err != nil:
	// answer 500
default:
	fmt.Println(user["Name"])
}
```

`GetAs` is a function because Go methods take no type parameters. A document that
does not decode into your type is `ErrInvalidArgument`, naming the key and the field.

## Connecting

    hoarddb://[user[:password]@]host[:port][,host...][/database[/bucket]][?params]

- **User and password.** From the URI, else `WithCredentials`, else the environment:
  `HOARDDB_ROOT_USER` (default `admin`) and `HOARDDB_ROOT_PASSWORD`. Percent-encode
  `@ : / ? #` inside a user name or password (`p%40ss`: %40, %3A, %2F, %3F, %23);
  an unencoded one can change which host the credentials go to. The driver never prints a password or
  token, in an error or a log line.
- **Several hosts** are tried in the order written; the first that completes TLS and
  login wins. A host that fails is skipped for a while. This is a fallback for a
  node that is down, not load balancing.
- **Parameters:** `fingerprint`, `authToken`, `clientName`, `dialTimeoutMs`
  (default 10000, per host). Anything else is an error that names the accepted ones.
  Settings that weaken security, such as `insecure` and `tls`, are refused in a
  URI on purpose: a URI is copied between environments.
- `Connect(ctx, "")` reads the URI from `HOARDDB_URI`.

## Trust: knowing it is your server

The server's certificate is self-signed, so there is no certificate authority to ask.
Identity is the server's key, written as a fingerprint (`SHA256:` and 64 hex
characters). The driver checks it **before** it sends a password, with TLS 1.3 and
nothing weaker. In the order a developer meets them:

1. **Pin it** (`?fingerprint=` or `WithFingerprint`). A mismatch is a hard failure
   naming both fingerprints. This is the production path.
2. **A server you have trusted before.** The driver reads `~/.hoarddb/known_servers`,
   the file the hoardDB command line tool writes. A key that changed for a recorded
   server is a hard failure, never a prompt.
3. **A prompt you provide** (`WithTrustPrompt(func(host, fingerprint string) bool)`).
   A library has no terminal, so without one an unknown server is refused with
   the steps above.
4. **`hoarddb.AcceptFirstSight`**, a ready-made prompt that trusts and records the
   first key a host shows, as SSH does. Local development only.
5. **`WithInsecureSkipVerify(reason)`** turns the check off. The reason is required
   and is logged at `WARN` when the client connects (see issue #6 below). It cannot be set from a URI.

`examples/trust` shows the pinned path end to end.

## Insert, upsert and replace

`Put` inserts a document under a key you choose and refuses a key that already
holds one: `ErrDocumentExists`, with nothing written. That makes it safe to
retry after a lost answer, because it never overwrites. `Upsert` replaces the
document that is there or creates it when there is none, and `UpsertMany` does
the same for a batch whose documents each carry an `_id_` member. Resending an
upsert after a lost answer stores the same document again, which is safe only when
nothing else writes the key in between; the driver never resends it for you. `Update` changes some fields and leaves the rest. A replaced
document keeps its store fields.

```go
users := client.Database("app").Bucket("User")
_, err := users.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}) // insert; a second call returns ErrDocumentExists
if errors.Is(err, hoarddb.ErrDocumentExists) {
	// the key was held, and nothing was overwritten
}
_, err = users.Upsert(ctx, "ada", hoarddb.Map{"Name": "Ada", "Age": 36}) // replace or create
if err != nil {
	log.Fatal(err)
}
_, err = users.Update(ctx, "ada", hoarddb.Update{Set: hoarddb.Map{"Age": 37}}) // change some fields
if err != nil {
	log.Fatal(err)
}
```

## Errors, retries and the write outcome

Every call returns one type, `*hoarddb.Error`. Match its class with `errors.Is`, read
its fields with `errors.As`. The server's own sentence is passed through verbatim,
because the fix is in it; the driver's sentences say what happened, what it means,
and what to run.

```go
users := client.Database("app").Bucket("User")
_, err := users.Upsert(ctx, "ada", hoarddb.Map{"Name": "Ada"})
var failure *hoarddb.Error
switch {
case err == nil:
case errors.Is(err, hoarddb.ErrDuplicateKey): // a unique index refused it
case errors.Is(err, hoarddb.ErrFieldType):    // a typed bucket refused the document
case errors.Is(err, hoarddb.ErrBatchTooLarge): // halve the batch and resend it
case errors.Is(err, hoarddb.ErrPermissionDenied):
case errors.As(err, &failure) && failure.Outcome == hoarddb.OutcomeUnknown:
	// The connection was lost mid-write. The driver did not resend it.
	// Read the data back before sending again: a resent push applies twice.
default:
	log.Fatal(err)
}
```

A refused write says whether it happened, on a single node as on a cluster:
`Error.Outcome` is `OutcomeNotApplied` for a refusal decided before anything was
applied (a missing bucket, a permission, the document limit, a duplicate key), and
a server-stamped outcome is passed through untouched. Where no answer vouches for
a write, `Error.Outcome` is `OutcomeUnknown` and the message ends with the driver's
"unknown outcome" advice after the server's sentence: a lost connection, a refusal
with no request ID, and an ordinary error frame on a data write (a put, insert,
push, pop, delete, update or blob or store-field write) that carries no stamp,
which an older server or a single node whose own store failed the write sends.
Read the data back before resending. `OVERLOADED` as the server's refusal of a
connection (one error frame with an all-zero request ID, then a close) serves nothing,
so the driver backs off and sends the request again, a write included; a data write
that was sent and is still refused when your deadline ends is unknown; one refused at
the dial, before anything was sent, is not. The same code under the request's
own ID is not that refusal: a read is still resent, but a write, data or DDL, is sent
once and is unknown, unless the server stamped an outcome. A DDL request
(create or drop a database or bucket, declare store fields) whose connection is lost,
or whose context ends while it is in flight, is `OutcomeUnknown` too, with its own
advice (read the state back, then send it again); a DDL request the server refuses
with an error frame, a read, and a login report no outcome, and an in-band refusal
(`success: false` with a message) takes the outcome the server stamped.

`Client.Health` also reports the node's `Host` facts (CPUs, memory, load average),
for display only.

The driver retries only what is safe to send twice: a read whose connection failed,
once, on a fresh connection; a read the cluster cannot answer yet, or a request the
server says it did not apply (`OutcomeNotApplied`), with backoff until your deadline;
a connection refused for load. It **never** resends a write whose outcome is unknown
(`OutcomeUnknown`: a push, a pop, an update, an increment) and **never retries a
login** the server refused. Pass a context with a deadline to every call; without
one `WithRequestTimeout` (30 s) applies.

`errors.Is(err, context.DeadlineExceeded)` means your own deadline ended the call;
`ErrTimeout` is the server's.

### Write concern

On a cluster, a write is answered after `one`, `majority` or `all` copies hold it.
Set a default for the client or a level for one call; the receipt says how many
copies confirmed:

```go
users := client.Database("app").Bucket("User")
receipt, err := users.Upsert(ctx, "bob", hoarddb.Map{"Name": "Bob"}, hoarddb.WithWriteConcern(hoarddb.Majority))
if err != nil {
	log.Fatal(err)
}
fmt.Println(receipt.CopiesConfirmed, "of", receipt.ReplicationFactor) // 0 of 0 on a single node: not reported
```

A single node has no replicas, so every level behaves the same there.

## Concurrency, the pool and what a handle costs

A `*Client` is safe for use by many goroutines. It holds up to 4 connections in a
point lane (`WithPoolSize`) and 2 in a bulk lane (`WithBulkPoolSize`: blobs, `Find`
and `Range` pages, `Search`, `Distinct`, `InsertMany`), opened on demand, one request
at a time on each, so a long scan does not delay a point read. A call that finds its
lane busy waits for its context. The server allows 64 connections per source address
by default, shared by every process on the machine, so keep the pool small.
Cancelling a call closes the connection it was using, because the server cannot be
told to stop. `Close` waits for every goroutine the client started.

**A handle costs a few bytes.** `Client.Database` and `Database.Bucket` allocate a
small struct holding a name and a pointer; `Bucket.Field` likewise. They do no I/O
and hold no connection, goroutine, timer, cache entry or metric, and the driver adds
no per-bucket metric (`PoolStats` is aggregate). A program that holds a million
handles holds a million small structs, and whether a bucket exists is learned by the
first call that uses it. hoardDB is built for very many small buckets, one per
entity, and the driver keeps nothing per bucket to match.

`ListBuckets` answers a whole database in one frame, with no paging: it is an
**operator call**, for a person at a prompt or a setup script, not something to run
per request. On a cluster `ListBuckets` and `ListDatabases` answer for the node you
are connected to.

## Support

| Driver | Server | Wire | Capabilities the driver uses |
|---|---|---|---|
| this checkout (pre-1.0, unreleased) | the test server built from commit `e26d6bf2`, named in `testing/server.json`, and later | protocol 1 (ALPN `hoarddb/1`) | `auth_token` (tokens instead of passwords after login), `write_concern`, `consistent_get` (`Consistent()` reads), `bucket_rf` (`WithReplicationFactor`, `AlterBucket`) |

A capability the server does not advertise is never sent: the call returns
`ErrUnsupportedByServer` naming the capability and the server's version, and nothing
goes on the wire. The driver is lockstep (one request per connection) and does not use
`pipelining` or `get_fast`. Unknown capabilities are ignored. CI runs the whole suite
against the pinned binary on one node and on three.

## Not in v1

User administration, backup and restore, and cluster operations (the unstable
`hoarddbwire` package can send them; the public API does not); routing by the ring in
this package (a `hoarddb.Client` talks to any node; package `ring` is the client that
routes by the ring); caller-visible pipelining; an object mapper or a query
builder. A secondary index exists on hash buckets only, because that is what the
server offers; `Search` answers on every type but blob, from an index where one
serves the filter and by a full scan otherwise.

## Known issues

Open, and not fixed here:
[#5](https://github.com/hoarddb/hoarddb-go/issues/5) an unreadable `known_servers` is
reported as unreachable, not untrusted;
[#6](https://github.com/hoarddb/hoarddb-go/issues/6) the insecure reason is not yet
logged on every attempt;
[#7](https://github.com/hoarddb/hoarddb-go/issues/7) the same-host key-conflict message
can read oddly;
[#8](https://github.com/hoarddb/hoarddb-go/issues/8) a `known_servers` file owned by
another user is not refused;
[#10](https://github.com/hoarddb/hoarddb-go/issues/10) the dropped-user authentication
case is skipped in tests;
[#21](https://github.com/hoarddb/hoarddb-go/issues/21) the test helper can lose a port
race ("address already in use");
[#22](https://github.com/hoarddb/hoarddb-go/issues/22) the pool can hand out an idle
connection the server already closed, which shows as one failed call under load.

## Packages

| Package | Purpose | Stability |
|---|---|---|
| `github.com/hoarddb/hoarddb-go` | `Connect`, `Client`, buckets, options, errors | the API to use |
| `.../hoarddbwire` | frames, opcodes, error codes, `Conn`; `.../hoarddbwire/trust` | **unstable**, for tools that speak the wire |

## Dependencies and licences

The standard library, and one BSON codec (Apache-2.0) used only behind
`internal/codec`; none of its types appear in this package's API. See `NOTICE`. The
driver does not import anything from the server, which has a different licence;
`go test` fails if it ever does, and `scripts/compliance-check.sh`  checks
the licence of every package the module builds, that the dependency graph is the
standard library, this module and the codec, and that no other database product is
named in the tree. The Gin example is a separate module and adds nothing to this one.

This driver is Apache License 2.0; see `LICENSE`.

## Testing

    go vet ./... && go test -short ./...

`-short` skips the tests that need a running hoardDB server. To run them, build
`hoardDB-server` from the hoardDB server repository and point the tests at the binary
(a test-server download is not available yet):

    export HOARDDB_SERVER_BIN=/path/to/hoardDB-server
    export HOARDDB_CLI_BIN=/path/to/hoarddb
    go test -race ./...

The integration tests start `hoardDB-server` on loopback with a fresh data directory
and stop it afterwards. Without `HOARDDB_SERVER_BIN` they fail; `-short` skips them for a quick loop. `HOARDDB_CLI_BIN` is the `hoarddb`
command-line client: a test that needs a second user has it create one, because the driver
does not derive the Argon2id credential `USER_CREATE` carries. `go test ./conformance/...`
runs the frozen vectors (`../testdata/`, beside this module) against the driver's codec and a real server.
`TestLeg3Replay` starts one server per vector, about 4 seconds each, and takes about
16 minutes: run the conformance leg with `-timeout 45m`, because Go's default of 10
minutes ends it with a panic that looks like a driver failure.
