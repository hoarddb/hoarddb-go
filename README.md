# hoardDB Go client

The Go module for hoardDB. Apache License 2.0.

> **Status: pre-release (v0.1.0).** The API can still change between versions.

    go get github.com/hoarddb/hoarddb-go

Go 1.23 or later. The module requires the `hoarddb-bson` codec and `hoarddb-wire`; nothing
else beyond the standard library.

## Two packages, and which one to use

A hoardDB v1.1 server is a ring server. A bucket declares fields, and a field can be a
store (a queue, a table, a heap) that every document carries for itself. The `hoarddb`
command line, `hoarddb dump` and `hoarddb restore` work on that data. This module holds
two clients, and only one of them reaches it.

| Package | Speaks | Reaches |
|---|---|---|
| `github.com/hoarddb/hoarddb-go/ring` | the ring protocol | the data the `hoarddb` command line shows |
| `github.com/hoarddb/hoarddb-go` (`hoarddb.Connect`) | the opcodes of the server's earlier single-store engine | a separate store on the server, described below |

**Use package `ring` for new work.** It is the first quick start below.

The root package `hoarddb` was written before the ring. Against a v1.1.0 server:

- **It works.** It connects, logs in with the same users, and reads and writes: the server
  still answers its opcodes. The second quick start below was run against a v1.1.0 server.
- **Its data is separate.** Its databases and buckets live in the earlier engine's own store.
  Nothing it creates is visible to the `hoarddb` command line or to package `ring`, nothing
  created there is visible to it, and `hoarddb dump` does not include it. A database named
  `app` can exist on both sides and they are two different databases.
- **Its data is on one node.** A v1.1.0 server refuses to start in the earlier engine's
  replicated mode, so on a cluster each node answers these opcodes from its own local store:
  a write through the root package stays on the node that took it and is not copied to the
  others. `AlterBucket` to a replication factor above 1 is refused, and a write-concern
  receipt reports 0 of 0 copies.
- The `examples/` directory, the `conformance` package and the integration tests use the
  root package.

## Quick start: package `ring`

**1. Start a server.** Build `hoarddb-server` from
[`github.com/hoarddb/hoarddb-server`](https://github.com/hoarddb/hoarddb-server) (no release
binary is published yet) and start it with a root password of your choosing:

    HOARDDB_ROOT_PASSWORD='choose-a-long-password' hoarddb-server

Started from a terminal with no password set, it asks for one instead. The root user is
`admin`. At every start the server logs the fingerprint of its key, which is how you know,
later, that you are talking to your server:

    {"msg":"TCP driver listener listening","address":"127.0.0.1:7433","fingerprint":"SHA256:8c63...2796", ...}

**2. Trust it.** In another shell, set the password and the fingerprint (copy the whole
value from the log line):

    export HOARDDB_ROOT_PASSWORD='choose-a-long-password'
    export HOARDDB_FINGERPRINT='SHA256:8c63...2796'

**3. Connect, create a bucket, write and read.** Make a module:

    mkdir hello && cd hello && go mod init hello

Save this as `main.go` in it:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-go/ring"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := ring.Dial(ctx, ring.Options{
		Addr:        "localhost:7433",
		User:        "admin",
		Password:    os.Getenv("HOARDDB_ROOT_PASSWORD"),
		Fingerprint: os.Getenv("HOARDDB_FINGERPRINT"),
	})
	if err != nil {
		log.Fatal(err) // the message says what to do next
	}
	defer client.Close()

	if _, err := client.CreateDatabase(ctx, "app", 0); err != nil {
		log.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, "app", "User", []ring.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Purchases", Type: "fifoStore"},
	}); err != nil {
		log.Fatal(err)
	}

	ada := client.Bucket("app", "User").Entity("ada") // a handle: nothing is sent yet
	if _, err := ada.Insert(ctx, ring.Doc{"Name": "Ada"}); err != nil {
		log.Fatal(err)
	}
	purchases := ada.Store("Purchases") // Ada's own queue, not a shared one
	if _, err := purchases.Push(ctx, bson.D{{Key: "item", Value: "coffee"}}); err != nil {
		log.Fatal(err)
	}
	profile, err := ada.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	waiting, err := purchases.Count(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(profile.Lookup("Name").StringValue(), "has", waiting, "purchase")
}
```

Add the driver. This comes after `main.go` on purpose: `go mod tidy` keeps only
the modules the code imports, so run before `main.go` exists it would drop the
requirement it was given.

    go get github.com/hoarddb/hoarddb-go

Run it:

    go run .
    Ada has 1 purchase

A second run stops at `CreateDatabase`, because the database exists: the error is
`CONFLICT` and its message names the database. `show databases` in the `hoarddb` command
line now lists `app`, and `show buckets in app` lists `User`.

`go test` compiles every Go block in this file (`readme_test.go`), so what you paste
is what builds.

### What package `ring` has

- Databases and buckets: `CreateDatabase`, `ShowDatabases`, `DescribeDatabase`,
  `CreateBucket`, `AlterBucket`, `DropBucket`, `DescribeBucket`, `ShowBuckets`.
- Documents: `Insert`, `Upsert`, `Update`, `Delete`, `Get`, `GetFields`, `List`, `Count`, and
  `GetMany` for up to 255 reads in one request.
- A document's stores: `Push`, `Pop`, `PopN`, `Peek`, `Claim`, `Ack`, `Release`, `Extend` on a
  `fifoStore` or `lifoStore` field; `Put`, `Take`, `Inc`, `StoreGet` on a `hashStore` or
  `btreeStore` field; `StoreCount` on any, and `StoreList` on any but a `blobStore`, which the
  server refuses.
- `NewBatch` sends up to 255 writes in one request. It is not a transaction.
- `WithRequestID` makes a retried write safe: the server answers a request ID it has already
  applied from its dedupe window instead of applying the write twice.

What it does not have yet: a connection URI, a connection pool, tokens, user
administration (use the `hoarddb` command line), a push to a `heapStore` field, and
routing by the ring (it talks to the node it dialled, which forwards). The package
documentation (`go doc github.com/hoarddb/hoarddb-go/ring`) and `handbook/api-go.md` have
the whole API; `handbook/` describes the protocol.

## Quick start: the root package `hoarddb`

This is the earlier-engine client described above. Everything from here to "Packages"
is about it. With the server of step 1 running, name the server and its fingerprint in a
URI; the user defaults to `admin` and the password is read from `HOARDDB_ROOT_PASSWORD`:

    export HOARDDB_URI='hoarddb://localhost:7433/app?fingerprint=SHA256:8c63...2796'

The password stays in the environment, out of the URI. Save this as `main.go` in a module
as above, `go get github.com/hoarddb/hoarddb-go`, and `go run .`:

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

    go run .
    Ada is 36

The `examples/` directory has a runnable program for each store type of this package;
each reads `HOARDDB_URI` and prints what it did.

## The root package: six store types

In the root package a bucket has one store type. The same verb has the same name, argument order and
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

### Copies of a bucket

`Database.AlterBucket` asks for a bucket's replication factor to be raised or lowered. A v1.1.0
server refuses a factor above 1 for this package's buckets with `ErrInvalidArgument`: the earlier
engine runs on one node only (see "Two packages" above). Copies are a property of a ring
database, set when it is created with package `ring` or the `hoarddb` command line.

```go
result, err := client.Database("app").AlterBucket(ctx, "orders", hoarddb.AlterBucketOptions{ReplicationFactor: 3})
if err != nil {
	log.Fatal(err) // on v1.1.0: the server's sentence says it has no second node for this bucket
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
  login wins. A host that fails is skipped for a while. **Against a v1.1.0 server, name one
  host:** each node keeps its own store for this package's data, so a second host is a
  different, separate set of buckets and not a fallback.
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
   and is logged at `WARN` when the client connects (see "Known issues"). It cannot be set from a URI.

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

A refused write says whether it happened:
`Error.Outcome` is `OutcomeNotApplied` for a refusal decided before anything was
applied (a missing bucket, a permission, the document limit, a duplicate key), and
a server-stamped outcome is passed through untouched. Where no answer vouches for
a write, `Error.Outcome` is `OutcomeUnknown` and the message ends with the driver's
"unknown outcome" advice after the server's sentence: a lost connection, a refusal
with no request ID, and an ordinary error frame on a data write (a put, insert,
push, pop, delete, update or blob or store-field write) that carries no stamp,
which a node whose own store failed the write sends.
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
once, on a fresh connection; a read the server cannot answer yet, or a request the
server says it did not apply (`OutcomeNotApplied`), with backoff until your deadline;
a connection refused for load. It **never** resends a write whose outcome is unknown
(`OutcomeUnknown`: a push, a pop, an update, an increment) and **never retries a
login** the server refused. Pass a context with a deadline to every call; without
one `WithRequestTimeout` (30 s) applies.

`errors.Is(err, context.DeadlineExceeded)` means your own deadline ended the call;
`ErrTimeout` is the server's.

### Write concern

`WithWriteConcern` (`One`, `Majority`, `All`) asks for a write to be answered after that many
copies hold it. Against a v1.1.0 server this package's buckets have one copy, so every level
behaves the same and the receipt reports no copies:

```go
users := client.Database("app").Bucket("User")
receipt, err := users.Upsert(ctx, "bob", hoarddb.Map{"Name": "Bob"}, hoarddb.WithWriteConcern(hoarddb.Majority))
if err != nil {
	log.Fatal(err)
}
fmt.Println(receipt.CopiesConfirmed, "of", receipt.ReplicationFactor) // 0 of 0: not reported
```

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
first call that uses it. hoardDB is built for many small buckets, thousands per node,
and the driver keeps nothing per bucket to match.

`ListBuckets` answers a whole database in one frame, with no paging: it is an
**operator call**, for a person at a prompt or a setup script, not something to run
per request. `ListBuckets` and `ListDatabases` answer for the node you are connected
to, and list this package's databases only, never the ring's.

## Support

| Client | Server | Wire | What was run |
|---|---|---|---|
| package `ring`, v0.1.0 | hoardDB v1.1.0 | protocol 1 (ALPN `hoarddb/1`), the ring opcodes | the first quick start of this file, against a v1.1.0 server |
| root package `hoarddb`, v0.1.0 | hoardDB v1.1.0, the earlier engine's store only | protocol 1, the earlier engine's opcodes | the second quick start of this file and the thirteen programs under `examples/` other than `gin`, against a v1.1.0 server. The integration and conformance suites were last run against the server commit named in `testing/server.json`, which is older than v1.1.0, and have not been re-run against v1.1.0 |

The root package uses these server capabilities: `auth_token` (tokens instead of passwords
after login), `write_concern`, `consistent_get` (`Consistent()` reads) and `bucket_rf`
(`WithReplicationFactor`, `AlterBucket`). A capability the server does not advertise is never
sent: the call returns `ErrUnsupportedByServer` naming the capability and the server's version,
and nothing goes on the wire. The root package is lockstep (one request per connection) and
does not use `pipelining` or `get_fast`. Unknown capabilities are ignored.

## Not in v1.1

What the root package does not do against a v1.1 server: reach the ring's data (package
`ring` does); user administration, backup and restore, and cluster operations (the unstable
`hoarddbwire` package can send the earlier engine's opcodes for them; the public API does
not); caller-visible pipelining; an object mapper or a query builder. A secondary index
exists on hash buckets only, because that is what the earlier engine offers; `Search` answers
on every type but blob, from an index where one serves the filter and by a full scan otherwise.

## Known issues

Open, and not fixed here, all in the root package:

- an unreadable `known_servers` is reported as unreachable, not untrusted;
- the insecure reason is not yet logged on every attempt;
- the same-host key-conflict message can read oddly;
- a `known_servers` file owned by another user is not refused;
- the dropped-user authentication case is skipped in tests;
- the test helper can lose a port race ("address already in use");
- the pool can hand out an idle connection the server already closed, which shows as one
  failed call under load.

## Packages

| Package | Purpose | Stability |
|---|---|---|
| `github.com/hoarddb/hoarddb-go/ring` | the ring client: `Dial`, `Client`, `Bucket`, `Entity`, `Store`, `Batch` | pre-release; the one to use for new work |
| `github.com/hoarddb/hoarddb-go` | the earlier-engine client: `Connect`, `Client`, buckets, options, errors | pre-release |
| `.../hoarddbwire` | frames, opcodes, error codes, `Conn`; `.../hoarddbwire/trust` | **unstable**, for tools that speak the wire |

## Dependencies and licences

The standard library, the BSON codec `github.com/hoarddb/hoarddb-bson` and the ring codec
`github.com/hoarddb/hoarddb-wire`, both Apache-2.0. The root package uses the BSON codec only
behind `internal/codec`, and none of its types appear in that package's API; package `ring`
returns documents as the codec's `bson.Raw`. See `NOTICE`. The module imports nothing from the
server, which has a different licence; `go test` fails if it ever does, and
`scripts/compliance-check.sh` checks the licence of every package the module builds and that
no other database product is named in the tree. The Gin example is a separate module and
adds nothing to this one.

This driver is Apache License 2.0; see `LICENSE`.

## Testing

    go vet ./... && go test -short ./...

`-short` skips the tests that need a running hoardDB server. To run them, build
`hoarddb-server` and the `hoarddb` command line from the hoardDB server repository and point
the tests at the binaries (a test-server download is not available yet):

    export HOARDDB_SERVER_BIN=/path/to/hoarddb-server
    export HOARDDB_CLI_BIN=/path/to/hoarddb
    go test -race ./...

The integration tests start the server on loopback with a fresh data directory and stop it
afterwards. Without `HOARDDB_SERVER_BIN` they fail. `HOARDDB_CLI_BIN` is the `hoarddb`
command-line client: a test that needs a second user has it create one. They were written
against the server commit named in `testing/server.json` and drive the root package; they
have not been re-run against v1.1.0, and the three-node tests and `testing/compose/` start
the earlier engine's replicated mode, which a v1.1.0 server refuses at start.
`go test ./conformance/...` runs the frozen vectors against the driver's codec and a real
server. `TestLeg3Replay` starts one server per vector: run the conformance leg with
`-timeout 45m`, because Go's default of 10 minutes ends it with a panic that looks like a
driver failure.
