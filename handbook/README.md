# hoardDB driver handbook

Everything a hoardDB client driver is built from, in one place. It is written for a developer, or an
agent, who has a driver repository, this handbook and the frozen test vectors, and nothing else: no
server source, no internal documents, no network path to the people who wrote the server.

Licensed under the Apache License, Version 2.0, like the drivers it describes. Copyright 2026
Boeger IT Sh.p.k.

## Precedence

1. **The frozen test vectors win** (`testdata/driver-vectors/v1.json`, `testdata/uri-vectors/v1.json`).
   They were produced by driving a real hoardDB server with its own encoder. Where this handbook and a
   vector disagree, the vector is right and this handbook has a bug: report it (see "Changing this
   handbook" below).
2. **Then this handbook.** The language-neutral chapters bind every driver. A language chapter
   (`api-go.md`) binds only its driver.
3. **Nothing else.** Not another database's driver, documentation, option names or error text. hoardDB
   drivers are clean-room: see "Clean room" below.

A vector marked `"status": "provisional: <reason>"` is informative, never a failure; the reason names
what it waits for.

This holds for the vectors that are the driver contract. `driver-vectors/v1.json` also still holds rows
captured from the server's earlier single-store engine; `testing.md` section 1 says which, and they do
not decide anything here.

**Where the client libraries are.** They are pre-release, and their APIs can still change.

- The Go ring client is the package `ring` of the module `github.com/hoarddb/hoarddb-go` (`api-go.md`).
  It speaks the ring opcodes this handbook describes. Its codec is the module
  `github.com/hoarddb/hoarddb-wire`.
- The root package `hoarddb` of the same Go module, the Node.js driver (`hoarddb-node`) and the Python
  driver (`hoarddb-python`) were written for the server's earlier single-store engine. They send that
  engine's opcodes, which this handbook does not document (`wire.md` section 4). A server still answers
  them, from a store of its own that the ring opcodes and the `hoarddb` command line never see.

A driver never imports a server package.

## The chapters

| File | What it decides | Binds |
|---|---|---|
| [`wire.md`](wire.md) | transport, ports, ALPN, the 22-byte frame, request IDs, the opcode table (the ring opcodes, keyed classes, topology and routing), BSON payload rules, limits, timeouts, connection-level refusals | every driver |
| [`trust.md`](trust.md) | TLS 1.3 trust without a certificate authority: fingerprints, pinning, the shared `known_servers` file, the named insecure mode; the connection URI | every driver |
| [`auth.md`](auth.md) | the AUTH exchange, the eight refusal reasons and what to do on each, tokens and refresh, the failed-login limiter, roles | every driver |
| [`errors.md`](errors.md) | the error frame, the ring codes, the outcomes that are not errors, the write outcome, the retry table, which connection survives which error | every driver |
| [`operations.md`](operations.md) | the model (a bucket declares fields, a field can be a store of any of the six types per document), what each ring verb means, which verbs each store answers, what is not available yet, cluster behaviour, request ids and retry | every driver |
| [`testing.md`](testing.md) | the vector format, the conformance legs, how to get a real server (one node and three) to test against, CI | every driver |
| [`api-go.md`](api-go.md) | the Go ring client's public API (package `ring`) | the Go driver |

## Words

hoardDB is the product's name in prose. **In code everything is lowercase**: the URI
scheme is `hoarddb://`, the ALPN string is `hoarddb/1`.

- **Database**: a namespace of buckets.
- **Bucket**: the unit of storage. A bucket declares **fields** and holds **documents**; it has no store
  type of its own.
- **Store field**: a field a bucket declares whose value, in **each document**, is itself a store of one
  of the six types: `fifoStore`, `lifoStore`, `hashStore`, `btreeStore`, `heapStore` or `blobStore`.
  Every user document carries its own queue, its own table, its own heap. The developer picks the
  type of each field; the server never chooses it.
- **Node**: one server process. A **cluster** is several nodes; each bucket has an **owner node** (the
  ring node that holds it) and replicas.
- **Entity**: the thing a document describes (a user, an order, a device). Data lives with the entity
  it describes.

hoardDB is built for many small buckets, thousands per node. A driver must therefore hold **no
per-bucket state of its own**: a bucket handle costs a few bytes, does no I/O when created and holds no
connection, goroutine, timer or metric. (The bucket's id and declaration, which a keyed request needs,
are fetched on first use and may be cached per bucket.)

## Clean room

Drivers are written from this handbook, the vectors and the server's observable behaviour only. A
driver author must not read, copy, port, translate or closely paraphrase the source, documentation,
comments, examples, error messages or test suites of any other database's drivers or connectors, and
must not name another database product in code, identifiers, error messages, documentation or examples.
BSON is a data format; using it is not a claim of compatibility with anything. A published library may
be used through its public API as a black box (the BSON codec is one); its source is not read.

Every contribution to a driver carries this attestation:

    Clean-room attestation. I implemented this from the hoardDB driver handbook, the frozen test
    vectors in testdata/, and the behaviour of a hoardDB server I ran. I did not read, copy or
    paraphrase the source, docs, comments, examples, error messages or tests of any other
    database's driver or connector. Libraries consulted for their public documentation only:
    <list, with licences>.

## Changing this handbook

The handbook and `testdata/` are maintained beside the server and **copied into the Go driver's
repository**, where they are read-only: a driver's build never edits them. The Node.js and Python
repositories carry an earlier handbook and earlier vectors, which describe the earlier engine's opcodes
those drivers send. If something here is wrong, missing, or
contradicted by a server you ran, stop and say so in the pull request (what you read, what you
observed, and the test or command that shows it); the maintainers fix it upstream and re-sync. A
driver that "fixes" a vector to make its own test pass has hidden a wire change from every other
driver.

This handbook describes the hoardDB server of release v1.1.0. The vectors record the server commit they
were generated from (`server_commit` in `testdata/driver-vectors/v1.json`).
