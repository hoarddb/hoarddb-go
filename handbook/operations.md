# Operations

What a driver can ask a hoardDB server, and what comes back. Language-neutral; binds every driver. The
bytes of each request and reply are in `wire.md` section 11, and the vectors win where they disagree.
This page says what each operation means.

## 1. The model

- A **database** is a namespace of buckets. A **bucket** declares fields. A field is a **scalar** (a
  string, an int, a datetime and so on) or a **store** of one of the six types: `fifoStore`,
  `lifoStore`, `hashStore`, `btreeStore`, `heapStore` or `blobStore`.
- A bucket holds **documents**, one per `_id_`. Every document carries its own copy of each store field:
  if bucket `User` declares `Purchases fifoStore`, every user has a queue of purchases of their own.
  The developer picks the store type of each field; the server never chooses it.
- A bucket has **no store type of its own**. `insert` and `upsert` set a document's scalar fields and
  nothing else. A store is written and read through its document and its field name.
- A request names a bucket by **id**, not by name. A driver learns the id, the field ids and the
  declaration versions from `describe_bucket` or `create_bucket` and sends them in the keyed header
  (`wire.md` section 11.3). When the declaration has changed, the server refuses with
  `SCHEMA_NOT_CURRENT`; the driver re-reads the declaration and sends the request again under the same
  request ID.
- There are no indexes yet. `RING_LIST` with a `filter` reads every document of the bucket.

## 2. Databases and buckets (`RING_ADMIN`)

One BSON command document `{cmd, ...}`; not a read. Per-command bytes: `wire.md` section 11.1 and the
vector rows `ring admin: ...`.

| `cmd` | What it does |
|---|---|
| `create_database` | `name`, `rf?` (replication factor, absent means 1), `storage?` (`disk`; `memory` is refused, the in-memory store is not built yet). A database that exists is `CONFLICT`. `rf` above 1 is `RF_UNSUPPORTED` while the cluster cannot hold that many copies |
| `create_bucket` | `database`, `name`, `fields[{name, type}]`. Repeating the same create answers with the existing bucket and creates nothing; the same name with other fields is `BUCKET_EXISTS`; a name beginning with `_` is `RESERVED_NAME`. The answer carries the bucket id and the field table the keyed header needs |
| `describe_bucket` | the declaration in force: bucket id, state, declaration version, field table |
| `show_buckets` | one page of a database's buckets (`limit?`, `cursor?`); `more` and `cursor` say whether and where to continue |
| `show_databases` | one page of the databases the session may see; there is no bucket count per database |
| `describe_database` | name, id, replication factor, storage, and one page of buckets |
| `alter_bucket` | adds or drops fields; a store's type never changes. To change a store's type, add a new field, copy, and drop the old one |
| `drop_bucket` | the bucket is `dropping` at once and its documents are removed in the background; `BUCKET_DROPPING` until the name is free again. It asks for no confirmation |
| `describe_node` | the node that answers: id, version, uptime, member state, cluster size, epoch, leader, default replication factor |
| `seeding_status`, `seeding_retry`, `seeding_forget` | the slots a node is copying, and the operator commands on them |
| `join`, `promote`, `replace_node` | cluster membership, on a node in cluster mode; they need the cluster administrator role |
| `create_user`, `alter_user`, `drop_user`, `grant_role`, `revoke_role`, `show_users`, `describe_user` | user administration (`auth.md` sections 6 and 7). The Go ring client does not send them yet; use the `hoarddb` command line |

- A **type word** is exact: the scalar words `string int32 int double decimal number bool datetime
  bsontimestamp binary uuid objectid object array regex null any`, or one of the six store words. A
  synonym is refused and the refusal names the word that works.
- A database name or a bucket name is 1 to 255 bytes of valid UTF-8, and is not `.` or `..` and holds no `/`, `\` or NUL byte; a longer one is refused before anything is written. A field name is 1 to 255 bytes of UTF-8 with no control character.
- A scalar's declared type is **not checked on write** yet: a field declared `int` accepts a string.
  The server does refuse a field name the bucket does not declare.
- **Not available yet:** `drop_database` and `alter_database`. A database's replication factor is set at
  create and cannot be changed afterwards. `leave` and `remove_node` are refused.

## 3. Documents (`RING_KEYED`)

| Verb | Class | What it does |
|---|---|---|
| insert | `0x01` | writes a new document's scalar fields. An `_id_` the bucket holds is `DOCUMENT_EXISTS`, and the held document is left as it was. The server mints an `_id_` when the caller sends none, and the reply carries it as `id` |
| upsert | `0x03` | replaces the scalar fields of the document under its `_id_`, or creates it. It keeps the document's stores. Safe to repeat |
| update | `0x02` | changes scalar fields of one existing document by operator, all or nothing |
| delete | `0x04` | removes the document and its stores |
| get | `0x81` | the document's scalar fields. A miss is `found: false` |
| get fields | `0x89` | the same with only the named scalar fields (1 to 64 names) |

- **An insert whose answer was lost** may have been stored. Resend it under the same request ID (section
  7), or send it as `upsert`; a new insert would answer `DOCUMENT_EXISTS` if the first landed.
- **update operators:** `set`, `unset` (a list of names), `inc`, `mul`, `min`, `max`, `push`, `addToSet`
  and `rename`. The modifiers of `push` and `addToSet` (such as `$each`) are refused by name. A field is named by at most one operator. An update of a
  missing document is `NOT_FOUND`.
- **Guard and return.** An `update` or `delete` may carry a guard, a list of field equalities (1 to 16
  terms, declared scalar fields only). When one differs, nothing is written; the reply is a success with
  `write_outcome: not_applied`, `reason: guard` and the current document. A return mode (`after` or
  `before`) asks for the document back in `document`; `after` on a delete is `INVALID_ARGUMENT`. Insert
  and upsert refuse both.
- **Expiry.** A bucket created with `expire at <Field>` hides a document whose datetime field is at or
  before now: a `get` of it is `NOT_FOUND` with `detail.expired: true` (`errors.md` section 5). Only
  `fifoStore` and `lifoStore` elements expire. The expiry instant is the caller's; the server never
  computes it.
- **Document limit:** a document over the server's limit is `INVALID_ARGUMENT` (`wire.md` section 10).

### Many documents

| Operation | Opcode | What it does |
|---|---|---|
| batch | `RING_BATCH` | 1 to 255 writes, to any buckets, in one request. The nine write classes only |
| get many | `RING_GET_MANY` | 1 to 255 `get` or `get fields` reads in one request; a miss is `found: false`; no snapshot, no atomicity |
| list | `RING_LIST` | one page of a bucket's documents, optionally filtered by scalar-field equality and projected with `fields`. It reads **every** document of the bucket: O(documents). Page with `cursor` while `more` is true (100 documents by default, at most 10,000, and at most 16 MiB). A cursor is not a snapshot |
| count | `RING_COUNT` | how many documents a bucket holds, or how many match a `filter` |

A batch is **not a transaction**: each write is applied or refused on its own, a refused write does not
stop the others, and writes to one document apply in the order sent. Retry is the whole frame under the
same request ID: the writes that were applied answer `deduped`, for as long as they are inside the
server's dedupe window (`replication.dedupe_window`). A driver refuses a 256th operation before sending
(`wire.md` section 11.8).

## 4. Stores

A store verb names the document and the field. Which verbs each store answers:

| Store | Verbs |
|---|---|
| `fifoStore`, `lifoStore` | `push`, `pop`, `peek`, `list`, `count`, `claim`, `ack`, `release`, `extend`, `lease_get` |
| `hashStore`, `btreeStore` | `put`, `get`, `take`, `inc`, `list`, `count` |
| `heapStore` | `push`, `pop`, `take` (by entry id), `peek`, `list`, `count`, `get` |
| `blobStore` | `count` |

The verbs that change a store are class `0x05` `STORE_VERB` with `verb` 1 push, 2 pop, 3 take, 4 put and
5 inc (`wire.md` section 11.3); the reads are classes `0x82` peek, `0x83` store list, `0x84` store count and
`0x88` store get. A verb a store does not answer is `INVALID_ARGUMENT`, and the message says what the
store takes. A document that does not exist, or an undeclared field, is `NOT_FOUND`.

**Not available yet** (the limitations page of the server's user documentation): a push to a heap from the
command line or from the Go ring client (its `Push` sends the fifo and lifo form, which a heap field
refuses), `put` on a blob store, and a chunked blob upload. There is no read that merges the newest
elements of many documents' stores; today it is one request per store.

- **peek** takes a count of 1 to 100 and answers the elements a run of pops would return, removing
  nothing. **list** pages a store in engine order, 1 to 1000 elements a page (default 100); `more` and
  `cursor` continue it. **count** answers `count`.
- **pop** removes the next elements and returns them in the answer, and the answer is the only copy. If
  the connection drops before the answer arrives, `peek` to see what is left, and do not pop again to
  look for them: a pop takes the next elements. For a `fifoStore` or `lifoStore`, use `claim` and `ack`.
- **take** removes one element of a hash or btree store by key and returns it; a key that is not there is
  `found: false`.
- **inc** adds to the numbers of the element under a key, creating it when absent, in one step at the
  owner. A retry with the same request ID is safe only while the server still remembers that ID; after
  that it adds again, so read the element first.
- **store get** reads one element of a hash, btree or heap store by key (1 to 1024 bytes). A fifo, lifo
  or blob field is `INVALID_ARGUMENT`.

### Leases

`claim` leases up to 100 messages (a count above 100 is clamped to 100; the Go client accepts 1 to 65535) of a `fifoStore` or `lifoStore` without
removing them. The reply carries `lease`, a 16-byte lease ID, and the `elements`. A claim of an empty
store answers an all-zero lease and no elements.

- `ack` removes the leased messages for good. `release` puts them back at once. `extend` moves the
  deadline. A lease lasts 30 seconds: a claim carries no lease length. `extend` names the new length,
  1 second to 1 hour from now.
- A lease that ends without an `ack` returns its messages to the queue. A message delivered 5 times
  without an `ack` is dead-lettered and leaves the queue; there is no verb yet to list, requeue or
  discard dead-lettered messages.
- **A lost claim reply:** send the claim again with the same request ID (the owner answers the lease ID
  with no elements and `deduped: true`), then `lease_get` on that lease. `found: false` means the lease
  ran out and the messages are back in the store: claim again with a new request ID.
- `lease_get` of an all-zero lease ID is refused.
- A lost `ack` costs a redelivery, never a loss.

### Sums and nearest

A store can keep sums and counts of its elements current (`sum`, `sum by`, `count by`, declared when the
bucket is created) and read them without paging (`aggregate`, class `0x8B`). `nearest` (class `0x8A`)
returns the elements of one store field closest to a query vector. Their forms are in the vector rows
`ring keyed: ...`; a driver that offers them sends the class and passes the refusal through.

## 5. The reply and the write outcome

A write's success reply says what happened in `write_outcome` (`wire.md` section 11.4):

| `write_outcome` | Means | A caller may |
|---|---|---|
| `applied` | the write is stored and the write concern was met | go on |
| `applied_degraded` | the write is stored, but fewer copies confirmed than asked (`copies_confirmed` says how many) | treat it as stored; it is not yet as durable as asked |
| `not_applied` | nothing was written: a guard did not hold | retry, or change the guard |
| `unknown` | the server cannot tell whether the write landed | read it back before resending anything that is not idempotent, or resend under the same request ID |

An `ERROR` frame carries no `write_outcome`. A refusal decided before anything was applied (permission,
an undeclared field, a missing bucket) wrote nothing. `UNAVAILABLE` from a forwarded write means the
outcome is unknown, and its message says so (`errors.md` section 4).

## 6. On a cluster

- A driver may connect to any node; a node that is not the document's owner forwards the request. A
  driver that routes holds the topology (`wire.md` section 11.6) and sends to the owner itself.
- The **owner node** of a bucket is the ring node that holds it. A request that reaches the wrong node
  because the driver's table is stale is answered `NOT_OWNER` or `STALE_EPOCH`, never wrongly (`wire.md`
  section 11.7).
- `copies_confirmed` counts the copies that confirmed the write. A database created with a replication
  factor above 1 needs a cluster that can hold it, and that is not yet a promise that every copy is
  written and readable (the limitations page of the server's user documentation).
- Dump and restore are the command line's (`hoarddb dump`, `hoarddb restore`); they are not driver verbs.

## 7. Request IDs, retry and dedupe

Every request carries a 16-byte request ID the client makes. A write sent again **under the same request
ID** is answered from the owner's dedupe window (`deduped: true`) instead of being applied twice. That
is how a driver retries a write whose answer it did not get: a timeout, or a connection that dropped. A
retry under a new request ID is a new write. The window holds the last `replication.dedupe_window`
writes (8192 by default).

The admin commands do not use the dedupe window: they are idempotent where the catalog allows it
(section 2).
