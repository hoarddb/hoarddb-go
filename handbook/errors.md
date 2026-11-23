# Errors, the write outcome, and what to retry

How a hoardDB server says no, and what a driver does about it. Language-neutral; binds every driver.
The single most important rule: **pass the server's message through verbatim.** The server writes its
refusals as sentences that name the fix ("... Run 'show buckets in app' to list what exists ..."). A
driver that replaces one with "server error" has destroyed the fix. **A driver acts on `code` and
`detail`, never on the text.**

## 1. The error frame

An error is an `ERROR` frame (`0xA2`) in place of the success opcode, echoing the request ID, with this
BSON payload (`wire.md` section 11.5):

| Field | Type | Present |
|---|---|---|
| `code` | integer | always; decode any integer width |
| `message` | string | always |
| `detail` | document | only for the codes that define one (section 2) |

Tolerance rules, because an older server and a future bug look the same to a driver:

- A `code` of `0`, or no `code`, on an `ERROR` frame is `INTERNAL`. **Never success.**
- An unknown code `n` is kept as `UNKNOWN(n)` with the message, and treated like `INTERNAL`.
- An `ERROR` frame may be followed at once by the server closing the connection. A driver reuses a
  connection after an `ERROR` frame only for a refusal it knows leaves it open (section 6); otherwise it
  discards the connection, or proves it with a `PING`.

A refusal is never a `RING_REPLY`: the opcode tells a success from a refusal.

The connection-level refusals of `wire.md` section 8 (`OVERLOADED`, `UNSUPPORTED_VERSION`) and
`NOT_AUTHENTICATED` use the same `{code, message}` shape, with the codes `0x0D`, `0x0A` and `0x0C`, which
are not in the ring's table. The Go client reads only the ring's table: it reports such a frame as a plain
error with no code and no server message.

## 2. The codes

The codes the ring sends are the table in `wire.md` section 11.5, with the exit status each gives and
whether to retry. The ones a developer meets first:

| Code | Name | The server sends it for |
|---|---|---|
| `0x01` | `NOT_FOUND` | a missing bucket or database; a document a store verb or `peek` needs and does not find; a `get` of an expired document (with `detail.expired: true`, section 5); an undeclared field; a missing user on an alter, drop, grant or revoke |
| `0x03` | `PERMISSION_DENIED` | the user's grants do not cover the request (`auth.md` section 5). It never says whether the bucket exists |
| `0x04` | `INVALID_ARGUMENT` | a malformed request; a verb the store does not answer (the message names what the store takes); a document over its limit; an update operator refused; an unknown type word; a frame over 32 MiB; a node-to-node opcode on the driver port |
| `0x06` | `UNAVAILABLE` | the cluster cannot answer here and now and nothing about the request is wrong; a forwarded write whose owner could not be reached (**the outcome is unknown**) |
| `0x0E` | `RESULT_TOO_LARGE` | a result over the node's ceiling, or any response over 32 MiB |
| `0x10` | `FIELD_TYPE` | a name the bucket does not declare, a guard on a store field, or a field the request cannot use that way; nothing was stored |
| `0x12` | `DOCUMENT_EXISTS` | an insert of an `_id_` the bucket already holds; the held document is left as it was |
| `0x13` to `0x2B` | the ring's own | `NOT_OWNER`, `STALE_EPOCH`, `OWNER_DOWN`, `NO_QUORUM`, `LEASE_LAPSED`, `SCHEMA_NOT_CURRENT`, `BUCKET_DROPPING`, `BUCKET_EXISTS`, `RESERVED_NAME`, `USER_EXISTS` and the rest, in `wire.md` section 11.5 |

`detail` has one shape per code: `NOT_OWNER` `{owner, owner_addr, epoch, slot}`, `STALE_EPOCH` `{epoch,
owner, owner_addr}`, `OWNER_DOWN` `{owner, missing_slots}`, `NO_QUORUM` `{voters, answered}`,
`LEASE_LAPSED` `{node, lapsed_ms}`, `RF_UNSUPPORTED` `{rf, voters, data_members}`, and `NOT_FOUND`
`{expired}`. A driver that gets a `detail` shape it does not know for a code refuses the frame.

`DOCUMENT_EXISTS` fix: `upsert` replaces, `update` changes fields. **An insert whose answer was lost may
have been stored**: resend it under the same request ID, or as `upsert`, never as a new `insert`.

## 3. Outcomes that are not errors

A refusal reaches a driver as an `ERROR` frame. These are successful answers a driver must not mistake
for refusals:

- **`found: false`** on `get`, `get fields`, `store_get`, `take`, `pop` and `lease_get`: the server
  looked and the document, key or lease is not there. A miss is an answer, never an empty frame. (A
  document that does not exist is `NOT_FOUND` for a `peek` or a field verb.)
- **`write_outcome: not_applied`** with `reason: guard`: an `update` or `delete` whose guard did not hold
  changed nothing. The reply carries `guard_field` and the current `document`.
- **`deduped: true`**: the answer came from the dedupe window; a retry did not apply the write again.
- **`create_bucket` of an identical bucket** answers the existing bucket and creates nothing.

## 4. The write outcome

A success reply to a write carries `write_outcome` (`operations.md` section 5): `applied`,
`applied_degraded` (stored, but fewer copies confirmed than asked), `not_applied` (a guard held it back)
or `unknown`. An `ERROR` frame carries none. So:

| The answer | The write | A caller may |
|---|---|---|
| a success with `applied` | stored | go on |
| a success with `applied_degraded` | stored on `copies_confirmed` copies | treat it as stored |
| a success with `not_applied` | not written | retry, or change the guard |
| a success with `unknown` | probably durable; the reply carries its `position` | read it back, or resend under the same request ID |
| an `ERROR` that is a refusal of the request (`PERMISSION_DENIED`, `INVALID_ARGUMENT`, `FIELD_TYPE`, `NOT_FOUND`, `DOCUMENT_EXISTS`) | not written | fix the request |
| `UNAVAILABLE` from a forwarded write | **unknown**; the message says so and that a retry with the same request ID is safe | resend under the same request ID |
| no answer at all (a timeout, a dropped connection) | **unknown** | resend under the same request ID |

**A pop's answer is the only copy of what it removed.** If it is lost, `peek` to see what is left and do
not pop again to look for the elements. For a `fifoStore` or `lifoStore`, `claim` and `ack` turn a lost
answer into a redelivery instead of a loss (`operations.md` section 4).

## 5. `NOT_FOUND` of an expired document

A bucket that declares `expire at <Field>` hides a document whose field is at or before now. A `get` of
one is an `ERROR` with code `NOT_FOUND` whose `detail` is `{expired: true}`; a plain miss has no detail.
A driver acts on `detail.expired`, never on the text, and still treats it as a not-found. It refuses
`{expired: false}`. `NOT_FOUND` is never retried. The vector is `ring keyed: get of an expired document`.

In Go, `ring.IsExpired(err)` reads the flag and `ring.ExpiresIn(d)` returns now plus `d` as a
time, from the client's clock, for filling an expire field.

## 6. The retry table

| Answer | Retry? |
|---|---|
| `NOT_OWNER` | once, to the owner `detail` names, under the **same request ID**; a second redirect is surfaced, never chased |
| `STALE_EPOCH` | one `RING_TOPOLOGY` refresh, then one re-send under the same request ID |
| `SCHEMA_NOT_CURRENT` | re-read the bucket's declaration, then send once more under the same request ID |
| `UNAVAILABLE` on a read | yes, with backoff, bounded by the caller's deadline |
| `UNAVAILABLE` on a write | yes, with backoff, **under the same request ID**; its message says the outcome is unknown |
| `NO_QUORUM`, `LEASE_LAPSED`, `DECL_UNAVAILABLE`, `BUCKET_ID_COLLISION`, `MINT_FAILED` | yes, later, bounded by the deadline |
| `OWNER_DOWN` | not silently: a driver never answers from a copy; it reports the code and the message, which says to retry in a few seconds or replace the node |
| `BUCKET_DROPPING` | wait: the name is free when the sweep finishes |
| `STATE_MOVED` | a DDL statement built on an old state: the caller re-runs it; a driver never retries DDL |
| `OVERLOADED` | yes, that one dial, with backoff; the pool is fine. It is the all-zero-request-ID refusal of `wire.md` section 8 |
| `NOT_AUTHENTICATED` | re-authenticate that connection once; then resend the request once if it is a read |
| `RESULT_TOO_LARGE` | not as sent: ask for a smaller page, project fewer fields, or narrow the filter |
| `NOT_FOUND`, `PERMISSION_DENIED`, `INVALID_ARGUMENT`, `FIELD_TYPE`, `DOCUMENT_EXISTS`, `BUCKET_EXISTS`, `RESERVED_NAME`, `USER_EXISTS`, `RF_UNSUPPORTED`, `CLUSTER_MISMATCH`, `NOT_A_DATA_NODE` | **never**: the same request fails the same way |
| `CONFLICT`, `LOG_BELOW_HORIZON`, `SEED_HELD`, `REPLACE_TARGET_HAS_DATA` | not automatically: an operator acts (the message names the command). A `CONFLICT` of a user change is a fence: re-read and repeat it (`auth.md` section 7) |
| `INTERNAL`, `UNKNOWN(n)` | never automatically |
| the connection failed before a complete response arrived | a read: once, on a fresh connection. A write: resend under the same request ID; without one, the outcome is unknown and the error says so. A `pop` without a request ID: never |
| an `AUTH_FAIL` | see `auth.md` sections 1, 4 and 8; never in a loop. `TOKEN_INVALID` is a revocation or an unusable token and is the password fallback of `auth.md` section 3 |

A retry is invisible to the caller except in the driver's aggregate statistics. A driver never retries a
write it cannot prove `not_applied` unless it resends under the same request ID.

**Connections that survive an `ERROR` frame:** every refusal a ring handler sends (`RESULT_TOO_LARGE`,
`FIELD_TYPE`, `DOCUMENT_EXISTS` and the rest of the table of `wire.md` section 11.5), a response replaced
by `RESULT_TOO_LARGE` for being over 32 MiB, and an oversize request on an authenticated connection
(`wire.md` section 2). The server closes after a node-to-node opcode on the driver port, an AUTH the
failed-login limiter refuses (`auth.md` section 4), a bad frame header, a request over 64 KiB before AUTH, a dropped or disabled user, and an
`INTERNAL` it cannot answer. A driver that is unsure discards the connection; that is always safe, and
costs one dial.

## 7. Errors the driver makes itself

Some failures never reach a server. Each is a sentence with three parts: what happened, what it means,
what to do. Codes for these are the driver's own (outside the wire range), and each is matchable:

| Situation | The sentence says |
|---|---|
| Server key unknown, nothing pinned | the host, the fingerprint it presented, and the ways to trust it (pin it, or accept it on first sight for local development) |
| Pinned or recorded key changed | both fingerprints; if the keys were rotated, which line of `known_servers` to delete; if not, that the connection may be intercepted; that the driver will not retry |
| Credentials refused | the user; that the driver does not retry; where the root password comes from (`HOARDDB_ROOT_PASSWORD`, or the one the server asked for on its first start) |
| `RATE_LIMITED` | that it is not a credential failure; to wait; that the driver stopped retrying |
| Unreachable | the address, the network error, and (for `localhost`) that the server listens on loopback by default and the container image on every interface |
| A request over 32 MiB | the arithmetic (`1 + 16 + payload` against 33,554,432), and the remedy (fewer writes in a batch, or a smaller value) |
| A batch of more than 255 operations | `at most 255 ... Split it into batches of 255 or fewer` |
| The caller's deadline passed | the operation and target; for a write, that the outcome is unknown and that a resend must use the same request ID |
| A bad URI | the part that is wrong and the expected shape (`trust.md` section 6) |

No message or log line contains a password or a token.

## 8. `RING_BATCH` and `RING_GET_MANY` answers

Both answer one `RING_REPLY` `{outcome, answers[], epoch, state_seq}` (`wire.md` section 11.8). `outcome`
is `ok` when every operation was answered with a reply and `partial` when any was answered with an error.
Each answer is `{reply: bin}` (the bytes of a lone `RING_REPLY`) or `{error: bin}` (the bytes of an
`ERROR` payload), in request order, and a driver decodes each with section 1 and `wire.md` section 11.4.

- A batch frame that is malformed, or one whose count is 0, is one `ERROR` and **nothing was done**.
- A refused operation does not stop the others. Authorization and forwarding are per operation.
- An operation that was **not attempted** (the frame's deadline ran out, or a reply budget had no room
  for a pop) is `UNAVAILABLE` saying `not attempted` and `not applied`: it is never `unknown`. **Resend
  the whole frame under the same request ID**: the applied operations answer `deduped: true` and the rest
  run. A subset retry is not offered.
- A reply that does not fit the budget keeps `write_outcome`, `position` and `deduped` and drops the
  elements; read them with `lease_get` or a lone read.
- A frame whose reply was lost leaves every write in it unknown: resend it under the same request ID.
