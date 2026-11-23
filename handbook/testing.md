# Testing a driver

How a hoardDB driver proves it is right: frozen vectors, a conformance harness, and real servers.
Language-neutral; binds every driver. **Tests are the contract**: a driver step is done when its tests
pass against a real server, and each test was seen to fail before the code that makes it pass.

## 1. The vector files

| File | Made by | Holds |
|---|---|---|
| `testdata/driver-vectors/v1.json` | **generated**, by driving a real hoardDB server with its own encoder; never edited by hand | frames, every operation's request and response, errors, auth, trust |
| `testdata/uri-vectors/v1.json` | **hand-maintained** (a URI never crosses the wire) | connection URIs that parse, and that are refused (`trust.md` section 6); an invalid case may carry `error_excludes`, substrings that must appear nowhere in the error message or its wrapped causes |
| `testdata/bson-vectors/v1.json` | **generated**, by the server's own BSON package, with no server running | the BSON byte layer: the bytes of every type and edge, malformed inputs, size limits (section 1.3) |
| `testdata/trust-vectors/v1.json` | **generated**, by the server's own `known_servers` code, with no server running | the `known_servers` rules of `trust.md` section 4: canonical host form, line grammar, lookup, saving, file safety (section 1.4) |
| `testdata/driver-vectors/ring-constants.json` | **generated**, from the codec module `hoarddb-wire` | the ring opcodes, class bytes, codes and limits as numbers, for a driver that does not import the Go codec |
| `testdata/driver-vectors/ring-inputs.json` | **generated**, from the ring request rows of `v1.json` | one input per ring request row, in the form the codec's `ringvec` command reads; each input encodes to exactly that row's `payload_hex` |
| `testdata/driver-vectors/consume-constants.json` | hand-maintained | the claim and lease limits and the backoff numbers of the earlier engine's consume loop |

**The `ring` group of `driver-vectors/v1.json` is the driver contract** (`wire.md` section 11), with the
`auth`, `trust`, `frame` and `session` groups and the `decode` rows that are not specific to the earlier
engine (the error with no code, a code the driver does not know, the `AUTH_FAIL` reasons, the
connection-level refusals and the oversize answer). The file also still holds rows captured from the
server's earlier single-store engine, and they are not part of the driver contract: the groups `write`,
`ddl`, `read`, `query`, `store-fields`, `batch-request`, `cluster` and `users`; the rows of `errors`,
`limits` and `wire-names` that send `PUT`, `GET`, `SCAN`, `FIFO_*`, `QUEUE_*`, `BLOB_WRITE`, `BATCH`, `CREATE_BUCKET` and
the other opcodes of that engine; the one `bson` row, which is a captured `GET` answer; and the `decode`
rows for its own codes and messages (`RESULT_TOO_LARGE` carrying `max_result_bytes` and `retained_bytes`,
`BATCH_TOO_LARGE`, `TIMEOUT`, `delete answered skipped`). A ring driver does not send those opcodes and
does not replay those rows. A ring node still answers them from the earlier engine, so a runner that does
replay them gets answers from that engine and not from the ring. The root package `hoarddb` of the Go
module is such a runner. The `cluster` rows, and every row whose `topology` is `cluster3`, were captured
on the earlier engine's replicated mode, which a v1.1.0 server refuses to start in: they can be decoded,
and they cannot be replayed against it.

All of them are Apache-2.0 and are read-only in a driver repository. A driver never edits them
(handbook `README.md`, "Changing this handbook").

### 1.1 `driver-vectors/v1.json`: the format

```json
{
  "format": "hoarddb-driver-vectors/1",
  "license": "Apache-2.0",
  "server_commit": "<40 hex>",
  "server_version": "<the server's version string>",
  "server_env": {"HOARDDB_ROOT_USER": "admin", "HOARDDB_ROOT_PASSWORD": "vectors-root-password"},
  "normalisations": ["<one sentence per normalisation applied>"],
  "vectors": [ { … }, … ]
}
```

`server_commit` is the server source the file was generated from. `server_env` is the environment
every replaying server starts with, on top of section 3.1's (the vectors' root credential is a fixed
test fixture, not a secret). `normalisations` says, in sentences, every way a captured value was
rewritten so that two generations are byte-identical; a reviewer can re-capture from them.

Every vector has `name` (unique), `group`, `kind` and `status` (`"frozen"`, or `"provisional: <what it
waits for>"`; a runner reports a provisional vector's result and never fails on it). `notes` is
optional prose: read it, it often says why the answer is what it is. The kinds:

**`frame`**: codec only. `frame_hex` is a whole frame (or, for a refusal, as much of one as the decoder
reads); `expect` is `{"result": "accept", "version", "length", "opcode", "request_id_hex",
"payload_hex"}` or `{"result": "refuse", "refuse_because"}` with `length-too-small`,
`length-too-large`, `bad-version` or `truncated`. A driver's decoder must produce exactly the decoded
fields, or refuse **without allocating the body**; its encoder, given the decoded fields of an accepted
frame, must produce `frame_hex` byte for byte.

**`exchange`**: one request and the server's answer, captured live.

```json
{
  "name": "put on a hash bucket",
  "group": "write", "kind": "exchange", "status": "frozen",
  "store_type": "hash", "topology": "single", "replay": true, "connection": "authenticated",
  "setup": [
    {"opcode": 128, "opcode_name": "CREATE_DB", "payload_json": {"name": "v049"}},
    {"opcode": 129, "opcode_name": "CREATE_BUCKET", "payload_json": {"database": "v049", "name": "users", "type": "hash", …}}
  ],
  "request": {
    "opcode": 16, "opcode_name": "PUT",
    "request_id_hex": "766563746f722d69642d763100000031",
    "payload_json": {"database": "v049", "bucket": "users", "key": "ada",
                     "data": {"Born": {"$numberInt": "1815"}, "Name": "Ada"}, "writeconcern": ""},
    "payload_hex": "…",
    "allowed_fields": ["database", "bucket", "key", "data", "writeconcern"]
  },
  "response": {
    "opcode": 16, "opcode_name": "PUT",
    "request_id_hex": "766563746f722d69642d763100000031",
    "payload_hex": "…",
    "payload_json": {"id": "ada", "success": true, "message": "", "seq": {"$numberLong": "1"}},
    "fields": ["id", "success", "message", "seq", "copies_confirmed", "replication_factor", "write_outcome"],
    "vary": ["seq"],
    "additive_fields": ["copies_confirmed", "replication_factor", "write_outcome"],
    "absent_fields": []
  }
}
```

- `payload_json` is canonical Extended JSON (version 2): integer widths, binary subtypes, datetimes and
  object ids are explicit. Both drivers' BSON codecs read it.
- `connection` is `authenticated` (the harness opens a connection and sends a password `AUTH` as the
  root user of `server_env` before anything else) or `fresh` (nothing is sent before the setup; the
  `AUTH` vectors and the pre-authentication ones).
- `setup` is a list of requests (opcode and payload) the harness sends first, on the same connection,
  each expected to succeed. Every exchange uses its own database name (`v001`, `v002`, …), so vectors
  are independent; the harness runs each one against a **fresh** server of its own, started with
  `server_env` and the vector's own `server_env` (an operator limit the vector needs, such as a small
  document limit), on the vector's `topology`: `single`, `static` (a ring vector with no server behind it) or `cluster3`
  (three nodes of the earlier engine's replicated mode, which v1.1.0 no longer starts; section 3.2). A `users` row also needs a server of its own holding no
  account but the root one, so that its `setup` starts from a user store nothing else has touched
  (section 1.2).
- **A lease id is a placeholder the setup fills.** The server mints lease ids, so a `lease_id` naming
  the exchange's lease is recorded as `"normalised"` in `payload_json`, and `payload_hex` carries those
  same bytes: in the request of `QUEUE_ACK`, `QUEUE_RELEASE` and `QUEUE_EXTEND`, and in a later `setup`
  step that releases a lease an earlier step claimed. A replay remembers the lease id of the most
  recent `QUEUE_CLAIM` answer while it sends the `setup` in order, and takes each such `lease_id` from
  it before encoding that step or the request (one vector's five claim-and-release pairs name five
  different leases, so the latest, not the last, is the one to use). The recorded value is not a lease
  id (one is 40 hexadecimal characters), so a request that sends it unsubstituted is refused as a
  malformed lease id, not answered as an unknown lease.
- `request.payload_hex` is what the server's own encoder produced. A driver does **not** have to match
  it byte for byte (field order and the omission of empty optional fields are free, `wire.md` section 3).
  It must: encode the request from its typed request type, then show that the bytes decode to a document
  whose fields are all in `allowed_fields` and which carries every field of `payload_json` with an equal
  value. Two allowances: a field whose `payload_json` value is empty (`""`, `null`, `0`, `false`, an
  empty array or document) may be absent instead, because the server's encoder writes some empty fields
  a driver omits; and integers are equal by value whatever their width (the server's encoder writes
  `protocol_version` as `int64`; a driver may write `int32`). A request with `payload_hex` and no
  `payload_json` is raw bytes on purpose (`GET_FAST`'s compact form, or a payload that is not BSON):
  the harness sends it as it is.
- `allowed_fields` is the request type's wire names minus the ones a client never sends (`wire.md`
  section 3). A driver's request type uses no other name.
- `response.payload_hex` is the server's answer as captured, then normalised. A driver decodes it into
  its typed response and shows the typed values equal `payload_json`, except the fields in `vary`
  (values that differ run to run: ids, sequence numbers, timestamps, versions, uptimes, tokens), whose
  **presence and BSON type** it checks instead. A `vary` entry is a path: `seq`, `data._id_`,
  `nodes[].joinedat` (`[]` is every element of an array). A path that ends in `[]` names the elements
  themselves, whatever their type, and replaces each whole instead of entering it: `item_ids[]` (a
  heap claim's minted handles) says every string of that array varies, so the harness checks the
  array's length and that each element is a string, never the values.
- `fields` is every wire name the answer's **type** can carry, nested names as paths
  (`buckets[].name`), whether or not this answer carries it. A driver's typed response may model any
  subset of it and must decode every one of them without failing. A server change that adds or removes
  one changes the vectors, so it cannot reach drivers unannounced.
- `additive_fields` may be present or absent and are never required (they appear on a cluster and not on
  one node, describe the capturing host, or come from a newer server). `absent_fields` must not be
  present.
- An `ERROR` answer adds `"error": {"code": 1, "name": "NOT_FOUND", "write_outcome": "not_applied",
  "message_contains": ["…"]}`, and an `AUTH_FAIL` answer `"auth_fail": {"reason": 1, "name":
  "CREDENTIALS", "message_contains": ["…"]}`: the error the driver must report. `message_contains`
  holds only stable parts of the server's sentence; the whole sentence is in `payload_json`, and a driver
  passes it through verbatim.
- `replay: false` marks an answer the harness cannot reproduce by sending the vector (a token minted at
  capture time, a user created or a node stopped before the request, or a setup too large for the
  file); `notes` says which. It is still a decode vector, and leg 3 skips it; the leg named in its notes
  exercises the behaviour live.

**`decode`**: a frame the server sends that is not the answer to a vector's request: an answer a test
cannot provoke on demand (an error with code `0`, an unknown code, an empty `AUTH_FAIL`), built with the
server's own types and encoder, or a connection-level refusal captured live (`OVERLOADED` and the ALPN
refusal, with a zero request ID; the answer to an oversize request header, whose bytes are in
`request_header_hex`). `frame_hex` is the frame; `expect` is what the driver reports:
`{"result": "error", "code", "name", "request_id_zero", "connection_fatal", "message_contains"?}`,
`{"result": "auth_fail", "auth_fail_reason", "name", "request_id_zero"}`, or `{"result": "answer",
"payload_json"}` (a success frame whose typed values must equal `payload_json`). A decode vector captured
on a tuned server carries its `server_env`.

**`trust`**: a key, `{"public_key_hex", "certificate_pem", "fingerprint"}` (the fingerprint each driver
must compute from the key, and from the certificate), or a `known_servers` case `{"known_servers":
{"file_text", "entries": {"host:port": "<raw key hex>"}}}` (the file loads, to exactly these entries) or
`{"known_servers": {"file_text", "error_contains": "…"}}` (loading the file is refused).

### 1.2 What the vectors cover

The `ring` group has at least one exchange per ring opcode and the refusals a strict decoder must make
(`wire.md` section 11.9 counts the rows); the AUTH success, every
`AUTH_FAIL` reason and the empty payload; the oversize-request answer; and BSON edge values (datetime to
the millisecond, object id, every binary subtype in use, `int32` against `int64`, `decimal128`).

What is deliberately not a vector, and where it is tested instead:

- Query pipelining (not offered by any server yet), and a server that **does not** answer (a bad
  version byte): there is no response to freeze; that is a harness leg.
- Anything whose request or setup is tens of megabytes, because the file would be: a request near the
  32 MiB frame limit and the response-frame guard. Leg 6 provokes them live.
- Refusals a test cluster cannot provoke deterministically (a membership change under a write, a
  forwarded write whose owner is unreachable, `CONFLICT`); `errors.md` names them, and a driver handles
  them by code and `detail`, which the decode vectors pin.
- User administration through `RING_ADMIN` (`auth.md` section 6) has no vector row in this repository.
  The `users` group holds rows of the earlier engine's user opcodes (`USER_CREATE` to `USER_REVOKE`, B5 to
  BB) and the rows that change an account between a token's mint and its use; the latter have
  `replay: false`, because the account and the change are made on the server's side.

### 1.3 `bson-vectors/v1.json`: the BSON codec corpus

A driver that writes its own BSON codec (Python and C++) is held to this file **byte for byte**. A
driver that uses a library for BSON does not need it. The bytes were written by the server's own
encoder and read by its decoder, never typed in by hand, and the generator lives beside the
server, where a check fails when the file and the encoder disagree, so a byte can only change in a
reviewed diff.

The header has `format` (`hoarddb-bson-vectors/1`), `server_commit`, `generated_by`, `limits` (the
depth limit and the frame and document sizes the vectors sit on) and two glossaries, `value_forms` and
`kinds`, which are the normative description of the rest. Values are written in a **typed JSON form**
so a test can build them without a BSON library: a document is `{"$doc": [[key, value], ...]}`, an
ordered list of pairs (order is the wire order, and a key may repeat); an int32 is
`{"$numberInt": "5"}`, an int64 `{"$numberLong": "5"}`, a double `{"$numberDouble": "1.5"}` (also
`"NaN"`, `"Infinity"`, `"-Infinity"`, `"-0"`), a decimal128 `{"$numberDecimal": "1.50"}`, a datetime
`{"$date": {"$numberLong": "-1"}}`, and so on for object id, binary, timestamp, regex and the rest;
strings, booleans, null and arrays are plain JSON. Every vector is one line, with `name`, `group`,
`kind` and optional `notes`. The kinds:

- **`encode`**: `document` and `hex`. Writing the document must give exactly these bytes, and reading the
  bytes must give the document back.
- **`decode`**: `hex` and `document`. Bytes a decoder must accept, with `reencodes_identically` saying
  whether writing the result gives the same bytes (false for an input a writer would normalise, such as
  array keys out of order or regex options). `last_wins_document` is the mapping form of a repeated key.
- **`reject`**: `hex`, `refusal` (`ProtocolError`) and `reason`. A decoder must refuse these bytes with
  that class, and never crash, hang, allocate by the declared length or raise a language-level
  exception. `server_decoder` records what the server does with the same bytes. Where it says
  `accepted`, the spec is stricter than the server (invalid UTF-8): the driver follows the spec, and
  `notes` says so. Nesting past the depth limit is refused by both: the server checks a document's depth
  on its bytes before it decodes them, with the same count and limit as the spec. Match on `refusal`, never on `reason`: `reason` labels
  the first rule the generator broke on purpose, but an input can break several rules at once and
  decoders check them in different orders, so two correct decoders can name different reasons for the
  same bytes.
- **`encode_refuse`**: `document` and `refusal` (`InvalidArgumentError`). An encoder must refuse before
  writing a byte (a key holding NUL).
- **`datetime_floor`**: `instant`, `expected_millis` and `hex`. Sub-millisecond digits floor toward
  negative infinity: `1969-12-31T23:59:59.9995Z` is `-1`, not `0`.
- **`size`**: a `recipe`, `document_bytes` and `sha256`, for documents too large to list as hex (24 MiB
  and the largest frame payload, each at the limit and one byte over). Build `{key: fill repeated
  fill_bytes times}`, encode it, and check the length and the SHA-256; `fits_one_frame` says whether a
  driver may send it.

A driver replays the file in its unit layer (section 4, no server needed) and reports each failing
vector by `name`. The file is read-only in a driver repository like the others, and a
driver never edits it; a change a driver needs is a change to the generator.

### 1.4 `trust-vectors/v1.json`: the `known_servers` rules

Every driver that reads or writes `known_servers` is held to this file. It was generated from the Go
implementation the CLI and the server use, never typed in by hand; a check beside the server fails when
the file and the code disagree. The header has `format` (`hoarddb-trust-vectors/1`), `server_commit`,
`generated_by`, `default_port`, `lock_wait_seconds`, and `keys`: two fixed ed25519 public keys, `a` and
`b`, with their hex and fingerprint, which the file texts below are built from. Every vector is one line
with `name`, `group` and optional `notes`. The groups:

- **`canonical`**: `input` and either `canonical` (the key that address is recorded and looked up
  under) or `refused: true` (the address is refused; the wording of the refusal is the driver's own).
  `trust.md` section 4.1.
- **`parse`**: `file_text` and either `entries` (`{"host:port": "<key hex>"}`, exactly what the file
  loads to) or `error_contains` (loading is refused). Section 4.2: CRLF, tabs, leading spaces, the
  separators that are not separators (U+00A0, U+0085, U+2028, form feed, vertical tab), text after the
  key, comments, blank and malformed lines, duplicate hosts.
- **`lookup`**: `file_text`, the dialled `host` and the `presented_key` hex, and `expect`: `known`,
  `unknown`, or `changed` with `recorded_fingerprint` and `presented_fingerprint` (the refusal names
  both).
- **`save`**: `file_text` (absent when the file does not exist yet), an `action` (`append`, recording a
  first-seen server, or `replace`, accepting a new key), `host` and `key`, and either `result_text`, the
  exact bytes of the file afterwards, or `error_contains`. Section 4.4.
- **`safety`**: one situation of the file or its directory (`target`, `kind`, `symlink`, `mode`,
  `owner`: `self`, `root` or `other`) and `expect`: `accept`, or `refuse` with `error_contains` (the
  command that fixes it). A driver builds each situation it can as a temporary file (the `self` rows) and
  checks the rest against its own decision function. Section 4.3.

A driver replays the file in its unit layer (section 4) and reports each failing vector by `name`.

## 2. The conformance harness

One runner per driver, in the driver's own repository and language (Go: a `conformance` package run by
`go test`), reporting one line per leg. Every driver must be green on every leg; it is the gate for every
later language too.

The Go repository's `conformance` package predates the ring: its server legs drive the root package
`hoarddb`, so they exercise the earlier engine, and its ring coverage is the codec (the ring rows are
decoded and the refusals refused, with no server). It has not been re-run against a v1.1.0 server.

| Leg | Asserts | Server |
|---|---|---|
| 1 Codec | every `frame`, `exchange` (decode and request-encode checks) and `decode` vector | none |
| 2 Trust | every `trust` vector: the identical fingerprint string from a key and from its certificate; `known_servers` parsing, and every vector of `trust-vectors/v1.json`; an unknown key is refused; a changed key is refused naming both; insecure mode is off unless asked for with a reason | one node |
| 3 Replay | every `ring` and `auth` exchange vector with `replay: true` and its topology, sent by the driver's **public API** where a verb exists and by its raw wire layer otherwise, compared as in 1.1 | one node |
| 4 Workload | create a database and a bucket that declares a scalar field and one store field of each type; for N documents, insert, then write and read each store through its document (`fifoStore` and `lifoStore` push, pop, peek, claim and ack; `hashStore` and `btreeStore` put, get, take; `heapStore` and `blobStore` as far as `operations.md` section 4 says they run); read back every value (field by field and BSON type for type; the server does not keep a document's field order, `wire.md` section 3) and report `hits=N misses=0`. A driver that returns nothing with no error fails here | one node |
| 5 Errors | the same bad requests produce the same code and error class, and the server's message verbatim: missing bucket, missing database, a database that exists, a bucket that exists with other fields, an `insert` of a held `_id_`, an unauthenticated request, a verb the store does not answer, an undeclared field | one node |
| 6 Limits | the driver refuses a frame over 32 MiB before sending, and a raw-socket test shows the server answers one by its request ID; a raw-socket test shows a bad version byte is closed without an answer; a 256th operation of a batch is refused by the driver before sending; a result over the ceiling is `RESULT_TOO_LARGE` and the **same connection** then answers `PING`; the per-address cap surfaces `OVERLOADED` with a zero request ID and the pool survives it (start the server with a small cap) | one node, tuned |
| 7 Lifecycle | the server killed mid-request: the driver reports a local `UNAVAILABLE`-class error, does not hang, and recovers on the next request after a restart; an idle connection the server closed is detected and replaced, not used | one node |
| 8 Auth | **on a dedicated server**: a wrong password is `CREDENTIALS` and the next attempt dials a new connection; a token minted on one connection authenticates another; an expired or invalid token falls back to the password exactly once; `RATE_LIMITED` is not retried in a loop; a dropped user's pooled connections close and the next call reports the refusal | one node, its own |
| 9 Cluster | a request sent to a node that is not the owner is forwarded, or is a `NOT_OWNER` the driver follows once under the same request ID; a stale topology is a `STALE_EPOCH` the driver refreshes once; a write resent under the same request ID after a lost answer answers `deduped`; a node that leaves does not break the connection. Not gated until the cluster recipe of section 3.2 is proven | three nodes |
| 10 Pipelining | requests in flight on one connection are matched to their replies by request ID, in any order | one node |

## 3. Getting a real server

The driver repositories do not contain the server's source. They test against a **server binary**, read
from `HOARDDB_SERVER_BIN`; the `hoarddb` command-line client, where a leg needs it, is read from
`HOARDDB_CLI_BIN`.

**No test-server binary is published yet.** Build both from the server repository,
`github.com/hoarddb/hoarddb-server`:

    git clone https://github.com/hoarddb/hoarddb-server.git
    cd hoarddb-server
    go build -o hoarddb-server ./cmd/server
    go build -o hoarddb ./cmd/cli       # the Go version is the one its go.mod names
    export HOARDDB_SERVER_BIN=$PWD/hoarddb-server HOARDDB_CLI_BIN=$PWD/hoarddb

`testing/server.json` and `scripts/fetch-server.sh` are for a download, once a release carries one. The
file names a tag, the server commit and, per platform, an asset name and its SHA-256:

```json
{ "tag": "test-server-<server commit, 12 hex>",
  "server_commit": "<40 hex>",
  "assets": { "linux-amd64": {"name": "<asset name>", "sha256": "<64 hex>"},
              "linux-arm64": {"name": "<asset name>", "sha256": "<64 hex>"} },
  "cli_assets": { "linux-amd64": {"name": "<asset name>", "sha256": "<64 hex>"},
                  "linux-arm64": {"name": "<asset name>", "sha256": "<64 hex>"} } }
```

`scripts/fetch-server.sh` downloads the asset for the machine's platform through the GitHub API, checks
the SHA-256 and prints the path; until a release exists it fails with HTTP 404. The binary is the hoardDB
server, under its own licence; it is a test fixture, never vendored into the driver's source tree.

Leg 8 uses the command-line client to create (and drop) a second user, because `create_user` carries a
client-derived Argon2id credential a driver with only the standard library and the BSON codec cannot
compute (`auth.md` sections 6 and 7).

### 3.1 One node

    HOARDDB_DATA_DIR=<empty dir>                  # a fresh directory per test server
    HOARDDB_TCP_LISTEN=127.0.0.1:<free port>      # the driver port
    HOARDDB_LISTEN=127.0.0.1:<free port>          # the ring identity (nothing else binds it)
    HOARDDB_NODE_ID=vectors-node                  # the node id the single-node answers carry
    HOARDDB_TCP_INTERNODE_LISTEN=127.0.0.1:<free port>
    HOARDDB_METRICS_LISTEN=127.0.0.1:<free port>  # default 9090 would collide between servers
    HOARDDB_ROOT_USER=admin
    HOARDDB_ROOT_PASSWORD=<a test password>
    HOME=<a private directory>                    # so no real ~/.hoarddb is touched

Start the binary with only these in its environment, and wait for the log line that carries
`"fingerprint":"SHA256:…"` (`trust.md` section 1): that is "ready", and it is the pin. Stop it with
SIGTERM, then SIGKILL after 15 s. Operator limits a leg needs are set the same way, for example
`HOARDDB_TCP_MAX_CONNS_PER_IP`, `HOARDDB_TCP_AUTH_MAX_FAILS`, `HOARDDB_QUERY_MAX_RESULT_BYTES`,
`HOARDDB_TCP_IDLE_TIMEOUT`.

`HOARDDB_NODE_ID` is what a cluster names a node by (section 3.2).

A host whose data disk looks more than 85% full makes the server protect itself: it reports
itself unhealthy. A test fixture on such a host, for example a cloud sandbox that reports a small disk,
sets the server's own `HOARDDB_DISK_HIGH_WATER` (a used-disk percent, 0 to 100; default `85`) in
the server's environment, for example `HOARDDB_DISK_HIGH_WATER=95`. `0` turns the guard off.

### 3.2 Three nodes, on one machine

A ring cluster is started with `HOARDDB_CLUSTER_ENABLED=true`, the first node listing no seeds, each later
node listing the nodes already in the cluster, and one `<node-id>=SHA256:<64 hex>` pin per node listed;
copies are set per database with `create database <name> { replication_factor: N }`
(the earlier replicated mode, with `HOARDDB_REPLICATION_FACTOR` and a seed list but no
`HOARDDB_CLUSTER_ENABLED`, is refused at start). That recipe has not been re-proven for the driver
tests, so leg 9 is not a gate until it is.

### 3.3 With containers

`testing/compose/` in the Go repository holds a three-node compose file built from a two-line image
(`testing/compose/server.Dockerfile`: the server binary on a small base image). It starts the nodes in
the earlier replicated mode (`HOARDDB_REPLICATION_FACTOR` and a seed list), which a v1.1.0 server refuses
at start, so it does not come up with a v1.1.0 binary. It has not been rewritten for the ring yet.

## 4. Test layers inside a driver

| Layer | What | Needs a server |
|---|---|---|
| Unit | codec (and, for a driver with its own BSON codec, the whole of `bson-vectors/v1.json`), error decode, URI parser, fingerprint, `known_servers` read and write (the whole of `trust-vectors/v1.json`), option validation, secret redaction | no |
| Component | the connection and the pool against an **in-process fake server** written with the language's own TLS: out-of-order responses, a socket closed mid-frame, a silent server (deadline), `OVERLOADED` on dial, a close after an error frame, an idle close, token expiry and fallback, ALPN refusal | no |
| Fuzz | the frame decoder, the URI parser, the error decoder: no crash, no allocation past the 32 MiB cap | no |
| Race and leaks | the race detector over everything; every background task counted before connect and after close, under load | no |
| Conformance | section 2 | yes |
| Examples | every runnable example and every README snippet is compiled, and run against a real server | yes |

**Failing first.** Every test is written before the code that makes it pass and is seen to fail on an
assertion (not a compile error or a timeout). A negative test is proven by breaking the guard it relies
on by hand, watching it fail, and restoring the code exactly.

## 5. Continuous integration

What a driver's pull-request checks are meant to cover: the unit, component and fuzz-seed tests with the
race detector; the linter; the vulnerability check; and the licence and boundary checks (the driver
imports no server code; every source file carries the Apache-2.0 header; no third-party type appears in
the public API; no other database product is named outside the allow-listed lines).

The conformance harness needs a server binary. Until a test-server binary is published it is run by hand
against a binary built from source (section 3). The Go repository's workflow today runs
`go vet ./... && go test -short ./...` on every pull request, and `-short` skips every test that needs
a server.
