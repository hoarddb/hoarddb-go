# Changelog

## Unreleased

### Tests

- Conformance leg 8 now runs the dropped-user case against a real server
  (`TestDroppedUserLosesItsConnections`). The harness creates the second user with the
  `hoarddb` CLI (`HOARDDB_CLI_BIN`), drops
  it, and checks that the server closed its pooled connections and that the next call reports the refusal. The
  driver is unchanged.

### Changed

- Error messages of package `ring` begin `ring: ` instead of `ringclient: `, the name of the in-tree package
  this client was copied from. Code that strips the prefix before printing a message has to strip the new one;
  `errors.Is`, the error codes and every request and reply payload are unchanged.
- `ring.Options.KnownServers` compares `Options.Addr` in the canonical form the hoardDB command line tool
  writes, so a line recorded for `db.internal:7433` is found when the program spells the same server
  `DB.internal:7433` or `db.internal.:7433` (and a hand-written line in another case is found too), and a
  line for `[2001:DB8:0:0:1:0:0:1]:7433` is found for `[2001:db8::1:0:0:1]:7433`. The lookup lowercases a
  host name and removes one trailing dot, writes an IPv4 address in dotted decimal, compresses an IPv6
  address in brackets, and writes the port (7433 when the address has none) without leading zeros. It never
  resolves a name and never treats a name as equal to an IP address, so a line for one server cannot vouch
  for another. The store's own writer records the canonical form as well, and a form the tool refuses is
  skipped or misses the lookup rather than being recorded under another server.
- The AUTH login of package `ring` names the client `hoarddb-go` instead of `ringclient`, so a server log
  tells a request from this driver apart from one the internal ring client sent. No request or reply payload
  changes.
- A string document id sent by package `ring` must be valid UTF-8 and take at most 1024 bytes encoded (each zero
  byte takes two). An id that is not is refused before anything is sent, with the fix in the message. The client
  this package was copied from sent any bytes and left the refusal to the server. Two more differences from that
  client: a request over the 32 MiB frame limit is refused before it is sent and the connection stays open, and
  `ring.Options.Fingerprint` takes one `SHA256:<64 hex characters>` value (the `host=fingerprint` form is refused).
- `hoarddbwire.OpRingList` to `OpRingBatch` take their numbers from `github.com/hoarddb/hoarddb-wire`, the module
  the server reads them from too. The numbers are unchanged. `hoarddbwire.OpRingGetMany` (`0xE1`,
  `RING_GET_MANY`) is new.
- The module now requires `github.com/hoarddb/hoarddb-wire` (Apache-2.0), the ring request and reply codecs, and
  `github.com/hoarddb/hoarddb-bson` v0.7.4. `scripts/compliance-check.sh --only deps` allows both and nothing else.
- A ring refusal with code `0x12` to `0x2C` is no longer folded into `ErrInternal`: it keeps its own class, so
  `errors.Is(err, ErrNotOwner)` and the other ring sentinels tell the refusals apart. Codes `0x19` and `0x1A`
  (node-to-node only), `0x0B` and any code above `0x2C` still fold into `ErrInternal`.
- The driver's BSON codec is now `github.com/hoarddb/hoarddb-bson` v0.7.0 (Apache-2.0), imported only in
  `internal/codec`; the previous third-party BSON module is no longer in the module graph. No exported function,
  wire payload or driver vector changes; `hoarddbwire.Document` and `hoarddbwire.Value` remain aliases, now of
  hoarddb-bson's `bson.Raw` and `bson.RawValue` instead of the previous module's, and the `go` line stays 1.23. The module requires the public
  `hoarddb-bson`; no token is needed.
- `ErrClaimEntryTooLarge` no longer has a code of its own: the error's `Code` is the wire's `CodeInvalidArgument`
  and `errors.Is(err, ErrClaimEntryTooLarge)` still tells the hard-cap refusal apart. `CodeClaimEntryTooLarge` is
  removed.
- Connection-string parse errors no longer repeat the connection string. They say what is wrong (for example
  "the port in the connection string is not a number") without quoting the text, and a connection string with an
  `@` after its first `/` or `?` is refused with the fix: percent-encode `/` and `?` in the user name and password.
  Query names and values, host text, ports and the scheme are not repeated either (a name from the accepted or
  refused parameter sets is still named), and a string with more than one `@` before its query is refused the same
  way. `Client`, `uri.URI`, `auth.Session` and `auth.Config` hide their credentials under every `fmt` verb.

### Removed

- `hoarddbwire` no longer holds a second copy of the ring codecs. `github.com/hoarddb/hoarddb-wire` reads and
  writes every ring request and reply but the topology, and package `ring` uses it. Removed from `hoarddbwire`,
  with no replacement there:
  - the keyed request: `RingKeyedRequest`, `DecodeRingKeyedBody`, `RingHeader`, `RingClass` and its classes
    (`RingInsert`, `RingUpsert`, `RingUpdate`, `RingDelete`, `RingStoreVerb`, `RingClaim`, `RingAck`, `RingRelease`,
    `RingExtend`, `RingGet`, `RingGetFields`, `RingStoreGet`, `RingStoreList`, `RingStoreCount`, `RingPeek`,
    `RingLeaseGet`), `RingField`, `RingFieldUpdate`, `RingUpdateCode` and the `RingUpdate...` codes, `RingGuardTerm`,
    `MaxRingGuardTerms`, `RingReturn` and its three values, `RingVerb` and its four values, `EncodeRingEnvelope`,
    `RingPushArg`, `RingHeapPushArg`, `RingPopArg`, `RingPutArg`, `RingTakeArg`, `RingHeapTakeArg`;
  - the reply: `RingReply`, `DecodeRingReply`, `RingPosition`, `ParseRingPosition`, `WriteAppliedDegraded`;
  - the batch: `RingBatchRequest`, `MaxRingBatchOperations`, `RingBatchReply`, `RingBatchAnswer`,
    `RingBatchAnswerBytes`, `RingBatchAnswers`, `DecodeRingBatchReply`;
  - the list and the count: `RingListRequest`, `RingCountRequest`, `RingEntry`, `DecodeRingEntries`;
  - the prologue and the redirect: `RingPrologue`, `RingPrologueLen`, `DecodeRingPrologue`, `RingRedirect`,
    `RingRedirectKind` and its three values, `RedirectFor`, `ErrorDetail.Usable`.

  `hoarddbwire.RequestFor` and `ResponseFor` no longer know `OpRingList`, `OpRingCount`, `OpRingKeyed` and
  `OpRingBatch`; the opcode numbers stay. What stays for the ring: `EncodeStringID` and `DecodeStringID`,
  `RingSlot`, the topology request and reply (`RingTopologyRequest`, `DecodeRingTopology`, `RingTopology`,
  `RingMember`, `RingSlotTable`), `ErrRingRequest` and `ErrRingReply`.
- The root package's ring router is removed. It was unexported and no exported function reached it, so no call
  changes: `Client.PoolStats` counted the router's per-member pools, which were always empty. Use package `ring`
  to send ring requests.

### Added

- Package `ring` has handles that keep data with what it describes. `Bucket.Entity(id)` is one document and
  `Entity.Store(field)` is one of that document's own stores; `Bucket.Field(id, field)` is both in one call.
  `client.Bucket("app", "User").Entity("ada").Store("Purchases").Push(ctx, purchase)` sends the request
  `Bucket.Push(ctx, "ada", "Purchases", purchase)` sends, and so on for every method: an `Entity` has `Get`,
  `GetFields`, `GetExpanded`, `Insert`, `Upsert`, `Update` and `Delete`; a `Store` has `Push`, `Put`, `Inc`,
  `IncAfter`, `Pop`, `PopN`, `Take`, `Peek`, `PeekKeyed`, `Nearest`, `Claim`, `Ack`, `Release`, `Extend`,
  `LeaseGet`, `Aggregate`, `Sum`, `SumBy` and `CountBy`, and `Get`, `List`, `ListExpanded`, `Count` and
  `Describe` for `Bucket.StoreGet`, `StoreList`, `StoreListExpanded`, `StoreCount` and `DescribeStore`. A handle
  sends nothing until a method is called. The flat `Bucket` methods are unchanged.

- `ring.Options.KnownServers` and `ring.Options.NoResend`. `KnownServers` is the path of a known-servers file
  (the one the hoardDB command line tool writes at `~/.hoarddb/known_servers`): `ring.Dial` connects only when the
  file records the address with the key the server presents, never asks and never adds a line; a server that is
  not recorded, or is recorded with another key, is refused before the password is sent and the error says how to
  fix it. `Fingerprint` decides when both are set, and `KnownServers` is used rather than `Insecure`. `NoResend`
  turns off the three stale-header resends: a keyed request, a batch or the moved keys of a get-many refused
  because the cached bucket header is stale (the schema moved, or the bucket was dropped and created again) are
  each sent at most once. A call can still send the DESCRIBE that learns a header, and a keyed call whose header
  predates an alter relearns it once before its one keyed frame. The refusal is returned as it came and the next
  call on that bucket describes it again. It is for benchmarks; the default is unchanged.
- The header cache of a `ring.Client` is split over 16 locks, and its key no longer allocates on a lookup. A cached
  bucket costs about 290 B plus 72 B per declared field of live heap (measured: 360 B for a bucket of one field,
  936 B for one of nine), and no descriptor, goroutine or timer; storing 10^6 declarations takes about 360 ns
  each, the second half no slower than 1.5 times the first (`TestMillionDescribesStayLinear`,
  `TestCachedDescribesCostBytesPerBucket`).
- `ring.Client.Leave`, `RemoveNode` and `ReplaceNode`: the three membership commands the package did not have.
  `Leave(ctx, id)` starts the orderly departure of a member, `RemoveNode(ctx, id)` removes a member that is down or
  dead, and `ReplaceNode(ctx, id, addr)` puts the machine at `addr` in that member's place. Each returns the
  `MembershipResult` that `Join` and `Promote` return. The bytes sent are those of the driver vectors
  `ring admin: leave`, `ring admin: remove_node` and `ring admin: replace_node`.
- Package `ring` (`github.com/hoarddb/hoarddb-go/ring`), the client for a ring cluster: `ring.Dial` returns a
  `Client` that pipelines requests on one connection, and a `Bucket` has the document calls (`Insert`, `Upsert`,
  `Update`, `Delete`, `Get`, `GetFields`, `GetMany`, `List`, `Count`), the store calls of a document's own
  stores (`Push`, `Put`, `Inc`, `Pop`, `Take`, `Peek`, `StoreGet`, `StoreList`, `StoreCount`, `Nearest`), leases
  (`Claim`, `Ack`, `Release`, `Extend`), aggregates, batches of up to 255 writes, the topology and the admin
  calls. Its documents and values are hoarddb-bson types and its error codes are hoarddb-wire's.
- One constant, one name and one sentinel for each ring error code `0x12` to `0x2C`: `CodeNotOwner` and
  `ErrNotOwner`, `CodeStaleEpoch` and `ErrStaleEpoch`, through `CodeClaimPending` and `ErrClaimPending`. A ring
  refusal that names the node owning the slot sets `Error.Owner` and `Error.OwnerAddr`; both are empty when the
  server named none.
- `Database.AlterBucket(ctx, name, AlterBucketOptions{ReplicationFactor: n})` changes how many copies
  of a bucket a cluster keeps (`ALTER_BUCKET`, `0x8E`), and returns an `AlterBucketResult` with the
  server's `Message`, the new and previous factor, and the members `Pending` a copy or `Releasing` one.
  It needs the `bucket_rf` capability (`ErrUnsupportedByServer`, nothing sent, without it) and a factor
  of at least 1. A refusal an operator must act on is `ErrInvalidArgument` with the server's sentence
  and is sent once; a refusal a retry clears is `ErrUnavailable` with `OutcomeNotApplied`, and the
  driver backs off and resends it until the caller's deadline. The vectors carry the eight `alter bucket`
  rows; `examples/alter` is the runnable example.
- `ListBuckets`, `Bucket.Info` and `Bucket.Stats` report the bucket owner's view of its copies in an embedded
  `BucketCopies`: `InSync`, `State`, `NotInSync` and `PendingCopies` (`PendingCopy{Node, Since}`), zero from a
  server that sends none. The wire layer decodes `nodes`, `previous_owner` and `pending_copy` of a stats answer too.
- `ErrBatchTooLarge` and `CodeBatchTooLarge` cover wire code `0x11` (`BATCH_TOO_LARGE`), a
  batch put over the largest batch the store commits atomically. The server's sentence is passed
  through verbatim and `Error.Outcome` carries the stamp it gave (`OutcomeNotApplied` on a
  standalone node, `OutcomeUnknown` from a cluster owner). A batch put still halves a
  `not_applied` refusal and resends the halves itself, as it did on the old in-band refusal; an
  `unknown` one is never split or resent and reaches the caller, who reads it back first. The
  connection is kept.

- `Database.Open(ctx, name)` returns the `*Bucket` after confirming it exists (one `Stats` round trip),
  with its store type available as `Bucket.Type()`; a missing bucket or database is `ErrNotFound`.
  `Database.CreateBucket` handles carry their type too, and a handle from `Database.Bucket` has an empty
  `Type()` because it does no I/O. Matches the Node driver's `database.open(name)`.
- `Bucket.Drop(ctx)` drops the bucket, the same as `Database.DropBucket(ctx, name)`; a missing bucket is
  `ErrNotFound`. Matches the Node driver's `bucket.drop()`.
- `Bucket.FindOne(ctx, filter, opts ...FindOption) (Document, error)` returns the first document that
  matches, for parity with the Node driver's `findOne`. It sends one SCAN with limit 1 and takes the
  options of `Find` (`Limit` and `PageSize` are overridden). A miss, or a bucket that is not there,
  is an error matching `ErrNotFound`; a document whose field holds null is found.

### Breaking

- A verb that returns a list answers an empty list and a nil error when nothing is there, and no
  longer returns `ErrEmpty`: `Bucket.Pop`, `Bucket.Peek`, `FieldStore.Pop` and `FieldStore.Peek`
  return a zero-length `[]Document`, and `Bucket.Claim` returns a `Claimed` with no `Items`
  (`Returned` above 0 still means expired leases went back and the caller claims again at once).
  This matches `DeadList` and the other hoardDB drivers.
- A one-value read that finds nothing returns an error matching `ErrNotFound`: `Get` of a missing
  key, a field `Get` of a missing element, and `Bucket.First` and `Bucket.Last` of an empty btree
  (these two were `ErrEmpty`).
- `ErrEmpty` and `CodeEmpty` are removed. No verb distinguishes "empty" from "not found" any more.
  `Consume` still backs off on an empty queue; it reads the empty `Claimed` instead of an error.

Migration:

```go
// before
docs, err := bucket.Pop(ctx)
if errors.Is(err, hoarddb.ErrEmpty) { /* drained */ }

// after
docs, err := bucket.Pop(ctx)
if err != nil { /* a real failure */ }
if len(docs) == 0 { /* drained */ }
```

`if errors.Is(err, hoarddb.ErrEmpty)` around `First` or `Last` becomes
`errors.Is(err, hoarddb.ErrNotFound)`.
