# Authentication

The first frame on every connection. Language-neutral; binds every driver.

## 1. The exchange

After TLS, the driver sends `AUTH` (`0x01`) with this BSON payload:

| Field | Type | Meaning |
|---|---|---|
| `username` | string | the user |
| `password` | string | the password; ignored when `token` is non-empty |
| `protocol_version` | integer | `1`, the protocol major (the ALPN `hoarddb/1`); `0` means the client did not say, and the Go ring client sends `0` |
| `capabilities` | array of strings | what the client supports; send `[]` today. **Never send `pipeline_window=<n>`**: it was never implemented. Unknown strings are ignored by the server |
| `client` | string | the driver's name and version, for the server's logs (the Go ring client sends `hoarddb-go`); a program may append its own name (the URI's `clientName`) |
| `token` | string, optional | a resumption token (section 3); when present, the password in the same frame is ignored |

**Success** is `AUTH_OK` (`0x02`):

| Field | Type | Meaning |
|---|---|---|
| `success` | bool | `true` |
| `token` | string | a resumption token, when the server has the `auth_token` capability |
| `message` | string | human text |
| `token_expires_at` | integer, Unix seconds | when this token expires |
| `token_deadline` | integer, Unix seconds | when the chain of refreshed tokens ends; never extended by a refresh |
| `protocol_version` | integer | the server's protocol major |
| `server_version` | string | the server's version |
| `capabilities` | array of strings | `wire.md` section 6 |
| `password_min_length` | integer | the server's shortest accepted new password, in characters (section 5) |

**Failure** is `AUTH_FAIL` (`0x03`) with `{reason (integer), message (string, optional)}`. An **empty**
`AUTH_FAIL` payload, which older servers send, is read as `CREDENTIALS`; zero is not a valid reason.

| `reason` | Name | What the driver does |
|---|---|---|
| 1 | `CREDENTIALS` | stop. Never retry the same password. The error says which user and that the driver does not retry |
| 2 | `TOKEN_EXPIRED` | fall back to the password, **once**, on a **new** connection |
| 3 | `TOKEN_INVALID` | same as 2 |
| 4 | `TOKEN_UNSUPPORTED` | the server has no tokens: discard tokens and use the password |
| 5 | `PROTOCOL_MISMATCH` | stop; the message names both versions; the error names the upgrade to do |
| 6 | `RATE_LIMITED` | back off; **no credential was checked**; do not loop (section 4) |
| 7 | `ALREADY_AUTHED` | a second password AUTH on a connection that used its one (section 2): a driver bug, or a retry on the same connection |
| 8 | `INTERNAL` | a server-side failure, or a node that has not yet received its user configuration, or a cluster follower whose copy of it is too old. Not a credential failure: retryable on another node or after a short wait |

The message is passed to the caller verbatim; the driver adds what happened and what to do, and never
a password or token.

## 2. One password attempt per connection

The server claims a connection's single password AUTH **before** it checks the password, so a failed
password AUTH spends it: a second password AUTH on the same connection is refused `ALREADY_AUTHED`.
**After any failed password AUTH, a driver closes the connection and dials a new one** if it tries
again at all. The one exception is the busy refusal that checked no credential (`RATE_LIMITED` issued
by the server's derivation gate), which gives the claim back.

A token AUTH is exempt: it may be sent on an already authenticated connection, which is how a
connection refreshes its token.

## 3. Tokens

A token is a **resumption credential**: minted by a successful password AUTH, presented only in
`AUTH.token` (never per request), and accepted on another connection to the same server. A token a
node does not accept (for example after failing over to another host) is answered `TOKEN_INVALID`,
which the fallback below handles. Integers here, as everywhere, are decoded at any width (`wire.md` section 3). Two lifetimes:

- `token_expires_at`: the token's own expiry (900 s by default).
- `token_deadline`: the end of the refresh chain (12 hours by default, jittered ±10 % per chain). A
  refresh returns a new token with a new `token_expires_at` and the **same** `token_deadline`.

A driver:

1. keeps the password in memory only as long as it may need it again (it will, at `token_deadline`);
2. after the first password AUTH, presents the **token**, not the password, on every new connection, so
   the server does not repeat a deliberately slow password verification for each connection a pool opens;
3. refreshes at **half** of the token's lifetime, with **one refresh in flight per client**
   (single-flight), by sending a token AUTH on a connection it already holds;
4. when `token_deadline` has passed, or the server answers `TOKEN_EXPIRED` / `TOKEN_INVALID`, falls back
   to the password **once**, staggering the fallback with jitter so a pool of connections does not
   stampede (see "What 'once' is scoped to" below);
5. ignores the token entirely when `capabilities` lacks `auth_token`.

**What "once" is scoped to.** One password login is in flight **per client, per host** at a time, not
one per connection. A pool that loses its token does not send a password AUTH on each of its
connections: the first connection that needs to re-authenticate takes the client's single login slot,
and every other connection waits for the token that login returns and resumes with it. The token is
accepted on another connection to the same server, which is what makes the wait work. The password
AUTH frame itself is unchanged: this is the driver's own serialisation, not a wire rule.

Why it matters: the server derives Argon2 under a process-wide slot budget (its `auth_max_derivations`,
`GOMAXPROCS` by default) and a login that cannot get a slot within five seconds is refused
`RATE_LIMITED`; every password AUTH also spends one of the connection's single attempts (section 2). A
pool of eight connections logging in at once spends eight derivations and can trip the per-address
failed-login limiter on one wrong password. The one exception is a `RATE_LIMITED` refusal that checked
no credential: the connection may retry the same AUTH after a backoff, which is what section 4's
figure is for.

A token is not an API key: it expires and is minted by a password. A driver offers no "API key"
option. A token supplied by the program (the URI's `authToken`, or an option) is used for the first
connection and refreshed like any other.

## 4. The failed-login limiter

The server counts consecutive authentication failures **per source address**:

- from the 5th failure, the next attempt is delayed 1 s, doubling per failure to a cap of 30 s;
- at 20 failures (operator-tunable; 0 disables the limiter) the address is refused outright for 60 s with
  `RATE_LIMITED`, and the connection is closed (the refusal's message says so: `The server is closing
  this connection; wait out the window, then reconnect and retry with the correct password.`; the busy
  refusal, which keeps the connection open, does not);
- the count is forgotten after 15 minutes without a failure, and cleared by a successful password AUTH;
- wrong passwords, unknown users and rejected tokens count; a `RATE_LIMITED` refusal does not.

A driver's own hold after a `RATE_LIMITED` refusal is **5 seconds, with jitter**, before it tries again
(at all). That is the handbook's rule for every driver, so two of them behave
alike against one server. The server names no figure and will not: its refusal says only "a jittered
backoff".
The refusal already says which refusal it is: the ledger's text names the failure count and the 60 s
window, and the derivation gate's text says the password was not checked and lists what ends the storm
(pooled connections, a session token, or a higher `HOARDDB_AUTH_MAX_DERIVATIONS`). A driver reports
that message and waits out the hold; it does not shorten the hold because its own deadline is shorter
— it reports the refusal when the deadline expires instead.

**A driver never retries a failed AUTH in a loop**, and never retries `CREDENTIALS`, `RATE_LIMITED` or
`PROTOCOL_MISMATCH` at all. A failed login in a loop is the credential-stuffing pattern the limiter
exists to stop, and it locks out every other process on the same machine. For the same reason, a test
suite that exercises failed logins runs them against **its own server**, never one shared with other
tests.

**What "never retries at all" is scoped to.** The rule is about **the credential and the host that
answered**, not about the client process. Nothing in a refusal ends the client, and a driver does not
tear its pool down because one AUTH was refused.

- **`CREDENTIALS` is never transient.** The server compares the supplied password against the stored
  credential and answers one sentence, `invalid username or password`, for a wrong password, an unknown
  user and a disabled user, and it answers the same way when the account's stored credential names key
  material this node no longer holds (the server-side reason stays in its log). A follower whose copy of
  the user configuration lags does **not** answer `CREDENTIALS` for a password the master already
  replaced: it either verifies against the copy it has (and refuses the old password exactly as the
  master would) or, past the staleness bound, answers `INTERNAL` (below). So a driver never re-sends a
  password it was told is wrong: not on this host, not on another, not after a wait. The one password
  this must not block is a **new** one: after the program changes a password, the client discards the
  old token and authenticates with the new password on a fresh connection.
- **`PROTOCOL_MISMATCH` is per host and per attempt, not per client.** The server names both versions
  (`server speaks protocol N, client sent M`) before any credential work. A driver fails **that host**
  with an error naming the upgrade, and keeps serving the other hosts of the pool: during a rolling
  upgrade one old node must not take every connection down with it. Only a single-host client has
  nothing left to talk to, and then the error the caller sees is that host's. A driver never retries
  the same host with the same protocol major, because the answer cannot change until one side is
  upgraded.
- **`RATE_LIMITED` is a wait, not a failure.** It asserts that no credential was checked. The per-address
  ledger's refusal (20 consecutive failures, tunable by `HOARDDB_TCP_AUTH_MAX_FAILS`) closes the
  connection, so the retry is a new dial after the back-off above; the derivation-gate refusal
  (`password was not checked: the server waited … for a free password-hashing slot`) leaves the
  connection open and gives back the connection's one password attempt, so the same connection may
  re-send the same AUTH. Neither is a credential failure and neither is retried in a loop.
- **`INTERNAL` is the one retryable refusal**, and only on another node or after a wait: it covers a
  node that has not yet received its user configuration and a follower past
  `auth_config_max_staleness_seconds` (300 s by default), both of which carry a message saying so. A
  driver may retry it, bounded by the caller's deadline, preferably on another host; it never retries it
  against the same host in a tight loop.
- **`TOKEN_EXPIRED` / `TOKEN_INVALID` / `TOKEN_UNSUPPORTED`** are the fallback of section 3, not a retry
  of a failed login: they are answered once, by falling back to the password on a new connection, and
  a second refusal of that password is `CREDENTIALS`, which ends it.

## 5. Users and roles

Every user holds grants of `{role, database}`. Roles: `reader`, `writer`, `db-manager`, `user-manager`,
`cluster-manager`, `backup-operator`, `restore-operator`, `root`. Every request is checked against the user's grants before
it runs; a refusal is `PERMISSION_DENIED` (`0x03`) with a message of the form `permission denied: user
"<user>" lacks "<action>" on "<resource>"`, followed by the user's roles and the `grant role …`
command an administrator would run. A driver's documentation says a refused call is a **role** problem,
never that every user is an administrator.

The root user exists from the first start: `admin` by default (`HOARDDB_ROOT_USER`), with the password
from `HOARDDB_ROOT_PASSWORD` (or the file named by `HOARDDB_ROOT_PASSWORD_FILE`); when neither is set on
a first start from a terminal the server asks for one, and a first start with no terminal refuses,
writes nothing and says to set one. Only the password's hash is stored; the password is never written to disk,
so a driver's tests supply `HOARDDB_ROOT_PASSWORD` rather than read it back.

When a user is dropped or disabled, the server closes that user's live connections (`wire.md`
section 8).

A driver that offers user administration derives the Argon2id credential itself and sends only that, so
the server never sees the new password and cannot measure it; section 6.2 gives the derivation, the
payloads and the policy the driver applies.

## 6. User administration: the seven `RING_ADMIN` commands

A driver that offers user administration sends `RING_ADMIN` (`wire.md` section 11.1) with one of the
commands `create_user`, `alter_user`, `drop_user`, `grant_role`, `revoke_role`, `show_users` or
`describe_user`. Every member name below is exact, and a request carries **no plaintext password**: the
driver derives the credential itself (section 6.2) and sends only that. This section describes what the
v1.1.0 server answers. The codec module `hoarddb-wire` v0.1.0, the Go ring client and the vectors in this
repository do not carry these commands yet, so a program manages users with the `hoarddb` command line
(`help create user`, `help alter user`, `help drop user`, `help grant` and `help revoke` show the
statements). Every command needs the **unscoped** `user-manager` or `root` grant
(`user-manager@*`): a grant on one database is not enough, for the reads as for the changes.

### 6.1 The requests

A **grant** is `{role, db}`: `role` is one of the role names of section 5 and `db` a database name or
`*`. An account holds at most 64 grants.

| `cmd` | Members, in this order | Does |
|---|---|---|
| `create_user` | `name`, `expect_drop_seq` (int64), `roles` (array of grants, may be empty), `credential` | creates the account, enabled. `expect_drop_seq` is the `user_drop_seq` of the last `show_users` or `describe_user` reply; a create is refused `CONFLICT` when a user was dropped since |
| `alter_user` | `name`, `expect_seq?` (int64), `credential?`, `enabled?`, `roles?` | at least one of `credential`, `enabled` and `roles`. `roles` replaces the whole list; an empty list removes every grant. A password reset is an alter with `credential` alone |
| `drop_user` | `name`, `expect_seq?` | removes the account |
| `grant_role` | `name`, `expect_seq?`, `role`, `db` | adds the grant; one already held succeeds and changes nothing |
| `revoke_role` | `name`, `expect_seq?`, `role`, `db` | removes the grant; one not held succeeds and changes nothing |
| `show_users` | none | every account, ordered by name |
| `describe_user` | `name` | one account |

A member a command does not take is `INVALID_ARGUMENT` naming it; a missing required member is
`INVALID_ARGUMENT` naming it (`create_user` without `expect_drop_seq` says to read it from `show_users`).
Both fences are `int64`. `expect_seq` is the `revision` the client read for that account; the change is
refused `CONFLICT` when the account's revision is another. Absent, the server takes the revision it holds.

**The request ID.** A change is sent under the `RING_ADMIN` frame's own 16-byte request ID, a fresh random
one for each change. A change under an **all-zero ID is refused** `INVALID_ARGUMENT`. Resend a change
whose answer was lost under the same ID, and the same command on the same account is answered success
and applied once (the server remembers an ID for ten minutes). The reads need no ID.

### 6.2 The credential the driver derives

`credential` is `{algo, salt, hash, memory, iterations, parallelism}`, members in that order:

| Member | Type | Value |
|---|---|---|
| `algo` | string | exactly `argon2id` |
| `salt` | binary | 16 to 64 bytes, fresh and random; a salt another account already stores is refused |
| `hash` | binary | exactly 32 bytes, the Argon2id output |
| `memory` | int64 | KiB, 1 to 2^32-1 |
| `iterations` | int64 | 1 to 2^32-1 |
| `parallelism` | int32 | 1 to 255 |

Derive with **Argon2id version `0x13`** and the password. The wire checks the shape only: the work-factor
window an account may use is the server's (`auth.argon2_memory` and `auth.argon2_iterations`), and a value
outside it is refused `INVALID_ARGUMENT` with a message naming the permitted range. The salt and the hash
are binary, not base64.

A driver does not send the password, so **the server cannot measure it**: the driver applies the password
policy itself, before deriving. It refuses a password shorter than `AUTH_OK`'s `password_min_length`,
counted in Unicode characters, with an error naming the length, the minimum and
`HOARDDB_PASSWORD_MIN_LENGTH`, and never the password. It uses the number from the server it is connected
to, never a constant of its own; absent or zero means the server reported none, and the driver refuses
rather than guess.

### 6.3 The replies

| `cmd` | Members, in order |
|---|---|
| `create_user`, `alter_user`, `drop_user`, `grant_role`, `revoke_role` | `name`, `revision` (int64), `result` (one line of text) |
| `show_users` | `revision`, `user_drop_seq`, `users` |
| `describe_user` | `user_drop_seq`, `user` |

A `user` is `{name, roles, enabled, epoch, created_ms, password_changed_ms, revision}`; the times are Unix
milliseconds. `revision` is what a later change of that account sends as `expect_seq`. **No reply carries a
salt, a hash or a key.** `describe_user`, or a change, of a user that does not exist is `NOT_FOUND`.

## 7. User-administration refusals

A refusal is an `ERROR` frame (`errors.md` section 1), never a `RING_REPLY`. The `code` names the cause, a
driver branches on it, and the message names the user or role and the fix and is passed through verbatim.

| Situation | Code |
|---|---|
| the caller lacks `user-manager@*` or `root@*` | `PERMISSION_DENIED` (one message for all seven commands; it tells the caller to ask an administrator to run `grant role user-manager@* to <user>`) |
| `create_user` of a name that exists | `USER_EXISTS` |
| `alter_user`, `drop_user`, `grant_role`, `revoke_role` or `describe_user` of a user that does not exist | `NOT_FOUND` |
| a user was dropped since the read (`expect_drop_seq`), or the account changed since the read (`expect_seq`) | `CONFLICT`: re-read with `describe_user` and repeat the change against what it shows |
| an unknown or retired role; an unacceptable credential; a salt another account uses; an empty or reserved name; more than 64 grants on one account; the user ceiling | `INVALID_ARGUMENT` |
| a change that would leave no enabled `user-manager`, `cluster-manager` or `root` | `INVALID_ARGUMENT`, `refusing to remove the last administrator ...`; grant the role to another enabled user first. Never retried |
| a node that does not serve them | `INVALID_ARGUMENT`: send the request to a node started by `hoarddb-server` |

The last administrator cannot be removed. The store always keeps at least one **enabled** account
holding `user-manager`, `cluster-manager` or `root`; `reader`, `writer`, `db-manager`, `backup-operator`
and `restore-operator` do not count. A `drop_user` of it, an `alter_user` that sets `enabled: false` on
it, and an `alter_user` or `revoke_role` that leaves it without those roles are refused.

**Retry.** Resend a change under the same request ID (section 6.1) after a lost answer or `UNAVAILABLE`:
a repeat of an applied change is answered success and is not applied twice. Every other code in the table
is `never` in `errors.md` section 6. In a cluster, users are the cluster's and the limitations page of the server's user
documentation lists the limits of this build (a member cut off from the others keeps serving the users it last applied).

## 8. What a user change does to a live session

A user change reaches a live session at its next AUTH or token refresh, never in the middle of a
request, and the refusal a driver sees depends on which kind of change it was. This is the part a
driver's session handling depends on, so it is stated rather than left to the server's internals.

| The change | A token minted before it | Why |
|---|---|---|
| a **password** change (`alter_user` with `credential`) | `TOKEN_INVALID` (`3`) on the next AUTH or refresh | the server bumps the account's revocation counter, and a token whose counter is stale is invalid |
| the account **disabled** (`alter_user` with `enabled: false`) | `TOKEN_INVALID` (`3`) on the next refresh; the server also **closes that user's live connections** | the counter is bumped, and a disabled account's sessions are closed |
| the account **dropped** | `TOKEN_INVALID` (`3`) on the next refresh; live connections are closed | the token's subject no longer exists |
| a **role set replaced** by `alter_user` with `roles` and neither `credential` nor `enabled: false` | still accepted, and the refreshed token carries only the roles both the record and the presented token hold: a removed role is gone, and an added one is not picked up | the counter is not bumped; a refresh narrows to the intersection of the record and the presented token, never widens |
| one grant **added or removed** by `grant_role` / `revoke_role` (that changed something: granting a grant the user holds, or revoking one it lacks, is a no-op that bumps nothing) | `TOKEN_INVALID` (`3`) on the next AUTH or refresh | each of these bumps the revocation counter; the live connection is not closed, and the password fallback of section 3 mints a token with the new roles |

**A revoked token is `TOKEN_INVALID`, never `CREDENTIALS`.** That distinction is what a session handler
is built on: `TOKEN_INVALID` is the fallback of section 3 (authenticate again with the password, once,
on a new connection), and `CREDENTIALS` means the password itself is wrong and is never retried. A driver
therefore must not treat `TOKEN_INVALID` as the end of the session: the password may still be right and
the user may simply have changed it elsewhere.

`TOKEN_EXPIRED` (`2`) is the other half of the same rule: it means a signature-valid token that is past
its own expiry or past `token_deadline`, which is the ordinary refresh boundary, not a revocation. A
driver tells them apart by the `reason` and handles both the same way (section 3 item 4).

Two consequences a driver states in its own documentation:

- a password change made by another process ends this client's session at its next refresh, and the
  client's next call authenticates with the password it holds — which is now the old one, so it fails
  `CREDENTIALS`. That is not a driver bug; the program must supply the new password.
- a role change takes effect at the next AUTH or refresh, not immediately: a call made in the seconds
  after a revoke may still be authorized with the old grants of the live connection. After `grant_role`
  or `revoke_role` the next refresh is refused `TOKEN_INVALID` and the password fallback of section 3
  authenticates with the new roles. After a role set replaced by `alter_user` the refresh succeeds and
  only narrows: a role newly added appears only on a **new password login**, because a refresh keeps
  what the presented token already carried. A driver that needs new authority now authenticates with
  the password again.
