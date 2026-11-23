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

package hoarddb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Database is a namespace of buckets. A handle is a name and a pointer to its
// client: it does no I/O and holds no connection, goroutine, timer, cache
// entry or metric, so a program may hold a million of them. Whether the
// database exists is learned by the first call that uses it.
type Database struct {
	client *Client
	name   string
}

// DatabaseInfo is one database of [Client.ListDatabases].
type DatabaseInfo struct {
	// Name is the database's name.
	Name string
	// BucketCount is how many buckets it holds.
	BucketCount int64
}

// Database returns a handle on the named database. It does no I/O.
func (c *Client) Database(name string) *Database {
	return &Database{client: c, name: name}
}

// Name is the database's name.
func (d *Database) Name() string { return d.name }

// DefaultDatabase is the database the connection string names
// (hoarddb://host/<database>[/<bucket>]). When it names none the error says
// where the segment goes.
func (c *Client) DefaultDatabase() (*Database, error) {
	if c.target.Database == "" {
		return nil, &Error{Code: CodeInvalidURI, Op: "default database", Message: "the connection string names no database. " +
			"Add it as the path: hoarddb://host/<database>, or use client.Database(name)"}
	}
	return c.Database(c.target.Database), nil
}

// DefaultBucket is the bucket the connection string names
// (hoarddb://host/<database>/<bucket>). When it names none the error says
// where the segment goes.
func (c *Client) DefaultBucket() (*Bucket, error) {
	database, err := c.DefaultDatabase()
	if err != nil {
		return nil, err
	}
	if c.target.Bucket == "" {
		return nil, &Error{Code: CodeInvalidURI, Op: "default bucket", Message: fmt.Sprintf(
			"the connection string names no bucket. Add it after the database: hoarddb://host/%s/<bucket>, or use client.Database(%q).Bucket(name)",
			database.name, database.name)}
	}
	return database.Bucket(c.target.Bucket), nil
}

// CreateDatabase creates a database. Creating one that exists is
// [ErrAlreadyExists] unless [WithIfNotExists] is given. Only that option applies
// to a database; the others are for [Database.CreateBucket].
//
// A database also comes into being when the first bucket is created in it, so
// calling this is only needed to have an empty one.
func (c *Client) CreateDatabase(ctx context.Context, name string, opts ...CreateOption) error {
	config, err := applyCreateOptions(opts)
	if err != nil {
		return err
	}
	if len(config.bucketOnly) > 0 {
		return createOptionError("%s is an option for a bucket, not for a database. Pass it to Database.CreateBucket; CreateDatabase takes only WithIfNotExists",
			config.bucketOnly[0])
	}
	var answer hoarddbwire.AckResponse
	err = c.do(ctx, call{op: "create database", target: name, opcode: hoarddbwire.OpCreateDB, answer: hoarddbwire.OpCreateDB,
		request: hoarddbwire.NameRequest{Name: name}, response: &answer})
	if err != nil {
		if config.ifNotExists && errors.Is(err, ErrAlreadyExists) {
			return nil
		}
		return err
	}
	if !answer.Success {
		return refusal("create database", name, answer.Message)
	}
	return nil
}

// DropDatabase drops a database and everything in it, at once. A database that
// does not exist is [ErrNotFound].
func (c *Client) DropDatabase(ctx context.Context, name string) error {
	var answer hoarddbwire.AckResponse
	err := c.do(ctx, call{op: "drop database", target: name, opcode: hoarddbwire.OpDeleteDB, answer: hoarddbwire.OpDeleteDB,
		request: hoarddbwire.NameRequest{Name: name}, response: &answer})
	if err != nil {
		return err
	}
	if !answer.Success {
		return refusal("drop database", name, answer.Message)
	}
	return nil
}

// ListDatabases lists the databases on the server.
func (c *Client) ListDatabases(ctx context.Context) ([]DatabaseInfo, error) {
	var answer hoarddbwire.ListDatabasesResponse
	err := c.do(ctx, call{op: "list databases", opcode: hoarddbwire.OpListDBs, answer: hoarddbwire.OpListDBs,
		request: hoarddbwire.EmptyRequest{}, response: &answer})
	if err != nil {
		return nil, err
	}
	databases := make([]DatabaseInfo, 0, len(answer.Databases))
	for _, entry := range answer.Databases {
		databases = append(databases, DatabaseInfo{Name: entry.Name, BucketCount: entry.BucketCount})
	}
	return databases, nil
}

// CreateBucket creates a bucket of the given store type and returns its handle.
//
// An existing bucket is [ErrAlreadyExists], whatever its type, unless
// [WithIfNotExists] is given and the existing bucket has the same type; then
// the call succeeds and the declarations passed here are not applied to it.
// Creating a bucket in a database that does not exist creates the database.
// A bucket name is at most 255 bytes.
func (d *Database) CreateBucket(ctx context.Context, name string, kind StoreType, opts ...CreateOption) (*Bucket, error) {
	config, err := applyCreateOptions(opts)
	if err != nil {
		return nil, err
	}
	declarations, err := config.declarations()
	if err != nil {
		return nil, err
	}
	target := d.name + "/" + name
	if config.replicationFactor != 0 {
		if serverInfo := d.client.ServerInfo(); !serverInfo.Has("bucket_rf") {
			return nil, &Error{Code: CodeUnsupportedByServer, Op: "create bucket", Target: target, Message: fmt.Sprintf(
				"WithReplicationFactor needs the server capability bucket_rf, which this server (version %s) did not advertise; "+
					"nothing was sent. Upgrade the server, or create the bucket without WithReplicationFactor and let the node's default apply",
				serverInfo.Version)}
		}
	}
	var answer hoarddbwire.CreateBucketResponse
	err = d.client.do(ctx, call{op: "create bucket", target: target, opcode: hoarddbwire.OpCreateBucket, answer: hoarddbwire.OpCreateBucket,
		request: hoarddbwire.CreateBucketRequest{
			Database: d.name, Name: name, Type: string(kind), Options: declarations,
			Encrypted: config.encrypted, ReplicationFactor: int64(config.replicationFactor),
		}, response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Success {
		return nil, refusal("create bucket", target, answer.Message)
	}
	if existedWithSameType(answer.Message, kind) && !config.ifNotExists {
		return nil, &Error{Code: CodeAlreadyExists, Op: "create bucket", Target: target, Message: answer.Message +
			". Pass hoarddb.WithIfNotExists() to accept an existing bucket of the same type"}
	}
	return &Bucket{db: d, name: name, kind: kind}, nil
}

// existedWithSameType reads the one in-band success that is not a creation:
// the server answers a create of an existing bucket of the same type with
// success true and "bucket "b" already exists (type: hash)"
// (handbook/errors.md section 3). The match is on the end of the sentence,
// which a bucket name cannot reach.
func existedWithSameType(message string, kind StoreType) bool {
	return strings.HasSuffix(message, " already exists (type: "+string(kind)+")")
}

// Open returns the handle on a bucket that exists, with its store type read
// from the server (one [Bucket.Stats] round trip) and available as
// [Bucket.Type]. A bucket or database that does not exist is [ErrNotFound]
// carrying the server's sentence, which names the command that creates it. Use
// [Database.Bucket] when the handle is wanted without the round trip.
func (d *Database) Open(ctx context.Context, name string) (*Bucket, error) {
	stats, err := d.Bucket(name).Stats(ctx)
	if err != nil {
		return nil, err
	}
	return &Bucket{db: d, name: name, kind: stats.Type}, nil
}

// DropBucket drops a bucket and its data, at once. A bucket that does not
// exist is [ErrNotFound].
func (d *Database) DropBucket(ctx context.Context, name string) error {
	target := d.name + "/" + name
	var answer hoarddbwire.AckResponse
	err := d.client.do(ctx, call{op: "drop bucket", target: target, opcode: hoarddbwire.OpDeleteBucket, answer: hoarddbwire.OpDeleteBucket,
		request: hoarddbwire.DropBucketRequest{Database: d.name, Name: name}, response: &answer})
	if err != nil {
		return err
	}
	if !answer.Success {
		return refusal("drop bucket", target, answer.Message)
	}
	return nil
}

// BucketInfo is one bucket of [Database.ListBuckets].
type BucketInfo struct {
	// Name and Type identify the bucket.
	Name string
	Type StoreType
	// Entries is how many documents it holds.
	Entries int64
	// Stores names the store fields its documents carry.
	Stores []string
	// ReplicationFactor is how many copies a cluster keeps, 0 on a single
	// node. Inherited says it is the node's default rather than the bucket's
	// own choice.
	ReplicationFactor int
	Inherited         bool
	// BucketCopies is the owner's view of the copies; a node that holds a copy
	// without owning the bucket lists its own "owner-elsewhere" row.
	BucketCopies
}

// ListBuckets lists every bucket of the database in one answer, with no paging:
// it is an operator call, and at hundreds of thousands of buckets the answer
// approaches the frame limit. A database that does not exist is [ErrNotFound].
//
// On a cluster every member answers with the whole cluster's buckets of the
// database, whichever nodes hold their copies, so the same call on any node
// lists the same buckets. [Client.ListDatabases] answers the same way. Only
// [Bucket.Stats] is forwarded to a holder.
func (d *Database) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	var answer hoarddbwire.ListBucketsResponse
	err := d.client.do(ctx, call{op: "list buckets", target: d.name, opcode: hoarddbwire.OpListBuckets, answer: hoarddbwire.OpListBuckets,
		request: hoarddbwire.DatabaseRequest{Database: d.name}, response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Found {
		return nil, &Error{Code: CodeNotFound, Op: "list buckets", Target: d.name, Message: answer.Message}
	}
	buckets := make([]BucketInfo, 0, len(answer.Buckets))
	for _, entry := range answer.Buckets {
		buckets = append(buckets, BucketInfo{
			Name: entry.Name, Type: StoreType(entry.Type), Entries: entry.Entries, Stores: entry.Stores,
			ReplicationFactor: int(entry.ReplicationFactor), Inherited: entry.Inherited,
			BucketCopies: bucketCopiesOf(entry.InSync, entry.State, entry.NotInSync, entry.PendingCopies),
		})
	}
	return buckets, nil
}

// DeclareStoreFields adds store fields to an existing bucket: every document of
// the bucket then carries its own store of each declared type. Declarations
// are never removed.
func (d *Database) DeclareStoreFields(ctx context.Context, bucket string, stores ...StoreSpec) error {
	target := d.name + "/" + bucket
	if len(stores) == 0 {
		return &Error{Code: CodeInvalidArgument, Op: "declare store fields", Target: target,
			Message: "no store fields were given. Pass at least one, for example hoarddb.StoreSpec{Name: \"Purchases\", Type: hoarddb.FIFO}"}
	}
	declared := make([]hoarddbwire.Document, 0, len(stores))
	for _, store := range stores {
		encoded, err := encodeWire(storeWireOf(store))
		if err != nil {
			return &Error{Code: CodeInvalidArgument, Op: "declare store fields", Target: target, Err: err, Message: err.Error()}
		}
		declared = append(declared, encoded)
	}
	var answer hoarddbwire.AckResponse
	err := d.client.do(ctx, call{op: "declare store fields", target: target, opcode: hoarddbwire.OpDeclareStoreFields,
		answer:  hoarddbwire.OpDeclareStoreFields,
		request: hoarddbwire.DeclareStoreFieldsRequest{Database: d.name, Name: bucket, Stores: declared}, response: &answer})
	if err != nil {
		return err
	}
	if !answer.Success {
		return refusal("declare store fields", target, answer.Message)
	}
	return nil
}
