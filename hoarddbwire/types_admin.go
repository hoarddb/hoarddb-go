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

package hoarddbwire

import (
	"math"
	"time"
)

// Databases, buckets and observability (handbook/operations.md section 4).

// NameRequest is CREATE_DB and DELETE_DB.
type NameRequest struct {
	Name string `bson:"name"`
}

// DatabaseRequest is LIST_BUCKETS.
type DatabaseRequest struct {
	Database string `bson:"database"`
}

// DropBucketRequest is DELETE_BUCKET. Its bucket member is called name, as
// CREATE_BUCKET's is, where the data verbs say bucket.
type DropBucketRequest struct {
	Database string `bson:"database"`
	Name     string `bson:"name"`
}

// CreateBucketRequest is CREATE_BUCKET. SchemaJSON is accepted and not read by
// the server. Options holds the declarations (stores, indices, fields,
// field_types) as one document. A ReplicationFactor of 0 is the node's
// default and is not sent; any other needs the bucket_rf capability.
type CreateBucketRequest struct {
	Database          string   `bson:"database"`
	Name              string   `bson:"name"`
	Type              string   `bson:"type"`
	SchemaJSON        string   `bson:"schemajson,omitempty"`
	Options           Document `bson:"options,omitempty"`
	Encrypted         bool     `bson:"encrypted,omitempty"`
	ReplicationFactor int64    `bson:"replication_factor,omitempty"`
}

// DeclareStoreFieldsRequest is DECLARE_STORE_FIELDS: Name is the bucket, and
// Stores has the shape of options.stores. Declarations are never removed.
type DeclareStoreFieldsRequest struct {
	Database string     `bson:"database"`
	Name     string     `bson:"name"`
	Stores   []Document `bson:"stores"`
}

// AlterBucketRequest is ALTER_BUCKET: it changes how many copies of a bucket a
// cluster keeps (handbook/operations.md section 4). A driver sends these three
// members and no other.
type AlterBucketRequest struct {
	Database          string `bson:"database"`
	Name              string `bson:"name"`
	ReplicationFactor int64  `bson:"replication_factor"`
}

// AlterBucketResponse answers ALTER_BUCKET. Success false is a refusal an
// operator must act on, whose reason is Message; PreviousFactor is then 0.
// Pending names the members an increase newly places, Releasing the members a
// decrease un-places; each is absent when empty.
type AlterBucketResponse struct {
	Success           bool     `bson:"success"`
	Message           string   `bson:"message,omitempty"`
	ReplicationFactor int64    `bson:"replication_factor"`
	PreviousFactor    int64    `bson:"previous_factor"`
	Pending           []string `bson:"pending,omitempty"`
	Releasing         []string `bson:"releasing,omitempty"`
}

// AckResponse answers CREATE_DB, DELETE_DB, CREATE_BUCKET, DELETE_BUCKET and
// DECLARE_STORE_FIELDS. Success false is a refusal whose reason is Message.
type AckResponse struct {
	Success bool   `bson:"success"`
	Message string `bson:"message,omitempty"`
}

// CreateBucketResponse answers CREATE_BUCKET. Placement is "whole" or "spread",
// the placement the bucket was created with; a refusal carries none.
type CreateBucketResponse struct {
	Success   bool   `bson:"success"`
	Message   string `bson:"message,omitempty"`
	Placement string `bson:"placement,omitempty"`
}

// PongResponse answers PING. A server may send no payload at all.
type PongResponse struct {
	OK      bool   `bson:"ok,omitempty"`
	Message string `bson:"message,omitempty"`
}

// DatabaseEntry is one database of LIST_DATABASES.
type DatabaseEntry struct {
	Name        string `bson:"name"`
	BucketCount int64  `bson:"bucketcount"`
}

// ListDatabasesResponse answers LIST_DATABASES.
type ListDatabasesResponse struct {
	Databases []DatabaseEntry `bson:"databases,omitempty"`
	Message   string          `bson:"message,omitempty"`
}

// BucketEntry is one bucket of LIST_BUCKETS. Stores is the names of its store
// fields. Inherited says the replication factor is the node's default. Outside
// is a member the server sends on a BUCKET_INFO row: the handbook does not
// define it, so a driver must not act on it, and the public API does not
// expose it.
type BucketEntry struct {
	Name              string   `bson:"name"`
	Type              string   `bson:"type"`
	Entries           int64    `bson:"entries"`
	Stores            []string `bson:"stores,omitempty"`
	ReplicationFactor int64    `bson:"replication_factor,omitempty"`
	Inherited         bool     `bson:"inherited,omitempty"`
	Outside           bool     `bson:"outside,omitempty"`
	// Pending and Dead count a queue's or heap's entries held under a lease and
	// on the dead list; each is absent at zero.
	Pending int64 `bson:"pending,omitempty"`
	Dead    int64 `bson:"dead,omitempty"`
	// InSync, State, NotInSync and PendingCopies are the owner's view of the
	// bucket's copies (operations.md, "Copies"); an older server sends none.
	InSync        int64         `bson:"in_sync,omitempty"`
	State         string        `bson:"state,omitempty"`
	NotInSync     []string      `bson:"not_in_sync,omitempty"`
	PendingCopies []PendingCopy `bson:"pending_copies,omitempty"`
}

// PendingCopy is a member still to receive its copy of a bucket. Since is Unix
// seconds on the owner's clock.
type PendingCopy struct {
	Node  string `bson:"node"`
	Since int64  `bson:"since"`
}

// ListBucketsResponse answers LIST_BUCKETS, a whole database in one frame.
// Encrypted is the server's word for it ("no"), not a boolean.
type ListBucketsResponse struct {
	Found     bool          `bson:"found"`
	Database  string        `bson:"database,omitempty"`
	Buckets   []BucketEntry `bson:"buckets,omitempty"`
	Message   string        `bson:"message,omitempty"`
	Encrypted string        `bson:"encrypted,omitempty"`
}

// StoreStats is one store field of a bucket in BUCKET_STATS.
type StoreStats struct {
	Name     string `bson:"name"`
	Engine   string `bson:"engine"`
	Parent   string `bson:"parent,omitempty"`
	Elements int64  `bson:"elements"`
}

// FieldDeclaration is one declared field of a bucket in BUCKET_STATS. Store is
// carried as the server sent it: no vector pins its type.
type FieldDeclaration struct {
	Name     string `bson:"name"`
	Type     string `bson:"type"`
	Required bool   `bson:"required,omitempty"`
	Store    any    `bson:"store,omitempty"`
}

// BucketStatsResponse answers BUCKET_STATS. WritesRefused is carried as the
// server sent it: no vector pins its type.
type BucketStatsResponse struct {
	Found         bool               `bson:"found"`
	Database      string             `bson:"database,omitempty"`
	Bucket        string             `bson:"bucket,omitempty"`
	Type          string             `bson:"type,omitempty"`
	Entries       int64              `bson:"entries,omitempty"`
	LogicalBytes  int64              `bson:"logical_bytes,omitempty"`
	Indexes       []string           `bson:"indexes,omitempty"`
	Message       string             `bson:"message,omitempty"`
	Stores        []StoreStats       `bson:"stores,omitempty"`
	Encrypted     string             `bson:"encrypted,omitempty"`
	Fields        []FieldDeclaration `bson:"fields,omitempty"`
	FieldTypes    string             `bson:"field_types,omitempty"`
	WritesRefused any                `bson:"writes_refused,omitempty"`
	// ReplicationFactor is the bucket's, and Inherited says it is the database's
	// rather than one the bucket declared; both are absent when the server sends
	// none.
	ReplicationFactor int64 `bson:"replication_factor,omitempty"`
	Inherited         bool  `bson:"inherited,omitempty"`
	// Placement is "whole" or "spread": where the bucket's documents live.
	Placement string `bson:"placement,omitempty"`
	// The owner's view of the bucket's copies (operations.md, "Copies"). Nodes
	// is the replica set, owner first; PreviousOwner and PendingCopy are lines
	// the server adds only in some states. All are absent from an older server.
	InSync        int64         `bson:"in_sync,omitempty"`
	State         string        `bson:"state,omitempty"`
	Nodes         []string      `bson:"nodes,omitempty"`
	NotInSync     []string      `bson:"not_in_sync,omitempty"`
	PendingCopies []PendingCopy `bson:"pending_copies,omitempty"`
	PreviousOwner string        `bson:"previous_owner,omitempty"`
	PendingCopy   string        `bson:"pending_copy,omitempty"`
}

// BucketInfoRequest asks for one bucket's declaration: BUCKET_INFO. It is the
// shape of BucketRequest under the name of the opcode it belongs to.
type BucketInfoRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
}

// StoreField is one declared store field of a bucket in BUCKET_INFO. Engine is
// the suffix-less catalog word: hash, btree, fifo, lifo, heap or blob.
type StoreField struct {
	Name   string `bson:"name"`
	Engine string `bson:"engine"`
}

// BucketInfoResponse answers BUCKET_INFO: the bucket's row of LIST_BUCKETS
// under Bucket, and what each of its store fields is under StoreFields, a
// sibling of Bucket that is absent for a bucket with no store fields. A
// missing bucket or database is an ERROR frame, not Found false.
type BucketInfoResponse struct {
	Found       bool         `bson:"found"`
	Database    string       `bson:"database,omitempty"`
	Bucket      BucketEntry  `bson:"bucket"`
	StoreFields []StoreField `bson:"store_fields,omitempty"`
}

// EncryptionStatus is the encryption part of HEALTH_CHECK.
type EncryptionStatus struct {
	Mode          string   `bson:"mode,omitempty"`
	Keystore      string   `bson:"keystore,omitempty"`
	Sealed        bool     `bson:"sealed,omitempty"`
	Problem       string   `bson:"problem,omitempty"`
	KEKKID        string   `bson:"kek_kid,omitempty"`
	NodeMasterKID string   `bson:"node_master_kid,omitempty"`
	NotSealed     []string `bson:"not_sealed,omitempty"`
}

// HostLoad is the machine's load average, in HealthHost.
type HostLoad struct {
	OneMinute      float64 `bson:"one_minute"`
	FiveMinutes    float64 `bson:"five_minutes"`
	FifteenMinutes float64 `bson:"fifteen_minutes"`
}

// HealthHost is the machine facts of HEALTH_CHECK (handbook/operations.md
// section 4). Only CPUs is always present: every other member is absent when
// the node cannot read it, and a zero here means absent. They are for display;
// a driver decides nothing from them. MemorySource is "cgroup" or "host".
type HealthHost struct {
	CPUs             int64     `bson:"cpus"`
	HostCPUs         int64     `bson:"host_cpus,omitempty"`
	MemoryTotalBytes int64     `bson:"memory_total_bytes,omitempty"`
	MemoryUsedBytes  int64     `bson:"memory_used_bytes,omitempty"`
	MemorySource     string    `bson:"memory_source,omitempty"`
	Load             *HostLoad `bson:"load,omitempty"`
}

// HealthResponse answers HEALTH_CHECK, with the opcode GET_RESPONSE. Capacity
// and Memtable describe the capturing host and are optional; they, and
// Reasons and MembershipDetail, whose shapes no vector pins, are carried as
// the server sent them. Membership is "confirmed", "not added yet",
// "membership refused" or "waiting for founders": a node that is not
// confirmed refuses requests that name a database or bucket. StoreRefusal is
// the server's reason for refusing writes, present only while it does (and
// Healthy is then false).
type HealthResponse struct {
	NodeID           string            `bson:"nodeid"`
	Uptime           string            `bson:"uptime,omitempty"`
	Version          string            `bson:"version,omitempty"`
	BucketCount      int64             `bson:"bucketcount,omitempty"`
	Healthy          bool              `bson:"healthy"`
	Reasons          any               `bson:"reasons,omitempty"`
	Capacity         Document          `bson:"capacity,omitempty"`
	Memtable         Document          `bson:"memtable,omitempty"`
	Encryption       *EncryptionStatus `bson:"encryption,omitempty"`
	Membership       string            `bson:"membership,omitempty"`
	MembershipDetail any               `bson:"membership_detail,omitempty"`
	StoreRefusal     string            `bson:"store_refusal,omitempty"`
	// Host is carried as the BSON value the server sent, so a malformed one
	// cannot fail the whole answer; [HealthResponse.HostFacts] reads it.
	Host Value `bson:"host,omitempty"`
}

// HostFacts reads Host as far as it can be read, the way a driver treats
// Capacity and Memtable: nil when Host is absent, is not a document, or has no
// integer cpus; a member of the wrong type is left at its zero value, and a
// load that is not a document, or has a non-numeric member, is left out or
// partly filled.
func (r HealthResponse) HostFacts() *HealthHost {
	document, isDocument := r.Host.DocumentOK()
	if !isDocument {
		return nil
	}
	cpus, readable := integerMember(document, "cpus")
	if !readable {
		return nil
	}
	host := &HealthHost{CPUs: cpus}
	host.HostCPUs, _ = integerMember(document, "host_cpus")
	host.MemoryTotalBytes, _ = integerMember(document, "memory_total_bytes")
	host.MemoryUsedBytes, _ = integerMember(document, "memory_used_bytes")
	if source, found := document.Lookup("memory_source").StringValueOK(); found {
		host.MemorySource = source
	}
	if load, found := document.Lookup("load").DocumentOK(); found {
		host.Load = &HostLoad{}
		host.Load.OneMinute, _ = numberMember(load, "one_minute")
		host.Load.FiveMinutes, _ = numberMember(load, "five_minutes")
		host.Load.FifteenMinutes, _ = numberMember(load, "fifteen_minutes")
	}
	return host
}

// numberMember reads a member that is an int32, an int64 or a double.
func numberMember(document Document, name string) (float64, bool) {
	value := document.Lookup(name)
	if number, ok := value.DoubleOK(); ok {
		return number, true
	}
	if number, ok := value.Int32OK(); ok {
		return float64(number), true
	}
	number, ok := value.Int64OK()
	return float64(number), ok
}

// integerMember reads a member that holds a whole number.
func integerMember(document Document, name string) (int64, bool) {
	number, ok := numberMember(document, name)
	if !ok || number != math.Trunc(number) {
		return 0, false
	}
	return int64(number), true
}

// NodeEntry is one node of LIST_NODES. Addr is the node's cluster address,
// never its driver port: a driver does not dial it.
type NodeEntry struct {
	NodeID      string    `bson:"nodeid"`
	Addr        string    `bson:"addr"`
	Fingerprint string    `bson:"fingerprint"`
	JoinedAt    time.Time `bson:"joinedat"`
	Status      string    `bson:"status"`
}

// ListNodesRequest is the request of LIST_NODES. History asks the node for
// the member set at each epoch it remembers; the public API does not send it.
type ListNodesRequest struct {
	History bool `bson:"history,omitempty"`
}

// MembershipMember is one member of a MembershipEpoch. Store is the member's
// store name when the node reports one.
type MembershipMember struct {
	ID    string `bson:"id"`
	Addr  string `bson:"addr"`
	Store string `bson:"store,omitempty"`
	// Driver is the member's driver address when the node reports one.
	Driver string `bson:"driver,omitempty"`
}

// MembershipEpoch is the member set at one epoch of the answering node's
// history.
type MembershipEpoch struct {
	Epoch   int64              `bson:"epoch"`
	Members []MembershipMember `bson:"members"`
}

// ListNodesResponse answers LIST_NODES, with the opcode GET_RESPONSE.
// SchemaChanges is the progress of a membership change, carried as sent.
// MembershipHistory is oldest first and HistoryReported says the node reports
// one; both come only when the request set History.
type ListNodesResponse struct {
	Nodes []NodeEntry `bson:"nodes,omitempty"`
	// MemberCount and Epoch describe the member set the node holds; Standalone
	// says the node runs without a cluster.
	MemberCount       int64             `bson:"member_count,omitempty"`
	Epoch             int64             `bson:"epoch,omitempty"`
	Standalone        bool              `bson:"standalone,omitempty"`
	SchemaChanges     Document          `bson:"schema_changes,omitempty"`
	MembershipHistory []MembershipEpoch `bson:"membership_history,omitempty"`
	HistoryReported   bool              `bson:"history_reported,omitempty"`
}
