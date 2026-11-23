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
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// StoreType is the kind of store a bucket is, or a store field is.
type StoreType string

// The six store types. A bucket has exactly one; a field of a bucket's
// documents may be a store of any of the six.
const (
	// FIFO is first in, first out.
	FIFO StoreType = "fifo"
	// LIFO is last in, first out.
	LIFO StoreType = "lifo"
	// Hash is a hash table.
	Hash StoreType = "hash"
	// BTree is a B+ tree with a b-tree index.
	BTree StoreType = "btree"
	// Heap is a heap with a b-tree index.
	Heap StoreType = "heap"
	// Blob holds binary payloads.
	Blob StoreType = "blob"
)

// StoreSpec declares a store field: every document of the bucket carries its
// own store of Type under Name. Stores nests further store fields inside the
// elements of this one.
type StoreSpec struct {
	Name   string
	Type   StoreType
	Stores []StoreSpec
}

// IndexSpec declares a secondary index, on hash buckets only. ExpireAfter, when
// positive, expires a document that long after the date its (single) field
// holds; the server counts whole seconds.
type IndexSpec struct {
	Fields      []string
	Unique      bool
	Name        string
	ExpireAfter time.Duration
}

// TypeMode says how a bucket's declared field types are enforced.
type TypeMode string

// The three modes. A declaration given without a mode is [Strict].
const (
	// Strict refuses a document whose field has the wrong type, with
	// [ErrFieldType]; nothing is stored.
	Strict TypeMode = "strict"
	// Warn accepts such a document and the server logs it.
	Warn TypeMode = "warn"
	// Off records the declaration and enforces nothing.
	Off TypeMode = "off"
)

// FieldSpec declares one field of a bucket's documents. Type is the server's
// word, case-insensitive: any, string, int32, int, double, decimal, number,
// bool, datetime, bsontimestamp, binary, uuid, objectid, object, array, regex
// or null. Members declares the fields of an object, and Elem the element type
// of an array (its Name is ignored).
type FieldSpec struct {
	Name     string
	Type     string
	Required bool
	Members  []FieldSpec
	Elem     *FieldSpec
}

// CreateOption changes what [Client.CreateDatabase] or [Database.CreateBucket]
// asks for.
type CreateOption func(*createConfig) error

// createConfig is the result of the options. bucketOnly names the options that
// mean nothing for a database, so CreateDatabase can refuse them by name.
type createConfig struct {
	ifNotExists       bool
	encrypted         bool
	replicationFactor int
	stores            []StoreSpec
	indexes           []IndexSpec
	typeMode          TypeMode
	fields            []FieldSpec
	placement         Placement
	bucketOnly        []string
}

func applyCreateOptions(opts []CreateOption) (*createConfig, error) {
	config := &createConfig{}
	for _, option := range opts {
		if err := option(config); err != nil {
			return nil, err
		}
	}
	return config, nil
}

func createOptionError(format string, args ...any) error {
	return &Error{Code: CodeInvalidArgument, Op: "create", Message: fmt.Sprintf(format, args...)}
}

// WithIfNotExists makes a create idempotent: a database, or a bucket of the
// same type, that already exists is accepted instead of reported as
// [ErrAlreadyExists]. An existing bucket of another type is still an error,
// and the declarations of the call are not applied to a bucket that exists.
func WithIfNotExists() CreateOption {
	return func(config *createConfig) error {
		config.ifNotExists = true
		return nil
	}
}

// WithEncrypted asks for at-rest encryption of the bucket. A server whose store
// is not encrypted refuses, and says how to turn encryption on.
func WithEncrypted() CreateOption {
	return func(config *createConfig) error {
		config.encrypted = true
		config.bucketOnly = append(config.bucketOnly, "WithEncrypted")
		return nil
	}
}

// WithReplicationFactor sets how many copies of the bucket a cluster keeps.
// It needs a server that advertises the bucket_rf capability; against any
// other the call returns [ErrUnsupportedByServer] and sends nothing. Zero
// leaves it to the node's default.
func WithReplicationFactor(copies int) CreateOption {
	return func(config *createConfig) error {
		if copies < 0 {
			return createOptionError("WithReplicationFactor(%d): a bucket keeps at least one copy; use 0 for the node's default", copies)
		}
		config.replicationFactor = copies
		config.bucketOnly = append(config.bucketOnly, "WithReplicationFactor")
		return nil
	}
}

// Placement says where a hash bucket's documents live in a cluster.
type Placement string

// The two placements. A bucket is [PlacementWhole] unless it is created as
// [PlacementSpread].
const (
	// PlacementWhole keeps the whole bucket on its owner node and its copies.
	PlacementWhole Placement = "whole"
	// PlacementSpread spreads a hash bucket's documents over the cluster by key.
	// Only hash buckets spread; the server refuses it on the other five types.
	PlacementSpread Placement = "spread"
)

// WithPlacement sets where the bucket's documents live. Only hash buckets
// accept [PlacementSpread]; the server's refusal names the type and the fix.
func WithPlacement(placement Placement) CreateOption {
	return func(config *createConfig) error {
		if placement != PlacementWhole && placement != PlacementSpread {
			return createOptionError("WithPlacement(%q): placement is %q or %q", string(placement), string(PlacementWhole), string(PlacementSpread))
		}
		config.placement = placement
		config.bucketOnly = append(config.bucketOnly, "WithPlacement")
		return nil
	}
}

// WithStores declares store fields: every document of the bucket carries its
// own store of each declared type, for example a queue of purchases per user.
func WithStores(stores ...StoreSpec) CreateOption {
	return func(config *createConfig) error {
		config.stores = append(config.stores, stores...)
		config.bucketOnly = append(config.bucketOnly, "WithStores")
		return nil
	}
}

// WithIndexes declares secondary indexes. Indexes exist on hash buckets only.
func WithIndexes(indexes ...IndexSpec) CreateOption {
	return func(config *createConfig) error {
		for _, index := range indexes {
			if index.ExpireAfter < 0 || (index.ExpireAfter > 0 && index.ExpireAfter < time.Second) {
				return createOptionError("WithIndexes: ExpireAfter %v must be zero (never expire) or at least a second; the server counts whole seconds", index.ExpireAfter)
			}
		}
		config.indexes = append(config.indexes, indexes...)
		config.bucketOnly = append(config.bucketOnly, "WithIndexes")
		return nil
	}
}

// WithFieldTypes declares the types of a bucket's fields and how strictly they
// are enforced. The zero mode means [Strict].
func WithFieldTypes(mode TypeMode, fields ...FieldSpec) CreateOption {
	return func(config *createConfig) error {
		switch mode {
		case "", Strict, Warn, Off:
		default:
			return createOptionError("WithFieldTypes: mode %q is not one of hoarddb.Strict, hoarddb.Warn or hoarddb.Off", string(mode))
		}
		config.typeMode = mode
		config.fields = append(config.fields, fields...)
		config.bucketOnly = append(config.bucketOnly, "WithFieldTypes")
		return nil
	}
}

// The wire shapes of the declarations (handbook/operations.md section 4).
type (
	storeWire struct {
		Name   string      `bson:"name"`
		Engine string      `bson:"engine"`
		Fields []storeWire `bson:"fields,omitempty"`
	}
	indexWire struct {
		Fields             []string `bson:"fields"`
		Name               string   `bson:"name,omitempty"`
		Unique             bool     `bson:"unique,omitempty"`
		ExpireAfterSeconds int64    `bson:"expire_after_seconds,omitempty"`
	}
	fieldWire struct {
		Name     string      `bson:"name,omitempty"`
		Type     string      `bson:"type"`
		Required bool        `bson:"required,omitempty"`
		Members  []fieldWire `bson:"members,omitempty"`
		Elem     *fieldWire  `bson:"elem,omitempty"`
	}
	declarationsWire struct {
		Stores     []storeWire `bson:"stores,omitempty"`
		Indices    []indexWire `bson:"indices,omitempty"`
		Fields     []fieldWire `bson:"fields,omitempty"`
		FieldTypes string      `bson:"field_types,omitempty"`
		Placement  string      `bson:"placement,omitempty"`
	}
)

func storeWireOf(spec StoreSpec) storeWire {
	nested := make([]storeWire, 0, len(spec.Stores))
	for _, inner := range spec.Stores {
		nested = append(nested, storeWireOf(inner))
	}
	if len(nested) == 0 {
		nested = nil
	}
	return storeWire{Name: spec.Name, Engine: string(spec.Type), Fields: nested}
}

func fieldWireOf(spec FieldSpec) fieldWire {
	wire := fieldWire{Name: spec.Name, Type: spec.Type, Required: spec.Required}
	for _, member := range spec.Members {
		wire.Members = append(wire.Members, fieldWireOf(member))
	}
	if spec.Elem != nil {
		elem := fieldWireOf(*spec.Elem)
		elem.Name = ""
		wire.Elem = &elem
	}
	return wire
}

func encodeWire(value any) (hoarddbwire.Document, error) {
	encoded, err := codec.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encoding a declaration: %w", err)
	}
	return encoded, nil
}

// declarations encodes the bucket's declarations as the options document, or
// nothing when there are none.
func (config *createConfig) declarations() (hoarddbwire.Document, error) {
	var wire declarationsWire
	for _, store := range config.stores {
		wire.Stores = append(wire.Stores, storeWireOf(store))
	}
	for _, index := range config.indexes {
		wire.Indices = append(wire.Indices, indexWire{
			Fields: index.Fields, Name: index.Name, Unique: index.Unique,
			ExpireAfterSeconds: int64(index.ExpireAfter / time.Second),
		})
	}
	for _, field := range config.fields {
		wire.Fields = append(wire.Fields, fieldWireOf(field))
	}
	if len(config.fields) > 0 {
		wire.FieldTypes = string(config.typeMode)
	} else if config.typeMode != "" {
		// A mode with no fields declares nothing; the server would have nothing
		// to apply it to, so say so rather than send a word it ignores.
		return nil, createOptionError("WithFieldTypes(%q) declares no fields; pass at least one FieldSpec, or leave the option out", string(config.typeMode))
	}
	wire.Placement = string(config.placement)
	if wire.Stores == nil && wire.Indices == nil && wire.Fields == nil && wire.Placement == "" {
		return nil, nil
	}
	return encodeWire(wire)
}
