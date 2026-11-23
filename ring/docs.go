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

package ring

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	ringwire "github.com/hoarddb/hoarddb-wire"
)

// Get reads document id and returns it as stored: its scalar fields, not its stores. A
// missing document is an error wrapping [ErrNotFound].
func (b *Bucket) Get(ctx context.Context, id string) (bson.Raw, error) {
	return b.readDocument(ctx, id, ringwire.RingClassGet, nil)
}

// readDocument sends a get (class RingClassGet, no fields) or a get_fields.
func (b *Bucket) readDocument(ctx context.Context, id string, class byte, fields []string) (bson.Raw, error) {
	encoded, _, err := encodedID(id)
	if err != nil {
		return nil, err
	}
	reply, err := b.keyed(ctx, nil, func(info BucketInfo) (ringwire.RingKeyedRequest, error) {
		read := readHeader(info, class, encoded)
		read.Fields = fields
		return ringwire.RingKeyedRequest{Read: &read}, nil
	})
	if err != nil {
		return nil, err
	}
	if reply.Found == nil || !*reply.Found || len(reply.Elements) != 1 {
		return nil, fmt.Errorf("ring: no document %q in bucket %s. Insert it first, or list the ids with List: %w", id, b.where(), ErrNotFound)
	}
	return bson.Raw(reply.Elements[0]), nil
}

// GetFields is Get returning only _id_ and the named scalar fields, in field-id order. A
// name the bucket does not declare, or that is a store field, is a FIELD_TYPE error that
// names the fix. A scalar the document never set is absent from the answer.
func (b *Bucket) GetFields(ctx context.Context, id string, fields []string) (bson.Raw, error) {
	return b.readDocument(ctx, id, ringwire.RingClassGetFields, fields)
}

// GetExpanded reads document id with the documents its fields point at, in one call. fields
// names the scalars to return (nil: every declared scalar); expand names the far documents to
// read, as in ExpandRequest; local lets a ready copy on the node answer. It returns the
// document and the expansion document (one member per expanded field, each {expanded: true,
// doc} or {expanded: false, reason}). A where that the document does not meet is ErrNotFound.
func (b *Bucket) GetExpanded(ctx context.Context, id string, fields []string, expand []ExpandRequest, local bool) (document, expanded bson.Raw, err error) {
	encoded, _, err := encodedID(id)
	if err != nil {
		return nil, nil, err
	}
	options, err := readOptions(expand, local)
	if err != nil {
		return nil, nil, err
	}
	reply, err := b.keyed(ctx, nil, func(bucketInfo BucketInfo) (ringwire.RingKeyedRequest, error) {
		read := readHeader(bucketInfo, ringwire.RingClassGetFields, encoded)
		read.Fields = fields
		if read.Fields == nil {
			for _, declared := range bucketInfo.Fields {
				if declared.Name != "_id_" && !strings.HasSuffix(declared.Type, "Store") {
					read.Fields = append(read.Fields, declared.Name)
				}
			}
		}
		read.Options = options
		return ringwire.RingKeyedRequest{Read: &read}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	if reply.Found == nil || !*reply.Found || len(reply.Elements) != 1 {
		return nil, nil, fmt.Errorf("ring: no document %q in bucket %s, or its expansion did not match the where. Insert it first, or list the ids with List: %w", id, b.where(), ErrNotFound)
	}
	if len(reply.Expanded) == 1 {
		expanded = bson.Raw(reply.Expanded[0])
	}
	return bson.Raw(reply.Elements[0]), expanded, nil
}

// storeVerb sends one push, pop, put, inc or take on a store field of document id.
func (b *Bucket) storeVerb(ctx context.Context, id, field string, verb byte, argument func() ([]byte, error), options []CallOption) (ringwire.RingReply, error) {
	encoded, _, err := encodedID(id)
	if err != nil {
		return ringwire.RingReply{}, err
	}
	arg, err := argument()
	if err != nil {
		return ringwire.RingReply{}, fmt.Errorf("ring: building the argument for %s of %q: %w", field, id, err)
	}
	return b.keyed(ctx, options, func(info BucketInfo) (ringwire.RingKeyedRequest, error) {
		declared, err := b.storeField(info, field)
		if err != nil {
			return ringwire.RingKeyedRequest{}, err
		}
		op := headerOp(info, ringwire.OpStoreVerb, encoded)
		op.Fields = []ringwire.FieldOp{{FieldID: declared.ID, Verb: verb, Value: arg}}
		return ringwire.RingKeyedRequest{Op: op}, nil
	})
}

func noArgument() ([]byte, error) { return nil, nil }

// Push appends element, a BSON document (a bson.D, a struct, a bson.Raw ...), to the
// fifoStore or lifoStore field of document id.
func (b *Bucket) Push(ctx context.Context, id, field string, element any, options ...CallOption) (WriteResult, error) {
	reply, err := b.storeVerb(ctx, id, field, ringwire.VerbPush, func() ([]byte, error) {
		raw, err := elementBytes(element)
		if err != nil {
			return nil, err
		}
		return ringwire.ElementArg(raw)
	}, options)
	if err != nil {
		return WriteResult{}, err
	}
	return writeResult(reply), nil
}

// Put stores element under key in the hashStore or btreeStore field of document id,
// replacing an element that has the key.
func (b *Bucket) Put(ctx context.Context, id, field, key string, element any, options ...CallOption) (WriteResult, error) {
	reply, err := b.storeVerb(ctx, id, field, ringwire.VerbPut, func() ([]byte, error) {
		raw, err := elementBytes(element)
		if err != nil {
			return nil, err
		}
		return ringwire.PutArg(key, raw)
	}, options)
	if err != nil {
		return WriteResult{}, err
	}
	return writeResult(reply), nil
}

// Inc adds to the numbers of the element stored under key in the hashStore or btreeStore field of
// document id, creating the element when the key is absent. increments maps dotted key paths of at
// most three levels ("d.14.t") to the amount to add, which is an int, int32, int64 or float64: a
// counter may go negative, and a decimal is refused, so keep money in integer minor units. It is one
// write at the owner, so two callers never lose an add. A retry with the same request id is answered from
// the recorded result only while the server remembers it, the last 8192 writes of the pair by default; after
// that it adds again, so read the element before resending an old request. Use [Bucket.IncAfter] to read the
// element back.
func (b *Bucket) Inc(ctx context.Context, id, field, key string, increments map[string]any, options ...CallOption) (WriteResult, error) {
	reply, err := b.incVerb(ctx, id, field, key, increments, false, options)
	if err != nil {
		return WriteResult{}, err
	}
	return writeResult(reply), nil
}

// IncAfter is [Bucket.Inc] returning the element as it stands after the add. On a [Batch] the
// element comes back in the [BatchAnswer] and the returned element is nil.
func (b *Bucket) IncAfter(ctx context.Context, id, field, key string, increments map[string]any, options ...CallOption) (bson.Raw, WriteResult, error) {
	reply, err := b.incVerb(ctx, id, field, key, increments, true, options)
	if err != nil {
		return nil, WriteResult{}, err
	}
	written := writeResult(reply)
	if b.batch != nil || len(reply.Elements) != 1 {
		return nil, written, nil
	}
	return bson.Raw(reply.Elements[0]), written, nil
}

func (b *Bucket) incVerb(ctx context.Context, id, field, key string, increments map[string]any, returnAfter bool, options []CallOption) (ringwire.RingReply, error) {
	return b.storeVerb(ctx, id, field, ringwire.VerbInc, func() ([]byte, error) {
		raw, err := incrementsBytes(increments)
		if err != nil {
			return nil, err
		}
		return ringwire.IncArg(key, raw, returnAfter)
	}, options)
}

// incrementsBytes is the increments document of an inc: the dotted paths in sorted order, so one
// call has one byte form, each with a number the owner can add.
func incrementsBytes(increments map[string]any) ([]byte, error) {
	paths := make([]string, 0, len(increments))
	for path := range increments {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	doc := make(bson.D, 0, len(paths))
	for _, path := range paths {
		switch amount := increments[path].(type) {
		case int:
			doc = append(doc, bson.E{Key: path, Value: int64(amount)})
		case int32, int64, float64:
			doc = append(doc, bson.E{Key: path, Value: amount})
		default:
			return nil, fmt.Errorf("inc takes numbers; a decimal is refused, store money in integer minor units. The amount for %q is a %T: send an int, int32, int64 or float64", path, increments[path])
		}
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encoding the increments: %w", err)
	}
	return raw, nil
}

// removed is the element a pop or take removed, or ErrNotFound when there was none.
func (b *Bucket) removed(reply ringwire.RingReply, id, field, what string) (bson.Raw, error) {
	if b.batch != nil {
		return nil, nil // queued on a Batch: the element comes back in its BatchAnswer
	}
	if reply.Found == nil || !*reply.Found || len(reply.Elements) != 1 {
		return nil, fmt.Errorf("ring: %s of field %q of %q in bucket %s found nothing: %w", what, field, id, b.where(), ErrNotFound)
	}
	return bson.Raw(reply.Elements[0]), nil
}

// Pop removes and returns the next element of a fifoStore (the oldest) or lifoStore (the
// newest) field. An empty store is an error wrapping [ErrNotFound]; use Peek or StoreCount
// to look without removing.
func (b *Bucket) Pop(ctx context.Context, id, field string, options ...CallOption) (bson.Raw, error) {
	reply, err := b.storeVerb(ctx, id, field, ringwire.VerbPop, noArgument, options)
	if err != nil {
		return nil, err
	}
	return b.removed(reply, id, field, "pop")
}

// PopN removes and returns up to n elements (1 to 100) of a fifoStore, lifoStore or heapStore
// field in one write, in the order n pops would take them. An empty store gives an empty
// slice and no error. The elements must fit the server's dedupe entry (about 1 KiB together);
// a larger pop is refused with the count that fits, and claim and ack carry no such limit.
func (b *Bucket) PopN(ctx context.Context, id, field string, n int, options ...CallOption) ([]bson.Raw, error) {
	if n < 1 || n > maxPeekCount {
		return nil, fmt.Errorf("ring: PopN asks for %d elements; ask for 1 to %d", n, maxPeekCount)
	}
	reply, err := b.storeVerb(ctx, id, field, ringwire.VerbPop, func() ([]byte, error) { return ringwire.PopArg(n) }, options)
	if err != nil {
		return nil, err
	}
	return rawList(reply.Elements), nil
}

// Take removes and returns the element stored under key in a hashStore or btreeStore field.
// An absent key is an error wrapping [ErrNotFound].
func (b *Bucket) Take(ctx context.Context, id, field, key string, options ...CallOption) (bson.Raw, error) {
	reply, err := b.storeVerb(ctx, id, field, ringwire.VerbTake, func() ([]byte, error) { return ringwire.TakeKeyArg(key) }, options)
	if err != nil {
		return nil, err
	}
	return b.removed(reply, id, field, "take")
}
