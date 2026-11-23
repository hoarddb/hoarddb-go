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
	"strings"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// blobConfig is the result of the [BlobOption]s of one call.
type blobConfig struct {
	metadata    any
	hasMetadata bool
	write       writeConfig
}

// BlobOption changes a blob write: [Bucket.PutBlob] and [FieldStore.PutBlob].
// [WithMetadata] is one; every [WriteOption] is one too.
type BlobOption interface{ applyBlob(*blobConfig) }

// blobOptionFunc is a BlobOption that only a blob write takes.
type blobOptionFunc func(*blobConfig)

func (option blobOptionFunc) applyBlob(config *blobConfig) { option(config) }

// WithMetadata stores a document of metadata with the blob: a struct with
// `bson:"..."` tags, a [Map] or a [Document].
func WithMetadata(metadata any) BlobOption {
	return blobOptionFunc(func(config *blobConfig) { config.metadata, config.hasMetadata = metadata, true })
}

// StoredBlob is a blob as it was read back.
type StoredBlob struct {
	// Key is the element key the blob is stored under and BlobID the server's
	// id for the payload.
	Key, BlobID string
	// Data is the blob's bytes and Size their number.
	Data []byte
	Size int64
	// Metadata is the document stored with the blob, nil when there was none.
	Metadata Document
}

// BlobReceipt is the answer to a blob write: the [Receipt], whose ID is the
// element key, and the blob's id and size.
type BlobReceipt struct {
	Receipt
	BlobID string
	Size   int64
}

// blobTooBigAdvice is the remedy for a blob that cannot fit one frame: the
// server's own blob limit is at most a frame less 64 KiB, so no setting makes it
// fit.
const blobTooBigAdvice = "A blob travels in one frame, so it cannot be larger than that. Store the content in pieces under several keys"

// PutBlob stores content under key in a blob bucket, with the metadata of
// [WithMetadata] when given, replacing what is there, at the write concern of
// [WithWriteConcern] when given. The receipt's ID is key.
// It travels on the bulk lane, so a large blob does not hold up point reads.
//
// The driver enforces one limit itself: content that cannot fit one 32 MiB
// frame is refused with [ErrInvalidArgument] before anything is sent. The
// server's own blob limit (16 MiB by default, set by its operator, which a
// driver cannot learn) is the server's refusal, passed through with its
// sentence. A write that fails with an unknown outcome is never resent.
func (b *Bucket) PutBlob(ctx context.Context, key string, content []byte, opts ...BlobOption) (BlobReceipt, error) {
	target := b.keyPath(key)
	var config blobConfig
	for _, option := range opts {
		option.applyBlob(&config)
	}
	if err := config.write.refusePriority("put blob", target); err != nil {
		return BlobReceipt{}, err
	}
	concern, err := b.db.client.writeConcern("put blob", target, config.write)
	if err != nil {
		return BlobReceipt{}, err
	}
	request := hoarddbwire.BlobWriteRequest{Database: b.db.name, Bucket: b.name, Key: key, Data: content, WriteConcern: concern}
	if config.hasMetadata {
		encoded, err := encodeDocument("put blob", target, config.metadata)
		if err != nil {
			return BlobReceipt{}, err
		}
		request.Metadata = encoded
	}
	var answer hoarddbwire.BlobWriteResponse
	err = b.db.client.do(ctx, call{op: "put blob", target: target, opcode: hoarddbwire.OpBlobWrite, answer: hoarddbwire.OpBlobWrite,
		lane: laneBulk, request: request, response: &answer, oversizeAdvice: blobTooBigAdvice})
	if err != nil {
		return BlobReceipt{}, err
	}
	if !answer.Success {
		return BlobReceipt{}, writeRefusal("put blob", target, answer.Message, answer.WriteReport)
	}
	receipt := Receipt{ID: key}
	receipt.CopiesConfirmed, receipt.ReplicationFactor = copiesOf(answer.WriteReport)
	return BlobReceipt{Receipt: receipt, BlobID: answer.BlobID, Size: answer.Size}, nil
}

// GetBlob reads the blob stored under key in a blob bucket: its bytes, size,
// id and metadata. A key that is not there is [ErrNotFound]; GetBlob never
// returns an empty blob for a miss. It travels on the bulk lane.
func (b *Bucket) GetBlob(ctx context.Context, key string) (*StoredBlob, error) {
	target := b.keyPath(key)
	var answer hoarddbwire.BlobReadResponse
	err := b.db.client.do(ctx, call{op: "get blob", target: target, opcode: hoarddbwire.OpBlobRead, answer: hoarddbwire.OpBlobReadResp,
		lane: laneBulk, request: hoarddbwire.KeyRequest{Database: b.db.name, Bucket: b.name, Key: key}, response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Found {
		// The blob's absence message names the key and the bucket
		// (handbook/errors.md section 3); any other found:false is a refusal.
		if strings.Contains(answer.Message, messageNotFoundIn) {
			return nil, &Error{Code: CodeNotFound, Op: "get blob", Target: target, Message: answer.Message}
		}
		return nil, refusal("get blob", target, answer.Message)
	}
	return &StoredBlob{Key: key, BlobID: answer.BlobID, Data: answer.Data, Size: answer.Size, Metadata: Document(answer.Metadata)}, nil
}
