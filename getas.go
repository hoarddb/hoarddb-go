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
	"fmt"
)

// GetAs is [Bucket.Get] followed by [Document.Decode]: the document stored
// under key, decoded into a T, which is a struct with `bson:"..."` tags or a
// [Map]. A key that is not there is [ErrNotFound] with the zero T, and a
// document that does not fit T is [ErrInvalidArgument] naming the key, with the
// decoder's complaint as the cause. It is a function because Go methods take no
// type parameters. Options are those of [Bucket.Get], [Consistent] among them.
func GetAs[T any](ctx context.Context, b *Bucket, key string, opts ...ReadOption) (T, error) {
	var decoded T
	stored, err := b.Get(ctx, key, opts...)
	if err != nil {
		return decoded, err
	}
	if err := stored.Decode(&decoded); err != nil {
		var zero T
		target := b.keyPath(key)
		return zero, &Error{Code: CodeInvalidArgument, Op: "get", Target: target, Err: err,
			Message: fmt.Sprintf("the document %s does not decode into %T (%v). Check the bson tags of %T against the stored fields, "+
				"or read it as a hoarddb.Map to see what is there", target, decoded, err, decoded)}
	}
	return decoded, nil
}
