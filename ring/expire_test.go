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
	"errors"
	"fmt"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"
)

func TestIsExpiredReadsTheStructuredDetailNotTheText(t *testing.T) {
	expired := &Error{Code: ringwire.CodeNotFound, Message: "any words at all", Detail: &ringwire.Detail{Expired: true}}
	plain := &Error{Code: ringwire.CodeNotFound, Message: "document \"x\" expired somewhere in this sentence"}
	if !IsExpired(fmt.Errorf("get: %w", expired)) {
		t.Error("a NOT_FOUND with detail expired was not reported expired")
	}
	if IsExpired(plain) {
		t.Error("a plain NOT_FOUND whose text mentions expired was reported expired: a driver acts on the detail, never the text")
	}
	if IsExpired(errors.New("other")) || IsExpired(nil) {
		t.Error("a non-refusal was reported expired")
	}
	if !errors.Is(expired, ErrNotFound) {
		t.Error("an expired miss is still ErrNotFound")
	}
}
