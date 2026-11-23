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

// Package codec is the one place this module touches a BSON library.
//
// Everything that crosses the wire is BSON; the rest of the driver calls
// [Marshal] and [Unmarshal] and never names the library behind them, so the
// library can be replaced without an API change (handbook/api-go.md section
// 4). No type from it appears in a public signature.
package codec

import (
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// Marshal encodes value, a struct with bson tags or a map, as one BSON
// document.
func Marshal(value any) ([]byte, error) {
	encoded, err := bson.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encoding BSON: %w", err)
	}
	return encoded, nil
}

// Unmarshal decodes one BSON document into target, a pointer to a struct with
// bson tags or to a map. An empty payload is a legal wire message (PONG, and
// an AUTH_FAIL from an older server) and leaves target untouched.
func Unmarshal(payload []byte, target any) error {
	if len(payload) == 0 {
		return nil
	}
	if err := bson.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decoding BSON: %w", err)
	}
	return nil
}
