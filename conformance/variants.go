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

package conformance

import (
	"encoding/json"
	"math"
	"strconv"
)

// ExtraMember is the name of the member WithExtraMembers adds: one no server
// has sent.
const ExtraMember = "zz_member_a_newer_server_added"

// WithExtraMembers returns a copy of the tree with an unknown member added to
// every document the driver models, as a newer server may do
// (handbook/wire.md section 3). Documents the driver carries raw are left as
// they are: they are the caller's, not the driver's.
func (shape Shape) WithExtraMembers(tree map[string]any) map[string]any {
	varied, _ := shape.vary(tree, false, func(node map[string]any) map[string]any {
		node[ExtraMember] = "added"
		return node
	}, nil).(map[string]any)
	return varied
}

// WithIntegerWidth returns a copy of the tree with every integer outside the
// raw documents rewritten to the given BSON type, "int32", "int64" or
// "double", where the value fits it. A datetime's milliseconds stay as they
// are: that member is not an integer field.
func (shape Shape) WithIntegerWidth(tree map[string]any, width string) map[string]any {
	varied, _ := shape.vary(tree, false, nil, func(text string) map[string]any {
		return rewriteInteger(text, width)
	}).(map[string]any)
	return varied
}

func rewriteInteger(text, width string) map[string]any {
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return nil
	}
	switch width {
	case "int32":
		if value >= math.MinInt32 && value <= math.MaxInt32 {
			return map[string]any{"$numberInt": text}
		}
	case "int64":
		return map[string]any{"$numberLong": text}
	case "double":
		if math.Abs(float64(value)) <= 1<<53 {
			return map[string]any{"$numberDouble": strconv.FormatFloat(float64(value), 'f', 1, 64)}
		}
	}
	return nil
}

// vary copies a tree, applying onDocument to each modelled document and
// onInteger to each integer wrapper. Either may be nil.
func (shape Shape) vary(value any, raw bool, onDocument func(map[string]any) map[string]any, onInteger func(string) map[string]any) any {
	switch node := value.(type) {
	case map[string]any:
		if len(node) == 1 {
			for key, member := range node {
				if key == "$date" {
					return node
				}
				if text, isText := member.(string); isText && !raw && onInteger != nil && (key == "$numberInt" || key == "$numberLong") {
					if rewritten := onInteger(text); rewritten != nil {
						return rewritten
					}
					return node
				}
			}
		}
		out := make(map[string]any, len(node)+1)
		for key, member := range node {
			if key != "" && key[0] == '$' {
				out[key] = member // the inside of an Extended JSON wrapper
				continue
			}
			out[key] = shape.vary(member, raw || shape.documents[key], onDocument, onInteger)
		}
		if !raw && onDocument != nil && !isWrapper(node) {
			out = onDocument(out)
		}
		return out
	case []any:
		out := make([]any, len(node))
		for index, element := range node {
			out[index] = shape.vary(element, raw, onDocument, onInteger)
		}
		return out
	default:
		return value
	}
}

func isWrapper(node map[string]any) bool {
	for key := range node {
		if len(key) > 0 && key[0] == '$' {
			return true
		}
	}
	return false
}

// ExtendedJSON renders a tree as canonical Extended JSON text.
func ExtendedJSON(tree map[string]any) ([]byte, error) { return json.Marshal(tree) }
