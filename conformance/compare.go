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
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// A tree is a BSON document read from canonical Extended JSON: maps, slices,
// strings, bools, json.Number and nil, with the Extended JSON wrappers
// ({"$numberInt": ...}, {"$binary": ...}, {"$date": ...}) as one-key maps.

// ParseExtended reads canonical Extended JSON into a tree. Null and empty
// input are the empty document, which is what an absent payload_json means.
func ParseExtended(raw []byte) (map[string]any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return map[string]any{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var tree map[string]any
	if err := decoder.Decode(&tree); err != nil {
		return nil, fmt.Errorf("reading Extended JSON: %w", err)
	}
	return tree, nil
}

// WireTree reads a BSON payload, as it came off the wire, into a tree. An
// empty payload is the empty document.
func WireTree(payload []byte) (map[string]any, error) {
	if len(payload) == 0 {
		return map[string]any{}, nil
	}
	rendered, err := codec.ToExtendedJSON(payload)
	if err != nil {
		return nil, err
	}
	return ParseExtended(rendered)
}

// Shape says which members of a payload are documents the driver carries raw,
// byte for byte: a stored document, a filter, an operator, a declaration.
// Inside one, nothing is normalised and nothing is added: an int32 stays an
// int32 and an empty string stays a member. Every other member is one the
// driver models in a typed value.
//
// Which members are raw depends on the payload, because a name means a
// document in one and a typed list in another: "items" is the documents of a
// bucket peek, pop or claim and the entries of a store-field peek.
type Shape struct{ documents map[string]bool }

// Members that carry documents in every request and every answer.
var (
	requestDocuments = []string{
		"data", "filter", "sort", "projection", "options", "stores", "metadata", "path", "after",
		"set", "inc", "push", "addtoset", "mul", "min", "max", "rename",
	}
	responseDocuments = []string{
		"data", "doc", "documents", "results", "value", "element", "metadata", "values",
		"capacity", "memtable", "schema_changes",
	}
)

// ShapeOf is the shape of the request or the answer of the operation named
// opcodeName, as a vector spells it ("PUT", "FIFO_PEEK").
func ShapeOf(opcodeName string, response bool) Shape {
	names := requestDocuments
	if response {
		names = responseDocuments
	}
	documents := make(map[string]bool, len(names)+1)
	for _, name := range names {
		documents[name] = true
	}
	switch opcodeName {
	case "FIFO_PEEK", "LIFO_PEEK", "FIFO_POP", "LIFO_POP", "HEAP_POP", "QUEUE_CLAIM":
		documents["items"] = response // the stored documents a peek, pop or claim answers
	}
	return Shape{documents: documents}
}

// Normalise rewrites a tree so that two trees that mean the same compare
// equal under the rules of handbook/testing.md section 1.1: integers are equal
// by value whatever their width, and a member whose value is empty ("", null,
// 0, false, an empty array or document) is the same as an absent one.
// Documents the shape calls raw are kept as they are.
func (shape Shape) Normalise(tree map[string]any) map[string]any {
	normalised, _ := shape.normaliseValue(tree, false).(map[string]any)
	return normalised
}

func (shape Shape) normaliseValue(value any, raw bool) any {
	switch node := value.(type) {
	case map[string]any:
		if !raw {
			if number, ok := wrappedNumber(node); ok {
				return number
			}
		}
		out := make(map[string]any, len(node))
		for key, member := range node {
			normalised := shape.normaliseValue(member, raw || shape.documents[key])
			if !raw && isEmpty(normalised) {
				continue
			}
			out[key] = normalised
		}
		return out
	case []any:
		out := make([]any, len(node))
		for index, element := range node {
			out[index] = shape.normaliseValue(element, raw)
		}
		return out
	default:
		return value
	}
}

// wrappedNumber folds {"$numberInt": "5"}, {"$numberLong": "5"} and a
// {"$numberDouble": "5.0"} that holds an integer to the one json.Number 5.
func wrappedNumber(node map[string]any) (json.Number, bool) {
	if len(node) != 1 {
		return "", false
	}
	for key, member := range node {
		text, isText := member.(string)
		if !isText {
			return "", false
		}
		switch key {
		case "$numberInt", "$numberLong":
			return json.Number(text), true
		case "$numberDouble":
			parsed, err := strconv.ParseFloat(text, 64)
			if err != nil || parsed != math.Trunc(parsed) || math.Abs(parsed) > 1<<53 {
				return "", false
			}
			return json.Number(strconv.FormatInt(int64(parsed), 10)), true
		}
	}
	return "", false
}

func isEmpty(value any) bool {
	switch node := value.(type) {
	case nil:
		return true
	case string:
		return node == ""
	case bool:
		return !node
	case json.Number:
		parsed, err := strconv.ParseFloat(string(node), 64)
		return err == nil && parsed == 0
	case []any:
		return len(node) == 0
	case map[string]any:
		return len(node) == 0
	default:
		return false
	}
}

// Diff compares two trees and returns "" when they are equal, otherwise the
// first difference found, named by its path. Map members are compared by name,
// never by order.
func Diff(want, got any) string { return diffAt("", want, got) }

func diffAt(path string, want, got any) string {
	switch wanted := want.(type) {
	case map[string]any:
		gotten, ok := got.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: want a document, got %s", display(path), describe(got))
		}
		for _, key := range sortedKeys(wanted) {
			member, present := gotten[key]
			if !present {
				return fmt.Sprintf("%s: the driver's value lacks the member %q that the vector has (%s)", display(path), key, describe(wanted[key]))
			}
			if difference := diffAt(join(path, key), wanted[key], member); difference != "" {
				return difference
			}
		}
		for _, key := range sortedKeys(gotten) {
			if _, present := wanted[key]; !present {
				return fmt.Sprintf("%s: the driver's value has the member %q (%s) that the vector does not", display(path), key, describe(gotten[key]))
			}
		}
		return ""
	case []any:
		gotten, ok := got.([]any)
		if !ok {
			return fmt.Sprintf("%s: want an array, got %s", display(path), describe(got))
		}
		if len(wanted) != len(gotten) {
			return fmt.Sprintf("%s: want %d elements, got %d", display(path), len(wanted), len(gotten))
		}
		for index := range wanted {
			if difference := diffAt(fmt.Sprintf("%s[%d]", path, index), wanted[index], gotten[index]); difference != "" {
				return difference
			}
		}
		return ""
	default:
		if fmt.Sprint(want) != fmt.Sprint(got) || fmt.Sprintf("%T", want) != fmt.Sprintf("%T", got) {
			return fmt.Sprintf("%s: want %s, got %s", display(path), describe(want), describe(got))
		}
		return ""
	}
}

func describe(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	if len(encoded) > 120 {
		return string(encoded[:120]) + "..."
	}
	return string(encoded)
}

func display(path string) string {
	if path == "" {
		return "the document"
	}
	return path
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func sortedKeys(tree map[string]any) []string {
	keys := make([]string, 0, len(tree))
	for key := range tree {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TopLevelNames is the sorted member names of a document.
func TopLevelNames(tree map[string]any) []string { return sortedKeys(tree) }

// A vary path names values that differ from run to run: "seq",
// "data._id_", "nodes[].joinedat", where [] is every element of an array.
// StripVary removes every value the paths name, in place.
func StripVary(tree map[string]any, paths []string) {
	for _, path := range paths {
		base, each := elementwise(path)
		visit(tree, strings.Split(base, "."), "", func(parent map[string]any, key, _ string) {
			if !each {
				delete(parent, key)
				return
			}
			// A path that ends in [] names every element of a list of plain
			// values (item_ids[]): the list keeps its length, its values go.
			if elements, isList := parent[key].([]any); isList {
				for index := range elements {
					elements[index] = nil
				}
			}
		})
	}
}

// elementwise splits a vary path that ends in [] into the list's own path and
// true: the path names each plain value of the list, not a member of each.
func elementwise(path string) (string, bool) {
	base, each := strings.CutSuffix(path, "[]")
	return base, each
}

// CheckVary checks what the vector promises about a varying value instead of
// its content: wherever the vector's own answer (want) has a value at a vary
// path, the answer under test (got) has one at the same place, of the same
// BSON type. Both are trees from the wire (WireTree), not normalised.
func CheckVary(want, got map[string]any, paths []string) error {
	return checkVary(want, got, paths, false)
}

// CheckVaryTyped is CheckVary for a tree the driver wrote back from its typed
// value, which proves the typed field exists and carries the right kind of
// value. A driver's integer type is one width, so the three numeric types are
// one kind here, and an empty value (an empty array, say) may be absent from
// the typed tree, as any empty member may.
func CheckVaryTyped(want, got map[string]any, paths []string) error {
	return checkVary(want, got, paths, true)
}

func checkVary(want, got map[string]any, paths []string, typed bool) error {
	for _, path := range paths {
		var failure error
		base, each := elementwise(path)
		check := func(wanted any, concrete string) {
			if failure != nil {
				return
			}
			if typed && isEmpty(normaliseValue0(wanted)) {
				return
			}
			counterpart, found := lookup(got, concrete)
			if !found {
				failure = fmt.Errorf("varying value %s is absent from the answer", concrete)
				return
			}
			expected, actual := bsonType(wanted), bsonType(counterpart)
			if typed {
				expected, actual = numberKind(expected), numberKind(actual)
			}
			if expected != actual {
				failure = fmt.Errorf("varying value %s has BSON type %s, the vector has %s", concrete, actual, expected)
			}
		}
		visit(want, strings.Split(base, "."), "", func(parent map[string]any, key, concrete string) {
			if !each {
				check(parent[key], concrete)
				return
			}
			elements, _ := parent[key].([]any)
			for index, element := range elements {
				check(element, fmt.Sprintf("%s[%d]", concrete, index))
			}
		})
		if failure != nil {
			return failure
		}
	}
	return nil
}

func numberKind(name string) string {
	switch name {
	case "numberInt", "numberLong", "numberDouble":
		return "number"
	}
	return name
}

// normaliseValue0 folds the number wrappers of one value so isEmpty can judge
// it, without a shape: a varying value is judged whole.
func normaliseValue0(value any) any { return Shape{}.normaliseValue(value, false) }

// visit calls fn for each member a path names, with the member's parent, its
// key and its concrete path (every [] replaced by the element's index).
func visit(node any, segments []string, concrete string, fn func(parent map[string]any, key, concrete string)) {
	if len(segments) == 0 {
		return
	}
	document, ok := node.(map[string]any)
	if !ok {
		return
	}
	segment := segments[0]
	each := strings.HasSuffix(segment, "[]")
	key := strings.TrimSuffix(segment, "[]")
	member, present := document[key]
	if !present {
		return
	}
	here := join(concrete, key)
	if !each {
		if len(segments) == 1 {
			fn(document, key, here)
			return
		}
		visit(member, segments[1:], here, fn)
		return
	}
	elements, ok := member.([]any)
	if !ok {
		return
	}
	for index, element := range elements {
		at := fmt.Sprintf("%s[%d]", here, index)
		if len(segments) == 1 {
			continue
		}
		visit(element, segments[1:], at, fn)
	}
}

// lookup finds the value at a concrete path ("nodes[1].joinedat").
func lookup(tree any, concrete string) (any, bool) {
	current := tree
	for _, part := range strings.Split(concrete, ".") {
		name, indexes := part, []int(nil)
		for strings.HasSuffix(name, "]") {
			open := strings.LastIndex(name, "[")
			index, err := strconv.Atoi(name[open+1 : len(name)-1])
			if err != nil {
				return nil, false
			}
			indexes = append([]int{index}, indexes...)
			name = name[:open]
		}
		document, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		if current, ok = document[name]; !ok {
			return nil, false
		}
		for _, index := range indexes {
			elements, ok := current.([]any)
			if !ok || index >= len(elements) {
				return nil, false
			}
			current = elements[index]
		}
	}
	return current, true
}

// bsonType names the BSON type of a tree value.
func bsonType(value any) string {
	switch node := value.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "bool"
	case []any:
		return "array"
	case map[string]any:
		if len(node) == 1 {
			for key := range node {
				if strings.HasPrefix(key, "$") {
					return strings.TrimPrefix(key, "$")
				}
			}
		}
		return "document"
	default:
		return fmt.Sprintf("%T", value)
	}
}
