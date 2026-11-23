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
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	ringwire "github.com/hoarddb/hoarddb-wire"
)

// Update names the changes of one update statement. Each member is a document of field
// name to operand, in the order the caller wrote it, except Unset (field names) and Rename
// (field name to the declared field it moves to). A field is named by at most one
// operation. All of it applies in one write or none of it does.
type Update struct {
	// Set replaces the value of each field.
	Set bson.D
	// Unset removes each field.
	Unset []string
	// Inc adds a number to each field; a missing field takes the amount.
	Inc bson.D
	// Mul multiplies each field; a missing field takes zero of the factor's type.
	Mul bson.D
	// Min and Max keep the smaller or larger of the stored value and the operand.
	Min, Max bson.D
	// Push appends one value to each array field.
	Push bson.D
	// AddToSet appends one value to each array field unless an equal one is there.
	AddToSet bson.D
	// Rename moves the value of each field into the declared field named by its string.
	Rename bson.D
}

// Update changes scalar fields of document id with set, unset, inc, mul, min, max, push,
// addToSet and rename. The document must exist. Retry an update whose
// outcome you do not know with the same [WithRequestID]: an inc or a push is not idempotent
// by value, and the owner answers the retry from its dedupe window.
func (b *Bucket) Update(ctx context.Context, id string, change Update, options ...CallOption) (WriteResult, error) {
	encoded, _, err := encodedID(id)
	if err != nil {
		return WriteResult{}, err
	}
	config := newCallConfig(options)
	reply, err := b.keyed(ctx, options, func(info BucketInfo) (ringwire.RingKeyedRequest, error) {
		fields, err := b.updateFields(info, change)
		if err != nil {
			return ringwire.RingKeyedRequest{}, err
		}
		op := headerOp(info, ringwire.OpUpdate, encoded)
		op.Fields = fields
		if err := b.applyGuard(info, &op, config); err != nil {
			return ringwire.RingKeyedRequest{}, err
		}
		return ringwire.RingKeyedRequest{Op: op}, nil
	})
	if err != nil {
		return WriteResult{}, err
	}
	return writeResult(reply), nil
}

// applyGuard puts the call's guard and return mode on an update or delete op. A guard names
// declared scalar fields, is sorted by field id, and holds at most ringwire.MaxGuardTerms terms.
func (b *Bucket) applyGuard(header BucketInfo, op *ringwire.Op, config callConfig) error {
	switch config.returnMode {
	case "":
	case ReturnAfter:
		if op.Class == ringwire.OpDelete {
			return &Error{Code: ringwire.CodeInvalidArgument, Message: fmt.Sprintf("a delete of %s has no after; use Return(ReturnBefore) to get the document back", b.where())}
		}
		op.Return = ringwire.ReturnAfter
	case ReturnBefore:
		op.Return = ringwire.ReturnBefore
	default:
		return fmt.Errorf("ring: return mode %q is not %q or %q", config.returnMode, ReturnAfter, ReturnBefore)
	}
	if len(config.guard) > ringwire.MaxGuardTerms {
		return &Error{Code: ringwire.CodeInvalidArgument, Message: fmt.Sprintf("a guard on %s names %d fields and the most it may name is %d (ringwire.MaxGuardTerms); split the check into two writes", b.where(), len(config.guard), ringwire.MaxGuardTerms)}
	}
	for _, element := range config.guard {
		field, ok := header.field(element.Key)
		switch {
		case !ok || element.Key == "_id_":
			return undeclaredField{fmt.Sprintf("ring: a guard on bucket %s names %q, which it does not declare. Its scalar fields are: %s. "+
				"Check the spelling; the document id is the id argument, not a field", b.where(), element.Key, header.declared(false))}
		case isStoreType(field.Type):
			return &Error{Code: ringwire.CodeFieldType, Message: fmt.Sprintf("a guard on bucket %s names %q, a %s store. Declared scalars: %s. "+
				"A guard compares scalar fields; to act on a store, claim or ack its elements", b.where(), element.Key, field.Type, header.declared(false))}
		}
		typed, err := typedValue(element.Value)
		if err != nil {
			return fmt.Errorf("ring: guard field %q of %s: %w", element.Key, b.where(), err)
		}
		if slices.ContainsFunc(op.Guard, func(term ringwire.GuardTerm) bool { return term.FieldID == field.ID }) {
			return fmt.Errorf("ring: a guard on %s names %q twice. Name each field once", b.where(), element.Key)
		}
		op.Guard = append(op.Guard, ringwire.GuardTerm{FieldID: field.ID, Value: typed})
	}
	slices.SortFunc(op.Guard, func(a, c ringwire.GuardTerm) int { return int(a.FieldID) - int(c.FieldID) })
	return nil
}

// updateFields turns the statement into update operations in field-id order.
func (b *Bucket) updateFields(info BucketInfo, change Update) ([]ringwire.FieldOp, error) {
	var fields []ringwire.FieldOp
	add := func(code byte, name string, operand []byte) error {
		field, ok := info.field(name)
		switch {
		case !ok || name == "_id_":
			return fmt.Errorf("ring: bucket %s declares no field %q. Its scalar fields are: %s. "+
				"Check the spelling; the document id is the id argument, not a field", b.where(), name, info.declared(false))
		case isStoreType(field.Type):
			return fmt.Errorf("ring: field %q of bucket %s is a %s store, and an update changes scalar fields. "+
				"Change a store with Push, Put, Pop or Take", name, b.where(), field.Type)
		}
		for _, existing := range fields {
			if existing.FieldID == field.ID {
				return fmt.Errorf("ring: field %q of bucket %s is named by two operations. Send one operation for each field", name, b.where())
			}
		}
		fields = append(fields, ringwire.FieldOp{FieldID: field.ID, OpCode: code, Value: operand})
		return nil
	}
	for _, part := range []struct {
		doc  bson.D
		code byte
	}{{change.Set, ringwire.UpdateSet}, {change.Inc, ringwire.UpdateInc}, {change.Mul, ringwire.UpdateMul}, {change.Min, ringwire.UpdateMin},
		{change.Max, ringwire.UpdateMax}, {change.Push, ringwire.UpdatePush}, {change.AddToSet, ringwire.UpdateAddToSet}} {
		for _, element := range part.doc {
			if err := b.refuseModifier(part.code, element); err != nil {
				return nil, err
			}
			typed, err := typedValue(element.Value)
			if err != nil {
				return nil, fmt.Errorf("ring: field %q of %s: %w", element.Key, b.where(), err)
			}
			if err := add(part.code, element.Key, typed); err != nil {
				return nil, err
			}
		}
	}
	for _, name := range change.Unset {
		if err := add(ringwire.UpdateUnset, name, nil); err != nil {
			return nil, err
		}
	}
	for _, element := range change.Rename {
		target, ok := element.Value.(string)
		if !ok {
			return nil, fmt.Errorf("ring: rename of %q in %s needs the target field's name as a string, got %T. Send {%s: \"<declared field>\"}", element.Key, b.where(), element.Value, element.Key)
		}
		to, declared := info.field(target)
		if !declared {
			return nil, fmt.Errorf("ring: rename of %q in %s: %q is not a declared field, and a field cannot get a name the bucket does not declare. "+
				"Declare it first with alter bucket (scalar fields: %s)", element.Key, b.where(), target, info.declared(false))
		}
		if err := add(ringwire.UpdateRenameTo, element.Key, binary.BigEndian.AppendUint16(nil, to.ID)); err != nil {
			return nil, err
		}
	}
	slices.SortStableFunc(fields, func(a, c ringwire.FieldOp) int { return int(a.FieldID) - int(c.FieldID) })
	return fields, nil
}

// refuseModifier refuses a push or addToSet operand that carries modifiers (members whose name
// starts with $), which are not built, by name.
// It reads the BSON the operand marshals to, so a bson.D, a bson.M or a struct are all seen.
func (b *Bucket) refuseModifier(code byte, element bson.E) error {
	if code != ringwire.UpdatePush && code != ringwire.UpdateAddToSet {
		return nil
	}
	kind, raw, err := bson.MarshalValue(element.Value)
	if err != nil || kind != bson.TypeEmbeddedDocument {
		return nil
	}
	inner, err := bson.Raw(raw).Elements()
	if err != nil {
		return nil
	}
	for _, member := range inner {
		if strings.HasPrefix(member.Key(), "$") {
			return fmt.Errorf("ring: %s of %q in %s does not take %s: only one plain value is appended. "+
				"Send the value itself, or one update for each value", map[byte]string{ringwire.UpdatePush: "push", ringwire.UpdateAddToSet: "addToSet"}[code], element.Key, b.where(), member.Key())
		}
	}
	return nil
}
