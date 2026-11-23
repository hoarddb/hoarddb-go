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
	"errors"
	"fmt"
	"slices"
)

// writeConfig is the result of the [WriteOption]s of one call.
type writeConfig struct {
	priority    int64
	hasPriority bool
	concern     WriteConcern
	hasConcern  bool
}

// WriteOption changes one write: Put, Insert, InsertMany, Delete, Update, Push
// and Pop on a [Bucket] or a [FieldStore]. Every WriteOption is a
// [BlobOption] too, so a blob write takes [WithWriteConcern].
type WriteOption func(*writeConfig)

// applyBlob applies the option to a blob write's write settings.
func (option WriteOption) applyBlob(config *blobConfig) { option(&config.write) }

func applyWriteOptions(opts []WriteOption) writeConfig {
	var config writeConfig
	for _, option := range opts {
		option(&config)
	}
	return config
}

// WithPriority sets the priority of a document pushed onto a heap bucket; the
// heap pops the highest first. A heap needs one on every push and no other
// store takes one, so on any call but [Bucket.Push] the option is an
// [ErrInvalidArgument] and nothing is sent, rather than a silently dropped
// number.
func WithPriority(priority int64) WriteOption {
	return func(config *writeConfig) {
		config.priority, config.hasPriority = priority, true
	}
}

// refusePriority is the local refusal of a priority given to a verb that has
// none.
func (config writeConfig) refusePriority(op, target string) error {
	if !config.hasPriority {
		return nil
	}
	return &Error{Code: CodeInvalidArgument, Op: op, Target: target, Message: fmt.Sprintf(
		"WithPriority(%d) belongs to Push on a heap bucket; %s takes no priority, so nothing was sent. "+
			"Remove the option, or push onto a heap bucket with Bucket.Push(ctx, doc, hoarddb.WithPriority(n))", config.priority, op)}
}

// ReadOption changes one read: [Bucket.Get].
type ReadOption func(*readConfig)

type readConfig struct{ consistent bool }

// Consistent makes [Bucket.Get] a quorum read: the server reads every copy of
// the document, repairs a stale one and returns the newest. It is the
// read-your-writes verb on a cluster, and costs a round trip to each copy. A
// server that does not advertise the consistent_get capability cannot answer
// it, so the call is an [ErrUnsupportedByServer] and nothing is sent.
func Consistent() ReadOption {
	return func(config *readConfig) { config.consistent = true }
}

// The most entries one call takes or shows (handbook/operations.md sections 2
// and 3, and testdata/driver-vectors/consume-constants.json).
const (
	peekLimitMax = 1000
	popLimitMax  = 100
)

// PeekOption changes one peek: [Bucket.Peek] and [FieldStore.Peek].
type PeekOption interface{ applyPeek(*peekConfig) }

type peekConfig struct {
	limit   int64
	invalid error
}

// PopOption changes one pop: [Bucket.Pop] and [FieldStore.Pop]. Every
// [WriteOption] is a PopOption, because a pop consumes: it is a write and
// carries a write concern. [WithLimit] is one too.
type PopOption interface{ applyPop(*popConfig) }

type popConfig struct {
	write   writeConfig
	limit   int64
	invalid error
}

func applyPopOptions(opts []PopOption) popConfig {
	var config popConfig
	for _, option := range opts {
		option.applyPop(&config)
	}
	return config
}

func (option WriteOption) applyPop(config *popConfig) { option(&config.write) }

// LimitOption is how many entries one call takes, shows or lists. [WithLimit]
// makes one; it is a [PeekOption], a [PopOption], a [ClaimOption] and a
// [DeadListOption], and each verb holds it to its own range.
type LimitOption struct{ n int }

// WithLimit sets how many entries one call asks for. The default is 1 (10 for
// [Bucket.DeadList]). The range is the verb's own: a peek shows 1 to 1000, a pop
// removes 1 to 100, a claim takes 1 to 100 and a dead list shows 1 to 1000. A
// number outside it is an [ErrInvalidArgument] and nothing is sent.
//
//	next, err := queue.Peek(ctx, hoarddb.WithLimit(5)) // look at the next five
//	taken, err := queue.Pop(ctx, hoarddb.WithLimit(5)) // remove and return them
func WithLimit(n int) LimitOption { return LimitOption{n: n} }

func (option LimitOption) applyPeek(config *peekConfig) {
	config.limit, config.invalid = option.within("a peek returns", peekLimitMax, "; use Pop to take them")
}

func (option LimitOption) applyPop(config *popConfig) {
	config.limit, config.invalid = option.within("a pop removes", popLimitMax, "; repeat the pop, or use Peek to read more without removing")
}

// within is the limit as the wire carries it, or the problem with it; what is
// the verb's own words for its range and advice ends the sentence.
func (option LimitOption) within(what string, most int, advice string) (int64, error) {
	if option.n < 1 || option.n > most {
		return 0, fmt.Errorf("WithLimit(%d): %s from 1 to %d entries%s", option.n, what, most, advice)
	}
	return int64(option.n), nil
}

// rangePageSize is how many entries one request of a [Cursor] asks for unless
// [PageSize] says otherwise.
const rangePageSize = 100

// Direction is the way [SortBy] orders a field.
type Direction int8

// The two directions of a sort.
const (
	// Ascending orders smallest first.
	Ascending Direction = 1
	// Descending orders largest first.
	Descending Direction = -1
)

type findConfig struct {
	limit    int64
	pageSize int64
	skip     int64
	sortBy   string
	sortDir  Direction
	project  []string
	invalid  error
	// given names the options the caller passed, so a verb whose request has no
	// place for one can refuse it instead of dropping it.
	given []string
}

// FindOption changes a query that returns a [Cursor]: [Bucket.Find],
// [Bucket.Search] and [Bucket.Range].
type FindOption func(*findConfig)

func applyFindOptions(opts []FindOption) findConfig {
	config := findConfig{pageSize: rangePageSize}
	for _, option := range opts {
		option(&config)
	}
	return config
}

// refuse is the local refusal of an option the verb's request cannot carry:
// dropping it silently would return rows the caller asked not to get.
func (config findConfig) refuse(op, target, instead string) error {
	if config.invalid != nil {
		return refusal(op, target, config.invalid.Error()+"; nothing was sent")
	}
	for _, name := range config.given {
		if slices.Contains(unsupportedBy[op], name) {
			return refusal(op, target, fmt.Sprintf("%s takes no %s option, so nothing was sent. %s", op, name, instead))
		}
	}
	return nil
}

// unsupportedBy lists, per verb, the options its request has no member for.
var unsupportedBy = map[string][]string{
	"range":  {"Skip", "SortBy", "Projection"},
	"search": {"Skip", "SortBy", "Projection", "PageSize"},
	"list":   {"Skip", "SortBy", "Projection"},
}

// Limit stops the cursor after n documents in all, however many pages that
// takes. The default is no limit.
func Limit(n int) FindOption {
	return func(config *findConfig) {
		config.given = append(config.given, "Limit")
		if n < 1 {
			config.invalid = fmt.Errorf("Limit(%d): a limit is at least 1; leave the option out to read everything", n)
			return
		}
		config.limit = int64(n)
	}
}

// PageSize sets how many documents one request asks the server for. The
// default is 100. A smaller page holds less in memory and costs more round
// trips.
func PageSize(n int) FindOption {
	return func(config *findConfig) {
		config.given = append(config.given, "PageSize")
		if n < 1 {
			config.invalid = fmt.Errorf("PageSize(%d): a page holds at least 1 document; the default is %d", n, rangePageSize)
			return
		}
		config.pageSize = int64(n)
	}
}

// Skip leaves out the first n matching documents, in the query's order. It is
// counted by the server, which still walks past them: skipping deep into a
// large result costs the walk.
func Skip(n int) FindOption {
	return func(config *findConfig) {
		config.given = append(config.given, "Skip")
		if n < 0 {
			config.invalid = fmt.Errorf("Skip(%d): a skip is zero or more; leave the option out to start at the first document", n)
			return
		}
		config.skip = int64(n)
	}
}

// SortBy orders the matches by one field, [Ascending] or [Descending]; the
// server sorts on one field only. Without it a query answers in the store's
// natural order, which for a queue is its pop order.
func SortBy(field string, direction Direction) FindOption {
	return func(config *findConfig) {
		config.given = append(config.given, "SortBy")
		switch {
		case field == "":
			config.invalid = errors.New("SortBy needs a field name; leave the option out to keep the store's natural order")
		case direction != Ascending && direction != Descending:
			config.invalid = fmt.Errorf("SortBy(%q, %d): the direction is hoarddb.Ascending or hoarddb.Descending", field, direction)
		default:
			config.sortBy, config.sortDir = field, direction
		}
	}
}

// Projection keeps only the named fields of each document; the server does not add
// _id_ back, so name it to keep it. A
// smaller document is a smaller page, which is also the cure for a query the
// server refuses as too large.
func Projection(fields ...string) FindOption {
	return func(config *findConfig) {
		config.given = append(config.given, "Projection")
		if len(fields) == 0 || slices.Contains(fields, "") {
			config.invalid = errors.New("Projection needs one or more field names, none of them empty; leave the option out to read whole documents")
			return
		}
		config.project = fields
	}
}
