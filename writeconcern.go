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

import "fmt"

// WriteConcern is how many copies of a bucket must hold a write before the
// server answers it (handbook/errors.md section 4). The copies counted are the
// ones in sync: a replica that is down is not waited for, so an [All] write
// with a replica down is answered success, and the [Receipt]'s
// CopiesConfirmed is below its ReplicationFactor. Durability is a number to
// read on the receipt, not a refusal.
//
// A write whose concern is not met in time is [ErrTimeout] with
// [OutcomeApplied]: the write is stored on the bucket's owner and reaches the
// other copies when they are reachable. The driver never resends it, and a
// caller must not resend a push or an increment, which would apply twice.
//
// On a single node there are no replicas: every level has the same effect,
// and receipts report 0 copies ("not reported"). A single node in this release
// does not sync each write to disk before answering it, whatever the level.
type WriteConcern string

// The write concerns a server knows. Without one, a write is answered at the
// server's configured default.
const (
	// One: the write is answered once one copy holds it.
	One WriteConcern = "one"
	// Majority: the write is answered once a majority of the bucket's in-sync
	// copies hold it.
	Majority WriteConcern = "majority"
	// All: the write is answered once every in-sync copy holds it.
	All WriteConcern = "all"
)

// known reports whether the server knows the word.
func (concern WriteConcern) known() bool {
	return concern == One || concern == Majority || concern == All
}

// unknownConcern is the sentence of a word the driver will not send. An empty
// word is the server's default on the wire, but a call cannot ask for it over
// the client's WithDefaultWriteConcern: leaving WithWriteConcern out gives the
// client's default, so the sentence says which default the caller gets.
func unknownConcern(source string, concern WriteConcern) string {
	remedy := "leave the option out for the server's default"
	if source == "WithWriteConcern" {
		remedy = "leave the option out for the client's WithDefaultWriteConcern, or the server's default when the client sets none; " +
			"a single call cannot ask for the server's default over the client's"
	}
	return fmt.Sprintf("%s(%q): the server knows the write concerns hoarddb.One, hoarddb.Majority and hoarddb.All; %s",
		source, string(concern), remedy)
}

// WithWriteConcern sets the write concern of one write, overriding the
// client's [WithDefaultWriteConcern]. Every write takes it: Put, Insert,
// InsertMany, Delete, Update, Push, Pop and PutBlob, on a bucket and on a
// [FieldStore] (a pop consumes, so it is a write). See [WriteConcern] for what
// each level guarantees and how the receipt reports it; Delete and Update
// return no receipt, and a refusal of either reports the copies on the
// [Error].
//
// A word other than [One], [Majority] and [All] is [ErrInvalidArgument], and a
// server that does not advertise the write_concern capability is
// [ErrUnsupportedByServer]: in both cases nothing is sent, and the error's
// Outcome is [OutcomeNotApplied].
func WithWriteConcern(concern WriteConcern) WriteOption {
	return func(config *writeConfig) { config.concern, config.hasConcern = concern, true }
}

// WithDefaultWriteConcern sets the write concern of every write the client
// sends that does not set its own with [WithWriteConcern]. Without it a write
// is answered at the server's configured default. A word other than [One],
// [Majority] and [All] is refused by [Connect].
func WithDefaultWriteConcern(concern WriteConcern) Option {
	return func(settings *config) error {
		if !concern.known() {
			return optionError("%s", unknownConcern("WithDefaultWriteConcern", concern))
		}
		settings.writeConcern = concern
		return nil
	}
}

// writeConcern is the word a write sends: the call's, else the client's
// default, else none, which the server reads as its own default. A word the
// server would refuse, or a level a server without the write_concern
// capability would drop in silence (handbook/wire.md section 6), is refused
// here, before anything is sent.
func (c *Client) writeConcern(op, target string, config writeConfig) (string, error) {
	concern, source := c.settings.writeConcern, "WithDefaultWriteConcern"
	if config.hasConcern {
		concern, source = config.concern, "WithWriteConcern"
	}
	if concern == "" && !config.hasConcern {
		return "", nil
	}
	if !concern.known() {
		return "", &Error{Code: CodeInvalidArgument, Op: op, Target: target, Outcome: OutcomeNotApplied,
			Message: unknownConcern(source, concern) + ". So nothing was sent, and the write was not applied"}
	}
	if serverInfo := c.ServerInfo(); !serverInfo.Has("write_concern") {
		return "", &Error{Code: CodeUnsupportedByServer, Op: op, Target: target, Outcome: OutcomeNotApplied, Message: fmt.Sprintf(
			"%s(%s) needs the server capability write_concern, which this server (version %s) did not advertise; "+
				"nothing was sent, so the write was not applied. Upgrade the server, or leave the option out for the server's default",
			source, concern, serverInfo.Version)}
	}
	return string(concern), nil
}
