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

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// AlterBucketOptions is what [Database.AlterBucket] changes.
type AlterBucketOptions struct {
	// ReplicationFactor is how many copies of the bucket the cluster keeps from
	// now on, at least 1. The wire has no word for "return to the node's
	// default"; a bucket that has been given a factor keeps declaring one.
	ReplicationFactor int
}

// AlterBucketResult is the server's answer to a [Database.AlterBucket] that
// was accepted.
type AlterBucketResult struct {
	// Message is the server's sentence: what changed, and the command that
	// shows the copies arriving.
	Message string
	// ReplicationFactor is the factor the bucket now asks for, and
	// PreviousFactor the one it asked for before. They are equal when nothing
	// changed.
	ReplicationFactor, PreviousFactor int
	// Pending names the members an increase newly places: they copy the bucket
	// by themselves, and writes keep going to the copies that hold it.
	Pending []string
	// Releasing names the members a decrease un-places: their copies are
	// deleted once the others are shown to hold everything they hold.
	Releasing []string
}

// AlterBucket changes how many copies of a bucket a cluster keeps. It needs a
// server that advertises the bucket_rf capability; against any other the call
// returns [ErrUnsupportedByServer] and sends nothing.
//
// The answer comes when the bucket's owner has recorded the change, not when
// the copies exist: [AlterBucketResult.Pending] and
// [AlterBucketResult.Releasing] say which members are copying or giving up the
// bucket. Raising the factor adds copies; lowering it deletes the copies of the
// members it names.
//
// Two kinds of refusal come back, and a caller tells them apart by class:
//
//   - A refusal an operator must act on, such as a factor larger than the
//     cluster, is [ErrInvalidArgument] carrying the server's sentence, which
//     names the fix. The same request meets the same answer until the operator
//     acts, so the driver sends it once.
//   - A refusal a retry clears (a membership change still being recorded, a
//     member still copying, a survivor that has not shown it holds what the
//     demoted copy holds, a member too old for the opcode) is [ErrUnavailable]
//     with [OutcomeNotApplied]: nothing changed, and the driver itself backs off
//     and sends the request again until the caller's deadline ends the waiting.
//     Give the call a deadline; when it is the error you receive, retry later.
func (d *Database) AlterBucket(ctx context.Context, name string, opts AlterBucketOptions) (*AlterBucketResult, error) {
	target := d.name + "/" + name
	if opts.ReplicationFactor < 1 {
		return nil, &Error{Code: CodeInvalidArgument, Op: "alter bucket", Target: target, Message: fmt.Sprintf(
			"AlterBucketOptions.ReplicationFactor is %d; a bucket keeps at least one copy. Pass the number of copies you want, for example AlterBucketOptions{ReplicationFactor: 3}",
			opts.ReplicationFactor)}
	}
	if serverInfo := d.client.ServerInfo(); !serverInfo.Has("bucket_rf") {
		return nil, &Error{Code: CodeUnsupportedByServer, Op: "alter bucket", Target: target, Message: fmt.Sprintf(
			"AlterBucket needs the server capability bucket_rf, which this server (version %s) did not advertise; "+
				"nothing was sent. Upgrade the server, or change the factor with the CLI of a server that has it",
			serverInfo.Version)}
	}
	var answer hoarddbwire.AlterBucketResponse
	err := d.client.do(ctx, call{op: "alter bucket", target: target, opcode: hoarddbwire.OpAlterBucket, answer: hoarddbwire.OpAlterBucket,
		request:  hoarddbwire.AlterBucketRequest{Database: d.name, Name: name, ReplicationFactor: int64(opts.ReplicationFactor)},
		response: &answer})
	if err != nil {
		return nil, err
	}
	if !answer.Success {
		return nil, refusal("alter bucket", target, answer.Message)
	}
	return &AlterBucketResult{
		Message: answer.Message, ReplicationFactor: int(answer.ReplicationFactor), PreviousFactor: int(answer.PreviousFactor),
		Pending: answer.Pending, Releasing: answer.Releasing,
	}, nil
}
