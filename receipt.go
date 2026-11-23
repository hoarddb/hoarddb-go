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

import "github.com/hoarddb/hoarddb-go/hoarddbwire"

// Receipt is the answer to a write of one document.
type Receipt struct {
	// ID is the document's key: the one the caller gave to [Bucket.Put], or the
	// one the server assigned to [Bucket.Insert] and [Bucket.Push].
	ID string
	// Seq is the store's sequence number for the write.
	Seq uint64
	// CopiesConfirmed is how many copies of the bucket hold the write, and
	// ReplicationFactor how many the bucket keeps. Both are 0 when the server
	// did not say, which is the case on a single node.
	CopiesConfirmed, ReplicationFactor int
}

// Entry is one key of a btree bucket and its document.
type Entry struct {
	Key   string
	Value Document
}

func receiptOf(answer hoarddbwire.InsertResponse) Receipt {
	receipt := Receipt{ID: answer.ID, Seq: uint64(max(answer.Seq, 0))}
	receipt.CopiesConfirmed, receipt.ReplicationFactor = copiesOf(answer.WriteReport)
	return receipt
}

// copiesOf reads the copy counts of a write's answer; absent is 0.
func copiesOf(report hoarddbwire.WriteReport) (confirmed, factor int) {
	if report.CopiesConfirmed != nil {
		confirmed = int(*report.CopiesConfirmed)
	}
	if report.ReplicationFactor != nil {
		factor = int(*report.ReplicationFactor)
	}
	return confirmed, factor
}
