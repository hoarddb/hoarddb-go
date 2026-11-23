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

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// DoOnBulkLane sends one empty-payload request on the bulk lane. No public verb
// uses the bulk lane until the blob and query steps, so the lane is exercised
// through the request layer directly.
func (c *Client) DoOnBulkLane(ctx context.Context, opcode, answer hoarddbwire.Opcode) error {
	return c.do(ctx, call{op: "bulk probe", opcode: opcode, answer: answer, lane: laneBulk})
}

// DoWire sends one request through the request layer and decodes the answer
// into response, for the verbs whose public form arrives in a later step.
func (c *Client) DoWire(ctx context.Context, op string, opcode, answer hoarddbwire.Opcode, request, response any) error {
	return c.do(ctx, call{op: op, opcode: opcode, answer: answer, request: request, response: response})
}
