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
	"slices"
	"strings"
)

// A vector in the snapshot can name a behaviour a later build step has not
// built yet. Deferred says which, so leg 1 stays green while it holds those
// rows back: a held vector that still fails is logged, and one that passes
// fails the leg, so a step deletes its names from this file when it lands.

// prefixRules tag a vector by the start of its name, first match wins.
var prefixRules = []struct{ prefix, tag string }{
	// The one ring refusal the driver does not share: its topology decoder reads
	// a widened integer in a reply, which the server's decoder refuses.
	{"ring topology reply refused: a widened integer", "server-canonical"},
	// CLUSTER_PROBE is a node-to-node opcode; no driver sends or reads it.
	{"cluster probe", "cluster-probe"},
}

// Deferred says why a vector is held back: a later phase owns it (a1Owned or
// a2Owned), or it is in drift. Rules apply in the order of this function, first
// match wins.
func Deferred(vector Vector) (tag string, held bool) {
	for _, rule := range prefixRules {
		if strings.HasPrefix(vector.Name, rule.prefix) {
			return rule.tag, true
		}
	}
	if vector.Request != nil {
		switch vector.Request.OpcodeName {
		case "BATCH":
			return "batch-verb", true
		}
	}
	if tag, owned := a1Owned[vector.Name]; owned {
		return tag, true
	}
	if tag, owned := a2Owned[vector.Name]; owned {
		return tag, true
	}
	if drift[vector.Name] {
		return "drift", true
	}
	return "", false
}

// DeferredNames is every name in a1Owned, a2Owned and drift, sorted.
func DeferredNames() []string {
	names := make([]string, 0, len(a1Owned)+len(a2Owned)+len(drift))
	for name := range a1Owned {
		names = append(names, name)
	}
	for name := range a2Owned {
		names = append(names, name)
	}
	for name := range drift {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// a1Owned maps the rows phase A1 turns green to the step that does it.
var a1Owned = map[string]string{}

// a2Owned maps the rows phase A2 turns green to the step that does it. Each
// step deletes its own names as its rows pass, so a row that passes is never
// held back by a stale name.
var a2Owned = map[string]string{}

// drift holds the legacy rows whose request or answer changed in the snapshot
// and that no A1 step owns, measured by running leg 1 with the rules above.
var drift = map[string]bool{}
