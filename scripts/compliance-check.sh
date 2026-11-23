#!/usr/bin/env bash
# Copyright 2026 Boeger IT Sh.p.k
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The release gates that are about what the module is made of, not what it does:
#
#   licences  every package this module builds is under a permitted licence
#   deps      the dependency graph is the standard library, this module, the
#             BSON codec and the ring wire codec, nothing else
#   names     no other database product is named anywhere in the tree, outside
#             a short allow-list of files that must name one to work
#
#   scripts/compliance-check.sh [--only licences|deps|names] [directory]
#
# Exit 0 when every selected check passes, 1 when one fails (each failure is
# printed with the file or package at fault), 2 for a usage error. The licence
# check needs the network the first time (it runs a pinned go-licenses).
set -uo pipefail

only=""
if [ "${1:-}" = "--only" ]; then
  only="${2:-}"
  shift 2 || { echo "compliance-check.sh: --only needs licences, deps or names" >&2; exit 2; }
  case "$only" in licences|deps|names) ;; *) echo "compliance-check.sh: --only needs licences, deps or names" >&2; exit 2 ;; esac
fi
root="$(cd "${1:-$(dirname "${BASH_SOURCE[0]}")/..}" && pwd)" || exit 2
cd "$root" || exit 2

failed=0
fail() { echo "FAIL: $*" >&2; failed=1; }
selected() { [ -z "$only" ] || [ "$only" = "$1" ]; }

# --- licences ---------------------------------------------------------------
# The licences the project accepts for a dependency. Anything else, and any
# package whose licence cannot be identified, fails.
permitted='^(MIT|BSD-2-Clause|BSD-3-Clause|Apache-2\.0|ISC|Unlicense|CC0-1\.0|MPL-2\.0)$'
go_licenses="${GO_LICENSES:-go run github.com/google/go-licenses@v1.6.0}"

check_licences() {
  local report
  if ! report="$($go_licenses report ./... 2>/dev/null)"; then
    fail "the licence report could not be produced (command: $go_licenses report ./...)"
    return
  fi
  while IFS=, read -r package _ licence; do
    [ -n "$package" ] || continue
    if ! [[ "$licence" =~ $permitted ]]; then
      fail "licence: $package is under \"$licence\", which is not on the permitted list"
    fi
  done <<<"$report"
  echo "licences: $(wc -l <<<"$report") packages checked"
}

# --- dependency graph -------------------------------------------------------
check_deps() {
  local module paths path
  module="$(go list -m)" || { fail "go list -m failed"; return; }
  paths="$(go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./...)" || { fail "go list -deps failed"; return; }
  for path in $paths; do
    case "$path" in
      "$module"|"$module"/*) ;;
      github.com/hoarddb/hoarddb-bson|github.com/hoarddb/hoarddb-bson/*) ;;
      github.com/hoarddb/hoarddb-wire|github.com/hoarddb/hoarddb-wire/*) ;;
      *) fail "dependency: $path is not the standard library, this module, the BSON codec (hoarddb-bson) or the ring wire codec (hoarddb-wire)" ;;
    esac
  done
  echo "deps: $(wc -w <<<"$paths") non-standard packages, all this module, the BSON codec or the ring wire codec"
}

# --- banned names -----------------------------------------------------------
# Words, matched whole and without regard to case. The list is another
# database's product names; the driver is built clean-room and says nothing
# about them.
banned='mongo|mongodb|mongosh|mysql|mariadb|postgres|postgresql|redis|cassandra|dynamodb|couchbase|couchdb|sqlite|memcached|rethinkdb|cockroachdb|elasticsearch|firestore|firebase|neo4j|scylla|scylladb|leveldb|rocksdb|oracle|sqlserver|influxdb|clickhouse|etcd'
# Files that must name one to work, and why:
#   examples/gin/go.*                   the web framework's own indirect requirement
#   internal/codec/                     the test that the previous BSON module is gone
#   boundary_test.go                    forbids the codec's name in the public API
#   client_test.go                      a foreign URI scheme must be refused
#   scripts/compliance-check.sh         this list
allowed=(examples/gin/go.mod examples/gin/go.sum internal/codec boundary_test.go client_test.go scripts/compliance-check.sh)

# A top-level vendor/ is not scanned: it is not part of the module (git does not
# track one), it appears only where a build vendors the dependencies, and it holds
# their source, whose copyright lines are theirs to write. The deps check limits
# which modules can be there and the licences check judges them.
check_names() {
  local hits file skip allow
  hits="$(grep -rIiwE "$banned" . --exclude-dir=.git --exclude-dir=.cache --exclude-dir='_readmecheck*' -l 2>/dev/null | sed 's|^\./||' | grep -v '^vendor/')"
  for file in $hits; do
    skip=0
    for allow in "${allowed[@]}"; do
      case "$file" in "$allow"|"$allow"/*) skip=1 ;; esac
    done
    if [ "$skip" -eq 0 ]; then
      fail "name: $file names another database product: $(grep -niwE "$banned" "$file" | head -3 | tr '\n' ' ')"
    fi
  done
  echo "names: tree scanned"
}

selected licences && check_licences
selected deps && check_deps
selected names && check_names
exit "$failed"
