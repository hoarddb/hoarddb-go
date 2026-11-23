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

# Downloads the test server binary named in testing/server.json for this
# machine, checks its SHA-256, caches it under .cache/server/<commit>/ and
# prints its path (and nothing else) on stdout.
#
#   export HOARDDB_SERVER_BIN=$(scripts/fetch-server.sh)
#
# With --cli it fetches the hoarddb command-line client the same way, from the
# manifest's "cli_assets". The tests use it to create a second user, which the
# driver cannot do itself (conformance/auth_test.go).
#
#   export HOARDDB_CLI_BIN=$(scripts/fetch-server.sh --cli)
#
# No test-server release is published yet, so this fails with HTTP 404 until one
# is. Until then set HOARDDB_SERVER_BIN to a hoardDB-server binary you built.
#
# Needs GITHUB_TOKEN or GH_TOKEN with read access to the repository's releases.
# GITHUB_REPOSITORY (owner/name) overrides the default repository.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$root/testing/server.json"
repository="${GITHUB_REPOSITORY:-hoarddb/hoarddb-go}"
token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
api="https://api.github.com/repos/$repository"

fail() { echo "fetch-server.sh: $*" >&2; exit 1; }

[ -n "$token" ] || fail "set GITHUB_TOKEN or GH_TOKEN to a token that can read the releases of $repository"
command -v jq >/dev/null || fail "jq is required"

case "$(uname -s)" in
  Linux) os=linux ;;
  *) fail "no test server binary is published for $(uname -s)" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) fail "no test server binary is published for $(uname -m)" ;;
esac
platform="$os-$arch"

section=assets
case "${1:-}" in
  "") ;;
  --cli) section=cli_assets ;;
  *) fail "unknown argument $1; the only option is --cli" ;;
esac

tag="$(jq -er '.tag' "$manifest")"
commit="$(jq -er '.server_commit' "$manifest")"
name="$(jq -er --arg s "$section" --arg p "$platform" '.[$s][$p].name' "$manifest")" \
  || fail "$manifest has no $section entry for $platform (for --cli, the test-server release must carry the hoarddb CLI and the manifest must name it)"
want="$(jq -er --arg s "$section" --arg p "$platform" '.[$s][$p].sha256' "$manifest")"

directory="$root/.cache/server/$commit"
target="$directory/$name"

sum_of() { sha256sum "$1" | cut -d' ' -f1; }

if [ -x "$target" ] && [ "$(sum_of "$target")" = "$want" ]; then
  echo "$target"
  exit 0
fi

mkdir -p "$directory"
rm -f "$target"
partial="$(mktemp "$directory/$name.XXXXXX")"
trap 'rm -f "$partial"' EXIT

release="$(curl -fsS -H "Authorization: Bearer $token" -H "Accept: application/vnd.github+json" \
  "$api/releases/tags/$tag")" || fail "cannot read release $tag of $repository"
asset_id="$(jq -er --arg n "$name" '.assets[] | select(.name == $n) | .id' <<<"$release")" \
  || fail "release $tag has no asset named $name"

curl -fsSL -H "Authorization: Bearer $token" -H "Accept: application/octet-stream" \
  -o "$partial" "$api/releases/assets/$asset_id" || fail "cannot download $name from release $tag"

got="$(sum_of "$partial")"
if [ "$got" != "$want" ]; then
  rm -f "$partial"
  fail "checksum mismatch for $name: expected $want, got $got (file deleted)"
fi

chmod 0755 "$partial"
mv "$partial" "$target"
trap - EXIT
echo "$target"
