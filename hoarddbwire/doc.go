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

// Package hoarddbwire is the hoardDB wire protocol: frames, opcodes, error
// codes, the connection that carries them, and the TLS trust policy that
// authenticates a server.
//
// UNSTABLE. Application code should not import this package; use the
// github.com/hoarddb/hoarddb-go package, whose API is stable. Any release may
// rename, retype or remove anything here with no deprecation period, because
// it follows the server's internal wire structs. Two parts are exempt, and
// only because they are the wire protocol itself: the frame layout, and the
// numeric values of opcodes and error codes, which change only with a protocol
// major version.
//
// This package was written from the hoardDB driver handbook (handbook/). It
// imports nothing from the server.
package hoarddbwire
