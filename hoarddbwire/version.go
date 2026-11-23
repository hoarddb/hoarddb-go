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

package hoarddbwire

// ALPNv1 is the application protocol the client offers in the TLS handshake.
// ALPN is mandatory: a client that offers none is refused with
// UNSUPPORTED_VERSION (handbook/wire.md section 1).
const ALPNv1 = "hoarddb/1"

// DefaultPort is the driver port. It is not 4433, which is the internode port.
const DefaultPort = 7433
