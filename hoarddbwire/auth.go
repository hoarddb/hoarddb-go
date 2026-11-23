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

// AuthRequest is the payload of an OpAuth frame, the first frame on every
// connection. A non-empty Token makes the server ignore Password in the same
// frame entirely; there is no in-frame fallback.
type AuthRequest struct {
	Username        string   `bson:"username"`
	Password        string   `bson:"password"`
	ProtocolVersion uint32   `bson:"protocol_version"`
	Capabilities    []string `bson:"capabilities"`
	Client          string   `bson:"client"`
	Token           string   `bson:"token,omitempty"`
}

// AuthResponse is the payload of an OpAuthOK frame. It is not the client-side
// login type: that one has no success field.
type AuthResponse struct {
	Success bool   `bson:"success"`
	Token   string `bson:"token"`
	Message string `bson:"message"`
	// TokenExpiresAt is this token's own expiry, Unix seconds. A client
	// refreshes at half of it.
	TokenExpiresAt int64 `bson:"token_expires_at"`
	// TokenDeadline is the absolute end of the refresh chain, Unix seconds,
	// inherited and never extended. When it passes, only the password works.
	TokenDeadline   int64    `bson:"token_deadline"`
	ProtocolVersion uint32   `bson:"protocol_version"`
	ServerVersion   string   `bson:"server_version"`
	Capabilities    []string `bson:"capabilities"`
	// PasswordMinLength is the server's shortest accepted new password, in
	// characters (handbook/auth.md section 5). Zero or absent means the server
	// reported none. This driver offers no user administration, so nothing
	// reads it yet; a driver that derives credentials refuses a shorter
	// password with it.
	PasswordMinLength int32 `bson:"password_min_length,omitempty"`
}

// Capability strings a server advertises in AuthResponse.Capabilities that
// this driver acts on. Unknown strings are ignored in both directions.
const (
	// CapabilityAuthToken means the server mints resumption tokens. Without it
	// a client replays the password and offers no token.
	CapabilityAuthToken = "auth_token"
)
