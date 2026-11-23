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

import "time"

// User administration (handbook/auth.md section 6). These opcodes are outside
// the v1 application API: the types exist so the conformance harness can
// encode and decode the vectored exchanges, and no client method sends them.

// Grant is one {role, db} pair of a user's role set.
type Grant struct {
	Role     string `bson:"role"`
	Database string `bson:"db"`
}

// CredentialParams are the Argon2id parameters of a Credential.
type CredentialParams struct {
	Memory      int64 `bson:"memory"`
	Iterations  int64 `bson:"iterations"`
	Parallelism int64 `bson:"parallelism"`
	SaltLen     int64 `bson:"salt_len"`
	HashLen     int64 `bson:"hash_len"`
}

// Credential is a password the driver derived itself; the password never
// travels. Salt and Hash are standard base64 with padding.
type Credential struct {
	Algo   string           `bson:"algo"`
	Salt   string           `bson:"salt"`
	Hash   string           `bson:"hash"`
	Params CredentialParams `bson:"params"`
}

// UserCreateRequest is USER_CREATE. Enabled is a pointer because an absent
// value means true.
type UserCreateRequest struct {
	RequestID   string     `bson:"request_id"`
	Username    string     `bson:"username"`
	Credentials Credential `bson:"credentials"`
	Roles       []Grant    `bson:"roles"`
	Enabled     *bool      `bson:"enabled,omitempty"`
}

// UserAlterRequest is USER_ALTER. Roles, Enabled and Credentials are
// optional with presence: an absent one leaves that part alone, and an empty
// but present Roles replaces the grant set with nothing.
type UserAlterRequest struct {
	RequestID   string      `bson:"request_id"`
	Username    string      `bson:"username"`
	Version     int64       `bson:"version"`
	Roles       *[]Grant    `bson:"roles,omitempty"`
	Enabled     *bool       `bson:"enabled,omitempty"`
	Credentials *Credential `bson:"credentials,omitempty"`
}

// UserDropRequest is USER_DROP.
type UserDropRequest struct {
	RequestID string `bson:"request_id"`
	Username  string `bson:"username"`
}

// UserGetRequest is USER_GET. USER_LIST sends an EmptyRequest.
type UserGetRequest struct {
	Username string `bson:"username"`
}

// UserGrantRequest is USER_GRANT and USER_REVOKE.
type UserGrantRequest struct {
	RequestID string `bson:"request_id"`
	Username  string `bson:"username"`
	Role      Grant  `bson:"role"`
}

// UserMutationResponse answers create, alter, drop, grant and revoke. Changed
// is present only on an alter.
type UserMutationResponse struct {
	Success bool     `bson:"success"`
	Message string   `bson:"message"`
	Version int64    `bson:"version"`
	Changed []string `bson:"changed,omitempty"`
}

// UserInfo is one account in a listing. No hash, salt or pepper is ever in it.
type UserInfo struct {
	Username          string    `bson:"username"`
	Roles             []Grant   `bson:"roles"`
	Enabled           bool      `bson:"enabled"`
	Epoch             int64     `bson:"epoch"`
	CreatedAt         time.Time `bson:"created_at"`
	PasswordChangedAt time.Time `bson:"password_changed_at"`
}

// UserListResponse answers USER_LIST; Master and MasterAddr name the node that
// serves mutations.
type UserListResponse struct {
	Users      []UserInfo `bson:"users"`
	Message    string     `bson:"message,omitempty"`
	Version    int64      `bson:"version"`
	Master     string     `bson:"master,omitempty"`
	MasterAddr string     `bson:"master_addr,omitempty"`
}

// UserGetResponse answers USER_GET. An unknown user is a success with Found
// false.
type UserGetResponse struct {
	Found   bool     `bson:"found"`
	User    UserInfo `bson:"user"`
	Version int64    `bson:"version"`
	Message string   `bson:"message,omitempty"`
}
