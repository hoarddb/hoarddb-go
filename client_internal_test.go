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
	"fmt"
	"strings"
	"testing"
)

// The session is the one place a client keeps its password and token
// (handbook/auth.md section 3, item 1): the parsed connection string and the
// option settings let go of them once the session has them.
func TestClientKeepsCredentialsOnlyInTheSession(t *testing.T) {
	for name, build := range map[string]func() (*Client, error){
		"from the connection string": func() (*Client, error) {
			return newClient("hoarddb://admin:uri-secret@127.0.0.1:1/?authToken=uri-token&fingerprint=SHA256:"+strings.Repeat("ab", 32), nil)
		},
		"from options": func() (*Client, error) {
			return newClient("hoarddb://127.0.0.1:1/?fingerprint=SHA256:"+strings.Repeat("ab", 32),
				[]Option{WithCredentials("admin", "option-secret"), WithToken("option-token")})
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, err := build()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			if client.settings.password != "" || client.settings.token != "" {
				t.Errorf("the client's settings still hold a credential (password set: %v, token set: %v)",
					client.settings.password != "", client.settings.token != "")
			}
			if client.target.Password != "" || client.target.AuthToken != "" {
				t.Errorf("the parsed connection string still holds a credential (password set: %v, token set: %v)",
					client.target.Password != "", client.target.AuthToken != "")
			}
			if client.dialer.settings != client.settings {
				t.Error("the dialer holds its own copy of the settings")
			}
		})
	}
}

// A credential must stay out of the text however a program formats a value:
// %s, %v, %+v, %#v and %q, on the client, the session it holds and the option
// settings.
func TestSecretHoldersRedactUnderEveryFormatVerb(t *testing.T) {
	const password = "Zq7xK9Wm3pL"
	client, err := newClient("hoarddb://127.0.0.1:1/?fingerprint=SHA256:"+strings.Repeat("ab", 32),
		[]Option{WithCredentials("admin", password), WithToken("tok-Hy6cDRt5nB")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	withSecret := config{username: "admin", password: password, token: "tok-Hy6cDRt5nB", poolSize: 1}
	values := map[string]any{
		"client":         client,
		"session":        client.session,
		"settings":       withSecret,
		"settings ptr":   &withSecret,
		"client pointer": &client,
	}
	for name, value := range values {
		for _, verb := range []string{"%s", "%v", "%+v", "%#v", "%q", "%x", "%d"} {
			text := fmt.Sprintf(verb, value)
			if strings.Contains(text, password) || strings.Contains(text, "Hy6cDRt5nB") {
				t.Errorf("%s formatted with %s repeats a credential: %s", name, verb, text)
			}
		}
	}
}
