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

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func environment(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestRunWithNothingListeningExplainsTheFix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	err := run(ctx, []string{"-uri", "hoarddb://admin:pw@127.0.0.1:1/"}, environment(nil), &stdout, &stderr)
	if err == nil {
		t.Fatal("connecting to a closed port must fail")
	}
	for _, want := range []string{"127.0.0.1:1", "is running"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should contain %q:\n%v", want, err)
		}
	}
	if stdout.Len() != 0 {
		t.Fatalf("nothing should be printed on failure: %q", stdout.String())
	}
}

func TestRunRejectsAMalformedFingerprintBeforeDialling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := run(ctx, nil, environment(map[string]string{"HOARDDB_FINGERPRINT": "not-a-fingerprint"}), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "SHA256:") {
		t.Fatalf("err = %v; the message should show the expected fingerprint shape", err)
	}
}

func TestRunUsesTheEnvironmentURI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := run(ctx, nil, environment(map[string]string{
		"HOARDDB_URI": "hoarddb://admin:pw@127.0.0.1:2/", "HOARDDB_FINGERPRINT": "SHA256:" + strings.Repeat("ab", 32),
	}), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "127.0.0.1:2") {
		t.Fatalf("err = %v; HOARDDB_URI should have chosen the address", err)
	}
}
