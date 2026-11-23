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

// Command trust connects to a hoardDB server the way a program should: it says
// which server it means, by fingerprint, before it sends a password.
//
//	export HOARDDB_ROOT_PASSWORD='...'      # printed by the server on first start
//	export HOARDDB_FINGERPRINT='SHA256:...' # printed by the server at every start
//	go run ./examples/trust
//
// With no fingerprint the program uses ~/.hoarddb/known_servers, the file the
// hoardDB command line tool writes. For a throwaway local server there is one
// more way, spelled out so it cannot be done by accident:
//
//	go run ./examples/trust -first-sight
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

func main() {
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		stop()
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("trust", flag.ContinueOnError)
	flags.SetOutput(stderr)
	connectionString := flags.String("uri", orDefault(getenv("HOARDDB_URI"), "hoarddb://localhost:7433"),
		"connection string: hoarddb://[user[:password]@]host[:port][/database]")
	firstSight := flags.Bool("first-sight", false,
		"trust the first key the server presents and remember it (local development only)")
	if err := flags.Parse(args); err != nil {
		return err
	}

	var options []hoarddb.Option
	if fingerprint := getenv("HOARDDB_FINGERPRINT"); fingerprint != "" {
		// The production path: pinned, compared before any credential is sent.
		options = append(options, hoarddb.WithFingerprint(fingerprint))
	}
	if *firstSight {
		options = append(options,
			hoarddb.WithTrustPrompt(hoarddb.AcceptFirstSight),
			hoarddb.WithLogger(slog.New(slog.NewTextHandler(stderr, nil))))
	}

	client, err := hoarddb.Connect(ctx, *connectionString, options...)
	if err != nil {
		return explain(err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Ping(ctx); err != nil {
		return explain(err)
	}
	info := client.ServerInfo()
	_, err = fmt.Fprintf(stdout, "connected: hoardDB %s, protocol %d, %d connection open\n",
		info.Version, info.ProtocolVersion, client.PoolStats().Open)
	return err
}

// explain adds nothing to an error the driver already wrote with its fix; it
// only shows how a program branches on the class.
func explain(err error) error {
	switch {
	case errors.Is(err, hoarddb.ErrUntrustedServer), errors.Is(err, hoarddb.ErrFingerprintMismatch), errors.Is(err, hoarddb.ErrServerKeyChanged):
		return fmt.Errorf("not connecting, the server could not be verified: %w", err)
	case errors.Is(err, hoarddb.ErrAuthFailed):
		return fmt.Errorf("the server was verified but refused the login: %w", err)
	default:
		return err
	}
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
