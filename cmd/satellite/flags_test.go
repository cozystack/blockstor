// SPDX-License-Identifier: Apache-2.0

/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"errors"
	"flag"
	"io"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite/stream"
)

func parseTestFlags(args ...string) (options, error) {
	fs := flag.NewFlagSet("satellite", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	return parseFlags(fs, args)
}

// A partial set of stream TLS flags refuses to start. Let through, it would
// quietly turn the stream off and every cross-node restore on the node would
// fall back to a resync.
func TestSatelliteRefusesAPartialStreamTLSConfiguration(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"--stream-tls-cert-file=/tls/tls.crt"},
		{"--stream-tls-cert-file=/tls/tls.crt", "--stream-tls-key-file=/tls/tls.key"},
		{"--stream-tls-ca-file=/tls/ca.crt"},
	} {
		_, err := parseTestFlags(args...)
		if !errors.Is(err, stream.ErrTLSIncomplete) {
			t.Errorf("flags %v = %v, want stream.ErrTLSIncomplete", args, err)
		}
	}
}

func TestSatelliteStartsWithTheStreamOff(t *testing.T) {
	t.Parallel()

	opts, err := parseTestFlags("--node-name=n1")
	if err != nil {
		t.Fatalf("no stream TLS flags = %v, want the stream off and no error", err)
	}

	if opts.nodeName != "n1" || opts.streamTLS.Configured() {
		t.Fatalf("options = %+v, want node n1 and the stream off", opts)
	}
}
