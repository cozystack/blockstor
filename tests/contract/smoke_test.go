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

package contract_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/rest"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/tests/contract"
)

// TestSmokeTraceReplay loads testdata/smoke/*.json and replays each
// trace against an in-process blockstor REST server. This is a
// regression guard for the API shape: any unintentional change to
// status codes or response bodies on these well-known endpoints
// triggers a diff.
//
// The traces are not captured from the Java oracle — they're authored
// here to pin blockstor's own contract. Cross-impl trace recording is
// future operational work.
func TestSmokeTraceReplay(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	traces, err := contract.LoadTracesDir("testdata/smoke")
	if err != nil {
		t.Fatalf("LoadTracesDir: %v", err)
	}

	if len(traces) == 0 {
		t.Fatalf("no smoke traces found")
	}

	results, err := contract.Replay(t.Context(), nil, "http://"+addr, traces)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	for _, result := range results {
		if !result.Match {
			t.Errorf("%s diverged: %s", result.Trace, strings.Join(result.Diffs, "; "))
		}
	}
}

// startServer boots an in-process blockstor REST server on a random
// loopback port. It picks a port by listening + closing (the small
// race window is acceptable for tests), starts the server, then waits
// until the port is reachable before returning.
func startServer(t *testing.T) (string, func()) {
	t.Helper()

	addr := pickFreeAddr(t)
	srv := &rest.Server{Addr: addr, Store: store.NewInMemory()}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = srv.Start(ctx)
	}()

	dialer := &net.Dialer{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()

			return addr, func() {
				cancel()

				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Errorf("server did not stop in 2s")
				}
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	t.Fatalf("server did not become ready at %s in 3s", addr)

	return "", func() {}
}

// pickFreeAddr asks the kernel for an unused TCP port on loopback,
// closes the placeholder listener, and returns the bind string.
func pickFreeAddr(t *testing.T) string {
	t.Helper()

	lc := &net.ListenConfig{}

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := ln.Addr().String()
	_ = ln.Close()

	return addr
}
