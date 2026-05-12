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
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
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
	baseURL, stop := resolveTarget(t)
	defer stop()

	traces, err := contract.LoadTracesDir("testdata/smoke")
	if err != nil {
		t.Fatalf("LoadTracesDir: %v", err)
	}

	if len(traces) == 0 {
		t.Fatalf("no smoke traces found")
	}

	results, err := contract.Replay(t.Context(), nil, baseURL, traces)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	for _, result := range results {
		if !result.Match {
			t.Errorf("%s diverged: %s", result.Trace, strings.Join(result.Diffs, "; "))
		}
	}
}

// resolveTarget returns a base URL to replay against. If
// BLOCKSTOR_BASEURL is set in the env (e.g. CI hits a deployed
// controller via port-forward), use it as-is and skip the in-process
// server. Otherwise boot one and tear it down via the returned func.
func resolveTarget(t *testing.T) (string, func()) {
	t.Helper()

	if url := os.Getenv("BLOCKSTOR_BASEURL"); url != "" {
		return url, func() {}
	}

	addr, stop := startServer(t)

	return "http://" + addr, stop
}

// startServer boots an in-process blockstor REST server on a random
// loopback port. It picks a port by listening + closing (the small
// race window is acceptable for tests), starts the server, then waits
// until the port is reachable before returning.
func startServer(t *testing.T) (string, func()) {
	t.Helper()

	addr := pickFreeAddr(t)
	srv := &rest.Server{
		Addr:   addr,
		Store:  store.NewInMemory(),
		Client: newFakeClient(t),
	}

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

// newFakeClient builds an in-memory controller-runtime client with
// the corev1 + blockstor schemes registered. The contract test
// harness wires this into the REST server so endpoints that touch
// the apiserver (controller properties, encryption passphrase
// secrets) don't 503 in-process. Mirrors pkg/rest's
// newFakeRESTClient — duplicated here because that helper is in a
// _test.go and not exported.
func newFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 to scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor to scheme: %v", err)
	}

	return fake.NewClientBuilder().WithScheme(scheme).Build()
}
