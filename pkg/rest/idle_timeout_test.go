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

package rest

import (
	"context"
	"net/http"
	"runtime"
	"testing"
	"time"
)

// TestIdleConnectionsAreClosed reproduces a client that builds a new
// transport for every call and never closes the old one. Each such
// call leaves a keep-alive connection open, and without an idle
// timeout the server holds it, with its goroutine and buffers, for as
// long as the client process lives. The test makes the calls, checks
// that they are being held, and then that the server lets them go once
// they have been idle for longer than its idle timeout.
func TestIdleConnectionsAreClosed(t *testing.T) {
	const calls = 40

	ca := newTestCA(t)
	paths := writeServerCerts(t, ca, t.TempDir(), "blockstor-apiserver")
	clientCert, clientKey := ca.issue(t, "blockstor-csi", false)

	tlsAddr := pickFreeAddr(t)
	srv := &Server{
		Addr: pickFreeAddr(t),
		TLS: &TLSOptions{
			Addr:         tlsAddr,
			CertFile:     paths.certFile,
			KeyFile:      paths.keyFile,
			ClientCAFile: paths.caFile,
		},
		idleTimeout: 300 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Start(ctx) }()

	t.Cleanup(func() {
		cancel()
		<-errCh
	})

	waitReachable(t, ctx, srv.Addr)
	waitReachable(t, ctx, tlsAddr)

	baseline := settledGoroutines()

	for range calls {
		client := clientFor(t, ca.certPEM, clientCert, clientKey)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+tlsAddr+"/v1/controller/version", nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}

		_ = resp.Body.Close()
	}

	// Every connection costs one goroutine on the server and two on
	// the client transport, so the calls must show up here; otherwise
	// the test would pass without exercising anything.
	held := runtime.NumGoroutine() - baseline
	if held < calls {
		t.Fatalf("only %d goroutines above baseline after %d calls, the connections are not being held", held, calls)
	}

	deadline := time.Now().Add(5 * time.Second)

	for {
		left := runtime.NumGoroutine() - baseline
		if left <= 5 {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines above baseline 5 s after %d idle connections, the server does not close them", left, calls)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// settledGoroutines waits for the goroutine count to stop changing, so
// the probe dials of waitReachable are not counted as held connections.
func settledGoroutines() int {
	n := runtime.NumGoroutine()

	for range 20 {
		time.Sleep(50 * time.Millisecond)

		m := runtime.NumGoroutine()
		if m == n {
			return n
		}

		n = m
	}

	return n
}
