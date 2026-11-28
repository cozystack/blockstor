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
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestIdleConnectionsAreClosed reproduces a client that builds a new
// transport for every call and never closes the old one. Each such
// call leaves a keep-alive connection open, and without an idle
// timeout the server holds it, with its goroutine and buffers, for as
// long as the client process lives. The test makes the calls, checks
// that the server is holding them, and then that it lets them go once
// they have been idle for longer than its idle timeout.
func TestIdleConnectionsAreClosed(t *testing.T) {
	const calls = 40

	ca := newTestCA(t)
	paths := writeServerCerts(t, ca, t.TempDir(), "blockstor-apiserver")
	clientCert, clientKey := ca.issue(t, "blockstor-csi", false)

	var (
		mu   sync.Mutex
		open = map[net.Conn]struct{}{}
	)

	openConns := func() int {
		mu.Lock()
		defer mu.Unlock()

		return len(open)
	}

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
		connState: func(c net.Conn, st http.ConnState) {
			mu.Lock()
			defer mu.Unlock()

			switch st {
			case http.StateNew:
				open[c] = struct{}{}
			case http.StateClosed, http.StateHijacked:
				delete(open, c)
			case http.StateActive, http.StateIdle:
			}
		},
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
	waitOpenConns(t, openConns, 0, "the reachability probes")

	for range calls {
		client := clientFor(t, ca.certPEM, clientCert, clientKey)
		t.Cleanup(client.CloseIdleConnections)

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

	// The calls finish well within the idle timeout, so every one of
	// them must still be open here; otherwise the test would pass
	// without exercising anything.
	if n := openConns(); n != calls {
		t.Fatalf("server holds %d connections after %d calls, want %d", n, calls, calls)
	}

	waitOpenConns(t, openConns, 0, "idle timeout")
}

// waitOpenConns waits up to 5 s for the server to hold want connections.
func waitOpenConns(t *testing.T, openConns func() int, want int, after string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		n := openConns()
		if n == want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("server still holds %d connections after %s, want %d", n, after, want)
		}

		time.Sleep(20 * time.Millisecond)
	}
}
