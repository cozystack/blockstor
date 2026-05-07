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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/version"
)

// startServer spins up the REST server on a free loopback port and returns
// its base URL plus a teardown function. Tests use this helper instead of
// reaching into Server.Start directly so cancellation semantics are uniform.
func startServer(t *testing.T) (string, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())

	lc := &net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := &Server{Addr: addr}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()

	// Wait for the listener to come back up under the server's control.
	dialer := &net.Dialer{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)

	for {
		c, dErr := dialer.DialContext(ctx, "tcp", addr)
		if dErr == nil {
			_ = c.Close()
			break
		}

		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("server never became reachable: %v", dErr)
		}

		time.Sleep(50 * time.Millisecond)
	}

	stop := func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Errorf("server did not stop within 2s after cancel")
		}
	}

	return "http://" + addr, stop
}

// TestVersionViaGolinstor verifies the canonical happy path: golinstor — the
// Go client every LINSTOR consumer in the ecosystem uses — calls
// /v1/controller/version against our REST server, unmarshals the response
// cleanly, and sees the values we promised in pkg/version.
func TestVersionViaGolinstor(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}

	c, err := lapi.NewClient(lapi.BaseURL(u))
	if err != nil {
		t.Fatalf("golinstor NewClient: %v", err)
	}

	got, err := c.Controller.GetVersion(t.Context())
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}

	if got.Version != version.LinstorVersion {
		t.Errorf("Version: got %q, want %q", got.Version, version.LinstorVersion)
	}

	// golinstor's struct field is named RestApiVersion (not RestAPIVersion);
	// keep our own Go-side identifier idiomatic but match golinstor on access.
	if got.RestApiVersion != version.RestAPIVersion {
		t.Errorf("RestApiVersion: got %q, want %q", got.RestApiVersion, version.RestAPIVersion)
	}

	if got.GitHash != version.LinstorGitHash {
		t.Errorf("GitHash: got %q, want %q", got.GitHash, version.LinstorGitHash)
	}

	if got.BuildTime != version.LinstorBuildTime {
		t.Errorf("BuildTime: got %q, want %q", got.BuildTime, version.LinstorBuildTime)
	}
}

// TestVersionRawJSON pins the on-wire JSON shape (snake_case keys, exact
// field set, content-type) so a future refactor cannot accidentally break
// the LINSTOR REST contract.
func TestVersionRawJSON(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	resp := httpGet(t, base+"/v1/controller/version")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q, want %q", ct, "application/json")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	// Decode into a generic map to assert the exact JSON shape.
	var m map[string]any

	err = json.Unmarshal(body, &m)
	if err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}

	wantKeys := []string{"version", "git_hash", "build_time", "rest_api_version"}
	for _, k := range wantKeys {
		if _, ok := m[k]; !ok {
			t.Errorf("missing JSON key %q in response: %s", k, body)
		}
	}

	if len(m) != len(wantKeys) {
		t.Errorf("got %d JSON keys, want %d. body=%s", len(m), len(wantKeys), body)
	}

	// Decoding into our typed struct should also succeed.
	var typed apiv1.ControllerVersion

	err = json.Unmarshal(body, &typed)
	if err != nil {
		t.Fatalf("typed unmarshal: %v", err)
	}
}

// TestVersionMethodNotAllowed verifies the endpoint refuses non-GET methods.
// We register only `GET /v1/controller/version`; ServeMux returns 405 for
// other methods on the same path.
func TestVersionMethodNotAllowed(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), method, base+"/v1/controller/version", nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("status: got %d, want 405", resp.StatusCode)
			}
		})
	}
}

// TestUnknownEndpointNotFound verifies that paths we have not implemented
// yet return 404. Once we implement them they will return 200/4xx with
// their own contracts, and this test moves with them.
func TestUnknownEndpointNotFound(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	resp := httpGet(t, base+"/v1/this-endpoint-does-not-exist")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestHealthzReturnsNoContent verifies the readiness probe path returns
// the canonical 204 with an empty body (no JSON decoding required).
func TestHealthzReturnsNoContent(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	resp := httpGet(t, base+"/v1/healthz")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status: got %d, want 204", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if len(body) != 0 {
		t.Errorf("body: got %d bytes, want empty (%q)", len(body), body)
	}
}

// TestServerShutdownOnContextCancel verifies the manager.Runnable contract:
// when the parent context is cancelled, Start returns nil within the
// shutdown grace window.
func TestServerShutdownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	lc := &net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := &Server{Addr: addr}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()

	// Wait until the server is actually serving before we cancel.
	dialer := &net.Dialer{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)

	for {
		c, dErr := dialer.DialContext(ctx, "tcp", addr)
		if dErr == nil {
			_ = c.Close()
			break
		}

		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("server never became reachable: %v", dErr)
		}

		time.Sleep(50 * time.Millisecond)
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Start returned non-nil after context cancel: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("Start did not return within 15s after context cancel")
	}
}

// TestServerListenError verifies that a fatal listener failure (port already
// in use) is surfaced through Start's return value rather than swallowed.
func TestServerListenError(t *testing.T) {
	// Grab a port and hold it so the server can't bind.
	lc := &net.ListenConfig{}
	hold, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = hold.Close() }()

	addr := hold.Addr().String()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	srv := &Server{Addr: addr}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Errorf("Start returned nil; want non-nil because the port was busy")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Start did not return within 5s on bind failure")
	}
}

// TestNeedLeaderElection pins the runnable's leader-election declaration.
// Today the REST server is read-mostly and runs on every replica; if we
// ever add a write path that requires a single leader, this test will fail
// and force the change to be intentional.
func TestNeedLeaderElection(t *testing.T) {
	srv := &Server{Addr: "127.0.0.1:0"}
	if got := srv.NeedLeaderElection(); got != false {
		t.Errorf("NeedLeaderElection: got %v, want false", got)
	}
}

func httpGet(t *testing.T, addr string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, addr, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	return resp
}
