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

package csidriver

// Test helpers shared between 11.W08 (CreateSnapshot) and 11.W09
// (CreateVolume from snapshot). We boot a real `pkg/rest` server
// backed by `pkg/store`'s in-memory implementation so the wire
// path under test is the same path linstor-csi hits in
// production: golinstor REST client → blockstor's HTTP mux →
// Store. The only thing we stub is the controller-runtime client
// for native-object endpoints (Secrets/ControllerConfig) which
// these snapshot flows do not touch.

import (
	"context"
	"net"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/rest"
	"github.com/cozystack/blockstor/pkg/store"
)

const testCSIRESTNamespace = "blockstor-system"

// pickFreeAddr binds an ephemeral port, then immediately closes
// it. There is a small race window before the caller binds, but
// it is acceptable for unit tests and mirrors the pattern used
// inside pkg/rest's own test suite.
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

// newFakeCtrlClient builds a controller-runtime fake client with
// the project's CRDs + core/v1 in scheme. blockstor's REST server
// requires a non-nil Client for endpoints that touch native
// objects; the snapshot endpoints under test here don't, but the
// constructor still expects a value.
func newFakeCtrlClient(t *testing.T) client.Client {
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

// startRESTServer launches a real `pkg/rest` server bound to a
// random localhost port and returns its base URL. The caller
// MUST defer the returned stop func. Mirrors the
// `startServerWithStore` helper from pkg/rest's own tests but
// lives here because Go does not export `_test.go` helpers
// across package boundaries.
func startRESTServer(t *testing.T, st store.Store) (string, func()) {
	t.Helper()

	srv := &rest.Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeCtrlClient(t),
		Namespace: testCSIRESTNamespace,
	}

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Start(ctx) }()

	// Wait until the listener accepts connections so the first
	// REST call doesn't race the server's bind.
	dialer := &net.Dialer{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)

	for {
		c, dErr := dialer.DialContext(ctx, "tcp", srv.Addr)
		if dErr == nil {
			_ = c.Close()

			break
		}

		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("REST server never became reachable: %v", dErr)
		}

		time.Sleep(50 * time.Millisecond)
	}

	stop := func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Errorf("REST server did not stop within 2s after cancel")
		}
	}

	return "http://" + srv.Addr, stop
}
