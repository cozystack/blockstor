//go:build integration

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

// Package harness wires the Tier 2 integration-test stack: envtest
// (in-process kube-apiserver+etcd) → controller-runtime manager with
// every reconciler we ship → blockstor REST server on a free port →
// in-process satellite mock writing Status fields. See
// docs/test-strategy.md for the tier architecture and
// tests/integration/README.md for how to run.
package harness

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// Env is the booted envtest stack: a live in-process kube-apiserver
// + etcd, the controller-runtime config to talk to it, and a typed
// client preloaded with our scheme. Stop is registered via
// t.Cleanup; callers should not invoke it directly.
type Env struct {
	Cfg    *rest.Config
	Client client.Client
	Stop   func()
}

// Start boots envtest with the blockstor CRDs from
// config/crd/bases/. It reads KUBEBUILDER_ASSETS (the path the
// controller-runtime setup-envtest tool prints) and fails the test
// with a clear message if unset — there is no useful fallback,
// because the envtest harness requires the etcd + kube-apiserver
// binaries to launch.
//
// The returned Env is ready for use; teardown is registered via
// t.Cleanup. Each call boots a fresh apiserver, so tests are
// trivially isolated.
func Start(t *testing.T) *Env {
	t.Helper()

	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Fatalf("KUBEBUILDER_ASSETS is unset; run `setup-envtest use --print path 1.34.x` and export it before running integration tests (see tests/integration/README.md)")
	}

	// scheme carries both the core clientgo types and the blockstor
	// CRD group, mirroring cmd/controller/main.go's init().
	scheme := clientgoscheme.Scheme
	utilruntime.Must(blockstoriov1alpha1.AddToScheme(scheme))

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdBasesPath(t)},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: assets,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("envtest start: %v", err)
	}

	envClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		_ = testEnv.Stop()

		t.Fatalf("envtest client: %v", err)
	}

	stop := func() {
		// envtest's Stop is best-effort; a 30s budget mirrors the
		// kubebuilder default Eventually timeout used in
		// internal/controller/suite_test.go.
		const stopBudget = 30 * time.Second

		deadline := time.Now().Add(stopBudget)

		for {
			err := testEnv.Stop()
			if err == nil {
				return
			}

			if time.Now().After(deadline) {
				t.Logf("envtest stop after deadline: %v", err)

				return
			}

			time.Sleep(time.Second)
		}
	}
	t.Cleanup(stop)

	return &Env{Cfg: cfg, Client: envClient, Stop: stop}
}

// crdBasesPath resolves the absolute path to config/crd/bases/
// regardless of which sub-package's working directory the test
// runs in. Anchors the lookup on this source file so go test's
// per-package CWD doesn't break the path.
func crdBasesPath(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed; cannot locate CRD bases")
	}

	// thisFile = <repo>/tests/integration/harness/envtest.go
	// Walk up four levels: harness → integration → tests → <repo>.
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")

	return filepath.Join(repoRoot, "config", "crd", "bases")
}
