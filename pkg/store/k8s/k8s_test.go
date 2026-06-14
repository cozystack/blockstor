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

package k8s_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
	"github.com/cozystack/blockstor/pkg/store/storetest"
)

// envtestFixture starts a local kube-apiserver+etcd, registers the blockstor
// CRDs against it, and tears it down on test end. We share one envtest across
// all subtests via TestMain to keep the suite under ~10s; each subtest gets a
// fresh per-store wrapper that wipes leftover objects.
type envtestFixture struct {
	env    *envtest.Environment
	client client.Client
}

//nolint:gochecknoglobals // shared envtest fixture across the package's tests
var fixture *envtestFixture

func TestMain(m *testing.M) {
	if !envtestAvailable() {
		// envtest binaries not installed — run only the
		// pure-function tests (they don't need k8s). Each
		// envtest-dependent test individually skips via
		// `if fixture == nil { t.Skip(...) }`.
		os.Exit(m.Run())
	}

	f, err := startEnvtest()
	if err != nil {
		// Asset directory present but startup failed (most
		// commonly: wrong-arch etcd/kube-apiserver on a shared
		// dev stand, or a kernel/permission quirk). Surface the
		// error so it's visible, but still run the package —
		// pure-function tests are valuable on their own and
		// envtest-dependent tests will skip cleanly.
		_, _ = os.Stderr.WriteString("envtest start: " + err.Error() + "\n")
		os.Exit(m.Run())
	}

	fixture = f

	code := m.Run()

	if stopErr := f.env.Stop(); stopErr != nil {
		_, _ = os.Stderr.WriteString("envtest stop: " + stopErr.Error() + "\n")
	}

	os.Exit(code)
}

// TestK8sNodeStore runs the shared NodeStore suite against the CRD-backed store.
func TestK8sNodeStore(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	storetest.RunNodeStore(t, func(t *testing.T) store.Store {
		t.Helper()
		t.Cleanup(func() { wipeAll(t, fixture.client) })

		return k8s.New(fixture.client)
	})
}

// TestK8sStoragePoolStore runs the shared StoragePoolStore suite.
func TestK8sStoragePoolStore(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	storetest.RunStoragePoolStore(t, func(t *testing.T) store.Store {
		t.Helper()
		t.Cleanup(func() { wipeAll(t, fixture.client) })

		return k8s.New(fixture.client)
	})
}

// TestK8sResourceGroupStore runs the shared ResourceGroupStore suite.
func TestK8sResourceGroupStore(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	storetest.RunResourceGroupStore(t, func(t *testing.T) store.Store {
		t.Helper()
		t.Cleanup(func() { wipeAll(t, fixture.client) })

		return k8s.New(fixture.client)
	})
}

// TestK8sResourceDefinitionStore runs the shared ResourceDefinitionStore suite.
func TestK8sResourceDefinitionStore(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	storetest.RunResourceDefinitionStore(t, func(t *testing.T) store.Store {
		t.Helper()
		t.Cleanup(func() { wipeAll(t, fixture.client) })

		return k8s.New(fixture.client)
	})
}

// TestK8sResourceStore runs the shared ResourceStore suite.
func TestK8sResourceStore(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	storetest.RunResourceStore(t, func(t *testing.T) store.Store {
		t.Helper()
		t.Cleanup(func() { wipeAll(t, fixture.client) })

		return k8s.New(fixture.client)
	})
}

// TestK8sVolumeDefinitionStore runs the shared VolumeDefinitionStore suite.
func TestK8sVolumeDefinitionStore(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	storetest.RunVolumeDefinitionStore(t, func(t *testing.T) store.Store {
		t.Helper()
		t.Cleanup(func() { wipeAll(t, fixture.client) })

		return k8s.New(fixture.client)
	})
}

// TestK8sVolumeDefinitionConcurrentAutoNumber is the BUG-048 (P1,
// availability) regression at the store layer against a REAL apiserver:
// N goroutines each call CreateAutoNumbered on the same RD concurrently.
// Every call must succeed and land at a DISTINCT VolumeNumber — the
// allocate-inside-RetryOnConflict loop must converge under genuine
// optimistic-lock 409s rather than dropping the racing creates.
//
// envtest's client is a direct (uncached) reader, so this exercises the
// retry-on-409 convergence; the cache-lag half of BUG-048 (which needs
// the informer-cached client + GetAPIReader fallback) is covered by the
// live-stand cli-matrix cell. Together they pin both halves.
func TestK8sVolumeDefinitionConcurrentAutoNumber(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	st := k8s.New(fixture.client)
	ctx := context.Background()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-conc"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const n = 6

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for range n {
		wg.Add(1)

		go func() {
			defer wg.Done()

			vd := apiv1.VolumeDefinition{SizeKib: 4096}
			if _, err := st.VolumeDefinitions().CreateAutoNumbered(ctx, "pvc-conc", &vd); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	for _, e := range errs {
		t.Errorf("concurrent CreateAutoNumbered failed (a volume was dropped): %v", e)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-conc")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(vds) != n {
		t.Fatalf("got %d VDs after %d concurrent auto-numbered creates, want %d (lost update)", len(vds), n, n)
	}

	seen := map[int32]bool{}
	for i := range vds {
		if seen[vds[i].VolumeNumber] {
			t.Fatalf("duplicate VolumeNumber %d", vds[i].VolumeNumber)
		}
		seen[vds[i].VolumeNumber] = true
	}

	for want := range int32(n) {
		if !seen[want] {
			t.Errorf("missing VolumeNumber %d in %v — a concurrent create was dropped", want, seen)
		}
	}
}

// TestK8sSnapshotStore runs the shared SnapshotStore suite.
func TestK8sSnapshotStore(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	storetest.RunSnapshotStore(t, func(t *testing.T) store.Store {
		t.Helper()
		t.Cleanup(func() { wipeAll(t, fixture.client) })

		return k8s.New(fixture.client)
	})
}

// TestK8sControllerPropsStore runs the shared ControllerPropsStore
// suite against the CRD-backed store (BUG-022: keeps the k8s
// implementation behaviourally identical to InMemory — the old
// process-local map drifted silently).
func TestK8sControllerPropsStore(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	storetest.RunControllerPropsStore(t, func(t *testing.T) store.Store {
		t.Helper()
		t.Cleanup(func() { wipeAll(t, fixture.client) })

		return k8s.New(fixture.client)
	})
}

// envtestAvailable returns whether KUBEBUILDER_ASSETS or a known asset
// directory exists; without binaries we cannot start envtest.
func envtestAvailable() bool {
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		return true
	}

	return findAssetDir() != ""
}

func findAssetDir() string {
	// kubebuilder's setup-envtest puts binaries under <repo>/bin/k8s/<ver-os-arch>.
	// Climb up from this file's directory to repo root.
	_, thisFile, _, _ := runtime.Caller(0) //nolint:dogsled // only the file path is useful
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
	base := filepath.Join(repoRoot, "bin", "k8s")

	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}

	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(base, e.Name())
		}
	}

	return ""
}

func startEnvtest() (*envtestFixture, error) {
	_, thisFile, _, _ := runtime.Caller(0) //nolint:dogsled // only the file path is useful
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(repoRoot, "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: findAssetDir(),
	}

	cfg, err := env.Start()
	if err != nil {
		return nil, err //nolint:wrapcheck // returned to TestMain which prints
	}

	if err := crdv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		_ = env.Stop()

		return nil, err //nolint:wrapcheck // same
	}

	c, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		_ = env.Stop()

		return nil, err //nolint:wrapcheck // same
	}

	return &envtestFixture{env: env, client: c}, nil
}

// wipeAll removes every Node and StoragePool CRD between subtests so the
// shared suite sees a clean store each time.
func wipeAll(t *testing.T, c client.Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, gvk := range []schema.GroupVersionKind{
		crdv1alpha1.GroupVersion.WithKind("NodeList"),
		crdv1alpha1.GroupVersion.WithKind("StoragePoolList"),
	} {
		err := c.DeleteAllOf(ctx, &crdv1alpha1.Node{},
			client.InNamespace(""), client.MatchingLabels{})
		_ = err
		_ = gvk
	}
	// Direct DeleteAllOf for both kinds:
	if err := c.DeleteAllOf(ctx, &crdv1alpha1.Node{}); err != nil {
		t.Logf("wipe Nodes: %v", err)
	}

	if err := c.DeleteAllOf(ctx, &crdv1alpha1.StoragePool{}); err != nil {
		t.Logf("wipe StoragePools: %v", err)
	}

	if err := c.DeleteAllOf(ctx, &crdv1alpha1.ResourceGroup{}); err != nil {
		t.Logf("wipe ResourceGroups: %v", err)
	}

	if err := c.DeleteAllOf(ctx, &crdv1alpha1.ResourceDefinition{}); err != nil {
		t.Logf("wipe ResourceDefinitions: %v", err)
	}

	if err := c.DeleteAllOf(ctx, &crdv1alpha1.Resource{}); err != nil {
		t.Logf("wipe Resources: %v", err)
	}

	if err := c.DeleteAllOf(ctx, &crdv1alpha1.Snapshot{}); err != nil {
		t.Logf("wipe Snapshots: %v", err)
	}

	// The ControllerConfig singleton backs ControllerProps (BUG-022);
	// leftover ExtraProps would leak between subtests.
	if err := c.DeleteAllOf(ctx, &crdv1alpha1.ControllerConfig{}); err != nil {
		t.Logf("wipe ControllerConfigs: %v", err)
	}
}
