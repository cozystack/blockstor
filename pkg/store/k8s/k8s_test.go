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
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
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
		// Skip the whole package: every test needs envtest, and there is no
		// useful test-time fallback (controller-runtime's fake client does
		// not honour our label selector / status subresource semantics).
		os.Exit(0)
	}

	f, err := startEnvtest()
	if err != nil {
		_, _ = os.Stderr.WriteString("envtest start: " + err.Error() + "\n")
		os.Exit(1)
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
}
