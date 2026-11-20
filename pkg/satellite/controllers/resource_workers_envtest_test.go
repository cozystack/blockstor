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

package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// concurrencyReader stands in for the time a pass spends waiting: every
// DRBD replica's pass reads its peers through the APIReader once, and
// the delay there keeps the pass in flight long enough to see how many
// run side by side.
type concurrencyReader struct {
	client.Reader

	delay    time.Duration
	passes   atomic.Int64
	inFlight atomic.Int64
	most     atomic.Int64
}

func (c *concurrencyReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	now := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)

	for {
		seen := c.most.Load()
		if now <= seen || c.most.CompareAndSwap(seen, now) {
			break
		}
	}

	time.Sleep(c.delay)
	c.passes.Add(1)

	return c.Reader.List(ctx, list, opts...) //nolint:wrapcheck // test double
}

// TestColdStartAppliesReplicasInParallel reproduces a node coming back
// from a reboot: every local replica is already in the API when the
// satellite starts, so all of them arrive with the initial list and
// each needs a pass. With one worker the node rejoined in the sum of
// its passes, about 13 minutes for 108 replicas at 7 s a pass. The
// passes must overlap, and no more of them than the configured workers.
func TestColdStartAppliesReplicasInParallel(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	const (
		local    = "n1"
		peer     = "n2"
		pool     = "lvm-thin"
		replicas = 12
		workers  = 4
		perPass  = 300 * time.Millisecond
	)

	cli, mgr := startBringupEnvtest(t)

	for _, node := range []string{local, peer} {
		bringupCreate(t, cli, &blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: node},
			Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		})
	}

	bringupCreate(t, cli, &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: pool + "." + local},
		Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: local, PoolName: pool, ProviderKind: "LVM_THIN"},
	})

	// Everything exists before the satellite starts, as after a reboot.
	for i := range replicas {
		rd := fmt.Sprintf("pvc-boot-%02d", i)
		bringupDefinition(t, cli, rd)
		bringupReplica(t, cli, rd, local, pool, int32(i), 0)
		bringupReplica(t, cli, rd, peer, pool, int32(i), 1)
	}

	reader := &concurrencyReader{Reader: mgr.GetAPIReader(), delay: perPass}

	rec := &ResourceReconciler{
		Client: mgr.GetClient(),
		Config: Config{
			NodeName: local,
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{pool: &noopProvider{}},
				NodeName:  local,
			}),
			Exec:            storage.NewFakeExec(),
			APIReader:       reader,
			ResourceWorkers: workers,
		},
	}

	err := rec.SetupWithManager(mgr)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup

	wg.Go(func() {
		mgrErr := mgr.Start(ctx)
		if mgrErr != nil {
			t.Errorf("manager: %v", mgrErr)
		}
	})

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	// The deadline only keeps a regression from hanging the run: the
	// passes one after another, twice over.
	bringupWaitFor(t, 2*replicas*perPass+10*time.Second, func() bool { return reader.passes.Load() >= replicas })

	most := reader.most.Load()
	if most < 2 {
		t.Errorf("at most %d pass in flight at a time: the replicas were brought up one by one", most)
	}

	if most > workers {
		t.Errorf("%d passes in flight at once, want at most %d workers", most, workers)
	}

	t.Logf("%d replicas, at most %d passes in flight with %d workers", replicas, most, workers)
}

func TestResourceWorkersDefault(t *testing.T) {
	var cfg Config
	if got := cfg.resourceWorkers(); got != DefaultResourceWorkers {
		t.Errorf("unset: got %d workers, want %d", got, DefaultResourceWorkers)
	}

	cfg.ResourceWorkers = 3
	if got := cfg.resourceWorkers(); got != 3 {
		t.Errorf("set to 3: got %d workers", got)
	}

	if DefaultResourceWorkers < 2 {
		t.Errorf("default of %d workers brings a rebooted node's replicas up one by one", DefaultResourceWorkers)
	}
}

func startBringupEnvtest(t *testing.T) (client.Client, ctrl.Manager) {
	t.Helper()

	_, thisFile, _, _ := runtime.Caller(0) //nolint:dogsled // only the file path is useful
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(repoRoot, "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest: %v", err)
	}

	t.Cleanup(func() { _ = env.Stop() })

	cli, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	return cli, mgr
}

func bringupCreate(t *testing.T, cli client.Client, obj client.Object) {
	t.Helper()

	err := cli.Create(t.Context(), obj)
	if err != nil {
		t.Fatalf("create %s: %v", obj.GetName(), err)
	}
}

func bringupDefinition(t *testing.T, cli client.Client, name string) {
	t.Helper()

	bringupCreate(t, cli, &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack:        []string{"DRBD", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 1 << 20}},
		},
	})
}

// bringupReplica creates a converged replica: finalizer in place and the
// allocator's node id, port and minor stamped, so its pass goes straight
// through to the apply.
func bringupReplica(t *testing.T, cli client.Client, rd, node, pool string, index, nodeID int32) {
	t.Helper()

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node, Finalizers: []string{SatelliteResourceFinalizer}},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: node, ResourceDefinitionName: rd, StoragePool: pool},
	}
	bringupCreate(t, cli, res)

	res.Status = blockstoriov1alpha1.ResourceStatus{
		DRBDNodeID: int32Ptr(nodeID),
		DRBDPort:   int32Ptr(7000 + index),
		DRBDMinor:  int32Ptr(1000 + index),
	}

	err := cli.Status().Update(t.Context(), res)
	if err != nil {
		t.Fatalf("allocate %s: %v", res.Name, err)
	}
}

func bringupWaitFor(t *testing.T, budget time.Duration, done func() bool) {
	t.Helper()

	deadline := time.Now().Add(budget)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", budget)
		}

		time.Sleep(50 * time.Millisecond)
	}
}
