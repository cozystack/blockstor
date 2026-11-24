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
	"slices"
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

// slowReader stands in for the cost of a real reconcile. Every DRBD
// replica's pass reads its peers through the APIReader once, so a
// delay there makes each pass take about as long as it does on a node
// that shells out to drbdadm and lists every Resource uncached.
type slowReader struct {
	client.Reader

	delay time.Duration
	lists atomic.Int64
}

func (s *slowReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	s.lists.Add(1)
	time.Sleep(s.delay)

	return s.Reader.List(ctx, list, opts...) //nolint:wrapcheck // test double
}

// TestNewResourceIsNotQueuedBehindIdleReplicas reproduces the field
// report: a node with many converged DRBD replicas whose peers'
// observers keep writing status took minutes to start on a newly
// created Resource, because the replicas' periodic re-checks and the
// peer status churn kept the single-worker queue full at the same
// priority as the new Resource.
//
// The idle replicas here cost more per pass, in total, than the
// re-check interval the satellite used to have, so with that re-check
// the queue never drains. A converged replica no longer gets a periodic
// pass at all; the new Resource must still get its first pass (the
// finalizer) within a couple of passes.
// formerRecheckInterval is the periodic re-check a converged DRBD
// replica used to get.
const formerRecheckInterval = 10 * time.Second

func TestNewResourceIsNotQueuedBehindIdleReplicas(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	const (
		local   = "n1"
		peer    = "n2"
		pool    = "lvm-thin"
		idle    = 30
		perPass = 500 * time.Millisecond
		// The new Resource may wait for the pass in flight and the one
		// the queue had already handed out; the budget is counted in
		// idle passes run before it, not in wall time, so a loaded
		// runner slows both sides alike.
		budgetPasses = 3
	)

	// Idle replicas take idle*perPass to go round, longer than the
	// re-check interval the satellite used to have: a periodic pass
	// would saturate the queue on its own.
	if idle*perPass <= formerRecheckInterval {
		t.Fatalf("test setup: %d replicas x %v does not saturate a %v re-check", idle, perPass, formerRecheckInterval)
	}

	cli, mgr := startSatelliteEnvtest(t)

	reader := &slowReader{Reader: mgr.GetAPIReader(), delay: perPass}

	rec := &ResourceReconciler{
		Client: mgr.GetClient(),
		Config: Config{
			NodeName: local,
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{pool: &noopProvider{}},
				NodeName:  local,
			}),
			Exec:      storage.NewFakeExec(),
			APIReader: reader,
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

	for _, node := range []string{local, peer} {
		mustCreate(t, cli, &blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: node},
			Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		})
	}

	mustCreate(t, cli, &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: pool + "." + local},
		Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: local, PoolName: pool, ProviderKind: "LVM_THIN"},
	})

	// Created after the manager started, so their events are not part
	// of the initial list and carry normal priority, as on a node that
	// has been running since the replicas were adopted.
	peers := make([]string, 0, idle)

	for i := range idle {
		rd := fmt.Sprintf("pvc-idle-%02d", i)
		createDRBDDefinition(t, cli, rd)
		createAllocatedReplica(t, cli, rd, local, pool, int32(i), 0)
		createAllocatedReplica(t, cli, rd, peer, pool, int32(i), 1)

		peers = append(peers, rd+"."+peer)
	}

	// Let every idle replica take its first pass.
	waitFor(t, 2*time.Duration(idle+5)*perPass, func() bool { return reader.lists.Load() >= idle })

	// The peers' observers keep writing status, as they do on every
	// kernel frame.
	churnCtx, stopChurn := context.WithCancel(ctx)
	defer stopChurn()

	wg.Go(func() { churnPeerStatus(churnCtx, cli, peers) })

	// Wait as long as a periodic re-check took to come due, so one
	// would be in the queue by now.
	time.Sleep(formerRecheckInterval)

	createDRBDDefinition(t, cli, "pvc-new")

	newRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-new." + local},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName: local, ResourceDefinitionName: "pvc-new", StoragePool: pool,
		},
	}
	mustCreate(t, cli, newRes)

	start := time.Now()
	passesBefore := reader.lists.Load()

	// The deadline only keeps a regression from hanging the run: a
	// full round of the idle replicas, twice over.
	waitFor(t, 2*idle*perPass, func() bool {
		var got blockstoriov1alpha1.Resource

		getErr := cli.Get(ctx, client.ObjectKeyFromObject(newRes), &got)

		return getErr == nil && slices.Contains(got.Finalizers, SatelliteResourceFinalizer)
	})

	// The new Resource's first pass adds the finalizer and returns
	// before the peer read, so every read counted here is an idle
	// replica's pass that ran ahead of it.
	ahead := reader.lists.Load() - passesBefore
	if ahead > budgetPasses {
		t.Errorf("new Resource waited behind %d idle passes, want at most %d", ahead, budgetPasses)
	}

	t.Logf("new Resource reached in %v after %d idle passes, %d idle replicas at %v per pass",
		time.Since(start), ahead, idle, perPass)
}

func startSatelliteEnvtest(t *testing.T) (client.Client, ctrl.Manager) {
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

func mustCreate(t *testing.T, cli client.Client, obj client.Object) {
	t.Helper()

	err := cli.Create(t.Context(), obj)
	if err != nil {
		t.Fatalf("create %s: %v", obj.GetName(), err)
	}
}

func createDRBDDefinition(t *testing.T, cli client.Client, name string) {
	t.Helper()

	mustCreate(t, cli, &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack:        []string{"DRBD", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 1 << 20}},
		},
	})
}

// createAllocatedReplica creates a converged replica: finalizer in
// place and the allocator's node id, port and minor stamped, so its
// passes go straight through to the apply.
func createAllocatedReplica(t *testing.T, cli client.Client, rd, node, pool string, index, nodeID int32) {
	t.Helper()

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node, Finalizers: []string{SatelliteResourceFinalizer}},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: node, ResourceDefinitionName: rd, StoragePool: pool},
	}
	mustCreate(t, cli, res)

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

// churnPeerStatus rewrites the peers' connection state round-robin
// until ctx ends, the way a peer's observer does on kernel frames.
func churnPeerStatus(ctx context.Context, cli client.Client, peers []string) {
	for tick := 0; ctx.Err() == nil; tick++ {
		name := peers[tick%len(peers)]

		var res blockstoriov1alpha1.Resource

		err := cli.Get(ctx, client.ObjectKey{Name: name}, &res)
		if err != nil {
			continue
		}

		res.Status.Connections = []blockstoriov1alpha1.ResourceConnectionStatus{
			{PeerNodeName: "n1", Connected: tick%2 == 0, Message: fmt.Sprintf("frame-%d", tick)},
		}

		_ = cli.Status().Update(ctx, &res)

		time.Sleep(20 * time.Millisecond)
	}
}

func waitFor(t *testing.T, budget time.Duration, done func() bool) {
	t.Helper()

	deadline := time.Now().Add(budget)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", budget)
		}

		time.Sleep(50 * time.Millisecond)
	}
}
