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
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestConvergedReplicasRunNoPeriodicPass reproduces a node whose replicas
// have all converged. Each used to ask for a full pass every ten seconds
// that found nothing to do: drbdadm parsing every .res on the node, a
// dozen drbdsetup runs and an uncached read of the peers. On a node with
// a hundred replicas that was about half a core spent while nothing
// happened, where LINSTOR's satellite spends a few millicores. A
// converged replica must wait for its next event, and kernel drift comes
// in through the drift check, which this node's drbdadm never reports.
func TestConvergedReplicasRunNoPeriodicPass(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	const (
		local    = "n1"
		peer     = "n2"
		pool     = "lvm-thin"
		replicas = 6

		// quiet is longer than the old re-check period, ten seconds,
		// and spans several drift checks.
		quiet = 13 * time.Second
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

	for i := range replicas {
		rd := fmt.Sprintf("pvc-idle-%02d", i)
		bringupDefinition(t, cli, rd)
		bringupReplica(t, cli, rd, local, pool, int32(i), 0)
		bringupReplica(t, cli, rd, peer, pool, int32(i), 1)
	}

	reader := &concurrencyReader{Reader: mgr.GetAPIReader()}

	rec := &ResourceReconciler{
		Client: mgr.GetClient(),
		Config: Config{
			NodeName: local,
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{pool: &noopProvider{}},
				NodeName:  local,
			}),
			Exec:               storage.NewFakeExec(),
			APIReader:          reader,
			DriftCheckInterval: time.Second,
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

	// The cold start: every replica arrives with the initial list, and
	// the passes it starts settle.
	bringupWaitFor(t, 30*time.Second, func() bool { return reader.passes.Load() >= replicas })
	time.Sleep(2 * time.Second)

	before := reader.passes.Load()
	time.Sleep(quiet)

	if n := reader.passes.Load() - before; n != 0 {
		t.Errorf("%d passes in %v on a node where nothing changed, want none", n, quiet)
	}
}

// TestDriftCheckReachesTheReplica: a replica drbdadm lists as adjustable
// gets a pass, counted as drift.
func TestDriftCheckReachesTheReplica(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	const (
		local = "n1"
		pool  = "lvm-thin"
		rd    = "pvc-drifted"
	)

	cli, mgr := startBringupEnvtest(t)

	bringupCreate(t, cli, &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: local},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	})
	bringupCreate(t, cli, &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: pool + "." + local},
		Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: local, PoolName: pool, ProviderKind: "LVM_THIN"},
	})
	bringupDefinition(t, cli, rd)
	bringupReplica(t, cli, rd, local, pool, 0, 0)

	exec := storage.NewFakeExec()

	rec := &ResourceReconciler{
		Client: mgr.GetClient(),
		Config: Config{
			NodeName: local,
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{pool: &noopProvider{}},
				NodeName:  local,
			}),
			Exec:               exec,
			APIReader:          mgr.GetAPIReader(),
			DriftCheckInterval: 200 * time.Millisecond,
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

	time.Sleep(2 * time.Second)

	if n := rec.passes.passCount(passDrift); n != 0 {
		t.Fatalf("%d drift passes while drbdadm listed nothing", n)
	}

	exec.Expect(listAdjustable, storage.FakeResponse{Stdout: []byte(rd + "\n")})

	bringupWaitFor(t, 10*time.Second, func() bool { return rec.passes.passCount(passDrift) > 0 })
}

// TestPeerStatusChurnStartsNoPass: each peer's observer rewrites its
// Resource's status on kernel frames, and on a busy node most of those
// writes change nothing the local apply reads. They used to start a
// full local pass each, on every worker at once. Only a peer change the apply reads may start one.
func TestPeerStatusChurnStartsNoPass(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	const (
		local    = "n1"
		peer     = "n2"
		pool     = "lvm-thin"
		replicas = 4
		churn    = 20
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

	for i := range replicas {
		rd := fmt.Sprintf("pvc-churn-%02d", i)
		bringupDefinition(t, cli, rd)
		bringupReplica(t, cli, rd, local, pool, int32(i), 0)
		bringupReplica(t, cli, rd, peer, pool, int32(i), 1)
	}

	reader := &concurrencyReader{Reader: mgr.GetAPIReader()}

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

	bringupWaitFor(t, 30*time.Second, func() bool { return reader.passes.Load() >= replicas })

	writePeerStatus := func(outOfSync int64) {
		t.Helper()

		for r := range replicas {
			var res blockstoriov1alpha1.Resource

			key := client.ObjectKey{Name: fmt.Sprintf("pvc-churn-%02d.%s", r, peer)}

			err := cli.Get(t.Context(), key, &res)
			if err != nil {
				t.Fatalf("get %s: %v", key.Name, err)
			}

			res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{
				VolumeNumber: 0, DiskState: "UpToDate", CurrentGI: "1234ABCD", OutOfSyncKib: outOfSync,
			}}

			err = cli.Status().Update(t.Context(), &res)
			if err != nil {
				t.Fatalf("write status of %s: %v", key.Name, err)
			}
		}
	}

	// The peers' creation, allocation and first disk state are peer
	// events the apply reads; start counting once they have all been
	// taken.
	writePeerStatus(0)
	time.Sleep(2 * time.Second)
	rec.passes.report()

	for i := range churn {
		writePeerStatus(int64(i + 1))
	}

	time.Sleep(2 * time.Second)

	if n := rec.passes.passCount(passPeer); n != 0 {
		t.Errorf("%d local passes for %d peer status writes that change nothing the apply reads", n, churn*replicas)
	}

	// A peer change the apply does read still reaches the local replica.
	var res blockstoriov1alpha1.Resource

	err = cli.Get(t.Context(), client.ObjectKey{Name: "pvc-churn-00." + peer}, &res)
	if err != nil {
		t.Fatalf("get peer: %v", err)
	}

	res.Spec.Props = map[string]string{"Aux/test": "changed"}

	err = cli.Update(t.Context(), &res)
	if err != nil {
		t.Fatalf("change peer spec: %v", err)
	}

	bringupWaitFor(t, 10*time.Second, func() bool { return rec.passes.passCount(passPeer) > 0 })
}
