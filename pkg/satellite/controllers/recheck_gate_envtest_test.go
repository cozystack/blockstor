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
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// holdingReader is concurrencyReader that can also hold one pass: armed,
// the next pass to read its peers blocks until released.
type holdingReader struct {
	concurrencyReader

	armed   atomic.Bool
	holding atomic.Bool
	release chan struct{}
}

func (h *holdingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if h.armed.CompareAndSwap(true, false) {
		h.holding.Store(true)
		<-h.release
		h.holding.Store(false)
	}

	return h.concurrencyReader.List(ctx, list, opts...)
}

// TestPeriodicRechecksRunOneAtATime reproduces a node whose replicas have
// all converged: every one of them asks for its periodic re-check, a
// full pass that finds nothing to do. With several workers those passes
// ran side by side and took all the CPU the satellite has, so a new
// replica's pass crawled next to them and linstor-csi's CreateVolume ran
// out of its minute. The re-checks must run one at a time, and a pass an
// event asked for must not wait for them.
func TestPeriodicRechecksRunOneAtATime(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	const (
		local    = "n1"
		peer     = "n2"
		pool     = "lvm-thin"
		replicas = 12
		workers  = 4
		perPass  = 200 * time.Millisecond
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

	reader := &holdingReader{
		concurrencyReader: concurrencyReader{Reader: mgr.GetAPIReader(), delay: perPass},
		release:           make(chan struct{}),
	}

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

	var released sync.Once

	t.Cleanup(func() {
		released.Do(func() { close(reader.release) })
		cancel()
		wg.Wait()
	})

	// The cold start: every replica arrives with the initial list.
	bringupWaitFor(t, 2*replicas*perPass+10*time.Second, func() bool { return reader.passes.Load() >= replicas })

	// From here on nothing changes, so every pass is a re-check. Wait
	// for each replica to have had one.
	reader.most.Store(0)

	bringupWaitFor(t, activeDRBDRequeue+replicas*((1+recheckCooldown)*perPass+recheckPoll)+10*time.Second,
		func() bool { return reader.passes.Load() >= 2*replicas })

	if most := reader.most.Load(); most > 1 {
		t.Errorf("%d periodic re-checks in flight at once with %d workers, want one at a time", most, workers)
	}

	// Hold the next re-check in the middle of its pass, then add a
	// replica: its pass has to go through while the re-check holds
	// the slot.
	reader.armed.Store(true)
	bringupWaitFor(t, activeDRBDRequeue+10*time.Second, reader.holding.Load)

	before := reader.passes.Load()

	bringupDefinition(t, cli, "pvc-new")
	bringupReplica(t, cli, "pvc-new", local, pool, replicas, 0)

	bringupWaitFor(t, 10*time.Second, func() bool { return reader.passes.Load() > before })

	if !reader.holding.Load() {
		t.Errorf("the held re-check let go before the new replica's pass")
	}

	released.Do(func() { close(reader.release) })
}
