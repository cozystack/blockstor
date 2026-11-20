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
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// passRecorder is a storage provider whose volume probe stands in for
// the time a pass takes and records which resources had one.
type passRecorder struct {
	noopProvider

	delay time.Duration

	mu   sync.Mutex
	seen map[string]struct{}
}

func (p *passRecorder) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	time.Sleep(p.delay)

	p.mu.Lock()
	p.seen[vol.ResourceName] = struct{}{}
	p.mu.Unlock()

	return p.noopProvider.VolumeStatus(ctx, vol)
}

func (p *passRecorder) passed() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	names := make([]string, 0, len(p.seen))
	for name := range p.seen {
		names = append(names, name)
	}

	return names
}

// TestColdStartBringsEveryReplicaUpUnderEventChurn reproduces the stall
// after a reboot: the replicas that are already up keep producing kernel
// events, the observer queues each of them again at normal priority, and
// the replicas still waiting for their first pass came from the initial
// list at low priority. Once more replicas are up than there are
// workers, their repeat passes held every worker and the rest of the
// node did not come up until the events died down.
func TestColdStartBringsEveryReplicaUpUnderEventChurn(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	const (
		local    = "n1"
		peer     = "n2"
		pool     = "lvm-thin"
		replicas = 24
		workers  = 4
		perPass  = 150 * time.Millisecond
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

	provider := &passRecorder{delay: perPass, seen: map[string]struct{}{}}
	trigger := make(chan event.GenericEvent, 1024)

	rec := &ResourceReconciler{
		Client: mgr.GetClient(),
		Config: Config{
			NodeName: local,
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{pool: provider},
				NodeName:  local,
			}),
			Exec:             storage.NewFakeExec(),
			APIReader:        mgr.GetAPIReader(),
			ResourceWorkers:  workers,
			ReconcileTrigger: trigger,
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

	// The observer: every replica that came up keeps producing kernel
	// events (connections, resyncs, peer disk states), each one a
	// wake-up for that replica.
	wg.Go(func() {
		for ctx.Err() == nil {
			for _, rd := range provider.passed() {
				select {
				case trigger <- event.GenericEvent{Object: &blockstoriov1alpha1.Resource{
					ObjectMeta: metav1.ObjectMeta{Name: rd + "." + local},
				}}:
				default:
				}
			}

			time.Sleep(10 * time.Millisecond)
		}
	})

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	// Two rounds of passes on every worker are plenty; starved, the
	// waiting replicas never get one while the events keep coming.
	budget := 2*replicas*perPass/workers + 10*time.Second

	deadline := time.Now().Add(budget)
	for len(provider.passed()) < replicas {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d replicas brought up within %v: the rest waited behind repeat passes of replicas already up",
				len(provider.passed()), replicas, budget)
		}

		time.Sleep(50 * time.Millisecond)
	}

	// Brought up, each replica gives the rank back, or the next node
	// event would queue it ahead of a replica created after the restart.
	bringupWaitFor(t, 5*time.Second, func() bool {
		for i := range replicas {
			if rec.bringUp.pending(fmt.Sprintf("pvc-churn-%02d.%s", i, local)) {
				return false
			}
		}

		return true
	})
}
