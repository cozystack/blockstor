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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedReadyWaitDefinition creates a one-volume definition and a pool on
// each of nodes, the shape linstor-csi's CreateVolume autoplaces into.
func seedReadyWaitDefinition(t *testing.T, st store.Store, rdName string, nodes ...string) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	for _, n := range nodes {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    100 << 20,
			TotalCapacity:   100 << 20,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}
}

// startReadyWaitServer starts a REST server that waits up to wait for
// placed replicas, polling the store every few milliseconds.
func startReadyWaitServer(t *testing.T, st store.Store, wait time.Duration) (string, func()) {
	t.Helper()

	prev := resourceReadyPollInterval
	resourceReadyPollInterval = 10 * time.Millisecond

	base, stop := startServerCustom(t, &Server{
		Addr:              pickFreeAddr(t),
		Store:             st,
		Client:            newFakeRESTClient(t),
		Namespace:         testRESTNamespace,
		ResourceReadyWait: wait,
	})

	return base, func() {
		stop()

		resourceReadyPollInterval = prev
	}
}

// buildPlacedReplicas plays the satellites: once want replicas of
// rdName exist, it waits a little, as building a replica takes, and
// then reports a device path for volume 0 on each of them.
func buildPlacedReplicas(ctx context.Context, st store.Store, rdName string, want int) <-chan error {
	done := make(chan error, 1)

	go func() {
		for {
			replicas, err := st.Resources().ListByDefinition(ctx, rdName)
			if err != nil {
				done <- err

				return
			}

			if len(replicas) < want {
				select {
				case <-ctx.Done():
					done <- ctx.Err()

					return
				case <-time.After(5 * time.Millisecond):
				}

				continue
			}

			time.Sleep(200 * time.Millisecond)

			for i := range replicas {
				replicas[i].Volumes = []apiv1.Volume{{
					VolumeNumber: 0,
					DevicePath:   fmt.Sprintf("/dev/drbd%d", 1000+i),
				}}

				if err := st.Resources().Update(ctx, &replicas[i]); err != nil {
					done <- err

					return
				}
			}

			done <- nil

			return
		}
	}()

	return done
}

// TestAutoplaceAnswersAfterReplicasAreBuilt pins the LINSTOR contract
// linstor-csi depends on: once the autoplace of CreateVolume returned,
// the replicas are built and a volume read on any of them carries a
// device path. Without it ControllerPublishVolume fails with "could
// not determine device path" and the attacher's exponential retry
// decides when the volume attaches.
func TestAutoplaceAnswersAfterReplicasAreBuilt(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seedReadyWaitDefinition(t, st, "pvc-ready", "n1", "n2")

	base, stop := startReadyWaitServer(t, st, 10*time.Second)
	defer stop()

	built := buildPlacedReplicas(ctx, st, "pvc-ready", 2)

	body, err := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-ready/autoplace", body)

	var rcs []apiv1.APICallRc

	decodeErr := json.NewDecoder(resp.Body).Decode(&rcs)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	if decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-ready")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(replicas) != 2 {
		t.Fatalf("placed %d replicas, want 2", len(replicas))
	}

	for i := range replicas {
		if len(replicas[i].Volumes) == 0 || replicas[i].Volumes[0].DevicePath == "" {
			t.Errorf("autoplace answered before the replica on %s had a device path", replicas[i].NodeName)
		}
	}

	for i := range rcs {
		if rcs[i].RetCode&maskWarn != 0 {
			t.Errorf("unexpected warning for replicas that became ready: %q", rcs[i].Message)
		}
	}

	if err := <-built; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestAutoplaceWarnsWhenReplicaIsNotBuiltInTime pins the bound: a
// replica that never reports a device path does not hold the answer
// past the wait, the placement stands, and each such node gets the
// warning LINSTOR gives for a resource that did not become ready.
func TestAutoplaceWarnsWhenReplicaIsNotBuiltInTime(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seedReadyWaitDefinition(t, st, "pvc-slow", "n1")

	base, stop := startReadyWaitServer(t, st, 100*time.Millisecond)
	defer stop()

	body, err := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	start := time.Now()
	resp := httpPost(t, base+"/v1/resource-definitions/pvc-slow/autoplace", body)
	elapsed := time.Since(start)

	var rcs []apiv1.APICallRc

	decodeErr := json.NewDecoder(resp.Body).Decode(&rcs)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	if decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}

	if elapsed > 5*time.Second {
		t.Errorf("autoplace took %s, the wait is bounded at 100ms", elapsed)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-slow")
	if err != nil || len(replicas) != 1 {
		t.Fatalf("placement did not stand: %d replicas, err %v", len(replicas), err)
	}

	warned := false

	for i := range rcs {
		if rcs[i].RetCode&maskWarn != 0 && rcs[i].ObjRefs["Node"] == "n1" {
			warned = true
		}
	}

	if !warned {
		t.Errorf("no not-ready warning for n1 in %+v", rcs)
	}
}
