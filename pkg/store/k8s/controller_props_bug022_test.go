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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// TestControllerPropsSeeCRDWritesWithoutRestart pins BUG-022: the
// CRD-backed store's ControllerProps() view used to be a
// process-local map populated once at construction and never synced
// from the ControllerConfig CRD again. `linstor controller
// set-property` persisted into Spec.ExtraProps via the REST shim, but
// every reconciler / placer read through Store.ControllerProps() —
// so controller-scope knobs (BalanceResourcesInterval / GracePeriod /
// Enabled, Autoplacer/Weights/*, AllowMixingStoragePoolDriver) were
// silent no-ops until a controller restart.
//
// The test reproduces the operator-day split exactly: construct the
// store FIRST (the long-running controller process), then drive the
// REST write path (PatchControllerExtraProps is what
// handleControllerPropsModify calls), then assert the SAME store
// instance observes the fresh value — no reconstruction, no restart.
func TestControllerPropsSeeCRDWritesWithoutRestart(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()

	// 1) The controller process boots: store constructed while the
	// ControllerConfig CRD does not exist yet.
	st := k8s.New(fixture.client)

	props, err := st.ControllerProps().Get(ctx)
	if err != nil {
		t.Fatalf("Get before any write: %v", err)
	}

	if len(props) != 0 {
		t.Fatalf("fresh cluster must read an empty props bag, got %v", props)
	}

	// 2) Operator runs `linstor controller set-property
	// BalanceResourcesInterval 1` — the REST handler lands it on
	// ControllerConfig.Spec.ExtraProps through this exact helper.
	err = k8s.PatchControllerExtraProps(ctx, fixture.client, func(m map[string]string) error {
		m[apiv1.PropBalanceResourcesInterval] = "1"
		m[apiv1.PropBalanceResourcesGracePeriod] = "0"

		return nil
	})
	if err != nil {
		t.Fatalf("simulate REST set-property: %v", err)
	}

	// 3) The running reconcilers' next ControllerProps().Get() MUST
	// observe the write. Pre-fix this returned the stale boot-time map
	// (empty) and the scheduled rebalance kept firing at the 5-minute
	// default.
	props, err = st.ControllerProps().Get(ctx)
	if err != nil {
		t.Fatalf("Get after set-property: %v", err)
	}

	if got := props[apiv1.PropBalanceResourcesInterval]; got != "1" {
		t.Errorf("BalanceResourcesInterval after CLI write: got %q, want %q (stale process-local map?)", got, "1")
	}

	if got := props[apiv1.PropBalanceResourcesGracePeriod]; got != "0" {
		t.Errorf("BalanceResourcesGracePeriod after CLI write: got %q, want %q", got, "0")
	}

	// 4) Drop-property is the symmetric half: deletion must also be
	// visible live.
	err = k8s.PatchControllerExtraProps(ctx, fixture.client, func(m map[string]string) error {
		delete(m, apiv1.PropBalanceResourcesInterval)

		return nil
	})
	if err != nil {
		t.Fatalf("simulate REST drop-property: %v", err)
	}

	props, err = st.ControllerProps().Get(ctx)
	if err != nil {
		t.Fatalf("Get after drop-property: %v", err)
	}

	if _, stale := props[apiv1.PropBalanceResourcesInterval]; stale {
		t.Errorf("BalanceResourcesInterval still visible after drop-property: %v", props)
	}
}
