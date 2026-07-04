// SPDX-License-Identifier: Apache-2.0

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

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// VD-resize size-bounds regression (adversarial round 4, 2026-07-03).
// The CREATE path gates size_kib into [4 MiB, 16 TiB] (Bug 155) so the
// satellite never hot-loops on `drbdadm create-md`. The RESIZE path
// (`PUT .../volume-definitions/{vn}`, `linstor vd set-size`) did not,
// so an out-of-range size could be persisted through the resize verb
// and wedge the satellite exactly as Bug 155 described. These envtest
// regressions drive the real apiserver + store round-trip and assert
// both the wire rejection and that the durable spec is left untouched.

// VD size bounds mirrored from pkg/rest/volume_definitions.go (Bug 155).
const (
	vdBoundsMinSizeKib int64 = 4 * 1024                // 4 MiB
	vdBoundsMaxSizeKib int64 = 16 * 1024 * 1024 * 1024 // 16 TiB
)

// vdBoundsPut issues a PUT with a JSON body and returns (status, body).
func vdBoundsPut(t *testing.T, url string, payload any) (int, []byte) {
	t.Helper()

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), groupGTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build PUT: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	return resp.StatusCode, body
}

// vdBoundsStoredSize reads the durable SizeKib of vol-0 on rd from the
// RD CRD (the source of truth the satellite would reconcile against).
func vdBoundsStoredSize(t *testing.T, stack *harness.Stack, rd string) int64 {
	t.Helper()

	var rdObj blockstoriov1alpha1.ResourceDefinition
	if err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: rd}, &rdObj); err != nil {
		t.Fatalf("get RD %q: %v", rd, err)
	}

	for i := range rdObj.Spec.VolumeDefinitions {
		if rdObj.Spec.VolumeDefinitions[i].VolumeNumber == 0 {
			return rdObj.Spec.VolumeDefinitions[i].SizeKib
		}
	}

	t.Fatalf("RD %q has no vol-0", rd)

	return 0
}

// TestVDResizeRejectsBelowFloor: `vd set-size` (PUT) with force=true to
// a positive size below the 4 MiB DRBD floor must be refused, and the
// stored size must stay unchanged. Pre-fix the resize path accepted it
// (200) and persisted the sub-floor size, reproducing the Bug 155
// satellite hot-loop through the resize verb.
func TestVDResizeRejectsBelowFloor(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	rd := seedRDWithVolume(t, stack, "r4-floor")
	orig := vdBoundsStoredSize(t, stack, rd)

	belowFloor := vdBoundsMinSizeKib - 1024 // 1 MiB below the floor, still > 0
	url := stack.RestURL + "/v1/resource-definitions/" + rd + "/volume-definitions/0"

	status, body := vdBoundsPut(t, url, map[string]any{"size_kib": belowFloor, "force": true})
	t.Logf("resize to %d KiB (below 4 MiB floor, force) → status=%d body=%s", belowFloor, status, string(body))

	if status >= 200 && status < 300 {
		t.Fatalf("resize ACCEPTED a sub-floor size %d KiB < %d KiB min (Bug-155 class via vd set-size)",
			belowFloor, vdBoundsMinSizeKib)
	}

	if got := vdBoundsStoredSize(t, stack, rd); got != orig {
		t.Fatalf("rejected sub-floor resize STILL mutated the stored size: got %d KiB, want %d", got, orig)
	}
}

// TestVDResizeRejectsAboveMax: `vd set-size` (PUT) grow above the 16 TiB
// ceiling (a pure grow, so only a max-bound gate can stop it) must be
// refused, and the stored size must stay unchanged.
func TestVDResizeRejectsAboveMax(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	rd := seedRDWithVolume(t, stack, "r4-max")
	orig := vdBoundsStoredSize(t, stack, rd)

	aboveMax := vdBoundsMaxSizeKib + (1024 * 1024) // 1 GiB past the ceiling
	url := stack.RestURL + "/v1/resource-definitions/" + rd + "/volume-definitions/0"

	status, body := vdBoundsPut(t, url, map[string]any{"size_kib": aboveMax})
	t.Logf("resize to %d KiB (above 16 TiB max) → status=%d body=%s", aboveMax, status, string(body))

	if status >= 200 && status < 300 {
		t.Fatalf("resize ACCEPTED an over-max size %d KiB > %d KiB max (Bug-155 class via vd set-size)",
			aboveMax, vdBoundsMaxSizeKib)
	}

	if got := vdBoundsStoredSize(t, stack, rd); got != orig {
		t.Fatalf("rejected over-max resize STILL mutated the stored size: got %d KiB, want %d", got, orig)
	}
}
