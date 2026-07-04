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

// Bug 433 — end-to-end regression for the per-volume DRBDMinor drop on a
// VD-scoped modify.
//
// RD.Spec.VolumeDefinitions[].DRBDMinor is the /dev/drbd<N> device
// identity, "identical on every node that hosts a replica" and — per the
// CRD contract — "a non-nil value is authoritative and is NEVER
// overwritten … the store-side VolumeDefinitions carry-across preserves
// the value through a REST modify" (api/v1alpha1/resourcedefinition_types.go).
//
// A VD-scoped modify (`vd set-size` / `vd set-property`) round-trips the
// entry through wireToCRDVD, which transcodes only VolumeNumber/SizeKib/
// Props/Flags and dropped DRBDMinor, so PatchVolumeDefinitionSpec wrote
// back an entry with a nil minor. In isolation the allocator re-picks the
// same value (self-heals); but once a lower minor has been freed (routine
// RD churn), the controller's allocate-if-nil pass hands the resized
// volume a DIFFERENT minor — a permanent device-identity change on a live
// volume, driven purely by a legal in-bounds modify.
//
// These exercise the WHOLE path (REST handler → store → controller
// allocator), unlike the store-layer L1 unit (pkg/store/k8s/
// drbd_minor_carry_bug433_test.go): they FAIL on the pre-fix tree with a
// diverged minor and PASS once wireToCRDVDPreserving carries it across.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// b433Put issues a PUT with a JSON body and returns (status, body).
func b433Put(t *testing.T, url string, payload any) (int, []byte) {
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

// b433VDMinor returns the durable DRBDMinor pointer for (rd, vn) read
// straight off the RD CRD (nil = unset).
func b433VDMinor(t *testing.T, stack *harness.Stack, rd string, vn int32) *int32 {
	t.Helper()

	var rdObj blockstoriov1alpha1.ResourceDefinition
	if err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: rd}, &rdObj); err != nil {
		t.Fatalf("get RD %q: %v", rd, err)
	}

	for i := range rdObj.Spec.VolumeDefinitions {
		if rdObj.Spec.VolumeDefinitions[i].VolumeNumber == vn {
			return rdObj.Spec.VolumeDefinitions[i].DRBDMinor
		}
	}

	return nil
}

// b433WaitMinor blocks until (rd, vn) has a non-nil DRBDMinor.
func b433WaitMinor(t *testing.T, stack *harness.Stack, rd string, vn int32) int32 {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if m := b433VDMinor(t, stack, rd, vn); m != nil {
			return *m
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("RD %q vol %d: DRBDMinor never allocated", rd, vn)

	return 0
}

// b433WaitMinorSettled waits until (rd, vn) has a non-nil DRBDMinor that
// stays constant for a full settle window, then returns it. This lets a
// racing allocator finish re-stamping before the assertion reads the
// final value (so a transient nil→re-heal can't be mistaken for stability).
func b433WaitMinorSettled(t *testing.T, stack *harness.Stack, rd string, vn int32) int32 {
	t.Helper()

	const settle = 2 * time.Second

	deadline := time.Now().Add(20 * time.Second)

	var (
		last     int32
		haveLast bool
		stableAt time.Time
	)

	for time.Now().Before(deadline) {
		m := b433VDMinor(t, stack, rd, vn)
		switch {
		case m == nil:
			haveLast = false
		case !haveLast || *m != last:
			last = *m
			haveLast = true
			stableAt = time.Now()
		default:
			if time.Since(stableAt) >= settle {
				return last
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	if haveLast {
		return last
	}

	t.Fatalf("RD %q vol %d: DRBDMinor never settled to a non-nil value", rd, vn)

	return 0
}

// b433DeleteRDResources deletes every fixture-node Resource of an RD via
// the k8s client so a subsequent RD delete isn't blocked by child refs.
func b433DeleteRDResources(t *testing.T, stack *harness.Stack, rd string) {
	t.Helper()

	ctx := context.Background()
	for _, node := range harness.FixtureNodes() {
		r := &blockstoriov1alpha1.Resource{}
		if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: rd + "." + node}, r); err != nil {
			continue
		}

		_ = stack.Env.Client.Delete(ctx, r)
	}
}

// b433WaitRDGone blocks until the RD object is fully absent from the API
// (so takenMinorsCluster no longer counts its minors).
func b433WaitRDGone(t *testing.T, stack *harness.Stack, rd string) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var rdObj blockstoriov1alpha1.ResourceDefinition
		if err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: rd}, &rdObj); err != nil {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("RD %q did not disappear within budget", rd)
}

// TestBug433VDResizePreservesDRBDMinor — FAIL-on-bug regression.
//
// A legal in-bounds `vd set-size` grow MUST NOT change the volume's DRBD
// device minor. On the pre-fix tree the resize wipes the minor
// (wireToCRDVD drops it) and, with a lower minor freed by a prior RD
// delete, the allocator re-stamps a DIFFERENT minor — a permanent
// device-identity change on a live volume. Passes only once
// PatchVolumeDefinitionSpec carries DRBDMinor across the wire round-trip.
func TestBug433VDResizePreservesDRBDMinor(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	// RD-A grabs the lowest minor; RD-B the next one up.
	rdA := seedRDWithVolume(t, stack, "b433-minor-a")
	minorA := b433WaitMinor(t, stack, rdA, 0)

	rdB := seedRDWithVolume(t, stack, "b433-minor-b")
	minorB := b433WaitMinor(t, stack, rdB, 0)

	t.Logf("RD-A minor=%d, RD-B minor=%d (RD-B's stable device identity)", minorA, minorB)

	// Free RD-A's (lower) minor so the allocator would hand it to RD-B on
	// any re-allocation.
	b433DeleteRDResources(t, stack, rdA)
	deleteRDViaREST(context.Background(), t, stack.RestURL, rdA)
	b433WaitRDGone(t, stack, rdA)

	// Legal in-bounds grow on RD-B vol0 (1 GiB -> 2 GiB).
	url := stack.RestURL + "/v1/resource-definitions/" + rdB + "/volume-definitions/0"
	status, body := b433Put(t, url, map[string]any{"size_kib": int64(2 * 1024 * 1024)})
	if status < 200 || status >= 300 {
		t.Fatalf("legal in-bounds resize was rejected: status=%d body=%s", status, string(body))
	}

	final := b433WaitMinorSettled(t, stack, rdB, 0)
	if final != minorB {
		t.Fatalf("`vd set-size` CHANGED the live volume's DRBD minor: %d -> %d. "+
			"The per-volume DRBDMinor is the /dev/drbd<N> device identity and the CRD "+
			"contract says a non-nil minor is NEVER overwritten and is preserved across a "+
			"REST modify (resourcedefinition_types.go). PatchVolumeDefinitionSpec round-trips "+
			"through wireToCRDVD, which drops DRBDMinor; the fix routes the write-back through "+
			"wireToCRDVDPreserving. On the stand this re-creates the DRBD device at a new minor "+
			"on a resized live volume.",
			minorB, final)
	}
}

// TestBug433VDPropModifyPreservesDRBDMinor — FAIL-on-bug regression.
//
// Same drop via a pure props modify (`vd set-property`): no size change,
// yet the entry is rebuilt through wireToCRDVD and loses its minor. With
// a lower minor freed, the resulting re-allocation diverges. A property
// edit must never touch the device identity.
func TestBug433VDPropModifyPreservesDRBDMinor(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	rdA := seedRDWithVolume(t, stack, "b433-minorp-a")
	minorA := b433WaitMinor(t, stack, rdA, 0)

	rdB := seedRDWithVolume(t, stack, "b433-minorp-b")
	minorB := b433WaitMinor(t, stack, rdB, 0)

	t.Logf("RD-A minor=%d, RD-B minor=%d", minorA, minorB)

	b433DeleteRDResources(t, stack, rdA)
	deleteRDViaREST(context.Background(), t, stack.RestURL, rdA)
	b433WaitRDGone(t, stack, rdA)

	// Legal props-only modify on RD-B vol0.
	url := stack.RestURL + "/v1/resource-definitions/" + rdB + "/volume-definitions/0"
	status, body := b433Put(t, url, map[string]any{
		"override_props": map[string]string{"Aux/bug433-probe": "x"},
	})
	if status < 200 || status >= 300 {
		t.Fatalf("legal props modify was rejected: status=%d body=%s", status, string(body))
	}

	final := b433WaitMinorSettled(t, stack, rdB, 0)
	if final != minorB {
		t.Fatalf("`vd set-property` CHANGED the live volume's DRBD minor: %d -> %d. "+
			"A property edit must not touch the device identity; wireToCRDVD drops DRBDMinor "+
			"on every VD-scoped modify — the fix routes the write-back through "+
			"wireToCRDVDPreserving.",
			minorB, final)
	}
}
