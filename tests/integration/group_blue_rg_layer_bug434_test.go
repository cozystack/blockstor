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

// Bug 434 — end-to-end regression for the layer-stack validation
// asymmetry.
//
// The RD/RG CREATE paths validate the layer stack (validateLayerStack:
// allowlist {DRBD,LUKS,STORAGE}, DRBD first, STORAGE terminal). But
// `rg modify` (handleRGUpdate) gated only place_count, so an invalid
// select_filter.layer_stack was merged and persisted unvalidated; and
// handleRDCreate ran validateRDCreateBody BEFORE inheritLayerStackFromRG,
// so an RD created against that RG inherited the invalid stack unchecked.
// Net: an invalid, unmaterialisable layer stack the DIRECT create path
// refuses (400) reached a persisted RD spec via the rg-modify → inherit
// chain.
//
// This drives the whole REST surface (rg create → rg modify → rd create
// inherit) and is fix-agnostic: it passes whether the RG-modify gate
// rejects the invalid stack OR the RD-create post-inherit re-validation
// refuses the RD. On the pre-fix tree the RD is persisted with the
// invalid [STORAGE,DRBD] stack → FAIL.
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

// b434HTTP issues an HTTP request with a JSON body and returns
// (status, body).
func b434HTTP(t *testing.T, method, url string, payload any) (int, []byte) {
	t.Helper()

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build %s: %v", method, err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	return resp.StatusCode, body
}

// b434LayerStackInvalidOrder reports whether stack has STORAGE appearing
// before DRBD — the specific invalid ordering this test injects (STORAGE
// must be terminal, DRBD must be first). Mirrors the create-path rejection.
func b434LayerStackInvalidOrder(stack []string) bool {
	storageIdx, drbdIdx := -1, -1

	for i, l := range stack {
		switch l {
		case "STORAGE":
			if storageIdx == -1 {
				storageIdx = i
			}
		case "DRBD":
			if drbdIdx == -1 {
				drbdIdx = i
			}
		}
	}

	return storageIdx != -1 && drbdIdx != -1 && storageIdx < drbdIdx
}

// TestBug434RGLayerModifyNoInvalidRDInherit — FAIL-on-bug regression.
//
// An invalid layer stack the DIRECT rd-create path refuses (400) must not
// reach a persisted RD via the `rg modify` (unvalidated) → inherit chain.
// Fix-agnostic: it passes whether Blue validates the RG modify (rg stays
// valid, RD inherits a valid stack) OR re-validates the RD after inherit
// (RD create is refused). On the pre-fix tree the RD is persisted with the
// invalid [STORAGE,DRBD] stack → FAIL.
func TestBug434RGLayerModifyNoInvalidRDInherit(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	const (
		rgName = "b434-rglayer-inherit"
		rdName = "b434-rd-layer-inherit"
	)

	invalid := []string{"STORAGE", "DRBD"}

	// Control: the DIRECT create path MUST refuse this stack. If it ever
	// starts accepting it, the whole premise (and the fix) is moot.
	dst, dbody := b434HTTP(t, http.MethodPost, stack.RestURL+"/v1/resource-definitions", map[string]any{
		"resource_definition": map[string]any{"name": "b434-rd-layer-control"},
		"layer_list":          invalid,
	})
	if dst >= 200 && dst < 300 {
		t.Fatalf("premise broken: direct rd-create ACCEPTED the invalid layer stack %v (status=%d body=%s)", invalid, dst, string(dbody))
	}

	// 1. Create an RG with a VALID layer stack.
	if s, b := b434HTTP(t, http.MethodPost, stack.RestURL+"/v1/resource-groups", map[string]any{
		"name":          rgName,
		"select_filter": map[string]any{"layer_stack": []string{"DRBD", "STORAGE"}},
	}); s < 200 || s >= 300 {
		t.Fatalf("RG create: status=%d body=%s", s, string(b))
	}

	// 2. `rg modify` with the INVALID layer stack (retry past seed cache-lag).
	rgURL := stack.RestURL + "/v1/resource-groups/" + rgName

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		s, _ := b434HTTP(t, http.MethodPut, rgURL, map[string]any{
			"select_filter": map[string]any{"layer_stack": invalid},
		})
		if s != http.StatusNotFound {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	// Let the RG (whatever its final stored stack) become cache-visible so
	// the RD-create inherit lookup observes it.
	time.Sleep(1500 * time.Millisecond)

	// 3. Create an RD inheriting from the RG (no explicit layer_list).
	b434HTTP(t, http.MethodPost, stack.RestURL+"/v1/resource-definitions", map[string]any{
		"resource_definition": map[string]any{
			"name":                rdName,
			"resource_group_name": rgName,
		},
	})

	// 4. Invariant: no persisted RD may carry the invalid layer ordering.
	var rd blockstoriov1alpha1.ResourceDefinition

	err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: rdName}, &rd)
	if err != nil {
		return // RD not persisted (create refused) — invariant holds.
	}

	if b434LayerStackInvalidOrder(rd.Spec.LayerStack) {
		t.Fatalf("RD %q persisted with an INVALID layer stack %v inherited from a modified RG. "+
			"The direct rd-create path refuses this exact stack (STORAGE must be terminal, DRBD first), "+
			"but pre-fix `rg modify` stored select_filter.layer_stack unvalidated and handleRDCreate "+
			"validated BEFORE inheritLayerStackFromRG — so the invalid stack reached the RD spec. On the "+
			"stand this yields an unmaterialisable layer chain.",
			rdName, rd.Spec.LayerStack)
	}
}
