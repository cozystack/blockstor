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
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestRDCloneCreatesCopy: POST .../clone duplicates a ResourceDefinition.
// Maps to `linstor resource-definition clone`.
func TestRDCloneCreatesCopy(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-1",
		Props: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "pvc-2"})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/clone", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-2")
	if err != nil {
		t.Fatalf("expected pvc-2 to exist: %v", err)
	}

	if got.Props["k"] != "v" {
		t.Errorf("Props not copied: %v", got.Props)
	}
}

// TestRDCloneSourceMissing: 404 if the source RD doesn't exist.
func TestRDCloneSourceMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "pvc-2"})

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/clone", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestRDCloneStatusReturnsComplete: GET .../clone/{target} on an
// existing target RD returns the upstream "COMPLETE" shape so
// golinstor/linstor-csi exits its polling loop.
func TestRDCloneStatusReturnsComplete(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/src-rd/clone/pvc-1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Status != "COMPLETE" {
		t.Errorf("status: got %q, want \"COMPLETE\"", got.Status)
	}
}

// TestRDCloneStatusUnknownRDReturns404: GET on a non-existent target
// RD must return 404 so linstor-csi surfaces a concrete error rather
// than spinning forever in CloneStatus.
func TestRDCloneStatusUnknownRDReturns404(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/src-rd/clone/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}

	var got []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) == 0 || !strings.Contains(strings.ToLower(got[0].Message), "not found") {
		t.Errorf("expected an operator-actionable not-found message, got %+v", got)
	}
}

// TestRDClonePostThenFollowLocation pins the contract between
// handleRDClone's `Location` field and handleRDCloneStatus's route.
// If anyone changes one without the other, golinstor's
// `WaitForCloneComplete` poll 404s and CSI clone-from-source breaks.
func TestRDClonePostThenFollowLocation(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "pvc-2"})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/clone", body)

	var started struct {
		Location   string `json:"location"`
		SourceName string `json:"source_name"`
		CloneName  string `json:"clone_name"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&started); err != nil {
		_ = resp.Body.Close()
		t.Fatalf("decode clone-started: %v", err)
	}

	_ = resp.Body.Close()

	if started.Location == "" {
		t.Fatalf("clone-started response missing Location")
	}

	statusResp := httpGet(t, base+started.Location)
	defer func() { _ = statusResp.Body.Close() }()

	if statusResp.StatusCode == http.StatusNotFound {
		t.Fatalf("follow-Location 404: Location %q does not match any registered route", started.Location)
	}

	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("follow-Location status: got %d, want 200", statusResp.StatusCode)
	}

	var status struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		t.Fatalf("decode clone-status: %v", err)
	}

	if status.Status != "COMPLETE" {
		t.Errorf("status: got %q, want \"COMPLETE\"", status.Status)
	}
}

// TestRDCloneTargetExists: 409 if the target name is taken.
func TestRDCloneTargetExists(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-2"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "pvc-2"})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/clone", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}
}
