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
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug-hunt v3 finding C.4: the lowercase-RD-name validator that
// `POST /v1/resource-definitions` runs (Bug 97) is bypassed on two
// sibling code paths that ALSO create a fresh RD entry from a user-
// supplied name:
//
//   - `linstor s r rst …  --to-resource <Bad>` (POST
//     /v1/resource-definitions/{rd}/snapshot-restore-resource/{snap})
//   - `linstor rg spawn  <rg> <Bad> …`        (POST
//     /v1/resource-groups/{rg}/spawn)
//
// Both call paths flow the raw `to_resource` / `resource_definition_name`
// value straight into Store.ResourceDefinitions().Create(); the k8s CRD
// store slugifies the metadata.name and the lowercased value no longer
// matches the spec.resourceDefinitionName admission webhook check, so
// the persist fails with a raw "metadata.name must equal …" leak AFTER
// a partial state mutation has already happened (RD entry observable in
// the linstor view, orphan finalizers, half-built downstream objects).
//
// These tests mirror the shape of TestBug97RDCreateRefused… for both
// alternate entry points: a 4xx envelope, no leak of the K8s
// metadata.name internal, and the in-memory Store must hold ZERO RD
// rows for the offending name after the request returns.

// TestSnapshotRestoreRejectsInvalidRdName drives the s-r-rst handler
// with several invalid `to_resource` shapes. Each must 400 with a
// LINSTOR-shaped envelope naming the offending object kind, and the
// Store must not carry an RD with that name (orphan state) afterwards.
func TestSnapshotRestoreRejectsInvalidRdName(t *testing.T) {
	t.Parallel()

	// BUG-047: mixed-case / uppercase / underscore / trailing-hyphen are
	// now valid upstream (and accepted by the shared validator), so the
	// reject set keeps only the forms upstream still refuses.
	cases := []struct {
		name string
		to   string
	}{
		{"leading-hyphen", "-bad"},
		{"leading-digit", "1bad"},
		{"embedded-dot", "bad.name"},
		{"embedded-space", "bad name"},
		{"path-separator", "bad/name"},
		{"single-char", "a"},
		{"empty", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src"}); err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
				Name:         "snap1",
				ResourceName: "src",
				Nodes:        []string{"n1"},
				VolumeDefinitions: []apiv1.SnapshotVolumeDef{
					{VolumeNumber: 0, SizeKib: 1024},
				},
			}); err != nil {
				t.Fatalf("seed snap: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(map[string]string{
				"to_resource": tc.to,
			})

			resp := httpPost(t, base+"/v1/resource-definitions/src/snapshot-restore-resource/snap1", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 400 (Bug C.4: invalid to_resource %q must be refused). Body: %s",
					resp.StatusCode, tc.to, got)
			}

			got, _ := readAllBody(resp)

			// The same Bug-97 envelope shape: name the offending kind and
			// MUST NOT leak the internal k8s hash-prefix path.
			if tc.to != "" && !strings.Contains(string(got), "resource definition") {
				t.Errorf("envelope must name the offending object kind: %s", got)
			}

			if strings.Contains(string(got), "metadata.name") {
				t.Errorf("envelope leaks the k8s metadata.name internal: %s", got)
			}

			// The store must not hold a partial RD entry for the rejected
			// name — that's the bug: the rejected name still landed an
			// orphan row before the CRD-side admission fired.
			rds, err := st.ResourceDefinitions().List(ctx)
			if err != nil {
				t.Fatalf("list RDs: %v", err)
			}

			for _, rd := range rds {
				if rd.Name == tc.to {
					t.Errorf("orphan RD %q persisted despite 400 — gate is leaky: %+v", tc.to, rd)
				}
			}
		})
	}
}

// TestSnapshotRestoreVolumeDefinitionRejectsInvalidRdName drives the
// sibling VD-only restore endpoint (Bug 225) with the same invalid
// `to_resource` shapes. Same gate, same envelope; the VD-only path
// doesn't create an RD but it does poke the Store for the target — the
// validator must still fire at the wire boundary so the operator sees
// one consistent failure mode across both restore handlers.
func TestSnapshotRestoreVolumeDefinitionRejectsInvalidRdName(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap1",
		ResourceName: "src",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		// BUG-047: embedded dot is still refused upstream (the
		// `<rd>.<node>` split-safety constraint); mixed-case names like
		// the old "hunt3-Bad" are now valid and no longer a reject case.
		"to_resource": "bad.name",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/src/snapshot-restore-volume-definition/snap1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug C.4: invalid to_resource on VD-restore must be refused). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "resource definition") {
		t.Errorf("envelope must name the offending object kind: %s", got)
	}

	if strings.Contains(string(got), "metadata.name") {
		t.Errorf("envelope leaks the k8s metadata.name internal: %s", got)
	}
}

// TestResourceGroupSpawnRejectsInvalidRdName drives the rg-spawn
// handler with several invalid `resource_definition_name` shapes. Each
// must 400 with a LINSTOR-shaped envelope, AND the Store must hold no
// RD row for the offending name afterwards (the bug: the partial RD
// entry survived the CRD-side rejection).
func TestResourceGroupSpawnRejectsInvalidRdName(t *testing.T) {
	t.Parallel()

	// BUG-047: mixed-case / uppercase / underscore / trailing-hyphen are
	// now valid upstream, so the reject set keeps only the still-invalid
	// forms.
	cases := []struct {
		name string
		rd   string
	}{
		{"leading-hyphen", "-bad"},
		{"leading-digit", "1bad"},
		{"embedded-dot", "bad.name"},
		{"embedded-space", "bad name"},
		{"path-separator", "bad/name"},
		{"single-char", "a"},
		{"empty", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg1"}); err != nil {
				t.Fatalf("seed RG: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, err := json.Marshal(apiv1.ResourceGroupSpawn{
				ResourceDefinitionName: tc.rd,
				VolumeSizes:            []int64{1024 * 1024},
			})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPost(t, base+"/v1/resource-groups/rg1/spawn", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 400 (Bug C.4: invalid resource_definition_name %q must be refused). Body: %s",
					resp.StatusCode, tc.rd, got)
			}

			got, _ := readAllBody(resp)

			if tc.rd != "" && !strings.Contains(string(got), "resource definition") {
				t.Errorf("envelope must name the offending object kind: %s", got)
			}

			if strings.Contains(string(got), "metadata.name") {
				t.Errorf("envelope leaks the k8s metadata.name internal: %s", got)
			}

			rds, err := st.ResourceDefinitions().List(ctx)
			if err != nil {
				t.Fatalf("list RDs: %v", err)
			}

			for _, rd := range rds {
				if rd.Name == tc.rd {
					t.Errorf("orphan RD %q persisted despite 400 — gate is leaky: %+v", tc.rd, rd)
				}
			}
		})
	}
}

// TestSnapshotRestoreAcceptsValidRdName is the happy-path guard: clean
// RFC-1123 names still round-trip through the s-r-rst handler. Without
// this, an over-strict gate would break the production case (csi clone
// flow uses `pvc-<uuid>` names that look like valid identifiers).
func TestSnapshotRestoreAcceptsValidRdName(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap1",
		ResourceName: "src",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource": "pvc-restored-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/src/snapshot-restore-resource/snap1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (valid name must still round-trip). Body: %s",
			resp.StatusCode, got)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-restored-1"); err != nil {
		t.Errorf("valid RD pvc-restored-1 not persisted: %v", err)
	}
}

// TestResourceGroupSpawnAcceptsValidRdName mirrors the happy-path guard
// for the rg-spawn handler. Production csi flows spawn `pvc-<uuid>`
// shaped RDs — those must still pass.
func TestResourceGroupSpawnAcceptsValidRdName(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg1"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-spawned-1",
		VolumeSizes:            []int64{1024 * 1024},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg1/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (valid name must still round-trip). Body: %s",
			resp.StatusCode, got)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-spawned-1"); err != nil {
		t.Errorf("valid RD pvc-spawned-1 not persisted: %v", err)
	}
}
