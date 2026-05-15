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
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 118 reproducers. Bug 117 from the v2 report turned out NOT to be
// a bug: upstream LINSTOR's documented contract for `r d <bogus-node>`
// and `sp d <bogus-node>` is "200 + WARN + already absent" exit 0, so
// the existing TestResourceDeleteUnknownNodeReturns200Warning and
// TestSPDeleteUnknownUsesWarnMaskNotInfo guards pin the right shape.
// Only Bug 118 is real: `r c <node> <rd> --storage-pool nonexistent`
// silently created a phantom Resource CRD with a non-existent SP.

// TestBug118ResourceCreateOnUnknownStoragePoolReturns404 pins that
// `linstor r c <node> <rd> --storage-pool nonexistent` (which lands
// as Props["StorPoolName"]="nonexistent" in the wire body) gets
// refused with 404 + LINSTOR envelope BEFORE the phantom Resource
// CRD is staged.
func TestBug118ResourceCreateOnUnknownStoragePoolReturns404(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "poke118"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "n1",
			Props:    map[string]string{"StorPoolName": "nonexistent-pool"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/poke118/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 4xx (Bug 118 SP must be validated). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)

	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	if !strings.Contains(rcs[0].Message, "nonexistent-pool") {
		t.Errorf("envelope must name the offending pool: %s", got)
	}

	if !strings.Contains(rcs[0].Message, "n1") {
		t.Errorf("envelope must name the target node: %s", got)
	}

	// Phantom Resource CRD must NOT have been persisted.
	if _, err := st.Resources().Get(ctx, "poke118", "n1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Resource poke118.n1 persisted despite 4xx: err=%v", err)
	}
}

// TestBug118ResourceCreateOnValidStoragePoolWorks is the happy-path
// counterpart: with the StoragePool CRD pre-seeded on the target
// node, the create must succeed and persist normally. Without this
// guard the new SP-existence check could regress every CSI placement.
func TestBug118ResourceCreateOnValidStoragePoolWorks(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rdok118"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "good-pool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed SP: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "n1",
			Props:    map[string]string{"StorPoolName": "good-pool"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/rdok118/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s", resp.StatusCode, got)
	}

	if _, err := st.Resources().Get(ctx, "rdok118", "n1"); err != nil {
		t.Errorf("Resource rdok118.n1 not persisted: %v", err)
	}
}

// TestBug118DisklessResourceCreateBypassesSPCheck pins that a
// DISKLESS replica create (linstor-csi's make-available fallback)
// does NOT trip the new SP-existence gate — diskless replicas have
// no backing pool and must keep working when only NodeName +
// Flags:[DISKLESS] are set.
func TestBug118DisklessResourceCreateBypassesSPCheck(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rddl118"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "n1",
			Flags:    []string{apiv1.ResourceFlagDiskless},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/rddl118/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (diskless must bypass SP gate). Body: %s",
			resp.StatusCode, got)
	}
}
