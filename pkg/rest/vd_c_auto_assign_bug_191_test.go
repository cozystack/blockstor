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
	"fmt"
	"net/http"
	"sort"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 191 (P2 SPEC): `linstor vd c <rd> <size>` without --vlmnr should
// auto-assign the next free VlmNr (upstream LINSTOR's documented
// behaviour: the field is "optional, auto-assigned if absent"). The
// pre-fix REST handler decoded an absent `volume_number` to Go's int32
// zero value and forwarded VlmNr=0 to the store; the second `vd c`
// invocation collided with Bug 140's FAIL_EXISTS_VLM_DFN refusal,
// breaking the "two volumes under one RD" workflow entirely for
// spec-conformant clients.
//
// The fix distinguishes "absent / null" from "explicit 0" by inspecting
// the raw JSON for the `volume_number` key — mirrors Bug 156's
// `disklessOnRemainingExplicitlyFalse` pattern. When absent/null, the
// handler walks the parent RD's existing VDs and assigns the smallest
// free non-negative VolumeNumber.

// TestBug191VDCreateAutoAssignsWhenVlmNrOmitted is the headline case:
// POST a bare `{"volume_definition":{"size_kib":32768}}` body with no
// `volume_number` key at all → handler MUST auto-assign VlmNr=0 on an
// empty parent RD and return success; a subsequent GET MUST show the
// VD persisted at the assigned number.
func TestBug191VDCreateAutoAssignsWhenVlmNrOmitted(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug191-omit"

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Raw JSON body with NO volume_number key. We can't use
	// json.Marshal(apiv1.VolumeDefinition{SizeKib: ...}) because
	// that struct has `json:"volume_number"` without omitempty —
	// it would always emit the field. The wire shape we test is
	// exactly what `linstor vd c <rd> 32M` produces.
	body := []byte(`{"volume_definition":{"size_kib":32768}}`)

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status: got %d, want 200", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		t.Fatalf("VD list: %v", err)
	}

	if len(vds) != 1 {
		t.Fatalf("VD count after auto-assign POST: got %d, want 1; vds=%+v", len(vds), vds)
	}

	if vds[0].VolumeNumber != 0 {
		t.Errorf("auto-assigned VolumeNumber on empty RD: got %d, want 0", vds[0].VolumeNumber)
	}

	if vds[0].SizeKib != 32768 {
		t.Errorf("persisted SizeKib: got %d, want 32768", vds[0].SizeKib)
	}
}

// TestBug191VDCreateExplicitVlmNrHonored pins the
// non-regression invariant: when the caller DOES pass
// `volume_number: 5`, the handler MUST honour the explicit value (not
// auto-assign over it). Without this guard the auto-assign branch
// would shadow legitimate explicit --vlmnr requests.
func TestBug191VDCreateExplicitVlmNrHonored(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug191-explicit"

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 5, SizeKib: 32 * 1024},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status: got %d, want 200", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		t.Fatalf("VD list: %v", err)
	}

	if len(vds) != 1 {
		t.Fatalf("VD count: got %d, want 1", len(vds))
	}

	if vds[0].VolumeNumber != 5 {
		t.Errorf("explicit VolumeNumber: got %d, want 5", vds[0].VolumeNumber)
	}
}

// TestBug191VDCreateTwoSequentialReceiveDifferentVlmNrs is the live
// reproducer: two consecutive `linstor vd c X 32M` (no --vlmnr) both
// succeed; the second receives VlmNr=1 instead of colliding on
// FAIL_EXISTS_VLM_DFN at VlmNr=0 (Bug 140's symptom is the consequence
// of Bug 191 — the duplicate-0 only happened because auto-assign was
// broken).
func TestBug191VDCreateTwoSequentialReceiveDifferentVlmNrs(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug191-two"

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"volume_definition":{"size_kib":32768}}`)

	first := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	_ = first.Body.Close()

	if first.StatusCode != http.StatusOK {
		t.Fatalf("first POST status: got %d, want 200", first.StatusCode)
	}

	second := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	_ = second.Body.Close()

	if second.StatusCode != http.StatusOK {
		t.Fatalf("second POST status: got %d, want 200 (was FAIL_EXISTS_VLM_DFN pre-fix)",
			second.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		t.Fatalf("VD list: %v", err)
	}

	if len(vds) != 2 {
		t.Fatalf("VD count after two auto-assigns: got %d, want 2; vds=%+v", len(vds), vds)
	}

	nums := []int32{vds[0].VolumeNumber, vds[1].VolumeNumber}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	if nums[0] != 0 || nums[1] != 1 {
		t.Errorf("auto-assigned VlmNrs: got %v, want [0 1]", nums)
	}
}

// TestBug191VDCreateAutoAssignFillsGaps pins the gap-fill rule: when
// existing VDs are 0 and 2, an auto-assigned create lands at the
// smallest free non-negative integer, which is 1 (not 3). The smallest-
// hole rule matches upstream LINSTOR's CtrlVlmDfnCrtApiCallHandler;
// scripts that probe `vd c` for a fresh slot rely on the predictable
// ordering.
func TestBug191VDCreateAutoAssignFillsGaps(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug191-gap"

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, vn := range []int32{0, 2} {
		err := st.VolumeDefinitions().Create(ctx, rdName,
			&apiv1.VolumeDefinition{VolumeNumber: vn, SizeKib: 32 * 1024})
		if err != nil {
			t.Fatalf("seed VD %d: %v", vn, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"volume_definition":{"size_kib":32768}}`)

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status: got %d, want 200", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		t.Fatalf("VD list: %v", err)
	}

	if len(vds) != 3 {
		t.Fatalf("VD count after gap-fill POST: got %d, want 3", len(vds))
	}

	have := map[int32]bool{}
	for i := range vds {
		have[vds[i].VolumeNumber] = true
	}

	for _, want := range []int32{0, 1, 2} {
		if !have[want] {
			t.Errorf("missing VlmNr=%d after gap-fill; have=%v", want, vdNums(vds))
		}
	}
}

// vdNums is a debug helper that returns the sorted list of
// VolumeNumbers on a VD slice — used in t.Errorf only.
func vdNums(vds []apiv1.VolumeDefinition) []int32 {
	out := make([]int32, 0, len(vds))
	for i := range vds {
		out = append(out, vds[i].VolumeNumber)
	}

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })

	return out
}

// fmtVDs keeps test diagnostics readable when the assertion fails on
// the store contents. Retained even when unused so a future test
// addition has a ready handle.
var _ = fmt.Sprintf
