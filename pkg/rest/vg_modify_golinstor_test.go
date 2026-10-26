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
	"testing"

	"github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The volume-group modify door takes golinstor's VolumeGroupModify whole:
// delete_namespaces drops every key below a namespace and leaves a key spelled
// as it, as upstream does.
func TestVolumeGroupModifyTakesGolinstorsWholeBody(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-vg",
		VolumeGroups: []apiv1.VolumeGroup{{
			VolumeNumber: 0,
			Props:        map[string]string{"Aux": "x", "Aux/a": "y", "keep": "z"},
		}},
	}); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(client.VolumeGroupModify{
		DeleteNamespaces: []string{"Aux"},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-vg/volume-groups/0", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("vg modify with golinstor's body = %d, want 200", resp.StatusCode)
	}

	rg, err := st.ResourceGroups().Get(ctx, "rg-vg")
	if err != nil {
		t.Fatalf("read the group: %v", err)
	}

	vg := rg.VolumeGroups[0]

	if vg.Props["Aux"] != "x" {
		t.Errorf("the key spelled as the namespace went: %v", vg.Props)
	}

	if _, ok := vg.Props["Aux/a"]; ok {
		t.Errorf("a key below the namespace stayed: %v", vg.Props)
	}

	if vg.Props["keep"] != "z" {
		t.Errorf("a key outside the namespace went: %v", vg.Props)
	}
}

// Flags are refused, and the refusal writes nothing: nothing sizes a spawned
// volume gross, so a 200 would answer for a change that does not happen.
func TestVolumeGroupModifyRefusesFlags(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-vgflags",
		VolumeGroups: []apiv1.VolumeGroup{{VolumeNumber: 0, Props: map[string]string{"Aux/a": "y"}}},
	}); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(client.VolumeGroupModify{
		DeleteNamespaces: []string{"Aux"},
		Flags:            []string{"GROSS_SIZE"},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-vgflags/volume-groups/0", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("vg modify with flags = %d, want 501", resp.StatusCode)
	}

	rg, err := st.ResourceGroups().Get(ctx, "rg-vgflags")
	if err != nil {
		t.Fatalf("read the group: %v", err)
	}

	vg := rg.VolumeGroups[0]

	if len(vg.Flags) != 0 {
		t.Errorf("a refused modify stored flags %v", vg.Flags)
	}

	if vg.Props["Aux/a"] != "y" {
		t.Errorf("a refused modify applied its namespace delete: %v", vg.Props)
	}
}

// Removing a flag, spelled `-<FLAG>` as golinstor documents it, is honoured:
// it is the only way to clear a flag a volume group already carries. A body
// that also sets one is refused whole.
func TestVolumeGroupModifyRemovesAFlag(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		flags     []string
		wantCode  int
		wantFlags []string
	}{
		{name: "remove", flags: []string{"-GROSS_SIZE"}, wantCode: http.StatusOK, wantFlags: nil},
		{
			name: "remove and set", flags: []string{"-GROSS_SIZE", "GROSS_SIZE"}, wantCode: http.StatusNotImplemented,
			wantFlags: []string{"GROSS_SIZE"},
		},
		{name: "bare dash", flags: []string{"-"}, wantCode: http.StatusNotImplemented, wantFlags: []string{"GROSS_SIZE"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
				Name: "rg-vgunflag",
				VolumeGroups: []apiv1.VolumeGroup{{
					VolumeNumber: 0, Flags: []string{"GROSS_SIZE"}, Props: map[string]string{"Aux/a": "y"},
				}},
			}); err != nil {
				t.Fatalf("seed the group: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(client.VolumeGroupModify{Flags: tc.flags, DeleteProps: []string{"Aux/a"}})

			resp := httpPut(t, base+"/v1/resource-groups/rg-vgunflag/volume-groups/0", body)
			_ = resp.Body.Close()

			if resp.StatusCode != tc.wantCode {
				t.Errorf("vg modify with flags %v = %d, want %d", tc.flags, resp.StatusCode, tc.wantCode)
			}

			rg, err := st.ResourceGroups().Get(ctx, "rg-vgunflag")
			if err != nil {
				t.Fatalf("read the group: %v", err)
			}

			vg := rg.VolumeGroups[0]

			if len(vg.Flags) != len(tc.wantFlags) || (len(vg.Flags) > 0 && vg.Flags[0] != tc.wantFlags[0]) {
				t.Errorf("flags after the modify = %v, want %v", vg.Flags, tc.wantFlags)
			}

			_, kept := vg.Props["Aux/a"]
			if refused := tc.wantCode != http.StatusOK; kept != refused {
				t.Errorf("props after the modify = %v: a refused modify keeps them, an accepted one applies them", vg.Props)
			}
		})
	}
}

// The create door refuses flags as the modify door refuses setting one: a flag
// it stored would be listed and do nothing. The same body without flags is
// created, so the refusal is the flags' alone.
func TestVolumeGroupCreateRefusesFlags(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		flags    []string
		wantCode int
	}{
		{name: "with flags", flags: []string{"GROSS_SIZE"}, wantCode: http.StatusNotImplemented},
		{name: "without flags", wantCode: http.StatusCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-vgcreate"}); err != nil {
				t.Fatalf("seed the group: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(client.VolumeGroup{VolumeNumber: 0, Flags: tc.flags})

			resp := httpPost(t, base+"/v1/resource-groups/rg-vgcreate/volume-groups", body)
			_ = resp.Body.Close()

			if resp.StatusCode != tc.wantCode {
				t.Errorf("vg create with flags %v = %d, want %d", tc.flags, resp.StatusCode, tc.wantCode)
			}

			rg, err := st.ResourceGroups().Get(ctx, "rg-vgcreate")
			if err != nil {
				t.Fatalf("read the group: %v", err)
			}

			if created := len(rg.VolumeGroups) == 1; created != (tc.wantCode == http.StatusCreated) {
				t.Errorf("volume groups after the create = %+v", rg.VolumeGroups)
			}
		})
	}
}

// `rg c` carries volume groups of its own, and a flag on one of them would be
// stored and listed and do nothing, the same as through the volume-group
// create door.
func TestResourceGroupCreateRefusesVolumeGroupFlags(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		flags    []string
		wantCode int
	}{
		{name: "with flags", flags: []string{"GROSS_SIZE"}, wantCode: http.StatusNotImplemented},
		{name: "without flags", wantCode: http.StatusCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(map[string]any{
				"name":          "rg-withvg",
				"volume_groups": []map[string]any{{"volume_number": 0, "flags": tc.flags}},
			})

			resp := httpPost(t, base+"/v1/resource-groups", body)
			_ = resp.Body.Close()

			if resp.StatusCode != tc.wantCode {
				t.Fatalf("rg create with volume-group flags %v = %d, want %d", tc.flags, resp.StatusCode, tc.wantCode)
			}

			_, err := st.ResourceGroups().Get(t.Context(), "rg-withvg")
			if created := err == nil; created != (tc.wantCode == http.StatusCreated) {
				t.Errorf("group stored = %v after a create answered %d", created, tc.wantCode)
			}
		})
	}
}
