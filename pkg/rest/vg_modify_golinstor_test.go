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
	"slices"
	"testing"

	"github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The volume-group modify door takes golinstor's VolumeGroupModify whole:
// delete_namespaces drops every key below a namespace and leaves a key spelled
// as it, as upstream does, and flags add or ('-'-prefixed) remove a flag.
func TestVolumeGroupModifyTakesGolinstorsWholeBody(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-vg",
		VolumeGroups: []apiv1.VolumeGroup{{
			VolumeNumber: 0,
			Props:        map[string]string{"Aux": "x", "Aux/a": "y", "keep": "z"},
			Flags:        []string{"OLD"},
		}},
	}); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(client.VolumeGroupModify{
		DeleteNamespaces: []string{"Aux"},
		Flags:            []string{"GROSS_SIZE", "-OLD"},
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

	if !slices.Equal(vg.Flags, []string{"GROSS_SIZE"}) {
		t.Errorf("flags = %v, want GROSS_SIZE added and OLD removed", vg.Flags)
	}
}
