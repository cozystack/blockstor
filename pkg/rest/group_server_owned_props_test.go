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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A group that already carries a server-owned prop, written before group
// create and modify refused one, does not hand it to what it spawns:
// linstor-csi fills group props from StorageClass parameters verbatim, so
// inherited, the marker would make every new volume restore another
// resource's snapshot.
func TestSpawnDoesNotInheritServerOwnedGroupProps(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name: "rg-owned",
		Props: map[string]string{
			store.RestoreFromSnapshotProp: "victim:snap",
			store.RollbackAbandonedProp:   store.RollbackInProgress,
			"DrbdOptions/Net/protocol":    "C",
		},
	}); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{ResourceDefinitionName: "pvc-owned"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-owned/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("spawn = %d, want 201", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-owned")
	if err != nil {
		t.Fatalf("read the spawned definition: %v", err)
	}

	for _, key := range []string{store.RestoreFromSnapshotProp, store.RollbackAbandonedProp} {
		if v, ok := rd.Props[key]; ok {
			t.Errorf("spawned definition inherited %s=%q from its group", key, v)
		}
	}

	if rd.Props["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("spawned definition lost an ordinary group prop: %v", rd.Props)
	}
}

func TestGroupCreateRefusesServerOwnedProps(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, rg := range []apiv1.ResourceGroup{
		{Name: "rg-props", Props: map[string]string{store.RestoreFromSnapshotProp: "victim:snap"}},
		{Name: "rg-override", OverrideProps: map[string]string{store.RestoreAdoptedProp: "x"}},
	} {
		body, err := json.Marshal(rg)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		resp := httpPost(t, base+"/v1/resource-groups", body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("rg create %s = %d, want 400", rg.Name, resp.StatusCode)
		}

		if _, err := st.ResourceGroups().Get(t.Context(), rg.Name); err == nil {
			t.Errorf("rg create %s stored the group despite the refusal", rg.Name)
		}
	}
}

func TestGroupModifyRefusesSettingServerOwnedProps(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name:  "rg-mod",
		Props: map[string]string{store.RestoreFromSnapshotProp: "old:snap"},
	}); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	set, _ := json.Marshal(apiv1.ResourceGroup{
		OverrideProps: map[string]string{store.RollbackAbandonedProp: store.RollbackInProgress},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-mod", set)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("rg modify setting %s = %d, want 400", store.RollbackAbandonedProp, resp.StatusCode)
	}

	// Removing one is how an operator cleans a group written before the
	// check, so it stays allowed.
	del, _ := json.Marshal(apiv1.ResourceGroup{DeleteProps: []string{store.RestoreFromSnapshotProp}})

	resp = httpPut(t, base+"/v1/resource-groups/rg-mod", del)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rg modify removing %s = %d, want 200", store.RestoreFromSnapshotProp, resp.StatusCode)
	}

	rg, err := st.ResourceGroups().Get(t.Context(), "rg-mod")
	if err != nil {
		t.Fatalf("read the group: %v", err)
	}

	if _, ok := rg.Props[store.RestoreFromSnapshotProp]; ok {
		t.Errorf("the removal did not land: %v", rg.Props)
	}

	if _, ok := rg.Props[store.RollbackAbandonedProp]; ok {
		t.Errorf("the refused set landed: %v", rg.Props)
	}
}

// Every spelling of a removal cleans a server-owned prop off a group: an
// override to an empty value is the one `rg set-property <rg> <key>` sends.
func TestGroupModifyRemovesServerOwnedPropsInEverySpelling(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		key  string
		body apiv1.ResourceGroup
	}{
		{
			name: "delete_props", key: store.RestoreFromSnapshotProp,
			body: apiv1.ResourceGroup{DeleteProps: []string{store.RestoreFromSnapshotProp}},
		},
		{
			name: "an empty override", key: store.RestoreFromSnapshotProp,
			body: apiv1.ResourceGroup{OverrideProps: map[string]string{store.RestoreFromSnapshotProp: ""}},
		},
		// A namespace covers the keys below it, so it reaches the one
		// server-owned key that sits in one.
		{
			name: "delete_namespaces", key: store.RestoreAdoptedProp,
			body: apiv1.ResourceGroup{DeleteNamespace: []string{"Blockstor"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
				Name:  "rg-clean",
				Props: map[string]string{tc.key: "old"},
			}); err != nil {
				t.Fatalf("seed the group: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(tc.body)

			resp := httpPut(t, base+"/v1/resource-groups/rg-clean", body)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("rg modify removing %s by %s = %d, want 200", tc.key, tc.name, resp.StatusCode)
			}

			rg, err := st.ResourceGroups().Get(t.Context(), "rg-clean")
			if err != nil {
				t.Fatalf("read the group: %v", err)
			}

			if _, ok := rg.Props[tc.key]; ok {
				t.Errorf("the removal did not land: %v", rg.Props)
			}
		})
	}
}
