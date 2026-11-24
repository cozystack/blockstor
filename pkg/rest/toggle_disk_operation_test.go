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
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestToggleDiskByOperation drives the REST 1.28 toggle-disk body that
// python-linstor sends for every `r td` once the controller advertises
// 1.28: the operation decides the direction. The bodyless route used to
// flip whatever the replica was, so `r td --diskless` on a diskless
// replica gave it a disk.
func TestToggleDiskByOperation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		flags        []string
		body         string
		status       int
		wantDiskless bool
		wantPool     string
	}{
		{
			"diskless stays diskless",
			[]string{apiv1.ResourceFlagDiskless},
			`{"operation":"INTO_DRBD_DISKLESS"}`, http.StatusOK, true, "",
		},
		{
			"diskful becomes diskless", nil,
			`{"operation":"INTO_DRBD_DISKLESS"}`, http.StatusOK, true, "",
		},
		{
			"diskful stays diskful", nil,
			`{"operation":"INTO_DRBD_DISKFUL"}`, http.StatusOK, false, "",
		},
		{
			"diskless becomes diskful on a pool",
			[]string{apiv1.ResourceFlagDiskless},
			`{"operation":"into_drbd_diskful","storage_pool":"thin"}`, http.StatusOK, false, "thin",
		},
		{
			"client is refused", nil,
			`{"operation":"INTO_DRBD_CLIENT"}`, http.StatusNotImplemented, false, "",
		},
		{
			"unknown operation is refused",
			[]string{apiv1.ResourceFlagDiskless},
			`{"operation":"INTO_NOWHERE"}`, http.StatusBadRequest, true, "",
		},
		{
			"missing operation is refused",
			[]string{apiv1.ResourceFlagDiskless},
			`{"storage_pool":"thin"}`, http.StatusBadRequest, true, "",
		},
		{
			"layer list is refused",
			[]string{apiv1.ResourceFlagDiskless},
			`{"operation":"INTO_DRBD_DISKFUL","layer_list":["DRBD","STORAGE"]}`, http.StatusNotImplemented, true, "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-td"}); err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			if err := st.Resources().Create(t.Context(), &apiv1.Resource{
				Name: "pvc-td", NodeName: "n1", Flags: tc.flags,
			}); err != nil {
				t.Fatalf("seed replica: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPut(t, base+"/v1/resource-definitions/pvc-td/resources/n1/toggle-disk", []byte(tc.body))
			_ = resp.Body.Close()

			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}

			got, err := st.Resources().Get(t.Context(), "pvc-td", "n1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}

			if diskless := slices.Contains(got.Flags, apiv1.ResourceFlagDiskless); diskless != tc.wantDiskless {
				t.Errorf("diskless = %v, want %v (flags %v)", diskless, tc.wantDiskless, got.Flags)
			}

			if tc.wantPool != "" && got.Props["StorPoolName"] != tc.wantPool {
				t.Errorf("StorPoolName = %q, want %q", got.Props["StorPoolName"], tc.wantPool)
			}
		})
	}
}

// TestToggleDiskMigrateFrom: migrate_from in the body is the REST 1.28
// spelling of `migrate-disk/{from}`.
func TestToggleDiskMigrateFrom(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-mig"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, res := range []apiv1.Resource{
		{Name: "pvc-mig", NodeName: "n1"},
		{Name: "pvc-mig", NodeName: "n2", Flags: []string{apiv1.ResourceFlagDiskless}},
	} {
		if err := st.Resources().Create(ctx, &res); err != nil {
			t.Fatalf("seed %s: %v", res.NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-mig/resources/n2/toggle-disk",
		[]byte(`{"operation":"INTO_DRBD_DISKFUL","migrate_from":"n1"}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}

	dst, err := st.Resources().Get(ctx, "pvc-mig", "n2")
	if err != nil {
		t.Fatalf("Get n2: %v", err)
	}

	if slices.Contains(dst.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("the migration target is still diskless: %v", dst.Flags)
	}

	resp = httpPut(t, base+"/v1/resource-definitions/pvc-mig/resources/n2/toggle-disk",
		[]byte(`{"operation":"INTO_DRBD_DISKLESS","migrate_from":"n1"}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("a migration into diskless: status %d, want 400", resp.StatusCode)
	}
}
