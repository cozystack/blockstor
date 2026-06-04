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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Corner-case I3b: SkipDisk visible surface in `linstor r l`.
//
// UG9 §SkipDisk (~4430-4459): when DrbdOptions/SkipDisk=True is set on a
// resource, `linstor r l` renders the State column as
// "UpToDate, Skip-Disk (R)"; the "(R)" indicates the prop is set at the
// Resource scope, and it becomes "(R, N)" when the prop is ALSO set at
// the Node scope.
//
// That marker is rendered entirely CLIENT-side by python-linstor from
// the props it receives in the resource-list response — blockstor does
// not render `r l` text itself. So the only server-side contract is
// that the SkipDisk prop set on a resource surfaces in the
// `/v1/view/resources` per-resource Props bag, so the CLI can derive
// the "(R)" marker. This pins that wiring: a regression that dropped
// the per-resource Props from the view (e.g. an over-eager redaction)
// would silently hide the Skip-Disk marker from operators.
func TestI3bSkipDiskPropSurfacesInResourceListView(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-skip",
		NodeName: "n1",
		Props: map[string]string{
			"DrbdOptions/SkipDisk": "True",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources?resources=pvc-skip")
	defer func() { _ = resp.Body.Close() }()

	var got []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d resources, want 1", len(got))
	}

	if got[0].Props["DrbdOptions/SkipDisk"] != "True" {
		t.Errorf("DrbdOptions/SkipDisk missing from resource-list Props (client cannot render the Skip-Disk (R) marker): Props=%v",
			got[0].Props)
	}
}
