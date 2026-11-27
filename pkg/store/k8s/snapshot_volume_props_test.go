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

package k8s_test

import (
	"testing"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// TestSnapshotVolumePropsSurviveTheAPIServer pins the recorded volume
// definition props through a real API server: a volume whose definition
// had props keeps them, and one whose definition had none is stored as
// recorded-empty, so it does not take the props its definition gains
// later. A restore copies what the snapshot serves.
func TestSnapshotVolumePropsSurviveTheAPIServer(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()
	st := k8s.New(fixture.client)

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-snapprops"})
	if err != nil {
		t.Fatalf("create rd: %v", err)
	}

	for _, vd := range []apiv1.VolumeDefinition{
		{VolumeNumber: 0, SizeKib: 1 << 20, Props: map[string]string{"DrbdOptions/Disk/block-size": "4096"}},
		{VolumeNumber: 1, SizeKib: 1 << 20},
	} {
		err = st.VolumeDefinitions().Create(ctx, "pvc-snapprops", &vd)
		if err != nil {
			t.Fatalf("create vd %d: %v", vd.VolumeNumber, err)
		}
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-snapprops")
	if err != nil {
		t.Fatalf("list vds: %v", err)
	}

	snap := &apiv1.Snapshot{Name: "snap-1", ResourceName: "pvc-snapprops", Nodes: []string{"n1"}}
	for _, vd := range vds {
		snap.VolumeDefinitions = append(snap.VolumeDefinitions, apiv1.SnapshotVolumeDef{
			VolumeNumber: vd.VolumeNumber, SizeKib: vd.SizeKib, VolumeDefinitionProps: vd.Props,
		})
	}

	err = st.Snapshots().Create(ctx, snap)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	var list crdv1alpha1.SnapshotList

	err = fixture.client.List(ctx, &list)
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}

	if len(list.Items) != 1 {
		t.Fatalf("snapshots: %d, want 1", len(list.Items))
	}

	refs := list.Items[0].Spec.VolumeDefinitions
	if refs[0].Props["DrbdOptions/Disk/block-size"] != "4096" {
		t.Errorf("volume 0 recorded %v, want block-size 4096", refs[0].Props)
	}

	if refs[1].Props == nil || len(refs[1].Props) != 0 {
		t.Errorf("volume 1 recorded %#v, want recorded-empty", refs[1].Props)
	}

	// The definition gains a size after the snapshot.
	err = st.VolumeDefinitions().PatchVolumeDefinitionSpec(ctx, "pvc-snapprops", 1, func(vd *apiv1.VolumeDefinition) error {
		vd.Props = map[string]string{"DrbdOptions/Disk/block-size": "4096"}

		return nil
	})
	if err != nil {
		t.Fatalf("patch vd 1: %v", err)
	}

	got, err := st.Snapshots().Get(ctx, "pvc-snapprops", "snap-1")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	if v := got.VolumeDefinitions[1].VolumeDefinitionProps["DrbdOptions/Disk/block-size"]; v != "" {
		t.Errorf("volume 1 serves block-size %q it did not have when the snapshot was taken", v)
	}
}
