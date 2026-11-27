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

package effectiveprops_test

import (
	"maps"
	"testing"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
)

// adoptedVolumeRD is a definition shaped like one LINSTOR created on
// ZFS: block-size and the discard options on its volume definition.
func adoptedVolumeRD() *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: v1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "Rg-1",
			ExtraProps:        map[string]string{"DrbdOptions/Disk/al-extents": "6007"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024, Props: map[string]string{
					"DrbdOptions/Disk/block-size":                "4096",
					"DrbdOptions/Disk/discard-zeroes-if-aligned": "yes",
					"DrbdOptions/Disk/rs-discard-granularity":    "16384",
					"DrbdOptions/Net/max-buffers":                "1",
					"Linstor/Drbd/freeze-block-size":             "True",
					"StorPoolName":                               "zfs1",
				}},
				{VolumeNumber: 1, SizeKib: 1024},
			},
		},
	}
}

func volumeDisk(t *testing.T, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, objs ...client.Object) map[int32]map[string]string {
	t.Helper()

	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).Build()

	effective, err := effectiveprops.Resolve(t.Context(), c, target, rd)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	got, err := effectiveprops.VolumeDiskOptions(t.Context(), c, target, rd, effective)
	if err != nil {
		t.Fatalf("VolumeDiskOptions: %v", err)
	}

	return got
}

func replica() *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: v1.ObjectMeta{Name: "pvc-1.n1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-1", NodeName: "n1"},
	}
}

// A volume definition's disk options are that volume's: block-size
// reaches the volume it is set on, under its bare name, and only the
// disk namespace does. A definition-level disk option is not a volume
// option, and a volume with none is absent from the result.
func TestVolumeDefinitionDiskOptionsAreTheVolumes(t *testing.T) {
	t.Parallel()

	got := volumeDisk(t, replica(), adoptedVolumeRD())

	want := map[int32]map[string]string{0: {
		"block-size":                "4096",
		"discard-zeroes-if-aligned": "yes",
		"rs-discard-granularity":    "16384",
	}}
	if !maps.EqualFunc(got, want, maps.Equal) {
		t.Errorf("VolumeDiskOptions = %v, want %v", got, want)
	}
}

// LINSTOR's per-volume precedence: resource, then volume definition,
// then volume group. The group's value fills a key the definition
// leaves unset, the definition's beats the group's, and the replica's
// own beats both.
func TestVolumeDiskOptionsFollowLinstorPrecedence(t *testing.T) {
	t.Parallel()

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: v1.ObjectMeta{Name: "rg-1"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{VolumeGroups: []blockstoriov1alpha1.ResourceGroupVolumeGroup{
			{VolumeNumber: 0, Props: map[string]string{
				"DrbdOptions/Disk/block-size":  "512",
				"DrbdOptions/Disk/on-io-error": "pass_on",
			}},
			{VolumeNumber: 1, Props: map[string]string{"DrbdOptions/Disk/block-size": "8192"}},
		}},
	}

	target := replica()
	target.Spec.ExtraProps = map[string]string{"DrbdOptions/Disk/discard-zeroes-if-aligned": "no"}

	got := volumeDisk(t, target, adoptedVolumeRD(), rg)

	want := map[int32]map[string]string{
		0: {
			"block-size":                "4096",
			"discard-zeroes-if-aligned": "no",
			"rs-discard-granularity":    "16384",
			"on-io-error":               "pass_on",
		},
		1: {"block-size": "8192"},
	}
	if !maps.EqualFunc(got, want, maps.Equal) {
		t.Errorf("VolumeDiskOptions = %v, want %v", got, want)
	}
}

// LINSTOR's auto rule for rs-discard-granularity: while
// DrbdOptions/auto-rs-discard-granularity is unset or true, the volume's
// value is the volume definition's alone. A value on the volume group
// or the replica does not reach the volume, whether or not the
// definition sets one; switched off anywhere in the chain, the ordinary
// precedence applies again.
func TestRsDiscardGranularityFollowsLinstorsAutoRule(t *testing.T) {
	t.Parallel()

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: v1.ObjectMeta{Name: "rg-1"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{VolumeGroups: []blockstoriov1alpha1.ResourceGroupVolumeGroup{
			{VolumeNumber: 1, Props: map[string]string{"DrbdOptions/Disk/rs-discard-granularity": "65536"}},
		}},
	}

	target := replica()
	target.Spec.ExtraProps = map[string]string{"DrbdOptions/Disk/rs-discard-granularity": "4096"}

	got := volumeDisk(t, target, adoptedVolumeRD(), rg)

	if v := got[0]["rs-discard-granularity"]; v != "16384" {
		t.Errorf("volume 0 rs-discard-granularity = %q, want the definition's 16384", v)
	}

	if v, ok := got[1]["rs-discard-granularity"]; ok {
		t.Errorf("volume 1 rs-discard-granularity = %q from the group or replica, want none", v)
	}

	rd := adoptedVolumeRD()
	rd.Spec.ExtraProps["DrbdOptions/auto-rs-discard-granularity"] = "false"

	got = volumeDisk(t, target, rd, rg)

	if v := got[0]["rs-discard-granularity"]; v != "4096" {
		t.Errorf("rule off: volume 0 rs-discard-granularity = %q, want the replica's 4096", v)
	}

	if v := got[1]["rs-discard-granularity"]; v != "4096" {
		t.Errorf("rule off: volume 1 rs-discard-granularity = %q, want the replica's 4096 over the group's", v)
	}
}

// TestVolumeFileSystemPropsFollowLinstorPrecedence: FileSystem/* is
// read per volume over resource, volume definition, volume group,
// resource definition and resource group, closer first, the order
// LINSTOR's MkfsUtils builds its PriorityProps in. linstor-csi v1.13.2
// puts the props on the volume definition, v1.11.2 on the resource
// definition, a StorageClass parameter on the resource group.
func TestVolumeFileSystemPropsFollowLinstorPrecedence(t *testing.T) {
	const fsType = "FileSystem/Type"

	rdWith := func(rdProps, vd0, vd1 map[string]string) *blockstoriov1alpha1.ResourceDefinition {
		return &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: v1.ObjectMeta{Name: "pvc-1"},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				ResourceGroupName: "rg-1",
				ExtraProps:        rdProps,
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
					{VolumeNumber: 0, SizeKib: 1024, Props: vd0},
					{VolumeNumber: 1, SizeKib: 1024, Props: vd1},
				},
			},
		}
	}

	rgWith := func(rgProps, vg0 map[string]string) *blockstoriov1alpha1.ResourceGroup {
		return &blockstoriov1alpha1.ResourceGroup{
			ObjectMeta: v1.ObjectMeta{Name: "rg-1"},
			Spec: blockstoriov1alpha1.ResourceGroupSpec{
				ExtraProps:   rgProps,
				VolumeGroups: []blockstoriov1alpha1.ResourceGroupVolumeGroup{{VolumeNumber: 0, Props: vg0}},
			},
		}
	}

	ext4 := map[string]string{fsType: "ext4", "FileSystem/MkfsParams": ""}
	xfs := map[string]string{fsType: "xfs"}

	cases := map[string]struct {
		target *blockstoriov1alpha1.Resource
		rd     *blockstoriov1alpha1.ResourceDefinition
		rg     *blockstoriov1alpha1.ResourceGroup
		want   map[int32]map[string]string
	}{
		"volume definition (linstor-csi v1.13.2)": {
			rd:   rdWith(nil, ext4, nil),
			rg:   rgWith(nil, nil),
			want: map[int32]map[string]string{0: ext4},
		},
		"resource definition (linstor-csi v1.11.2)": {
			rd:   rdWith(ext4, nil, nil),
			rg:   rgWith(nil, nil),
			want: map[int32]map[string]string{0: ext4, 1: ext4},
		},
		"resource group (StorageClass parameter)": {
			rd:   rdWith(nil, nil, nil),
			rg:   rgWith(xfs, nil),
			want: map[int32]map[string]string{0: xfs, 1: xfs},
		},
		"volume group outranks the resource definition": {
			rd:   rdWith(ext4, nil, nil),
			rg:   rgWith(nil, xfs),
			want: map[int32]map[string]string{0: {fsType: "xfs", "FileSystem/MkfsParams": ""}, 1: ext4},
		},
		"volume definition outranks the volume group": {
			rd:   rdWith(nil, ext4, nil),
			rg:   rgWith(nil, xfs),
			want: map[int32]map[string]string{0: ext4, 1: nil},
		},
		"resource outranks the volume definition": {
			target: func() *blockstoriov1alpha1.Resource {
				res := replica()
				res.Spec.Props = map[string]string{fsType: "xfs", "StorPoolName": "zfs1"}

				return res
			}(),
			rd:   rdWith(nil, ext4, nil),
			rg:   rgWith(nil, nil),
			want: map[int32]map[string]string{0: {fsType: "xfs", "FileSystem/MkfsParams": ""}, 1: xfs},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			target := tc.target
			if target == nil {
				target = replica()
			}

			c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(tc.rg).Build()

			got, err := effectiveprops.VolumeFileSystemProps(t.Context(), c, target, tc.rd)
			if err != nil {
				t.Fatalf("VolumeFileSystemProps: %v", err)
			}

			for vol := range int32(2) {
				if !maps.Equal(got[vol], tc.want[vol]) {
					t.Errorf("volume %d: got %v, want %v", vol, got[vol], tc.want[vol])
				}
			}
		})
	}
}
