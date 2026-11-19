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

	got, err := effectiveprops.VolumeDiskOptions(t.Context(), c, target, rd)
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
