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

package dispatcher_test

import (
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// TestExternalMetadataRouting: scenario 6.18 (StorPoolNameDrbdMeta).
// When the target Resource (or the parent RD) carries the LINSTOR-
// compatible `StorPoolNameDrbdMeta` prop, dispatcher.BuildDesired
// must stamp the value into DesiredVolume.MetaPool so the satellite
// can carve the external metadata device and the .res renderer can
// emit the matching `meta-disk <path>;` line.
//
// We exercise three scope variants:
//
//   - Resource-level prop wins over RD-level (most-specific
//     overrides — matches UG9's precedence semantics);
//   - RD-level prop is honoured when Resource doesn't set one;
//   - Diskless replicas suppress MetaPool (no backing disk →
//     nothing to attach metadata to).
//
// The data-pool routing (StorPoolName) is asserted alongside so
// regressing the existing pool resolution would also fail this test.
func TestExternalMetadataRouting(t *testing.T) {
	rdName := "pvc-1"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				// RD-scope StorPoolNameDrbdMeta — should be picked
				// up when the Resource doesn't override it.
				"StorPoolNameDrbdMeta": "ssd-meta",
			},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	cases := []struct {
		name      string
		target    *blockstoriov1alpha1.Resource
		wantPool  string
		wantMeta  string
		wantPeers int
		comment   string
	}{
		{
			name: "rd-scope-prop",
			target: &blockstoriov1alpha1.Resource{
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					StoragePool:            "data-hdd",
				},
			},
			wantPool: "data-hdd",
			wantMeta: "ssd-meta",
			comment:  "RD-level prop is honoured when Resource doesn't override",
		},
		{
			name: "resource-overrides-rd",
			target: &blockstoriov1alpha1.Resource{
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					StoragePool:            "data-hdd",
					Props: map[string]string{
						// Most-specific scope wins.
						"StorPoolNameDrbdMeta": "nvme-meta",
					},
				},
			},
			wantPool: "data-hdd",
			wantMeta: "nvme-meta",
			comment:  "Resource-scope prop overrides RD-scope",
		},
		{
			name: "diskless-suppresses-meta",
			target: &blockstoriov1alpha1.Resource{
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               "n1",
					Flags:                  []string{"DISKLESS"},
					Props: map[string]string{
						// Even if set, a diskless replica has no
						// disk to attach metadata to. The
						// dispatcher must not propagate.
						"StorPoolNameDrbdMeta": "nvme-meta",
					},
				},
			},
			wantPool: "",
			wantMeta: "",
			comment:  "Diskless replica suppresses MetaPool",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dispatcher.BuildDesired(tc.target, nil, nil, nil, rd, nil)
			if got == nil {
				t.Fatalf("BuildDesired returned nil")
			}

			if len(got.Volumes) != 1 {
				t.Fatalf("want 1 volume, got %d", len(got.Volumes))
			}

			vol := got.Volumes[0]
			if vol.StoragePool != tc.wantPool {
				t.Errorf("%s: StoragePool=%q want %q", tc.comment, vol.StoragePool, tc.wantPool)
			}

			if vol.MetaPool != tc.wantMeta {
				t.Errorf("%s: MetaPool=%q want %q", tc.comment, vol.MetaPool, tc.wantMeta)
			}
		})
	}
}

// TestMetaPoolDefaultsToInternal: when no scope carries
// StorPoolNameDrbdMeta, MetaPool must stay empty so the renderer
// falls back to the default `meta-disk internal;` line. Guards
// against the dispatcher accidentally stamping a non-zero
// default into DesiredVolume.MetaPool.
func TestMetaPoolDefaultsToInternal(t *testing.T) {
	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			StoragePool:            "data-hdd",
		},
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if len(got.Volumes) != 1 {
		t.Fatalf("want 1 volume, got %d", len(got.Volumes))
	}

	if vol := got.Volumes[0]; vol.MetaPool != "" {
		t.Errorf("MetaPool=%q want \"\" (internal default)", vol.MetaPool)
	}
}
