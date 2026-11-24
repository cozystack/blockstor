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

package controllers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// The block-size LINSTOR writes on a volume definition has to reach the
// intent the satellite renders from. Without it a fresh attach of an
// adopted volume (reboot, down and up, a new replica) comes up with
// DRBD's default 512-byte logical sector instead of the 4096 the volume
// was created and formatted with.
func TestTheIntentCarriesAVolumeDefinitionsDiskOptions(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-adopted"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 4 * 1024, Props: map[string]string{
					"DrbdOptions/Disk/block-size": "4096",
				}},
			},
		},
	}

	replica := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-adopted.node-a"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-adopted",
			NodeName:               "node-a",
			StoragePool:            "pool-a",
		},
	}

	r := newPassphraseTestReconciler(t, rd, replica)

	desired, err := r.buildDesiredFromCRD(t.Context(), replica, rd, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if len(desired.GetVolumes()) != 1 {
		t.Fatalf("volumes = %d, want 1", len(desired.GetVolumes()))
	}

	if got := desired.GetVolumes()[0].GetDiskOptions()["block-size"]; got != "4096" {
		t.Errorf("volume 0 block-size = %q, want 4096", got)
	}
}
