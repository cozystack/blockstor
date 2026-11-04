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

package dispatcher_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/drbd"
)

func spelledReplica(rd, node string) blockstoriov1alpha1.Resource {
	return blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rd,
			NodeName:               node,
			StoragePool:            "data",
		},
	}
}

func spelledRD(props map[string]string) *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props:             props,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 4096}},
		},
	}
}

// A replica written before the store kept CR names in spec names its
// definition Pvc-Mix. Its satellite names the resource, and derives the
// port and minor it falls back to, from the CR name, the way every other
// replica of the definition does.
func TestAnOldMixedCaseReplicaIsNamedByItsCRName(t *testing.T) {
	t.Parallel()

	rd := spelledRD(nil)
	old := spelledReplica("Pvc-Mix", "n1")
	current := spelledReplica("pvc-mix", "n1")

	gotOld := dispatcher.BuildDesired(&old, nil, nil, nil, rd, nil)
	gotCurrent := dispatcher.BuildDesired(&current, nil, nil, nil, rd, nil)

	if gotOld.GetName() != "pvc-mix" {
		t.Errorf("resource name %q, want the CR name pvc-mix", gotOld.GetName())
	}

	for _, key := range []string{"port", "minor"} {
		if a, b := gotOld.GetDrbdOptions()[key], gotCurrent.GetDrbdOptions()[key]; a != b {
			t.Errorf("%s %s under Pvc-Mix, %s under pvc-mix; want one", key, a, b)
		}
	}
}

// A peer stored as Pvc-Mix carrying the day0 generation its satellite
// minted under the CR name is a fresh sibling, not a peer with data.
func TestAnOldMixedCasePeersDay0GenerationIsRecognised(t *testing.T) {
	t.Parallel()

	peer := spelledReplica("Pvc-Mix", "n2")
	peer.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{
		VolumeNumber: 0,
		DiskState:    string(drbd.DiskStateUpToDate),
		CurrentGI:    drbd.Day0GIFor("pvc-mix", 0),
	}}

	target := spelledReplica("pvc-mix", "n1")

	if got := dispatcher.BuildDesired(&target, []blockstoriov1alpha1.Resource{peer}, nil, nil, spelledRD(nil), nil); got.GetPeerHasData() {
		t.Error("a day0 sibling stored as Pvc-Mix reads as a peer with data")
	}
}

// A restore names its source the way the source's satellite named it on
// disk, by its CR name; the cross-cluster form is left as it is.
func TestARestoreSourceIsNamedByItsCRName(t *testing.T) {
	t.Parallel()

	for source, want := range map[string]string{
		"Pvc-Src:snap-1":        "pvc-src:snap-1",
		"remote:Pvc-Src:snap-1": "remote:Pvc-Src:snap-1",
	} {
		target := spelledReplica("pvc-mix", "n1")
		got := dispatcher.BuildDesired(&target, nil, nil, nil, spelledRD(map[string]string{"BlockstorRestoreFromSnapshot": source}), nil)

		if vols := got.GetVolumes(); len(vols) != 1 || vols[0].GetSourceSnapshot() != want {
			t.Errorf("source %q became %v; want %q", source, vols, want)
		}
	}
}
