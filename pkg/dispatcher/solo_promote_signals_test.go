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
)

// The satellite's solo-promote may force Primary only on a replica that
// is the definition's only copy and is not being deleted. These pin the
// two signals BuildDesired hands it for that.

func soloSignalsRD() *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}
}

func soloSignalsReplica(node string, flags ...string) blockstoriov1alpha1.Resource {
	return blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               node,
			StoragePool:            "thin1",
			Flags:                  flags,
		},
	}
}

// TestBuildDesired_Deleting: a deletion timestamp on the replica or the
// definition, the DELETE flag `rd d` sets on the definition first, or
// the DELETING mark of `r d` makes the replica Deleting.
func TestBuildDesired_Deleting(t *testing.T) {
	now := metav1.Now()

	cases := []struct {
		name   string
		mutate func(target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition)
		want   bool
	}{
		{name: "live", mutate: func(*blockstoriov1alpha1.Resource, *blockstoriov1alpha1.ResourceDefinition) {}},
		{
			name: "replica deletion timestamp", want: true,
			mutate: func(target *blockstoriov1alpha1.Resource, _ *blockstoriov1alpha1.ResourceDefinition) {
				target.DeletionTimestamp = &now
			},
		},
		{
			name: "replica DELETING", want: true,
			mutate: func(target *blockstoriov1alpha1.Resource, _ *blockstoriov1alpha1.ResourceDefinition) {
				target.Spec.Flags = []string{"DELETING"}
			},
		},
		{
			name: "definition DELETE flag", want: true,
			mutate: func(_ *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) {
				rd.Spec.Flags = []string{"DELETE"}
			},
		},
		{
			name: "definition deletion timestamp", want: true,
			mutate: func(_ *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) {
				rd.DeletionTimestamp = &now
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := soloSignalsReplica("n1")
			rd := soloSignalsRD()
			tc.mutate(&target, rd)

			dr := dispatcher.BuildDesired(&target, []blockstoriov1alpha1.Resource{target}, nil, nil, rd, nil)
			if dr.GetDeleting() != tc.want {
				t.Errorf("Deleting = %v, want %v", dr.GetDeleting(), tc.want)
			}
		})
	}
}

// TestBuildDesired_OtherDiskfulReplica: any diskful sibling counts,
// including a deactivated one and one without a node-id, both of which
// the desired peer list leaves out; diskless and tiebreaker siblings do
// not.
func TestBuildDesired_OtherDiskfulReplica(t *testing.T) {
	now := metav1.Now()
	deleting := soloSignalsReplica("n2")
	deleting.DeletionTimestamp = &now

	replica := func(flags ...string) *blockstoriov1alpha1.Resource {
		r := soloSignalsReplica("n2", flags...)

		return &r
	}

	cases := []struct {
		name    string
		sibling *blockstoriov1alpha1.Resource
		want    bool
	}{
		{name: "alone"},
		{name: "diskless witness", sibling: replica("DISKLESS")},
		{name: "tiebreaker", sibling: replica("TIE_BREAKER")},
		{name: "diskful without node-id", sibling: replica(), want: true},
		{name: "deactivated diskful", sibling: replica("INACTIVE"), want: true},
		{name: "diskful being deleted", sibling: &deleting, want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := soloSignalsReplica("n1")
			replicas := []blockstoriov1alpha1.Resource{target}

			if tc.sibling != nil {
				replicas = append(replicas, *tc.sibling)
			}

			dr := dispatcher.BuildDesired(&target, replicas, nil, nil, soloSignalsRD(), nil)
			if dr.GetOtherDiskfulReplica() != tc.want {
				t.Errorf("OtherDiskfulReplica = %v, want %v", dr.GetOtherDiskfulReplica(), tc.want)
			}

			if len(dr.GetPeerNames()) != 0 {
				t.Errorf("precondition: the sibling must be outside the desired peers, got %v", dr.GetPeerNames())
			}
		})
	}
}
