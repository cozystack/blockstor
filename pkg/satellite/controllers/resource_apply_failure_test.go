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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// TestRecordApplyFailureFollowsThePass: a resize waiting for a replica
// ends with the satellite's error only if the replica says its pass
// failed, and at which size. A pass that succeeds takes the record away
// again, so a later grow does not end on an old error.
func TestRecordApplyFailureFollowsThePass(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-fail"
		rdName = "pvc-fail"
	)

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: node, ResourceDefinitionName: rdName},
	}

	cli := fake.NewClientBuilder().
		WithScheme(newToggleDiskTestScheme(t)).
		WithObjects(res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{Client: cli, Config: Config{NodeName: node}}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 2048}},
		},
	}

	read := func() *blockstoriov1alpha1.Resource {
		t.Helper()

		var got blockstoriov1alpha1.Resource

		err := cli.Get(t.Context(), client.ObjectKey{Name: res.Name}, &got)
		if err != nil {
			t.Fatalf("get Resource: %v", err)
		}

		return &got
	}

	current := read()

	err := reconciler.recordApplyFailure(t.Context(), current, rd, []*intent.ResourceApplyResult{
		{Name: rdName, NodeName: node, Ok: false, Message: "resize volume: lvextend failed"},
	})
	if err != nil {
		t.Fatalf("record failure: %v", err)
	}

	failure := read().Status.ApplyFailure
	if failure == nil || failure.Message != "resize volume: lvextend failed" ||
		len(failure.Volumes) != 1 || failure.Volumes[0].SizeKib != 2048 {
		t.Fatalf("ApplyFailure after a failed pass: %+v", failure)
	}

	current = read()

	err = reconciler.recordApplyFailure(t.Context(), current, rd, []*intent.ResourceApplyResult{
		{Name: rdName, NodeName: node, Ok: true},
	})
	if err != nil {
		t.Fatalf("clear failure: %v", err)
	}

	if failure := read().Status.ApplyFailure; failure != nil {
		t.Fatalf("ApplyFailure after a pass that succeeded: %+v", failure)
	}
}

// TestGrowPendingRequeues: a grow held back by a peer completes when the
// peer resizes, which wakes nothing on this satellite, so the pass that
// found it short must be repeated to report the size.
func TestGrowPendingRequeues(t *testing.T) {
	t.Parallel()

	if growPending([]*intent.ResourceApplyResult{{Ok: true}}) {
		t.Error("a pass with nothing pending asked to be repeated")
	}

	if !growPending([]*intent.ResourceApplyResult{{Ok: true}, {Ok: true, GrowPending: true}}) {
		t.Error("a pass that left a grow short of its device was not repeated")
	}
}
