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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestExpectedPeerNamesFor_FiltersLocalAndEmpty pins the contract
// the Bug 342 v3 wire site relies on: the local satellite's own node
// MUST be filtered out (drbdsetup show -j enumerates remote peers
// only — leaving the local name in would always trigger Pass-1
// "unexpected peer" against the local kernel slot, which doesn't
// exist), and empty-name entries must be dropped defensively.
func TestExpectedPeerNamesFor_FiltersLocalAndEmpty(t *testing.T) {
	t.Parallel()

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-worker-0"},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "worker-0"},
	}

	peers := []blockstoriov1alpha1.Resource{
		{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-worker-0"}, Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-0"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-worker-1"}, Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-worker-2"}, Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-2"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-empty"}, Spec: blockstoriov1alpha1.ResourceSpec{NodeName: ""}},
	}

	got := expectedPeerNamesFor(target, peers)
	slices.Sort(got)

	want := []string{"worker-1", "worker-2"}
	if !slices.Equal(got, want) {
		t.Errorf("expectedPeerNamesFor = %v, want %v", got, want)
	}
}

// TestExpectedPeerNamesFor_EmptyPeersIsEmpty: nil-safe empty slice
// for an isolated resource (no peers — single-replica case). Returns
// a non-nil empty slice so callers don't need to nil-check before
// ranging.
func TestExpectedPeerNamesFor_EmptyPeersIsEmpty(t *testing.T) {
	t.Parallel()

	target := &blockstoriov1alpha1.Resource{Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-0"}}

	got := expectedPeerNamesFor(target, nil)
	if got == nil {
		t.Errorf("expectedPeerNamesFor(nil peers) must return non-nil empty slice")
	}

	if len(got) != 0 {
		t.Errorf("expected empty, got %v", got)
	}
}

// TestVolNumsOf_ExtractsVolumeNumbers pins the contract the Bug 342
// v3 Pass-3 zombie probe relies on: every VolumeDefinition's
// VolumeNumber lands in the returned slice in declaration order so
// downstream Adm.Show consumers can index it stably.
func TestVolNumsOf_ExtractsVolumeNumbers(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
				{VolumeNumber: 1, SizeKib: 2048},
				{VolumeNumber: 7, SizeKib: 4096},
			},
		},
	}

	got := volNumsOf(rd)
	want := []int32{0, 1, 7}

	if !slices.Equal(got, want) {
		t.Errorf("volNumsOf = %v, want %v", got, want)
	}
}

// TestVolNumsOf_EmptyIsEmpty: a volume-less RD (degenerate case;
// shouldn't happen in production but exercised here so the helper's
// nil-safety is pinned).
func TestVolNumsOf_EmptyIsEmpty(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{}

	got := volNumsOf(rd)
	if got == nil {
		t.Errorf("volNumsOf(empty RD) must return non-nil empty slice")
	}

	if len(got) != 0 {
		t.Errorf("expected empty, got %v", got)
	}
}
