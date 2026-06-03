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

package k8s

import (
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestWithDeletingFlag pins the E4 two-phase-delete surface helper:
// when a CRD carries a DeletionTimestamp the wire Flags slice gains
// the upstream-canonical DELETE token (rendered as DELETING by the
// CLI's State column); when it doesn't, the slice is passed through
// untouched.
func TestWithDeletingFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		in       []string
		deleting bool
		want     []string
	}{
		{
			name:     "not deleting passes through nil",
			in:       nil,
			deleting: false,
			want:     nil,
		},
		{
			name:     "not deleting passes through existing flags",
			in:       []string{apiv1.ResourceFlagTieBreaker},
			deleting: false,
			want:     []string{apiv1.ResourceFlagTieBreaker},
		},
		{
			name:     "deleting appends DELETE to empty",
			in:       nil,
			deleting: true,
			want:     []string{apiv1.ResourceFlagDelete},
		},
		{
			name:     "deleting appends DELETE keeping siblings",
			in:       []string{apiv1.ResourceFlagDiskless},
			deleting: true,
			want:     []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagDelete},
		},
		{
			name:     "deleting is idempotent when DELETE already present",
			in:       []string{apiv1.ResourceFlagDelete},
			deleting: true,
			want:     []string{apiv1.ResourceFlagDelete},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := withDeletingFlag(tc.in, tc.deleting)
			if !slices.Equal(got, tc.want) {
				t.Errorf("withDeletingFlag(%v, %v) = %v, want %v", tc.in, tc.deleting, got, tc.want)
			}
		})
	}
}

// TestWithDeletingFlagDoesNotMutateInput guards the no-aliasing
// contract: a caller that re-reads the original Spec.Flags slice
// after the projection must not see DELETE leak back into the CRD.
func TestWithDeletingFlagDoesNotMutateInput(t *testing.T) {
	t.Parallel()

	in := []string{apiv1.ResourceFlagDiskless}
	_ = withDeletingFlag(in, true)

	if slices.Contains(in, apiv1.ResourceFlagDelete) {
		t.Fatalf("withDeletingFlag mutated its input slice: %v", in)
	}
}

// TestCrdToWireRDSurfacesDeletingFlag pins E4 at the RD projection
// boundary: an RD CRD with a DeletionTimestamp (finalizer-blocked,
// e.g. a downed satellite) surfaces DELETE on the wire so `rd l`
// renders the State column as DELETING in the interim.
func TestCrdToWireRDSurfacesDeletingFlag(t *testing.T) {
	t.Parallel()

	now := metav1.NewTime(time.Now())
	crd := &crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleting-rd",
			DeletionTimestamp: &now,
			Finalizers:        []string{"blockstor.cozystack.io/satellite-resource"},
		},
	}
	SetOriginalName(&crd.ObjectMeta, "deleting-rd")

	wire := crdToWireRD(crd)
	if !slices.Contains(wire.Flags, apiv1.ResourceFlagDelete) {
		t.Fatalf("crdToWireRD did not surface DELETE on a deleting RD: flags=%v", wire.Flags)
	}

	// A live RD (no DeletionTimestamp) must NOT carry the flag.
	live := &crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "live-rd"},
	}
	SetOriginalName(&live.ObjectMeta, "live-rd")

	if slices.Contains(crdToWireRD(live).Flags, apiv1.ResourceFlagDelete) {
		t.Fatalf("crdToWireRD surfaced DELETE on a live RD")
	}
}

// TestCrdToWireResourceSurfacesDeletingFlag pins E4 at the per-replica
// projection boundary: a Resource CRD with a DeletionTimestamp surfaces
// DELETE so `r l` renders DELETING for the stuck replica while the
// satellite-resource finalizer is still held.
func TestCrdToWireResourceSurfacesDeletingFlag(t *testing.T) {
	t.Parallel()

	now := metav1.NewTime(time.Now())
	crd := &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleting-rsc",
			DeletionTimestamp: &now,
			Finalizers:        []string{"blockstor.cozystack.io/satellite-resource"},
		},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "deleting-rd",
			NodeName:               "worker-1",
		},
	}

	wire := crdToWireResource(crd)
	if !slices.Contains(wire.Flags, apiv1.ResourceFlagDelete) {
		t.Fatalf("crdToWireResource did not surface DELETE on a deleting Resource: flags=%v", wire.Flags)
	}

	live := &crdv1alpha1.Resource{
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "live-rd",
			NodeName:               "worker-1",
		},
	}
	if slices.Contains(crdToWireResource(live).Flags, apiv1.ResourceFlagDelete) {
		t.Fatalf("crdToWireResource surfaced DELETE on a live Resource")
	}
}
