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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestAdoptAuthoritativeFlags_OverridesStaleInactive is the core Bug
// 350 regression: the reconcile-entry r.Get read the c-r informer
// cache and still saw INACTIVE set after `linstor r activate`
// cleared it. The uncached APIReader-backed peer list carries the
// target's own AUTHORITATIVE (cleared) flags. adoptAuthoritativeFlags
// must overwrite the stale cached INACTIVE with the authoritative
// empty set so the desired state no longer drives `drbdadm down` on
// the just-reactivated replica.
func TestAdoptAuthoritativeFlags_OverridesStaleInactive(t *testing.T) {
	t.Parallel()

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-worker-2"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-2",
			ResourceDefinitionName: "pvc-1",
			// STALE cached view: INACTIVE still set post-activate.
			Flags: []string{"INACTIVE"},
		},
	}

	// Uncached APIReader list (includes the target itself) with the
	// AUTHORITATIVE flags: `r activate` cleared INACTIVE.
	peers := []blockstoriov1alpha1.Resource{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-worker-1"},
			Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "worker-1", ResourceDefinitionName: "pvc-1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1-worker-2"},
			Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "worker-2", ResourceDefinitionName: "pvc-1", Flags: []string{}},
		},
	}

	adoptAuthoritativeFlags(target, peers)

	if len(target.Spec.Flags) != 0 {
		t.Fatalf("stale INACTIVE not overridden: Flags = %v, want empty", target.Spec.Flags)
	}
}

// TestAdoptAuthoritativeFlags_AdoptsNewlySetFlag pins the symmetric
// direction: when the authoritative view has GAINED a flag the cache
// hasn't seen yet (e.g. INACTIVE just set), adoption must surface it
// so deactivate converges promptly rather than waiting for the cache.
func TestAdoptAuthoritativeFlags_AdoptsNewlySetFlag(t *testing.T) {
	t.Parallel()

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-2",
			ResourceDefinitionName: "pvc-1",
			Flags:                  []string{},
		},
	}

	peers := []blockstoriov1alpha1.Resource{
		{Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-2", ResourceDefinitionName: "pvc-1", Flags: []string{"INACTIVE"}}},
	}

	adoptAuthoritativeFlags(target, peers)

	if !slices.Equal(target.Spec.Flags, []string{"INACTIVE"}) {
		t.Fatalf("authoritative INACTIVE not adopted: Flags = %v, want [INACTIVE]", target.Spec.Flags)
	}
}

// TestAdoptAuthoritativeFlags_NoTargetEntryKeepsCachedFlags: when the
// peer list does NOT contain the target (e.g. the target's CRD was
// just deleted, or a unit test built the peer set from a different
// reader), adoption is a no-op and the cached flags stand — exactly
// the pre-fix behaviour, never a silent wipe.
func TestAdoptAuthoritativeFlags_NoTargetEntryKeepsCachedFlags(t *testing.T) {
	t.Parallel()

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-2",
			ResourceDefinitionName: "pvc-1",
			Flags:                  []string{"INACTIVE"},
		},
	}

	// Only OTHER nodes present — the target's own entry is missing.
	peers := []blockstoriov1alpha1.Resource{
		{Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-1", ResourceDefinitionName: "pvc-1", Flags: []string{}}},
	}

	adoptAuthoritativeFlags(target, peers)

	if !slices.Equal(target.Spec.Flags, []string{"INACTIVE"}) {
		t.Fatalf("missing-target no-op broke: Flags = %v, want [INACTIVE] (unchanged)", target.Spec.Flags)
	}
}

// TestAdoptAuthoritativeFlags_MatchesOnBothNodeAndRD guards against a
// cross-RD false match: a same-node Resource for a DIFFERENT RD must
// NOT be adopted (the matcher keys on NodeName AND
// ResourceDefinitionName).
func TestAdoptAuthoritativeFlags_MatchesOnBothNodeAndRD(t *testing.T) {
	t.Parallel()

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-2",
			ResourceDefinitionName: "pvc-1",
			Flags:                  []string{"INACTIVE"},
		},
	}

	// Same node, different RD — must be ignored. The correct entry
	// (pvc-1) follows and is what gets adopted.
	peers := []blockstoriov1alpha1.Resource{
		{Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-2", ResourceDefinitionName: "pvc-OTHER", Flags: []string{"DISKLESS"}}},
		{Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-2", ResourceDefinitionName: "pvc-1", Flags: []string{}}},
	}

	adoptAuthoritativeFlags(target, peers)

	if len(target.Spec.Flags) != 0 {
		t.Fatalf("cross-RD match leaked: Flags = %v, want empty", target.Spec.Flags)
	}
}
