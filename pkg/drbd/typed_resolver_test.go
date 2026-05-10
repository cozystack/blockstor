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

package drbd_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestResolveDRBDOptionsAllNilReturnsNil pins the short-circuit: an
// RD with no DRBD config that doesn't inherit from anywhere returns
// nil so callers can avoid running the .res renderer's per-section
// logic on an empty struct.
func TestResolveDRBDOptionsAllNilReturnsNil(t *testing.T) {
	t.Parallel()

	got := drbd.ResolveDRBDOptions(nil, nil, nil, nil)
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

// TestResolveDRBDOptionsLowerScopeOverrides pins the override
// hierarchy: Resource > RD > RG > Controller. Resource-level Net.Protocol=A
// must win over the RG's "C" and the controller's "B".
func TestResolveDRBDOptionsLowerScopeOverrides(t *testing.T) {
	t.Parallel()

	controller := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{Protocol: "B"},
	}
	rg := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{Protocol: "C"},
	}
	resource := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{Protocol: "A"},
	}

	got := drbd.ResolveDRBDOptions(controller, rg, nil, resource)
	if got.Net.Protocol != "A" {
		t.Errorf("Protocol: got %q, want A (Resource overrides everything)", got.Net.Protocol)
	}
}

// TestResolveDRBDOptionsFieldByFieldMerge pins the per-section,
// per-field merge: each non-nil scope contributes only its non-empty
// fields; lower scopes inherit upper scopes' fields they don't
// explicitly override.
//
// RG sets Protocol=C, MaxBuffers=8000.
// RD sets MaxBuffers=4000 (overrides RG's 8000), leaves Protocol unset.
// Result: Protocol=C (inherited from RG), MaxBuffers=4000 (RD wins).
func TestResolveDRBDOptionsFieldByFieldMerge(t *testing.T) {
	t.Parallel()

	rgMax := int32(8000)
	rdMax := int32(4000)

	rg := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{
			Protocol:   "C",
			MaxBuffers: &rgMax,
		},
	}
	rd := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{
			MaxBuffers: &rdMax,
			// Protocol left empty — must inherit
		},
	}

	got := drbd.ResolveDRBDOptions(nil, rg, rd, nil)

	if got.Net.Protocol != "C" {
		t.Errorf("Protocol: got %q, want C (RD didn't override → inherited from RG)", got.Net.Protocol)
	}

	if got.Net.MaxBuffers == nil || *got.Net.MaxBuffers != 4000 {
		t.Errorf("MaxBuffers: got %v, want 4000 (RD overrides RG's 8000)", got.Net.MaxBuffers)
	}
}

// TestResolveDRBDOptionsNilPointerNotSet pins the nil-vs-set
// discipline for `*bool` and `*int32`: a `nil` value at any scope
// means "not overridden, inherit"; only non-nil sets the field.
//
// RG sets AllowTwoPrimaries=false (explicit pointer to false). RD
// has no Net options at all (nil). Resource sets AllowTwoPrimaries=true.
// Result: true. The RG's explicit "false" was overridden by Resource's
// non-nil "true".
func TestResolveDRBDOptionsNilPointerNotSet(t *testing.T) {
	t.Parallel()

	allowFalse := false
	allowTrue := true

	rg := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{AllowTwoPrimaries: &allowFalse},
	}
	resource := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{AllowTwoPrimaries: &allowTrue},
	}

	got := drbd.ResolveDRBDOptions(nil, rg, nil, resource)

	if got.Net.AllowTwoPrimaries == nil || !*got.Net.AllowTwoPrimaries {
		t.Errorf("AllowTwoPrimaries: got %v, want true (Resource overrides)", got.Net.AllowTwoPrimaries)
	}
}

// TestResolveDRBDOptionsExplicitFalsePersists pins that an explicit
// `*bool` set to false at lower scope still wins — the resolver must
// not treat "false" as "not set" (a regression that did `if *src.X
// { out.X = src.X }` would silently drop explicit-false overrides).
func TestResolveDRBDOptionsExplicitFalsePersists(t *testing.T) {
	t.Parallel()

	allowTrue := true
	allowFalse := false

	rg := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{AllowTwoPrimaries: &allowTrue},
	}
	rd := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{AllowTwoPrimaries: &allowFalse},
	}

	got := drbd.ResolveDRBDOptions(nil, rg, rd, nil)

	if got.Net.AllowTwoPrimaries == nil {
		t.Fatalf("AllowTwoPrimaries: got nil, want explicit-false")
	}

	if *got.Net.AllowTwoPrimaries {
		t.Errorf("AllowTwoPrimaries: got true, want false (RD's explicit-false overrides RG)")
	}
}

// TestResolveDRBDOptionsSecretRefOverride pins SharedSecretRef
// override — a Resource-level Secret reference replaces a cluster-
// wide one entirely (we don't merge the Secret contents, that
// happens at the satellite).
func TestResolveDRBDOptionsSecretRefOverride(t *testing.T) {
	t.Parallel()

	clusterRef := &corev1.LocalObjectReference{Name: "cluster-secret"}
	rdRef := &corev1.LocalObjectReference{Name: "rd-specific-secret"}

	controller := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{SharedSecretRef: clusterRef},
	}
	rd := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{SharedSecretRef: rdRef},
	}

	got := drbd.ResolveDRBDOptions(controller, nil, rd, nil)

	if got.Net.SharedSecretRef == nil || got.Net.SharedSecretRef.Name != "rd-specific-secret" {
		t.Errorf("SharedSecretRef: got %v, want {Name: rd-specific-secret}", got.Net.SharedSecretRef)
	}
}

// TestResolveDRBDOptionsHandlersMerge pins the per-key handler
// override semantics: lower scopes overwrite same-key entries from
// higher scopes; an explicit empty-string value DELETES the entry
// (mirrors `linstor c sp DrbdOptions/Handlers/fence-peer ""` upstream).
func TestResolveDRBDOptionsHandlersMerge(t *testing.T) {
	t.Parallel()

	controller := &blockstoriov1alpha1.DRBDOptions{
		Handlers: map[string]string{
			"fence-peer":           "/usr/bin/cluster-fence",
			"before-resync-target": "/usr/bin/cluster-before-resync",
		},
	}
	rg := &blockstoriov1alpha1.DRBDOptions{
		Handlers: map[string]string{
			"fence-peer": "/usr/bin/rg-specific-fence", // overrides
		},
	}
	rd := &blockstoriov1alpha1.DRBDOptions{
		Handlers: map[string]string{
			"before-resync-target": "", // deletes the inherited entry
			"split-brain":          "/usr/bin/rd-specific-split-brain",
		},
	}

	got := drbd.ResolveDRBDOptions(controller, rg, rd, nil)

	if got.Handlers["fence-peer"] != "/usr/bin/rg-specific-fence" {
		t.Errorf("fence-peer: got %q, want /usr/bin/rg-specific-fence", got.Handlers["fence-peer"])
	}

	if _, exists := got.Handlers["before-resync-target"]; exists {
		t.Errorf("before-resync-target: must be deleted by RD's empty-string override; still got %q",
			got.Handlers["before-resync-target"])
	}

	if got.Handlers["split-brain"] != "/usr/bin/rd-specific-split-brain" {
		t.Errorf("split-brain: got %q, want /usr/bin/rd-specific-split-brain", got.Handlers["split-brain"])
	}
}

// TestResolveDRBDOptionsCrossSectionMerge pins that different
// sections can come from different scopes simultaneously: Net from
// RG, Resource (resource-options block) from RD, Handlers from
// Controller — all preserved in the output.
func TestResolveDRBDOptionsCrossSectionMerge(t *testing.T) {
	t.Parallel()

	controller := &blockstoriov1alpha1.DRBDOptions{
		Handlers: map[string]string{"fence-peer": "/cluster-fence"},
	}
	rg := &blockstoriov1alpha1.DRBDOptions{
		Net: &blockstoriov1alpha1.DRBDNetOptions{Protocol: "C"},
	}
	rd := &blockstoriov1alpha1.DRBDOptions{
		Resource: &blockstoriov1alpha1.DRBDResourceOptions{Quorum: "majority"},
	}

	got := drbd.ResolveDRBDOptions(controller, rg, rd, nil)

	if got.Net.Protocol != "C" {
		t.Errorf("Net.Protocol: got %q, want C", got.Net.Protocol)
	}

	if got.Resource.Quorum != "majority" {
		t.Errorf("Resource.Quorum: got %q, want majority", got.Resource.Quorum)
	}

	if got.Handlers["fence-peer"] != "/cluster-fence" {
		t.Errorf("Handlers[fence-peer]: got %q, want /cluster-fence", got.Handlers["fence-peer"])
	}
}
