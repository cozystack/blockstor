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

// Bug 265 (HIGH): the LUKS passphrase key namespace mismatched
// between the controller and the dispatcher.
//
// The operator sets the cluster-scope master passphrase via
// `linstor controller set-property DrbdOptions/EncryptPassphrase
// <passphrase>` — same shape upstream LINSTOR uses (Bug 95 enforces
// its presence as a hard prerequisite for any LUKS-layered RD).
// The REST/effectiveprops layer surfaces it under that EXACT key:
// `DrbdOptions/EncryptPassphrase`.
//
// The dispatcher, however, only looked for
// `DrbdOptions/Encryption/passphrase` (note the inner namespace +
// lower-case `passphrase`) and lifted just that key onto the
// satellite-facing `LuksPassphrase` wire prop. Result: the cluster
// master key set by the operator NEVER reached the satellite, the
// satellite reconciler tripped on `LUKS in layer stack but
// Props.LuksPassphrase empty`, and every LUKS RD looped forever in
// "Props.LuksPassphrase empty" failures.
//
// Fix: dispatcher must read the SAME key the controller writes —
// `DrbdOptions/EncryptPassphrase`. The legacy
// `DrbdOptions/Encryption/passphrase` shape stays accepted for
// backwards-compat (alias-read) but the canonical key is the upstream
// one.
//
// This test pins the contract: cluster-scope `DrbdOptions/
// EncryptPassphrase` in effectiveProps MUST propagate to the wire
// as `LuksPassphrase`. Without the fix the dispatcher drops the
// master key on the floor.
func TestBug265LUKSMasterPassphraseReachesSatellite(t *testing.T) {
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug265-luks"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-bug265-luks",
			NodeName:               "n1",
			StoragePool:            "data-hdd",
		},
	}

	// Effective props as the resolver would produce after merging the
	// ControllerConfig.Spec.ExtraProps where the operator stamped
	// `DrbdOptions/EncryptPassphrase` via `linstor controller set-
	// property`. This is the EXACT shape pkg/effectiveprops.Resolve
	// returns (the resolver copies ControllerConfig.ExtraProps onto
	// the output via maps.Copy).
	effectiveProps := map[string]string{
		"DrbdOptions/EncryptPassphrase": "topsecret",
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, effectiveProps)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	pass, ok := got.Props["LuksPassphrase"]
	if !ok || pass == "" {
		t.Errorf("LuksPassphrase not propagated to wire after operator set "+
			"DrbdOptions/EncryptPassphrase; got props=%v. "+
			"The dispatcher must alias-read the cluster-scope master key "+
			"under upstream-LINSTOR's canonical name, not just the inner "+
			"DrbdOptions/Encryption/passphrase namespace. (Bug 265)",
			got.Props)
	}

	if pass != "topsecret" {
		t.Errorf("LuksPassphrase mismatched: got %q, want %q", pass, "topsecret")
	}
}

// TestBug265LUKSLegacyEncryptionPassphraseStillWorks pins the
// backwards-compat half: any caller that still emits the legacy
// `DrbdOptions/Encryption/passphrase` shape (the pre-fix dispatcher
// surface) MUST keep working, so a half-upgraded cluster where one
// of the prop tiers writes the legacy form doesn't break.
func TestBug265LUKSLegacyEncryptionPassphraseStillWorks(t *testing.T) {
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug265-legacy"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-bug265-legacy",
			NodeName:               "n1",
			StoragePool:            "data-hdd",
		},
	}

	effectiveProps := map[string]string{
		"DrbdOptions/Encryption/passphrase": "legacy-shape",
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, effectiveProps)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	pass, ok := got.Props["LuksPassphrase"]
	if !ok || pass != "legacy-shape" {
		t.Errorf("legacy DrbdOptions/Encryption/passphrase no longer propagates; "+
			"got props=%v. Backwards-compat alias must keep working.",
			got.Props)
	}
}
