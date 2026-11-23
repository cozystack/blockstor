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
	"github.com/cozystack/blockstor/pkg/passphrase"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// Bug 265 once had the master passphrase lifted from the resolved
// props onto the `LuksPassphrase` wire key the satellite read. Every LUKS
// volume now has its own password, stored wrapped on its replica, so the
// canonical master-key property is dropped like any other passphrase
// property and nothing reaches the wire in its place.
func TestBug265MasterPassphrasePropNeverReachesTheWire(t *testing.T) {
	got := dispatcher.BuildDesired(luksTestTarget("pvc-bug265-luks", nil), nil, nil, nil,
		luksTestRD("pvc-bug265-luks"), map[string]string{passphrase.PropKeyCanonical: "topsecret"})
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	assertNoPassphraseOnWire(t, got)
}

// luksTestRD and luksTestTarget are a LUKS-layered definition and one
// diskful replica of it, for the passphrase tests below.
func luksTestRD(name string) *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}
}

func luksTestTarget(name string, props map[string]string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: name,
			NodeName:               "n1",
			StoragePool:            "data-hdd",
			Props:                  props,
		},
	}
}

// assertNoPassphraseOnWire fails when any key that would carry the
// master passphrase made it into the wire props or the DRBD options.
func assertNoPassphraseOnWire(t *testing.T, got *intent.DesiredResource) {
	t.Helper()

	for key, value := range got.Props {
		if passphrase.IsPlaintextPropKey(key) {
			t.Errorf("wire props carry %s=%q; no passphrase property may reach the LUKS layer", key, value)
		}
	}

	for key, value := range got.DrbdOptions {
		if passphrase.IsPlaintextPropKey(key) {
			t.Errorf("DRBD options carry %s=%q", key, value)
		}
	}
}

// The early per-RD alias `DrbdOptions/Encryption/passphrase` is a
// property, kept in plaintext on its CR, and is never read: when it is
// all there is, no passphrase reaches the satellite and the LUKS layer
// fails the apply with its actionable message.
func TestBug265LUKSLegacyEncryptionPassphraseIsIgnored(t *testing.T) {
	rd := luksTestRD("pvc-bug265-legacy")
	target := luksTestTarget("pvc-bug265-legacy", nil)

	effectiveProps := map[string]string{
		passphrase.PropKeyLegacy: "legacy-shape",
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, effectiveProps)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	assertNoPassphraseOnWire(t, got)
}
