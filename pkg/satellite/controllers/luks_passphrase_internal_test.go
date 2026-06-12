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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// Bug 023 (satellite half): the master passphrase stamped by
// `linstor encryption create-passphrase` lives in a Secret, but the
// dispatch chain only lifted the LUKS key from the controller-scope
// props (`DrbdOptions/EncryptPassphrase` via ControllerConfig.
// ExtraProps). A Secret-only cluster passed the REST RD-create gate
// and then every replica apply looped on `LUKS in layer stack but
// Props.LuksPassphrase empty`. These tests pin
// injectLUKSMasterPassphrase: the Secret value must enter the
// effective-props bag under the canonical key (so BuildDesired's
// existing lift to the `LuksPassphrase` wire prop fires), with the
// legacy prop paths keeping precedence.

const testControllerNS = "blockstor-system"

// newPassphraseTestReconciler builds a ResourceReconciler over a
// fake client seeded with the given objects.
func newPassphraseTestReconciler(t *testing.T, objs ...client.Object) *ResourceReconciler {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 to scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor to scheme: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	return &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  "node-a",
			Namespace: testControllerNS,
		},
	}
}

// passphraseSecret is the Secret `linstor encryption
// create-passphrase` would have written (pkg/rest/encryption.go).
func passphraseSecret(value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      passphrase.DefaultSecretName,
			Namespace: testControllerNS,
		},
		Data: map[string][]byte{passphrase.SecretKey: []byte(value)},
	}
}

// luksRD returns an RD whose layer stack carries LUKS.
func luksRD() *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-luks-023"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 4 * 1024},
			},
		},
	}
}

// TestBug023SecretPassphraseReachesWireProps is the end-to-end pin
// for the satellite half: Secret-only cluster → injection fills the
// canonical prop → BuildDesired lifts it onto the `LuksPassphrase`
// wire prop the satellite's LUKS layer reads (pkg/satellite
// reconciler.go), and strips it from the rendered DRBD options.
func TestBug023SecretPassphraseReachesWireProps(t *testing.T) {
	t.Parallel()

	r := newPassphraseTestReconciler(t, passphraseSecret("super-secret-023"))
	rd := luksRD()
	effectiveProps := map[string]string{}

	r.injectLUKSMasterPassphrase(t.Context(), rd, effectiveProps)

	if got := effectiveProps[passphrase.PropKeyCanonical]; got != "super-secret-023" {
		t.Fatalf("injection: canonical key got %q, want the Secret value", got)
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rd.Name,
			NodeName:               "node-a",
			StoragePool:            "pool-a",
		},
	}

	desired := dispatcher.BuildDesired(target, nil, nil, nil, rd, effectiveProps)
	if desired == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if got := desired.Props["LuksPassphrase"]; got != "super-secret-023" {
		t.Errorf("LuksPassphrase wire prop: got %q, want the Secret value "+
			"(satellite reconciler reads exactly this key)", got)
	}

	if _, leaked := desired.DrbdOptions[passphrase.PropKeyCanonical]; leaked {
		t.Errorf("passphrase leaked into DrbdOptions — would render into the .res options block")
	}
}

// TestBug023LegacyPropKeepsPrecedence: an operator-set controller
// prop must win over the Secret so existing LUKS volumes keep
// unlocking with the key they were formatted with.
func TestBug023LegacyPropKeepsPrecedence(t *testing.T) {
	t.Parallel()

	r := newPassphraseTestReconciler(t, passphraseSecret("secret-value"))

	for _, key := range []string{passphrase.PropKeyCanonical, passphrase.PropKeyLegacy} {
		effectiveProps := map[string]string{key: "prop-value"}

		r.injectLUKSMasterPassphrase(t.Context(), luksRD(), effectiveProps)

		if got := effectiveProps[key]; got != "prop-value" {
			t.Errorf("legacy prop %q overwritten: got %q, want prop-value", key, got)
		}

		if key == passphrase.PropKeyLegacy {
			if _, added := effectiveProps[passphrase.PropKeyCanonical]; added {
				t.Errorf("canonical key injected despite legacy prop present — " +
					"pickLUKSPassphrase would switch keys under existing volumes")
			}
		}
	}
}

// TestBug023InjectionSkipsNonLUKS: non-LUKS RDs must not trigger a
// Secret round-trip nor carry the passphrase.
func TestBug023InjectionSkipsNonLUKS(t *testing.T) {
	t.Parallel()

	r := newPassphraseTestReconciler(t, passphraseSecret("secret-value"))
	rd := luksRD()
	rd.Spec.LayerStack = []string{"DRBD", "STORAGE"}
	effectiveProps := map[string]string{}

	r.injectLUKSMasterPassphrase(t.Context(), rd, effectiveProps)

	if len(effectiveProps) != 0 {
		t.Errorf("non-LUKS RD got props injected: %v", effectiveProps)
	}
}

// TestBug023InjectionToleratesMissingSecret: a LUKS RD on a cluster
// without any passphrase must not inject (the apply chain surfaces
// the established missing-key error) and must not error out.
func TestBug023InjectionToleratesMissingSecret(t *testing.T) {
	t.Parallel()

	r := newPassphraseTestReconciler(t)
	effectiveProps := map[string]string{}

	r.injectLUKSMasterPassphrase(t.Context(), luksRD(), effectiveProps)

	if len(effectiveProps) != 0 {
		t.Errorf("missing Secret must inject nothing, got: %v", effectiveProps)
	}
}

// TestBug023InjectionHonoursSecretRef: a ControllerConfig pinning a
// custom PassphraseSecretRef must route the lookup at that Secret —
// same resolution rule the REST writer uses, so the satellite reads
// the Secret the operator's create-passphrase actually wrote.
func TestBug023InjectionHonoursSecretRef(t *testing.T) {
	t.Parallel()

	custom := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "operator-passphrase",
			Namespace: testControllerNS,
		},
		Data: map[string][]byte{passphrase.SecretKey: []byte("custom-secret")},
	}

	cfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			PassphraseSecretRef: &blockstoriov1alpha1.PassphraseSecretRef{
				Name: "operator-passphrase",
			},
		},
	}

	r := newPassphraseTestReconciler(t, custom, cfg)
	effectiveProps := map[string]string{}

	r.injectLUKSMasterPassphrase(t.Context(), luksRD(), effectiveProps)

	if got := effectiveProps[passphrase.PropKeyCanonical]; got != "custom-secret" {
		t.Errorf("SecretRef lookup: got %q, want custom-secret", got)
	}
}
