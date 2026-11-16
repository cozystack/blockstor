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

// A passphrase in a property, under any key or spelling, never reaches the
// apply chain, LUKS or not: every volume has its own password, and a
// property is plaintext on its CR. Nothing is put in its place either; the
// intent carries no passphrase at all.
func TestPassphrasePropsNeverReachTheIntent(t *testing.T) {
	t.Parallel()

	plainRD := luksRD()
	plainRD.Spec.LayerStack = []string{"DRBD", "STORAGE"}

	for _, rd := range []*blockstoriov1alpha1.ResourceDefinition{luksRD(), plainRD} {
		for _, key := range []string{
			passphrase.PropKeyCanonical, passphrase.PropKeyLegacy, passphrase.PropKeyVolume,
			passphrase.WirePropKey, "drbdoptions/ENCRYPTPASSPHRASE",
		} {
			effectiveProps := map[string]string{key: "prop-value", "Aux/keep": "yes"}

			dropPlaintextPassphraseProps(t.Context(), rd, effectiveProps)

			if len(effectiveProps) != 1 || effectiveProps["Aux/keep"] != "yes" {
				t.Errorf("stack %v, prop %q: props after the drop = %v, want only Aux/keep",
					rd.Spec.LayerStack, key, effectiveProps)
			}

			target := &blockstoriov1alpha1.Resource{
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rd.Name,
					NodeName:               "node-a",
					StoragePool:            "pool-a",
					Props:                  map[string]string{key: "own-prop"},
				},
			}

			desired := dispatcher.BuildDesired(target, nil, nil, nil, rd, effectiveProps)
			for got := range desired.Props {
				if passphrase.IsPlaintextPropKey(got) {
					t.Errorf("stack %v, prop %q: intent carries %q", rd.Spec.LayerStack, key, got)
				}
			}

			for got := range desired.DrbdOptions {
				if passphrase.IsPlaintextPropKey(got) {
					t.Errorf("stack %v, prop %q: DRBD options carry %q", rd.Spec.LayerStack, key, got)
				}
			}
		}
	}
}
