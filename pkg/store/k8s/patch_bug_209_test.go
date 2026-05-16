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

package k8s_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Bug 209 (pokeV16): operator-set typed RD spec field Spec.Encryption
// is wiped on REST modify. Same root-cause class as Bug 208 / Bug 206
// — wireToCRDRDSpec rebuilds the spec from the wire, dropping the
// typed Encryption pointer that has no wire counterpart on
// apiv1.ResourceDefinition. RD Update DOES carry-across
// VolumeDefinitions but not Encryption.

func TestBug209_RDUpdateWipesEncryption(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-enc"},
		Spec: crdv1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "rg-enc",
			Encryption: &crdv1alpha1.EncryptionConfig{
				PassphraseSecretRef: &corev1.LocalObjectReference{Name: "luks-passphrase"},
			},
		},
	}
	k8s.SetOriginalName(&seed.ObjectMeta, "rd-enc")

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.ResourceDefinition{}).
		Build()

	s := k8s.New(cli)

	cur, err := s.ResourceDefinitions().Get(context.Background(), "rd-enc")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if cur.Props == nil {
		cur.Props = map[string]string{}
	}
	cur.Props["Aux/foo"] = "bar"

	if err := s.ResourceDefinitions().Update(context.Background(), &cur); err != nil {
		t.Fatalf("update: %v", err)
	}

	var got crdv1alpha1.ResourceDefinition
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "rd-enc"}, &got); err != nil {
		t.Fatalf("get post-update: %v", err)
	}

	if got.Spec.Encryption == nil {
		t.Fatalf("Spec.Encryption was WIPED by the routine RD Update (Bug 209); LUKS passphrase ref lost")
	}
	if got.Spec.Encryption.PassphraseSecretRef.Name != "luks-passphrase" {
		t.Fatalf("Encryption.PassphraseSecretRef clobbered: got %q want %q",
			got.Spec.Encryption.PassphraseSecretRef.Name, "luks-passphrase")
	}
}

func TestBug209_RDPatchWipesEncryption(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-enc2"},
		Spec: crdv1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "rg-enc",
			Encryption: &crdv1alpha1.EncryptionConfig{
				PassphraseSecretRef: &corev1.LocalObjectReference{Name: "luks-passphrase"},
			},
		},
	}
	k8s.SetOriginalName(&seed.ObjectMeta, "rd-enc2")

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.ResourceDefinition{}).
		Build()

	s := k8s.New(cli)

	err := s.ResourceDefinitions().PatchResourceDefinitionSpec(context.Background(), "rd-enc2", func(rd *apiv1.ResourceDefinition) error {
		if rd.Props == nil {
			rd.Props = map[string]string{}
		}
		rd.Props["Aux/foo"] = "bar"
		return nil
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	var got crdv1alpha1.ResourceDefinition
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "rd-enc2"}, &got); err != nil {
		t.Fatalf("get post-patch: %v", err)
	}

	if got.Spec.Encryption == nil {
		t.Fatalf("Spec.Encryption was WIPED by the routine RD Patch (Bug 209); LUKS passphrase ref lost")
	}
}
