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

// `ResourceDefinition.spec.encryption.passphraseSecretRef` must be
// visible when it is ignored — and must NOT wedge the volume.
//
// The field is declared in api/v1alpha1/drbd_options.go and nothing
// reads it, so an RD that sets it is encrypted with the CLUSTER key
// while the operator holds a Secret that opens nothing.
//
// An earlier revision failed the apply. Review caught two problems
// with that: it wedges RDs which reconcile correctly today (the field
// has always been inert, so a live cluster may carry RDs that set it
// and run fine), and buildDesiredFromCRD's error goes to
// controller-runtime's backoff, so the object carried no trace of why.
// The volume now keeps working and the Resource carries
// EncryptionKeyIgnored instead.
//
// These drive `buildDesiredFromCRD` — the real call path — rather than
// the helpers directly. A direct unit test would stay green if the
// call site were deleted, which is the revertible-green trap: the
// behaviour has to be pinned where it is wired in.

package controllers

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// perRDKeyResource is the replica CRD buildDesiredFromCRD renders.
func perRDKeyResource(rdName string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + ".node-a"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "node-a",
		},
	}
}

// withPerRDKey pins a per-RD passphrase Secret on an RD.
func withPerRDKey(rd *blockstoriov1alpha1.ResourceDefinition, secretName string) *blockstoriov1alpha1.ResourceDefinition {
	rd.Spec.Encryption = &blockstoriov1alpha1.EncryptionConfig{
		PassphraseSecretRef: &corev1.LocalObjectReference{Name: secretName},
	}

	return rd
}

// readKeyIgnoredCondition re-reads the Resource and returns its
// EncryptionKeyIgnored Condition, or nil when absent.
func readKeyIgnoredCondition(t *testing.T, r *ResourceReconciler, name string) *metav1.Condition {
	t.Helper()

	var got blockstoriov1alpha1.Resource

	err := r.Get(t.Context(), client.ObjectKey{Name: name}, &got)
	if err != nil {
		t.Fatalf("re-read Resource %s: %v", name, err)
	}

	return meta.FindStatusCondition(got.Status.Conditions,
		blockstoriov1alpha1.ConditionEncryptionKeyIgnored)
}

// TestPerRDPassphraseSecretRefIsSurfacedNotRefused is the canonical
// red: the apply must SUCCEED (no wedge) and the Resource must carry
// the Condition naming the ignored Secret.
func TestPerRDPassphraseSecretRefIsSurfacedNotRefused(t *testing.T) {
	t.Parallel()

	rd := withPerRDKey(luksRD(), "tenant-owned-key")
	res := perRDKeyResource(rd.Name)
	r := newPassphraseTestReconciler(t, passphraseSecret("cluster-master"), rd, res)

	desired, err := r.buildDesiredFromCRD(t.Context(), res, rd, nil)
	if err != nil {
		t.Fatalf("apply must NOT be wedged by an unimplemented per-RD key: %v", err)
	}

	// It still provisions, on the cluster key — that is the whole
	// point of not refusing.
	if got := desired.GetProps()["LuksPassphrase"]; got != "cluster-master" {
		t.Errorf("LuksPassphrase wire prop: got %q, want the cluster passphrase", got)
	}

	cond := readKeyIgnoredCondition(t, r, res.Name)
	if cond == nil {
		t.Fatal("no EncryptionKeyIgnored Condition; the operator's key is ignored with no trace on the object")
	}

	if cond.Status != metav1.ConditionTrue {
		t.Errorf("Condition status: got %q, want True", cond.Status)
	}

	// The operator has to be able to act on it: the message must name
	// the field and the Secret, and say which key is actually in use.
	for _, want := range []string{"passphraseSecretRef", "tenant-owned-key", "CLUSTER"} {
		if !strings.Contains(cond.Message, want) {
			t.Errorf("Condition message %q does not mention %q", cond.Message, want)
		}
	}
}

// TestPerRDPassphraseConditionClearsWhenRefRemoved pins that the
// Condition tracks the live spec instead of latching. An operator who
// removes the misleading field must see the warning go away.
func TestPerRDPassphraseConditionClearsWhenRefRemoved(t *testing.T) {
	t.Parallel()

	rd := withPerRDKey(luksRD(), "tenant-owned-key")
	res := perRDKeyResource(rd.Name)
	r := newPassphraseTestReconciler(t, passphraseSecret("cluster-master"), rd, res)

	_, err := r.buildDesiredFromCRD(t.Context(), res, rd, nil)
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}

	if cond := readKeyIgnoredCondition(t, r, res.Name); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("precondition: want the Condition True after the first pass, got %+v", cond)
	}

	// Operator drops the field. Re-read the Resource so the reconciler
	// sees the Condition it just wrote, as it would on a real pass.
	var live blockstoriov1alpha1.Resource
	if err := r.Get(t.Context(), client.ObjectKey{Name: res.Name}, &live); err != nil {
		t.Fatalf("re-read: %v", err)
	}

	rd.Spec.Encryption = nil

	_, err = r.buildDesiredFromCRD(t.Context(), &live, rd, nil)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}

	cond := readKeyIgnoredCondition(t, r, res.Name)
	if cond == nil {
		t.Fatal("Condition vanished entirely; want it flipped to False so the history stays readable")
	}

	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Condition status after the ref was removed: got %q, want False", cond.Status)
	}
}

// TestLUKSRDWithoutPerRDKeyStampsNothing is the positive control: the
// everyday encrypted volume must not grow a Condition. Without this a
// stamper that fired unconditionally would look correct, and every
// Resource in the cluster would carry a meaningless entry plus an
// apiserver write per reconcile.
func TestLUKSRDWithoutPerRDKeyStampsNothing(t *testing.T) {
	t.Parallel()

	rd := luksRD()
	res := perRDKeyResource(rd.Name)
	r := newPassphraseTestReconciler(t, passphraseSecret("cluster-master"), rd, res)

	desired, err := r.buildDesiredFromCRD(t.Context(), res, rd, nil)
	if err != nil {
		t.Fatalf("LUKS RD on the cluster passphrase must build: %v", err)
	}

	if got := desired.GetProps()["LuksPassphrase"]; got != "cluster-master" {
		t.Errorf("LuksPassphrase wire prop: got %q, want the cluster passphrase", got)
	}

	if cond := readKeyIgnoredCondition(t, r, res.Name); cond != nil {
		t.Errorf("unexpected Condition on an ordinary encrypted volume: %+v", cond)
	}
}

// TestPerRDPassphraseIgnoredWithoutLUKS pins the scope. On a stack
// without LUKS the field is inert decoration — there is no wrong key
// to warn about, and warning would be noise.
func TestPerRDPassphraseIgnoredWithoutLUKS(t *testing.T) {
	t.Parallel()

	rd := luksRD()
	rd.Spec.LayerStack = []string{"DRBD", "STORAGE"}
	rd = withPerRDKey(rd, "tenant-owned-key")
	res := perRDKeyResource(rd.Name)
	r := newPassphraseTestReconciler(t, passphraseSecret("cluster-master"), rd, res)

	_, err := r.buildDesiredFromCRD(t.Context(), res, rd, nil)
	if err != nil {
		t.Fatalf("unencrypted RD must build: %v", err)
	}

	if cond := readKeyIgnoredCondition(t, r, res.Name); cond != nil {
		t.Errorf("unexpected Condition on a stack without LUKS: %+v", cond)
	}
}

// TestPerRDPassphraseEmptySecretNameStampsNothing pins the boundary: a
// present-but-empty ref carries no operator intent, so there is
// nothing to report.
func TestPerRDPassphraseEmptySecretNameStampsNothing(t *testing.T) {
	t.Parallel()

	rd := withPerRDKey(luksRD(), "")
	res := perRDKeyResource(rd.Name)
	r := newPassphraseTestReconciler(t, passphraseSecret("cluster-master"), rd, res)

	_, err := r.buildDesiredFromCRD(t.Context(), res, rd, nil)
	if err != nil {
		t.Fatalf("empty passphraseSecretRef.name must build: %v", err)
	}

	if cond := readKeyIgnoredCondition(t, r, res.Name); cond != nil {
		t.Errorf("unexpected Condition for an empty secret name: %+v", cond)
	}
}
