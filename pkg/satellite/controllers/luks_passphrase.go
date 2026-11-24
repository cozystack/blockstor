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
	"context"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// dropPlaintextPassphraseProps removes every property that would carry a
// LUKS passphrase from the resolved effective-props bag, and names the
// ones that held a value in a log line, never the values. Each LUKS volume
// has its own password, stored wrapped on its replica (luks_keys.go); a
// passphrase in a property is never a key to anything, and a property is
// plaintext on its CR.
func dropPlaintextPassphraseProps(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition, effectiveProps map[string]string) {
	ignored := passphrase.DropPlaintextProps(effectiveProps)

	if rd == nil || len(ignored) == 0 || !layerStackHasLUKS(rd.Spec.LayerStack) {
		return
	}

	log.FromContext(ctx).Info("ignoring LUKS passphrase properties; every volume has its own password",
		"resourceDefinition", rd.Name, "properties", ignored)
}

// perRDKeyFieldPath names the field in operator-facing messages.
const perRDKeyFieldPath = "spec.encryption.passphraseSecretRef"

// perRDKeySecretName returns the Secret an RD pins through
// `spec.encryption.passphraseSecretRef`, or "" when it pins none.
//
// The field is declared in api/v1alpha1/drbd_options.go and is NOT
// implemented: nothing reads it. The only non-test code that touches
// Spec.Encryption is pkg/store/k8s/resource_definitions.go, which
// PRESERVES it across updates (Bug 209) so it survives a reconciler
// write: a field defended against loss and never consumed. An RD that
// sets it gets the same per-replica passwords, wrapped under the
// cluster master key, as every other RD (docs/encryption-keys.md).
//
// Scoped to LUKS-layered RDs by the caller: on a stack without LUKS the
// field is inert decoration and there is no wrong key to warn about.
func perRDKeySecretName(rd *blockstoriov1alpha1.ResourceDefinition) string {
	if rd == nil || !layerStackHasLUKS(rd.Spec.LayerStack) {
		return ""
	}

	if rd.Spec.Encryption == nil || rd.Spec.Encryption.PassphraseSecretRef == nil {
		return ""
	}

	return rd.Spec.Encryption.PassphraseSecretRef.Name
}

// noteIgnoredPerRDKey keeps ConditionEncryptionKeyIgnored on the
// Resource in step with the parent RD's spec.
//
// Deliberately NOT a refusal. An earlier revision failed the apply
// here; review caught that it wedges RDs which reconcile correctly
// today (the field has always been inert, so an existing cluster may
// carry RDs that set it and run fine on the cluster key) and that the
// wedge left nothing on the object saying why, because
// buildDesiredFromCRD's error goes to controller-runtime's backoff and
// never reaches Status. Moving a working volume to a failing one is
// the wrong trade here; the silence was the actual defect, so the fix
// is to break the silence.
//
// Best-effort: a stamp failure is logged and swallowed. The Condition
// is an advisory annotation on an otherwise healthy apply, and wedging
// the reconcile on a transient apiserver blip would recreate exactly
// the failure mode this replaced.
//
// Writes only on a state CHANGE. SSA would make a repeat patch a
// no-op at the apiserver, but it still costs a request per reconcile
// on every encrypted volume in the cluster; the guard keeps the steady
// state free.
func (r *ResourceReconciler) noteIgnoredPerRDKey(ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) {
	if target == nil {
		return
	}

	existing := meta.FindStatusCondition(target.Status.Conditions, blockstoriov1alpha1.ConditionEncryptionKeyIgnored)
	cond := encryptionKeyIgnoredCondition(perRDKeySecretName(rd), existing)

	if conditionAlreadyIs(existing, cond.Status, cond.Message) {
		return
	}

	// Never stamp False on an RD that never had the Condition: a
	// cluster full of ordinary volumes would otherwise grow a
	// meaningless "no per-resource key" entry on every Resource.
	if cond.Status == metav1.ConditionFalse && existing == nil {
		return
	}

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Resource",
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: target.Name},
		Status: blockstoriov1alpha1.ResourceStatus{
			Conditions: []metav1.Condition{cond},
		},
	}

	// No ForceOwnership: SSA's listMap merge on `type` lets this
	// writer own only its own Condition entry, leaving
	// MetadataCreated / FilesystemFormatted / KernelLoaded intact.
	err := r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner("blockstor-satellite-encryption-key"))
	if err != nil {
		log.FromContext(ctx).Error(err,
			"stamp EncryptionKeyIgnored Condition",
			"resource", target.Name)
	}
}

// encryptionKeyIgnoredCondition builds the EncryptionKeyIgnored
// Condition for an RD whose per-RD Secret is `secret` ("" for none).
// A message-only change (the RD repointed at another Secret) keeps the
// time the status last changed, as meta.SetStatusCondition does.
func encryptionKeyIgnoredCondition(secret string, existing *metav1.Condition) metav1.Condition {
	cond := metav1.Condition{
		Type:    blockstoriov1alpha1.ConditionEncryptionKeyIgnored,
		Status:  metav1.ConditionFalse,
		Reason:  "NoPerResourceKey",
		Message: "no per-resource key pinned; using the cluster master key",
	}

	if secret != "" {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "PerResourceKeyUnimplemented"
		cond.Message = perRDKeyFieldPath + " names Secret " + secret +
			", but blockstor does not read this field: this volume's password is " +
			"wrapped under the CLUSTER master key, not a key from the referenced " +
			"Secret. See docs/encryption-keys.md"
	}

	cond.LastTransitionTime = metav1.Now()
	if existing != nil && existing.Status == cond.Status {
		cond.LastTransitionTime = existing.LastTransitionTime
	}

	return cond
}

// conditionAlreadyIs reports whether the existing Condition already
// carries `status` and `message`, so the caller can skip a redundant
// apiserver write. The message is compared too: it names the Secret,
// and a repointed RD must not keep the old name.
func conditionAlreadyIs(existing *metav1.Condition, status metav1.ConditionStatus, message string) bool {
	return existing != nil && existing.Status == status && existing.Message == message
}

// layerStackHasLUKS reports whether the RD's layer stack names LUKS
// (case-insensitive, mirroring pkg/satellite's needsLUKS).
func layerStackHasLUKS(stack []string) bool {
	for _, layer := range stack {
		if strings.EqualFold(layer, "LUKS") {
			return true
		}
	}

	return false
}
