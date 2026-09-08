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

// injectLUKSMasterPassphrase folds the cluster master passphrase
// from the encryption Secret into the resolved effective-props bag
// when a LUKS-layered RD would otherwise dispatch without one.
//
// Bug 023: the upstream-standard flow — `linstor encryption
// create-passphrase` then `rd create --layer-list drbd,luks,storage`
// — stores the passphrase in a Secret (pkg/rest/encryption.go), but
// the dispatch chain only lifted it from the controller-scope props
// (`DrbdOptions/EncryptPassphrase`, surfaced via ControllerConfig.
// ExtraProps → effectiveprops.Resolve → dispatcher.BuildDesired →
// the `LuksPassphrase` wire prop the satellite's LUKS layer reads).
// A Secret-only cluster therefore passed the REST-side RD-create
// gate but every replica apply looped on `LUKS in layer stack but
// Props.LuksPassphrase empty`.
//
// The injection deliberately reuses the EXACT downstream channel the
// legacy prop travels: the value lands under the canonical
// `DrbdOptions/EncryptPassphrase` key in effectiveProps, so
// BuildDesired's pickLUKSPassphrase lifts it onto `LuksPassphrase`
// and strips it from the rendered .res options exactly as before.
// Precedence is unchanged — an operator-set controller prop (either
// spelling) wins over the Secret, so existing LUKS volumes keep
// unlocking with the key they were formatted with.
//
// Failure posture: a Secret read error is logged and swallowed —
// the apply chain then surfaces the established, actionable
// `Props.LuksPassphrase empty` condition on the Resource instead of
// wedging the whole reconcile on a transient apiserver blip.
//
// No-ops when:
//   - the RD has no LUKS layer (the overwhelmingly common path —
//     zero Secret round-trips);
//   - either passphrase prop key is already present (legacy path);
//   - Config.Namespace is empty (unit-test rigs without a
//     controller namespace).
func (r *ResourceReconciler) injectLUKSMasterPassphrase(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition, effectiveProps map[string]string) {
	if rd == nil || effectiveProps == nil || !layerStackHasLUKS(rd.Spec.LayerStack) {
		return
	}

	if effectiveProps[passphrase.PropKeyCanonical] != "" ||
		effectiveProps[passphrase.PropKeyLegacy] != "" {
		return
	}

	if r.Config.Namespace == "" {
		return
	}

	pass, err := passphrase.Read(ctx, r.secretReader(), r.Config.Namespace)
	if err != nil {
		log.FromContext(ctx).Error(err,
			"read cluster master passphrase Secret; LUKS apply will surface the missing key",
			"namespace", r.Config.Namespace)

		return
	}

	if pass == "" {
		return
	}

	effectiveProps[passphrase.PropKeyCanonical] = pass
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
// write — a field defended against loss and never consumed. An RD that
// sets it is silently encrypted with the CLUSTER passphrase.
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
// today — the field has always been inert, so an existing cluster may
// carry RDs that set it and run fine on the cluster key — and that the
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

	secret := perRDKeySecretName(rd)
	want := metav1.ConditionFalse
	reason := "NoPerResourceKey"
	msg := "no per-resource key pinned; using the cluster passphrase"

	if secret != "" {
		want = metav1.ConditionTrue
		reason = "PerResourceKeyUnimplemented"
		msg = perRDKeyFieldPath + " names Secret " + secret +
			", but blockstor derives every LUKS key from the single cluster passphrase " +
			"and does not read this field. This volume is encrypted with the CLUSTER " +
			"passphrase, not the referenced Secret. See docs/byok-design.md"
	}

	if conditionAlreadyIs(target.Status.Conditions, blockstoriov1alpha1.ConditionEncryptionKeyIgnored, want) {
		return
	}

	// Never stamp False on an RD that never had the Condition: a
	// cluster full of ordinary volumes would otherwise grow a
	// meaningless "no per-resource key" entry on every Resource.
	if want == metav1.ConditionFalse &&
		meta.FindStatusCondition(target.Status.Conditions, blockstoriov1alpha1.ConditionEncryptionKeyIgnored) == nil {
		return
	}

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Resource",
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: target.Name},
		Status: blockstoriov1alpha1.ResourceStatus{
			Conditions: []metav1.Condition{{
				Type:               blockstoriov1alpha1.ConditionEncryptionKeyIgnored,
				Status:             want,
				Reason:             reason,
				Message:            msg,
				LastTransitionTime: metav1.Now(),
			}},
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

// conditionAlreadyIs reports whether conditions already carry `typ` at
// `status`, so the caller can skip a redundant apiserver write.
func conditionAlreadyIs(conditions []metav1.Condition, typ string, status metav1.ConditionStatus) bool {
	existing := meta.FindStatusCondition(conditions, typ)

	return existing != nil && existing.Status == status
}

// secretReader picks the uncached APIReader when wired. The cached
// client would spin up a cluster-wide Secret informer on first Get —
// the exact Bug 110 stall class when RBAC scopes the satellite to
// named Secrets only. The direct reader does one GET against the
// apiserver instead.
func (r *ResourceReconciler) secretReader() client.Reader {
	if r.Config.APIReader != nil {
		return r.Config.APIReader
	}

	return r.Client
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
