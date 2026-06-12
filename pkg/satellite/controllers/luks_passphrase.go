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
