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

package rest

import (
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// internalAnnotationPrefix is the canonical blockstor controller
// namespace for annotations the reconcilers stamp onto Resource /
// ResourceDefinition / ResourceGroup / Snapshot CRDs for their own
// bookkeeping (e.g. `blockstor.io/volume-numbers`,
// `blockstor.io/peer-changed`). The reconciler MUST keep reading
// these from the in-process struct, so the struct field stays — only
// the wire boundary strips them.
const internalAnnotationPrefix = "blockstor.io/"

// internalAnnotationSuffix matches the per-bug controller scratch
// namespace pattern `bug<NNN>.blockstor.cozystack.io/<key>`. The
// reconciler uses this scheme to scope bug-specific state to a single
// Reconcile loop without polluting the top-level
// `blockstor.io/` namespace. Bug-hunt v0.1.3 Finding 2: this pattern
// (e.g. `bug136.blockstor.cozystack.io/resize-pending-size-kib-vol-0`)
// was leaking onto the wire alongside the canonical prefix; the
// suffix check covers every existing and future `bug<N>` namespace
// without enumerating each prefix individually.
const internalAnnotationSuffix = ".blockstor.cozystack.io/"

// isInternalAnnotationKey returns true when `key` belongs to a
// blockstor-controller-private annotation namespace and must NOT
// leak onto the REST wire. The two patterns covered (see the
// per-const docs above) are:
//
//   - prefix `blockstor.io/`                — canonical controller namespace
//   - any prefix ending in `.blockstor.cozystack.io/` — per-bug scratch
//
// Anything else (e.g. operator-set `kubectl.kubernetes.io/last-applied-
// configuration`, third-party tooling annotations) is passed through
// unchanged — those keys are part of the K8s object's contract with
// the cluster operator, not blockstor's internal state.
func isInternalAnnotationKey(key string) bool {
	if key == "" {
		return false
	}

	if strings.HasPrefix(key, internalAnnotationPrefix) {
		return true
	}

	if i := strings.Index(key, internalAnnotationSuffix); i > 0 {
		// Require the suffix to anchor against a `/`-bounded
		// namespace (`<ns>/<name>` shape per K8s annotation rules).
		// `i > 0` ensures there's a non-empty `<ns>` segment in
		// front of the suffix.
		return true
	}

	return false
}

// stripInternalAnnotations returns a defensive copy of `annotations`
// with every blockstor-internal key dropped. Returns nil when the
// input is nil and an empty (but non-nil) map when every key was
// internal — that's the right shape for `json:"annotations,omitempty"`
// (an empty map is omitted by the encoder, so the wire field
// disappears entirely on a fully-internal annotation bag).
//
// Bug-hunt v0.1.3 Finding 2: every Resource / ResourceDefinition /
// ResourceGroup / Snapshot type carries `Annotations map[string]string`
// for the reconciler's bookkeeping. The reconciler MUST keep reading
// these from the in-process struct, so the field can't be removed.
// The fix is purely at the REST wire boundary: every list / get
// handler clones the visible-on-wire copy through this helper so
// the controller's internal state never escapes to clients (golinstor,
// linstor-csi, piraeus-operator, the python CLI).
//
// The returned map is always a fresh allocation — callers can mutate
// it without disturbing the source struct's annotations.
func stripInternalAnnotations(annotations map[string]string) map[string]string {
	if annotations == nil {
		return nil
	}

	out := make(map[string]string, len(annotations))
	for k, v := range annotations {
		if isInternalAnnotationKey(k) {
			continue
		}

		out[k] = v
	}

	return out
}

// stripInternalAnnotationsFromResourcesWithVolumes is the slice sweep
// used by the per-RD `r l` list and the cluster-wide `view/resources`
// aggregate. Mutates each entry's embedded `Resource.Annotations`
// field in place — the slice is owned by the handler at this point
// (Resources().List returns a value copy), so the strip is local to
// the response.
func stripInternalAnnotationsFromResourcesWithVolumes(resources []apiv1.ResourceWithVolumes) {
	for i := range resources {
		resources[i].Annotations = stripInternalAnnotations(resources[i].Annotations)
	}
}

// stripInternalAnnotationsFromRDs scrubs the per-RD wire shape used
// by `rd l` (list) and `rd s <name>` (get-single, wrapped in a
// 1-element slice by the caller for re-use).
func stripInternalAnnotationsFromRDs(rds []apiv1.ResourceDefinition) {
	for i := range rds {
		rds[i].Annotations = stripInternalAnnotations(rds[i].Annotations)
	}
}

// stripInternalAnnotationsFromRGs is the RG-side sweep — covers
// `rg l` (list) and `rg s <name>` (get) the same way as
// stripInternalAnnotationsFromRDs covers the RD side.
func stripInternalAnnotationsFromRGs(rgs []apiv1.ResourceGroup) {
	for i := range rgs {
		rgs[i].Annotations = stripInternalAnnotations(rgs[i].Annotations)
	}
}

// stripInternalAnnotationsFromSnapshots is the Snapshot-side sweep —
// covers `s l` (cluster-wide view), `s l --resource <rd>` (per-RD
// list), and `s l <rd> <snap>` (get-single, wrapped in a 1-element
// slice by the caller). Mirrors the redactSnapshotsInPlace shape
// already used for sensitive-key scrubbing on the same handlers.
func stripInternalAnnotationsFromSnapshots(snaps []apiv1.Snapshot) {
	for i := range snaps {
		snaps[i].Annotations = stripInternalAnnotations(snaps[i].Annotations)
	}
}
