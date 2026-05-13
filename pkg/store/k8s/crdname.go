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

package k8s

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AnnotationLinstorName preserves the original LINSTOR-side name when
// it had to be slugified to satisfy DNS-1123 metadata.Name rules.
// Pass-through names are not annotated.
const AnnotationLinstorName = "blockstor.io/linstor-name"

// SetOriginalName tags meta with the original LINSTOR name when the
// slugifier rewrote it in any way — including case-only differences
// like `DfltRscGrp` → `dfltrscgrp`.
//
// The case-only round-trip matters because LINSTOR identifiers ARE
// case-insensitive on the wire, but a handful of well-known names
// have a canonical CamelCase spelling (notably `DfltRscGrp`) that
// callers grep for verbatim — linstor-csi's `defaultResourceGroup`
// constant, the upstream `linstor rg l` table header sort order, and
// operator runbooks that copy-paste the name.
// Returning the lowercased `dfltrscgrp` looks "fine" to a Java client
// but breaks string-equality comparisons in downstream code.
//
// So: bytewise-different originals are always annotated. meta.Name
// remains the lowercased k8s-addressable slug (so `kubectl get
// resourcegroup dfltrscgrp` keeps working); OriginalName() returns
// the canonical CamelCase spelling for wire output.
func SetOriginalName(meta *metav1.ObjectMeta, original string) {
	if meta.Name == original {
		return
	}

	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}

	meta.Annotations[AnnotationLinstorName] = original
}

// OriginalName returns the LINSTOR name the user gave us. If the
// annotation is set we trust it; otherwise meta.Name was already
// rfc1123-clean and is the original.
func OriginalName(meta *metav1.ObjectMeta) string {
	if v, ok := meta.Annotations[AnnotationLinstorName]; ok {
		return v
	}

	return meta.Name
}

// Name turns a LINSTOR-shaped name (which Java accepts in mixed case
// and with characters k8s rejects) into something the API server will
// store without complaining. LINSTOR identifiers are case-insensitive
// (`DfltRscGrp` and `dfltrscgrp` address the same object), so we
// lowercase first — if the lowercased form is rfc1123-valid, return
// it unchanged. Otherwise we replace non-allowed runs with '-' and
// append a short SHA-256 prefix so distinct LINSTOR names never
// collide on the same k8s name.
//
// linstor-csi names PVCs like `vol-<uuid>` (already lowercase) but
// upstream LINSTOR's `DfltRscGrp` and csi-sanity's `DeleteSnapshot-
// volume-1-…` need normalisation. Lowercasing in this function is
// the single normalization point: every store Get/Update/Delete
// routes through Name(), so the same input lands on the same CRD
// regardless of case. The canonical CamelCase spelling (when the
// caller supplied one) round-trips via the `blockstor.io/linstor-name`
// annotation — see SetOriginalName / OriginalName.
func Name(in string) string {
	lower := strings.ToLower(in)

	if lower != "" && len(lower) <= maxK8sName && rfc1123.MatchString(lower) {
		return lower
	}

	var builder strings.Builder

	prevDash := true

	for _, r := range lower {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			builder.WriteRune(r)

			prevDash = false
		default:
			if !prevDash {
				builder.WriteByte('-')

				prevDash = true
			}
		}
	}

	slug := strings.Trim(builder.String(), "-")

	digest := sha256.Sum256([]byte(in))
	prefix := hex.EncodeToString(digest[:hashPrefixBytes])
	out := prefix + "-" + slug

	if len(out) > maxK8sName {
		out = out[:maxK8sName]
	}

	return out
}

// rfc1123 covers k8s metadata.Name validation; declared once at
// package init so the regex compile cost amortises.
var rfc1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

const (
	// maxK8sName is the metadata.Name length cap (DNS-1123).
	maxK8sName = 253

	// hashPrefixBytes drives K8sName's collision-resistant prefix
	// length. 4 bytes → 8 hex chars → 2^32 distinct slugs, plenty
	// for storage objects.
	hashPrefixBytes = 4
)
