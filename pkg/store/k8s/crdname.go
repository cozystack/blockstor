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

// SetOriginalName tags meta with the original LINSTOR name when it
// disagrees with meta.Name (i.e. the slugifier rewrote it). No-op when
// they match — keeps the annotation map empty for the common case.
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
// store without complaining. If the input already conforms it passes
// through unchanged; otherwise we lowercase, replace non-allowed runs
// with '-', and append a short SHA-256 prefix so distinct LINSTOR
// names never collide on the same k8s name.
//
// linstor-csi names PVCs like `vol-<uuid>` (already lowercase) but
// csi-sanity uses `DeleteSnapshot-volume-1-…` style — both must work.
func Name(in string) string {
	if in != "" && len(in) <= maxK8sName && rfc1123.MatchString(in) {
		return in
	}

	lower := strings.ToLower(in)

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
