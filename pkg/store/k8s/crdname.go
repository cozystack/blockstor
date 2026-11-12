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

package k8s

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cozystack/blockstor/pkg/naming"
)

// AnnotationLinstorName preserves the original LINSTOR-side name when
// it had to be slugified to satisfy DNS-1123 metadata.Name rules.
// Pass-through names are not annotated.
const AnnotationLinstorName = "blockstor.org/linstor-name"

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

// Name is the CR name the store keeps an object under: naming.Key of its
// LINSTOR name. The registered spelling round-trips through the
// AnnotationLinstorName annotation; see SetOriginalName and OriginalName.
func Name(in string) string {
	return naming.Key(in)
}
