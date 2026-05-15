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
	"errors"
	"fmt"
	"regexp"
)

// Bug 97: every user-supplied LINSTOR identifier (RD name, RG name,
// Node name, StoragePool name, Snapshot name) lands as a Kubernetes
// `metadata.name` once the CRD store writes it. The k8s store's
// `Name()` helper (pkg/store/k8s/crdname.go) slugifies + hashes the
// input to keep distinct LINSTOR-cased names from colliding on the
// same k8s name, but that mangling runs UNCONDITIONALLY — including
// for whitespace-only or empty-after-trim inputs.
//
// The result before this gate: `linstor rd c "  "` got slugged into
// `<8-char-hex>-` (a bare trailing hyphen), the k8s API server
// refused it as not-RFC-1123, and the raw apimachinery error leaked
// to the operator:
//
//	ResourceDefinition.blockstor.io "6c179f21-" is invalid:
//	metadata.name: Invalid value: "6c179f21-": a lowercase RFC 1123
//	subdomain must consist of …
//
// That message exposes (a) the internal hash-prefix scheme, and (b)
// the k8s-layer details the LINSTOR wire surface is supposed to
// hide. We refuse the name at the REST boundary BEFORE Name()
// mangles it, with a LINSTOR-shaped envelope that names the offending
// input and the rule it violated.
//
// The chosen ruleset is RFC 1123 subdomain (lowercase alphanumerics +
// hyphen, can't start/end with hyphen, max 253 chars). Upstream
// LINSTOR's wire-side regex is `^[A-Za-z][A-Za-z0-9_-]{1,47}$` —
// stricter on length (48) but permissive on case + underscore. We
// pick RFC 1123 because the k8s store is authoritative for storage:
// any name that would later fail the metadata.Name regex is rejected
// up front, so the operator sees ONE consistent failure mode rather
// than "linstor said OK, but kubectl says no".

// rfc1123SubdomainName is the wire-level identifier regex applied to
// every user-supplied LINSTOR name (Bug 97). Mirrors
// pkg/store/k8s/crdname.go's `rfc1123` regex (used by Name() to
// short-circuit when the input is already clean). The duplication is
// deliberate — pkg/rest must not import pkg/store/k8s.
//
// Pattern: a lowercase alphanumeric, optionally followed by
// alphanumerics or hyphens, ending with an alphanumeric. The dotted
// form `a.b.c` k8s allows is NOT permitted for LINSTOR names because
// `<rd>.<node>` is the metadata.name convention for Resource CRDs —
// an embedded '.' in either side would shift the split and cause
// silent collisions (the same gate fires for Resource create at
// pkg/rest/autoplace.go).
var rfc1123SubdomainName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// maxLinstorName caps the wire-side identifier length. RFC 1123
// subdomain allows up to 253 chars; we mirror the same limit here so
// the REST gate doesn't accept anything the k8s store would later
// refuse. (`<rd>.<node>` Resource CRD names would still fit comfortably
// since each side is independently bounded.)
const maxLinstorName = 253

// ErrInvalidLinstorName is the static sentinel for Bug-97 rejections.
// Callers wrap it via fmt.Errorf("%w: …") with object-kind + name +
// the violated rule, so the LINSTOR envelope's `message` field carries
// the exact identifier the operator passed and the exact rule it
// broke. Sentinel-shaped to match validateLayerStack's ErrUnsupportedLayer.
var ErrInvalidLinstorName = errors.New("invalid name")

// ErrLinstorNameRequired is the empty-input sibling of
// ErrInvalidLinstorName. Distinct sentinel so callers can pattern-match
// on "missing" vs "malformed" without parsing the message string;
// keeps the empty-name case lint-clean (err113 forbids dynamic-only
// errors). Callers wrap via `fmt.Errorf("%w: …", …)` with the object
// kind so the operator-facing message still reads naturally.
var ErrLinstorNameRequired = errors.New("name is required")

// validateLinstorName enforces the RFC 1123 subdomain rules at the
// REST wire boundary. Empty/whitespace-only input is rejected with a
// distinct message because the underlying regex accepts neither and
// the empty case is the one the python CLI's `rd c "  "` invocation
// produces.
//
// The `kind` argument names the object being created ("resource
// definition", "node", "resource group", …) so the envelope's
// `message` reads naturally: `resource definition name "  " is not a
// valid identifier`. The literal value is double-quoted in the
// returned error so whitespace-only inputs are visible — without the
// quotes a bare two-space name renders as a blank gap in operator logs.
func validateLinstorName(kind, name string) error {
	if name == "" {
		return fmt.Errorf("%w: %s", ErrLinstorNameRequired, kind)
	}

	if len(name) > maxLinstorName {
		return fmt.Errorf("%w: %s name %q exceeds %d characters",
			ErrInvalidLinstorName, kind, name, maxLinstorName)
	}

	if !rfc1123SubdomainName.MatchString(name) {
		return fmt.Errorf("%w: %s name %q is not a valid identifier "+
			"(lowercase alphanumerics and '-', must start and end with "+
			"an alphanumeric, no spaces, no uppercase)",
			ErrInvalidLinstorName, kind, name)
	}

	return nil
}
