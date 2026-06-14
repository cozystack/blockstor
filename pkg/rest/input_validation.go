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
	"errors"
	"fmt"
	"regexp"
)

// Bug 97: every user-supplied LINSTOR identifier (RD name, RG name,
// Node name, StoragePool name, Snapshot name) lands as a Kubernetes
// `metadata.name` once the CRD store writes it. The k8s store's
// `Name()` helper (pkg/store/k8s/crdname.go) lowercases the input,
// short-circuits when the lowercased form is already rfc1123-clean,
// and otherwise slugifies + hashes it so distinct LINSTOR-cased names
// never collide on the same k8s name. That mangling used to run
// UNCONDITIONALLY — including for whitespace-only or empty-after-trim
// inputs.
//
// The result before this gate: `linstor rd c "  "` got slugged into
// `<8-char-hex>-` (a bare trailing hyphen), the k8s API server
// refused it as not-RFC-1123, and the raw apimachinery error leaked
// to the operator:
//
//	ResourceDefinition.blockstor.cozystack.io "6c179f21-" is invalid:
//	metadata.name: Invalid value: "6c179f21-": a lowercase RFC 1123
//	subdomain must consist of …
//
// That message exposes (a) the internal hash-prefix scheme, and (b)
// the k8s-layer details the LINSTOR wire surface is supposed to
// hide. We refuse the name at the REST boundary BEFORE Name() mangles
// it, with a LINSTOR-shaped envelope that names the offending input
// and the rule it violated.
//
// BUG-047 (csi-sanity conformance): the original gate enforced an
// RFC 1123 subdomain (lowercase-only, no underscore) — STRICTER than
// upstream LINSTOR. csi-sanity's CreateVolume names carry uppercase
// hex (e.g. `pvc-2A1B4B95EA8C4D7E`); the upstream linstor-csi sidecar
// forwards the CO name verbatim as the RD name, so the lowercase-only
// gate rejected 19 conformance specs with code=Internal.
//
// Oracle evidence (upstream LINSTOR controller, dev stand 2026-06-14)
// established the ACTUAL upstream ruleset for resource-definition
// names:
//
//	TestUpperCase123      → SUCCESS (uppercase accepted, case preserved)
//	pvc-2A1B4B95EA8C4D7E  → SUCCESS (csi-sanity-shaped name accepted)
//	Has_Underscore_1      → SUCCESS (underscore accepted)
//	foo-                  → SUCCESS (trailing hyphen accepted)
//	CaseTest / casetest   → second create "already exists" (case-INSENSITIVE)
//	1foo                  → ERROR "Cannot begin with character '1'"
//	a                     → ERROR "Name length 1 is less than minimum length 2"
//	48 chars              → SUCCESS; 49 chars → ERROR "max length 48"
//	foo.bar / "Foo Bar" / foo/bar → ERROR "Cannot contain character …"
//
// i.e. upstream's wire regex is `^[A-Za-z][A-Za-z0-9_-]{1,47}$`: a
// leading ASCII letter, then 1–47 of {letter, digit, '_', '-'}, total
// length 2–48. We now MIRROR that ruleset exactly so blockstor is
// neither stricter (the csi-sanity gap) nor laxer than upstream. The
// k8s store's case-insensitive Name() folds `Foo`/`foo` onto the same
// CRD, matching upstream's case-insensitive identity, so the relaxed
// gate stays collision-safe end-to-end.
//
// We still reject the dotted form `a.b.c` (upstream rejects '.' too)
// because `<rd>.<node>` is the metadata.name convention for Resource
// CRDs — an embedded '.' would shift the split and cause silent
// collisions (the same gate fires for Resource create at
// pkg/rest/autoplace.go).

// linstorName is the wire-level identifier regex applied to every
// user-supplied LINSTOR name (Bug 97, relaxed for BUG-047). Mirrors
// upstream LINSTOR's `^[A-Za-z][A-Za-z0-9_-]{1,47}$`: a leading ASCII
// letter, followed by 1–47 of {letter, digit, '_', '-'}. The length
// bounds it encodes (min 2 via the `{1,47}` tail, max 48) are belt-
// and-suspenders alongside the explicit maxLinstorName check, which
// emits a clearer "exceeds N characters" envelope for the over-cap
// case (Bug 360). Pattern duplication vs pkg/store/k8s is deliberate —
// pkg/rest must not import pkg/store/k8s.
var linstorName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{1,47}$`)

// maxLinstorName caps the wire-side identifier length. Upstream
// LINSTOR's wire-level regex tops out at 48 characters
// (DRBD_RES_NAME_MAX), but the k8s store has a second hard ceiling that
// matters more in practice: every user-supplied name flows into a
// Kubernetes `metadata.labels` value on the Resource CRD
// (`LabelResourceDefinition`, `LabelNodeName` in pkg/store/k8s/
// resources.go). k8s label VALUES are bounded to 63 characters by the
// apimachinery validator — anything longer slips past `rd c` (which
// only writes a metadata.name, the 253-char regime) and explodes on
// the next `r c` with `metadata.labels: Invalid value: …: must be no
// more than 63 characters`. The RD is now a zombie: it lives in the
// catalogue and accepts `vd c`, but the first replica create fails
// inside the store layer, leaving a partial state the operator can
// only clean up manually.
//
// Bug 360 (hunt-v4): cap the wire-side limit at 48 so the rd-create
// rejection happens up front, BEFORE the partial-create lands and
// BEFORE the k8s label cap can ever fire. 48 also matches upstream
// LINSTOR's documented identifier length for forward-compat with any
// caller that relies on the upstream limit (BUG-047 oracle confirmed:
// 48 chars → SUCCESS, 49 → "Name length 49 is greater than maximum
// length 48").
const maxLinstorName = 48

// minLinstorName is upstream LINSTOR's lower length bound: a single
// character is refused ("Name length 1 is less than minimum length
// 2", BUG-047 oracle). The regex already encodes this via its `{1,47}`
// tail, but we check it explicitly so the operator gets the same
// "too short" envelope upstream emits rather than the generic
// "not a valid identifier" message.
const minLinstorName = 2

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

// validateLinstorName enforces upstream LINSTOR's identifier rules at
// the REST wire boundary (BUG-047): a leading ASCII letter, then
// {letter, digit, '_', '-'}, length 2–48. Empty/whitespace-only input
// is rejected with a distinct message because the underlying regex
// accepts neither and the empty case is the one the python CLI's
// `rd c "  "` invocation produces.
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

	if len(name) < minLinstorName {
		return fmt.Errorf("%w: %s name %q is shorter than %d characters",
			ErrInvalidLinstorName, kind, name, minLinstorName)
	}

	if !linstorName.MatchString(name) {
		return fmt.Errorf("%w: %s name %q is not a valid identifier "+
			"(letters, digits, '_' and '-'; must start with a letter; "+
			"no '.', no spaces, no path separators)",
			ErrInvalidLinstorName, kind, name)
	}

	return nil
}
