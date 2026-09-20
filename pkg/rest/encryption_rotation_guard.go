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
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Rotation guard for the cluster master passphrase.
//
// blockstor uses the cluster passphrase VERBATIM as the cryptsetup
// passphrase for every LUKS-layered volume: the Secret value travels
// injectLUKSMasterPassphrase → effective props → dispatcher
// `LuksPassphrase` → `cryptsetup luksFormat/luksOpen --key-file -`
// (pkg/satellite/reconciler.go). There is no key-encryption-key and
// no per-volume data key, so the stored value IS the key that opens
// the LUKS headers on disk.
//
// Upstream LINSTOR's `encryption modify-passphrase` is safe because
// there the master passphrase only WRAPS per-volume keys held in the
// controller DB — a rotation re-wraps them and never touches a LUKS
// header. blockstor inherited the verb without that model.
//
// Consequence, pre-guard: PUT /v1/encryption/passphrase verified the
// old value, wrote the new one, and returned `200 Master passphrase
// modified` while every existing LUKS header kept the OLD key. The
// damage is deferred and quiet:
//
//   - Nothing is destroyed. Cryptsetup.Format probes `isLuks` first
//     and no-ops on an already-formatted device (pkg/luks/luks.go),
//     so no header rewrite and no key-slot wipe.
//   - Mappers already open STAY open — they are kernel dm-crypt
//     mappings and outlive a satellite pod restart. The cluster
//     therefore looks healthy until a node reboots, a replica is
//     attached elsewhere, or a resize fires (Resize takes the key
//     too). Only then does luksOpen fail with the new key and the
//     apply bubble `luks open ...`.
//
// So the operator gets a success line, and the cluster strands its
// encrypted volumes at an unpredictable later moment. docs/layer-
// stack.md already documented rotation as unsupported ("operators
// should drop the RD and recreate"); the API did not enforce it.
//
// This guard makes the refusal explicit: while any ResourceDefinition
// carries a LUKS layer, a rotation that would change the value is
// rejected with 409 unless the caller opts in with `force`.
//
// DELIBERATE DIVERGENCE from upstream LINSTOR — recorded in
// docs/cli-parity-known-deltas.md. It lapses once per-volume data
// keys land (docs/byok-design.md §4), because rotation then re-wraps
// keys instead of replacing them.

// rotationGuardMaxNamed bounds how many resource-definition names the
// refusal envelope spells out. A cluster can hold thousands of
// encrypted RDs and the message is rendered by the python CLI into a
// terminal; naming a handful plus a total is the actionable shape.
const rotationGuardMaxNamed = 5

// luksRDNames returns the sorted names of every ResourceDefinition a
// rotation of the cluster passphrase would actually strand.
//
// "Actually" is doing work here. Review found the first version's set
// both too wide and too narrow:
//
//   - Too narrow: it read `rd.LayerStack` only. An RD created through
//     the Kubernetes door can leave that empty and inherit LUKS from
//     its ResourceGroup's SelectFilter (the REST door stamps the
//     inherited stack onto the RD at create time, kubectl does not).
//     Those RDs are encrypted and were being missed. We now resolve
//     the stack the way the rest of the codebase does, through
//     apiv1.ResolveLayerStack.
//
//   - Too wide: an RD that pins its OWN passphrase via
//     `DrbdOptions/Encryption/passphrase` does not use the cluster
//     value, so rotating the cluster passphrase cannot strand it.
//     Blocking on those RDs refuses a rotation that is in fact
//     harmless. Excluding them is safe under either precedence
//     branch: if the legacy cluster property is also set it wins over
//     both the per-RD prop and the Secret, so rotating the SECRET
//     still changes nothing for that RD.
//
// Sorted so the refusal message is deterministic; the store's List
// order is not guaranteed and a flapping message body would make the
// envelope untestable.
//
// Reads through the Store's normal (cached) path on purpose. The
// sibling Secret access deliberately uses the uncached reader
// (pkg/store/k8s/k8s.go) because a stale MISS there provisions with
// no key at all. Here a stale miss can only mean an RD created
// moments ago is absent from the list — and an RD that young has no
// LUKS header yet, so there is nothing for a rotation to strand.
func (s *Server) luksRDNames(ctx context.Context) ([]string, error) {
	rds, err := s.Store.ResourceDefinitions().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list resource definitions")
	}

	rgLayers, err := s.resourceGroupLayerStacks(ctx)
	if err != nil {
		return nil, err
	}

	var names []string

	for i := range rds {
		rd := &rds[i]

		stack := apiv1.ResolveLayerStack(rd.LayerStack, rgLayers[rd.ResourceGroupName])
		if !layerStackCarriesLUKS(stack) {
			continue
		}

		if rd.Props[drbdPerRDPassphraseKey] != "" {
			continue
		}

		names = append(names, rd.Name)
	}

	sort.Strings(names)

	return names, nil
}

// drbdPerRDPassphraseKey is the per-RD LUKS passphrase property — the
// upstream `linstor rd set-property` spelling the dispatcher accepts
// as the second-precedence source (pkg/dispatcher.pickLUKSPassphrase).
//
//nolint:gosec // property NAME, not a secret value
const drbdPerRDPassphraseKey = "DrbdOptions/Encryption/passphrase"

// resourceGroupLayerStacks maps resource-group name → its
// SelectFilter layer stack, so an RD that inherits LUKS rather than
// declaring it is still seen.
func (s *Server) resourceGroupLayerStacks(ctx context.Context) (map[string][]string, error) {
	rgs, err := s.Store.ResourceGroups().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list resource groups")
	}

	out := make(map[string][]string, len(rgs))
	for i := range rgs {
		out[rgs[i].Name] = rgs[i].SelectFilter.LayerStack
	}

	return out, nil
}

// layerStackCarriesLUKS reports whether the wire-shape layer stack
// names LUKS. Case-insensitive, mirroring pkg/validate's
// NormalizeLayerStack and the satellite's needsLUKS — an RD created
// through `--layer-list luks,storage` stores the lower-cased spelling
// on some paths and the guard must not miss it.
func layerStackCarriesLUKS(stack []string) bool {
	for _, layer := range stack {
		if strings.EqualFold(layer, apiv1.LayerKindLUKS) {
			return true
		}
	}

	return false
}

// rotationForced reports whether the caller opted out of the guard,
// accepting that the encrypted volumes will stop opening.
//
// Two spellings, mirroring the `vd s` shrink escape hatch (known-
// deltas row 60) so operators meet one idiom across both destructive
// verbs: a `force` body field, or `?force=true` on the query string.
// The query string wins only when the body did not already say true;
// either saying yes is enough.
func rotationForced(r *http.Request, body passphraseModifyRequest) bool {
	if body.Force {
		return true
	}

	forced, err := strconv.ParseBool(r.URL.Query().Get("force"))
	if err != nil {
		return false
	}

	return forced
}

// rotationGuardMessage renders the operator-facing refusal. Names up
// to rotationGuardMaxNamed RDs and always states the total, so the
// operator can tell a two-volume dev cluster from a production one
// without paging through the list.
func rotationGuardMessage(names []string) string {
	var b strings.Builder

	b.WriteString("cluster passphrase rotation refused: ")
	b.WriteString(strconv.Itoa(len(names)))
	b.WriteString(" resource definition(s) carry a LUKS layer and were formatted with the current passphrase (")

	named := names
	if len(named) > rotationGuardMaxNamed {
		named = named[:rotationGuardMaxNamed]
	}

	b.WriteString(strings.Join(named, ", "))

	if len(names) > len(named) {
		b.WriteString(", …")
	}

	b.WriteString("). blockstor uses the cluster passphrase directly as the LUKS key, " +
		"so rotating it leaves every existing header locked with the old value and the " +
		"volumes stop opening on the next attach, reboot or resize. " +
		"Re-key or recreate those resource definitions first. " +
		"To rotate anyway and strand them, repeat with force=true — note that the " +
		"`linstor` CLI has no flag for it, so this needs a direct call: " +
		"curl -X PUT '<controller>/v1/encryption/passphrase?force=true' " +
		"-d '{\"old_passphrase\":\"…\",\"new_passphrase\":\"…\"}'")

	return b.String()
}

// guardPassphraseRotation returns false when the request must not
// proceed, having already written the refusal envelope.
//
// Fail-closed on a store error: a rotation is a rare, high-blast-
// radius operator action and "I could not determine whether encrypted
// volumes exist" is not a licence to proceed. The caller sees 500 and
// retries, which is the cheap outcome; the alternative is a silent
// strand of every encrypted volume in the cluster.
func (s *Server) guardPassphraseRotation(ctx context.Context, w http.ResponseWriter, r *http.Request, body passphraseModifyRequest) bool {
	if rotationForced(r, body) {
		return true
	}

	names, err := s.luksRDNames(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError,
			"cannot verify whether encrypted volumes exist before rotating the cluster passphrase: "+err.Error())

		return false
	}

	if len(names) == 0 {
		return true
	}

	writeError(w, http.StatusConflict, rotationGuardMessage(names))

	return false
}
