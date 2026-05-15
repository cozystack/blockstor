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
	"encoding/json"
	"maps"
	"net/http"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerResourceDefinitions wires /v1/resource-definitions CRUD. Spawn,
// Clone, snapshot-restore, and per-volume endpoints land in later slices.
func (s *Server) registerResourceDefinitions(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/resource-definitions", s.requireStore(s.handleRDList))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}", s.requireStore(s.handleRDGet))
	mux.HandleFunc("POST /v1/resource-definitions", s.requireStore(s.handleRDCreate))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}", s.requireStore(s.handleRDUpdate))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}", s.requireStore(s.handleRDDelete))
}

func (s *Server) handleRDList(w http.ResponseWriter, r *http.Request) {
	rds, err := s.Store.ResourceDefinitions().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Optional filter the upstream LINSTOR CLI sends on
	// `linstor rd l --resource-definitions <name>...`: the Python
	// CLI urlencodes the names as `?resource_definitions=a&resource_definitions=b`,
	// golinstor as the comma-joined `?resource_definitions=a,b` form.
	// Without honouring this, `rd l --resource-definitions X` returns
	// the full RD list and the CLI renders ALL RDs — Bug 61.
	// Semantics mirror upstream: case-insensitive name match,
	// unknown names => empty list (NOT 404); missing param => no filter.
	nameFilter := multiValueQuery(r, "resource_definitions")
	if len(nameFilter) > 0 {
		filtered := rds[:0]

		for i := range rds {
			if matchAnyFold(nameFilter, rds[i].Name) {
				filtered = append(filtered, rds[i])
			}
		}

		rds = filtered
	}

	// Defensive non-nil: linstor-csi's RD-list decoder treats a `null`
	// body as malformed. Both store backends `make()` their result,
	// but pin the invariant at the wire edge.
	if rds == nil {
		rds = []apiv1.ResourceDefinition{}
	}

	// `?with_volume_definitions=true` is the upstream LINSTOR query
	// the Python CLI sends on `linstor vd l` — it expects RDs with
	// their VDs inlined under `volume_definitions`. Without this
	// handling, `vd l` renders an empty table even when VDs exist
	// (the Python CLI never falls back to per-RD GETs).
	if r.URL.Query().Get("with_volume_definitions") == "true" {
		for i := range rds {
			vds, vdErr := s.Store.VolumeDefinitions().List(r.Context(), rds[i].Name)
			if vdErr != nil {
				writeError(w, http.StatusInternalServerError, vdErr.Error())

				return
			}

			rds[i].VolumeDefinitions = vds
		}
	}

	// Inherited-prop visibility on the read side: `linstor rd lp` calls
	// resource_dfn_list with a name filter and renders the bare `props`
	// map. Without an inheritance merge the operator's `c sp <key> <v>`
	// is invisible at the RD scope even though it is effective at the
	// DRBD layer. Stamp the merged scope-tagged map AND inline the
	// inherited keys into `props` (locally-set RD keys still win).
	for i := range rds {
		mergeErr := s.stampRDEffectiveProps(r.Context(), &rds[i])
		if mergeErr != nil {
			writeStoreError(w, mergeErr)

			return
		}
	}

	writeJSON(w, http.StatusOK, rds)
}

func (s *Server) handleRDGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	// CreateVolume hot path: a `GET /resource-definitions/{rd}` after a
	// fresh spawn / RD-create may land on a sibling apiserver replica
	// whose informer cache trails the write. Retry on NotFound to
	// absorb the cache lag — see pkg/rest/cache_retry.go.
	rd, err := getRDWithCacheRetry(r.Context(), s.Store, name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Inherited-prop visibility (Bug-105 follow-up): merge the
	// Controller→RG→RD chain so `linstor rd lp <rd>` renders inherited
	// entries. See stampRDEffectiveProps for the precedence rules.
	err = s.stampRDEffectiveProps(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, rd)
}

// stampRDEffectiveProps populates rd.EffectiveProps with the merged
// scope-tagged Controller→RG→RD view, and inlines inherited keys into
// rd.Props (without overwriting locally-set RD keys) so the python CLI's
// `rd lp` table — which reads the bare `props` map — sees inherited
// entries. Mirrors the parent-fetch soft-fail semantics of
// effectivePropsForRD: a missing parent RG yields a partial map rather
// than a 5xx, since the read-side handler MUST stay forgiving on a
// half-migrated cluster.
//
// Idempotent: calling on an RD whose Props already contains every
// merged key (e.g. a re-render on the same response) is a no-op.
func (s *Server) stampRDEffectiveProps(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	eff, err := effectivePropsForRD(ctx, s.Client, s.Store, rd)
	if err != nil {
		return err
	}

	if len(eff) == 0 {
		return nil
	}

	rd.EffectiveProps = eff

	if rd.Props == nil {
		rd.Props = map[string]string{}
	}

	for key, entry := range eff {
		// RD-scope keys are already in rd.Props verbatim; only inline
		// parent-scope keys the RD didn't override. The scope check
		// uses the entry's recorded origin so an RD that locally
		// shadows an inherited key keeps its own value untouched.
		if entry.Scope == apiv1.EffectivePropScopeResourceDefinition {
			continue
		}

		if _, alreadySet := rd.Props[key]; alreadySet {
			continue
		}

		rd.Props[key] = entry.Value
	}

	return nil
}

func (s *Server) handleRDCreate(w http.ResponseWriter, r *http.Request) {
	var body apiv1.ResourceDefinitionCreate

	dec := json.NewDecoder(r.Body)
	// upstream LINSTOR has tolerated extra fields here historically; mirror
	// that to keep golinstor (and any home-grown clients) happy.
	err := dec.Decode(&body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	rd := body.ResourceDefinition
	if body.ExternalName != "" && rd.ExternalName == "" {
		rd.ExternalName = body.ExternalName
	}

	if rd.Name == "" {
		writeError(w, http.StatusBadRequest, "resource definition name is required")

		return
	}

	err = validateLayerStack(rd.LayerStack)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// Bug 95: refuse to create a LUKS-stacked RD when no encryption
	// passphrase has been set on the controller scope. Without the
	// passphrase the satellite's LUKS layer has no key material to
	// seed `cryptsetup luksFormat` with, so the previous behaviour
	// (silently dropping LUKS at the projection edge) hid the
	// misconfiguration as plaintext-on-DRBD. A 400 here surfaces the
	// missing prerequisite at the create call instead.
	err = s.refuseLUKSWithoutPassphrase(r.Context(), rd.LayerStack)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// Upstream LINSTOR parity: every RD belongs to an RG. The
	// well-known DfltRscGrp serves as the catch-all for clients that
	// don't specify one (linstor-csi, the legacy CSI shipper, manual
	// `linstor rd create` without `--resource-group`, etc). Without
	// this default some CLI subcommands fail open lookups and operator
	// workflows that walk `rd → rg → spawn args` break silently.
	err = s.ensureDefaultRGAssignment(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Sticky LayerStack inheritance from the parent RG (Bug 54).
	// `linstor rd c <rd> --resource-group <rg>` does NOT carry layer_stack
	// on the wire — the upstream CLI relies on the controller to walk
	// rd → rg and stamp the RG's SelectFilter.LayerStack onto the RD at
	// create time. Without this stamp, RD.LayerStack stays empty, the
	// dispatcher's `rd.Spec.LayerStack` read at pkg/dispatcher/dispatcher.go
	// surfaces nil, and the satellite's `needsDRBD(layers []string)` legacy
	// default treats empty == DRBD+STORAGE — so an `STORAGE`-only RG ends
	// up spawning DRBD-stacked Resources, contradicting the operator's
	// SelectFilter.
	err = s.inheritLayerStackFromRG(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = s.Store.ResourceDefinitions().Create(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource definition created: " + rd.Name,
	}})
}

// DefaultResourceGroupName is the well-known RG every RD falls into
// when the caller didn't pin one. Matches upstream LINSTOR's
// `DfltRscGrp` literal so golinstor / linstor-csi callers that walk
// rd → rg discovery see the expected name.
const DefaultResourceGroupName = "DfltRscGrp"

// ensureDefaultRGAssignment sets rd.ResourceGroupName to the default
// when the caller didn't supply one, and lazily creates the
// well-known RG on first use. An explicit caller-supplied RG is left
// alone (existence is the caller's concern — matches upstream's
// "RD-create doesn't validate RG existence at the wire layer").
// Idempotent across concurrent RD-create races: ErrAlreadyExists from
// the RG-create path is swallowed.
func (s *Server) ensureDefaultRGAssignment(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if rd.ResourceGroupName != "" {
		return nil
	}

	rd.ResourceGroupName = DefaultResourceGroupName

	_, err := s.Store.ResourceGroups().Get(ctx, DefaultResourceGroupName)
	if err == nil {
		return nil
	}

	if !errors.Is(err, store.ErrNotFound) {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	// Description left empty to match upstream LINSTOR's auto-created
	// `DfltRscGrp` verbatim (Bug 57). Upstream's `linstor rg l` shows an
	// empty Description column for this RG; tools and runbooks compare
	// the full row byte-for-byte, so any "helpful" prose here diverges
	// from canonical wire output.
	defaultRG := apiv1.ResourceGroup{
		Name: DefaultResourceGroupName,
	}

	err = s.Store.ResourceGroups().Create(ctx, &defaultRG)
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	return nil
}

// inheritLayerStackFromRG copies the parent RG's SelectFilter.LayerStack
// onto the RD when the RD itself didn't carry one. Caller-supplied
// rd.LayerStack wins (explicit > inherited), matching the same precedence
// rule v1.ResolveLayerStack applies at the dispatch read-side. The result
// is stamped onto the stored RD so a later `linstor rg modify` that flips
// the parent's LayerStack does NOT retroactively re-layer existing RDs —
// "sticky at create time", just like upstream LINSTOR's behaviour.
//
// NotFound on the parent RG is swallowed: the lazily-created DfltRscGrp
// in ensureDefaultRGAssignment may race with this lookup on a sibling
// apiserver replica, and the default RG carries no SelectFilter anyway,
// so silently falling through leaves rd.LayerStack empty (= the legacy
// DRBD+STORAGE default, applied downstream).
func (s *Server) inheritLayerStackFromRG(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if len(rd.LayerStack) > 0 {
		return nil
	}

	if rd.ResourceGroupName == "" {
		return nil
	}

	rg, err := s.Store.ResourceGroups().Get(ctx, rd.ResourceGroupName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}

		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	if len(rg.SelectFilter.LayerStack) == 0 {
		return nil
	}

	rd.LayerStack = append([]string(nil), rg.SelectFilter.LayerStack...)

	return nil
}

// drbdEncryptPassphraseKey is the upstream LINSTOR controller-scope
// property the operator sets via `linstor controller set-property
// DrbdOptions/EncryptPassphrase <passphrase>` to seed the cluster's
// LUKS master key material. Bug 95 requires its presence as a hard
// prerequisite for accepting an RD whose LayerStack carries LUKS —
// without it, the satellite's `cryptsetup luksFormat` has no key to
// format the volume with and the layer chain silently fell back to
// plaintext-on-DRBD before this gate landed.
//
//nolint:gosec // prop key name, not a credential
const drbdEncryptPassphraseKey = "DrbdOptions/EncryptPassphrase"

// ErrLUKSRequiresPassphrase is the static sentinel surfaced as a 400
// when an RD-create body asks for a LUKS layer but the controller
// has no `DrbdOptions/EncryptPassphrase` set. Sentinel-shaped to
// match the layer-validation style; the handler wraps it with the
// human-readable remediation hint.
var ErrLUKSRequiresPassphrase = errors.New(
	"LUKS layer requires DrbdOptions/EncryptPassphrase to be set first")

// refuseLUKSWithoutPassphrase rejects an RD-create whose LayerStack
// includes LUKS when the controller-scope props bag has no
// `DrbdOptions/EncryptPassphrase` set (Bug 95). Returns nil for the
// happy path: non-LUKS stack, or LUKS stack with the prerequisite
// already in place. The caller surfaces the returned error as a 400
// so the operator sees the missing prerequisite immediately instead
// of waiting for replicas to come up plaintext.
//
// Falls through (nil) when the controller client is unconfigured —
// the test-only code paths that build a Server without a Client
// can't read ControllerConfig anyway, and forcing a 400 there would
// regress every unit-test that creates a LUKS RD against an in-memory
// store. The k8s-deployed apiserver always has a real client wired.
func (s *Server) refuseLUKSWithoutPassphrase(ctx context.Context, layers []string) error {
	if !apiv1.LayerInStack(layers, apiv1.LayerKindLUKS) {
		return nil
	}

	if s.Client == nil {
		return nil
	}

	ctrl, err := controllerScopeProps(ctx, s.Client)
	if err != nil {
		return errors.Wrap(err, "read controller props for LUKS prereq check")
	}

	if ctrl[drbdEncryptPassphraseKey] != "" {
		return nil
	}

	return errors.Wrapf(ErrLUKSRequiresPassphrase,
		"run `linstor controller set-property %s <passphrase>` "+
			"before creating a LUKS-layered RD",
		drbdEncryptPassphraseKey)
}

// resourceDefinitionModifyBody is the shape upstream golinstor sends
// on `PUT /v1/resource-definitions/{rd}` — driven by `linstor rd
// set-property`, `linstor rd modify --resource-group`, and similar
// CLI subcommands. Top-level fields are the modify delta, not the
// full RD spec; the bare RD wire shape doesn't carry these
// modify-only keys.
type resourceDefinitionModifyBody struct {
	OverrideProps    map[string]string `json:"override_props,omitempty"`
	DeleteProps      []string          `json:"delete_props,omitempty"`
	DeleteNamespaces []string          `json:"delete_namespaces,omitempty"`
	DrbdPeerSlots    int32             `json:"drbd_peer_slots,omitempty"`
	DrbdPort         int32             `json:"drbd_port,omitempty"`
	// resource_group: upstream linstor CLI's `rd modify --resource-group`
	// (matches golinstor `ResourceDefinitionCreate.ResourceGroup`).
	ResourceGroup string `json:"resource_group,omitempty"`
	// resource_group_name: legacy callers that PUT the full RD shape
	// instead of the modify envelope (matches the read-side
	// `ResourceDefinition` wire field). Accept both — first non-empty wins.
	ResourceGroupName string `json:"resource_group_name,omitempty"`
}

func (s *Server) handleRDUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	var patch resourceDefinitionModifyBody

	err := json.NewDecoder(r.Body).Decode(&patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// PUT semantics for the upstream linstor CLI's `rd set-property`
	// are MERGE, not REPLACE — golinstor sends only the override_props
	// / delete_props delta and expects the rest of the RD spec to be
	// preserved. A naïve Decode(&fullRD) + Update wipes the whole
	// spec (VolumeDefinitions vanish, the resource reconciler can't
	// spawn replicas, the cluster stalls). Fetch + merge instead.
	existing, err := s.Store.ResourceDefinitions().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if existing.Props == nil && len(patch.OverrideProps) > 0 {
		existing.Props = map[string]string{}
	}

	maps.Copy(existing.Props, patch.OverrideProps)

	for _, k := range patch.DeleteProps {
		delete(existing.Props, k)
	}

	rgChange := patch.ResourceGroup
	if rgChange == "" {
		rgChange = patch.ResourceGroupName
	}

	if rgChange != "" {
		existing.ResourceGroupName = rgChange
	}

	err = s.Store.ResourceDefinitions().Update(r.Context(), &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource definition modified: " + existing.Name,
	}})
}

// handleRDDelete drops an RD plus cascades the per-replica teardown.
//
// Idempotent on NotFound (Bug 56 sibling): CSI spec § DeleteVolume
// mandates idempotence at the volume layer, and an RD whose last
// child finished its finalizer drain may already be gone by the time
// a retry arrives. Folding the missing RD into a 200 + warn-mask
// envelope keeps the retry loop short and matches the same upstream
// "WARNING: … not found." exit 0 shape applied to resource delete.
func (s *Server) handleRDDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	// Scenario 4.W11: refuse the delete if any Snapshot still hangs
	// off this RD. Upstream LINSTOR's CtrlRscDfnDeleteApiCallHandler.
	// ensureNoSnapDfns raises FAIL_EXISTS_SNAPSHOT_DFN on the same
	// input with `"Cannot delete <rd> because it has snapshots."`;
	// the operator drops the snapshots first, then retries.
	//
	// Must run BEFORE cascadeDeleteResources — once the cascade
	// stamps DeletionTimestamp on every replica, a failed RD-delete
	// leaves the cluster half-torn-down (children gone, parent
	// kept, snapshots orphaned) which no retry can reconcile.
	snaps, err := s.Store.Snapshots().ListByDefinition(r.Context(), name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	if len(snaps) > 0 {
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsSnapshotDfn,
			Message: "Cannot delete resource definition '" + name + "' because it has snapshots.",
			ObjRefs: map[string]string{
				objRefRscDfn: name,
			},
		}})

		return
	}

	// Cascade the delete to all child Resource replicas BEFORE
	// dropping the RD itself. Without this, child Resources are
	// orphaned: no DeletionTimestamp is ever stamped on them, the
	// satellite reconciler's finalizer chain never fires, and
	// `drbdadm down` never runs — leaving DRBD kernel state
	// (minors, ports, peer entries) live on every satellite. The
	// next RD-create with the same name then collides with a
	// stale port (.res) or sees a half-configured peer.
	//
	// Upstream LINSTOR does this server-side too: deleting an RD
	// flags the RD with DELETE, then walks Resources and stamps
	// DELETE on each. The satellite drives the teardown from
	// there. Our equivalent is: per-child Resources().Delete()
	// stamps DeletionTimestamp on the CRD; the satellite's
	// existing `blockstor.io.blockstor.io/satellite-resource`
	// finalizer then drains DRBD before the apiserver removes
	// the object.
	err = s.cascadeDeleteResources(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = s.Store.ResourceDefinitions().Delete(r.Context(), name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnRDNotFound,
				Message: "resource definition already absent: " + name,
			}})

			return
		}

		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource definition deleted: " + name,
	}})
}

// cascadeDeleteResources enumerates every Resource replica under the
// named RD and deletes them so the satellite's finalizer can drain
// DRBD state per-node. ErrNotFound from a per-child Delete is
// swallowed — a Resource that already vanished (race with another
// controller, or a previous partial cascade) shouldn't fail the
// whole RD-delete.
//
// Returns the first non-NotFound error so the caller can surface it
// via writeStoreError. A NotFound from the parent RD lookup is
// treated as "no children to cascade" (idempotent: re-running an
// RD-delete that already cleared its replicas must still let the
// final Store.ResourceDefinitions().Delete see its own NotFound and
// produce the right HTTP code).
func (s *Server) cascadeDeleteResources(ctx context.Context, rdName string) error {
	children, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}

		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	for i := range children {
		err = s.Store.Resources().Delete(ctx, rdName, children[i].NodeName)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err //nolint:wrapcheck // surfaced via writeStoreError
		}
	}

	return nil
}
