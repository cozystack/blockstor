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
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// Server-side dual-Primary for live migration.
//
// A VM live migration needs its disk writable on two nodes for a moment:
// the source keeps running while the target starts. DRBD allows that only
// with allow-two-primaries, and only over a protocol C connection. Since
// LINSTOR 1.35 the controller owns that setting instead of the CSI driver:
//
//   - make-available with auto_manage_dual_primary on the migration target
//     looks up the node the resource is in use on (the source) and opens
//     the connection between the two: allow-two-primaries=yes, plus
//     protocol C when the protocol in effect is another one;
//   - unmake-available on the source, once it let go of the device, closes
//     it again, puts back the protocol it replaced, and removes the
//     resource there when it is only a diskless attachment.
//
// The settings land on the resource connection between source and target,
// so the other connections of the resource keep their protocol. When both
// ends are diskless they land on the definition instead, as LINSTOR does.
// Marks on the definition record what was changed so unmake-available can
// put back exactly that:
//
//	Internal/Drbd/LiveMigrate/SourceNode     migration source
//	Internal/Drbd/LiveMigrate/TargetNode     migration target
//	Internal/Drbd/LiveMigrate/ProtocolSetOn  RSC_CONN or RSC_DFN, when the protocol was changed
//	Internal/Drbd/LiveMigrate/PrevProtocol   the protocol set explicitly at that scope before
//
// The prop names, the values and the return codes are LINSTOR's, so a
// client, a dump or an operator reading them sees the same thing on
// either controller.
const (
	liveMigrateNamespace         = "Internal/Drbd/LiveMigrate"
	liveMigrateSourceProp        = liveMigrateNamespace + "/SourceNode"
	liveMigrateTargetProp        = liveMigrateNamespace + "/TargetNode"
	liveMigrateProtocolSetOnProp = liveMigrateNamespace + "/ProtocolSetOn"
	liveMigratePrevProtocolProp  = liveMigrateNamespace + "/PrevProtocol"

	protocolSetOnDefinition = "RSC_DFN"
	protocolSetOnConnection = "RSC_CONN"

	allowTwoPrimariesProp = "DrbdOptions/Net/allow-two-primaries"
	drbdProtocolProp      = "DrbdOptions/Net/protocol"
	drbdProtocolC         = "C"
	drbdOptionYes         = "yes"
)

// Return codes of the dual-Primary refusals: LINSTOR's FAIL_EXISTS_LIVE_MIGRATE
// and FAIL_INVLD_LAYER_STACK, so golinstor's errors.Is matches them.
const (
	apiCallRcFailExistsLiveMigrate int64 = 901
	apiCallRcFailInvldLayerStack   int64 = 225
)

// dualPrimaryRefusalError is a make-available or unmake-available the server
// turns down, with the status and the envelope to answer it with.
type dualPrimaryRefusalError struct {
	status int
	rc     apiv1.APICallRc
}

func (e *dualPrimaryRefusalError) Error() string { return e.rc.Message }

func (e *dualPrimaryRefusalError) write(w http.ResponseWriter) {
	writeJSON(w, e.status, []apiv1.APICallRc{e.rc})
}

// inUse reports whether the satellite of a replica reports the device open.
// A replica the satellite has not reported on yet is not in use.
func inUse(res *apiv1.Resource) bool {
	return res.State.InUse != nil && *res.State.InUse
}

// drbdDiskless reports whether a replica is a DRBD client: diskless, or a
// tiebreaker, which is diskless as well.
func drbdDiskless(res *apiv1.Resource) bool {
	return slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) ||
		slices.Contains(res.Flags, apiv1.ResourceFlagTieBreaker)
}

// migrationSource answers which node a live migration to target starts
// from: the one node the resource is in use on. "" means there is nothing
// to prepare, because the resource is in use nowhere (a plain attach,
// which a client that cannot tell the two apart sends with the flag set
// as well) or already in use on target itself.
//
// In use on two nodes is accepted only as the repeat of the migration the
// marks record, which is the state between the two make-available calls
// of a retried attach. A mark naming another pair refuses the call: that
// migration has to be finished and reverted first.
func migrationSource(rdName string, props map[string]string, replicas []apiv1.Resource, target string) (string, error) {
	var using []string

	for i := range replicas {
		if inUse(&replicas[i]) {
			using = append(using, replicas[i].NodeName)
		}
	}

	sort.Strings(using)

	markSource, markTarget := props[liveMigrateSourceProp], props[liveMigrateTargetProp]
	marked := markSource != "" && markTarget != ""

	switch len(using) {
	case 0:
		return "", nil
	case 1:
		source := using[0]

		if marked && (!naming.Same(markSource, source) || !naming.Same(markTarget, target)) {
			return "", conflictingMigration(rdName, markSource, markTarget)
		}

		if naming.Same(source, target) {
			return "", nil
		}

		return source, nil
	}

	// In use on several nodes: only the repeat of the marked migration.
	if marked && naming.Same(markTarget, target) {
		if source := markedSourceAmong(using, markSource, markTarget); source != "" {
			return source, nil
		}
	}

	return "", &dualPrimaryRefusalError{
		status: http.StatusConflict,
		rc: apiv1.APICallRc{
			RetCode: apiCallRcError | apiCallRcFailExistsLiveMigrate,
			Message: fmt.Sprintf("Resource '%s' is in use on multiple nodes", rdName),
			Cause:   "The live-migration source cannot be determined.",
			ObjRefs: map[string]string{objRefRscDfn: rdName},
		},
	}
}

// markedSourceAmong answers the marked source when the nodes in use are
// exactly the two ends of the marked migration, and "" otherwise.
func markedSourceAmong(using []string, markSource, markTarget string) string {
	if len(using) != 2 {
		return ""
	}

	source := ""

	for _, node := range using {
		switch {
		case naming.Same(node, markSource):
			source = node
		case naming.Same(node, markTarget):
		default:
			return ""
		}
	}

	return source
}

func conflictingMigration(rdName, source, target string) error {
	return &dualPrimaryRefusalError{
		status: http.StatusConflict,
		rc: apiv1.APICallRc{
			RetCode: apiCallRcError | apiCallRcFailExistsLiveMigrate,
			Message: fmt.Sprintf("A live migration of resource '%s' from node '%s' to node '%s' is already prepared",
				rdName, source, target),
			Correc: fmt.Sprintf("Finish that migration and revert it with 'unmake-available %s %s' first.",
				source, rdName),
			ObjRefs: map[string]string{objRefRscDfn: rdName},
		},
	}
}

// requireDRBDForDualPrimary refuses auto_manage_dual_primary on a resource
// without DRBD: only DRBD can be Primary on two nodes. LINSTOR also takes
// a resource on a shared storage pool here, activated on both nodes;
// blockstor has no shared activation, so it refuses those as well.
func requireDRBDForDualPrimary(rdName string, layers []string) error {
	if apiv1.LayerInStack(layers, "DRBD") {
		return nil
	}

	return &dualPrimaryRefusalError{
		status: http.StatusBadRequest,
		rc: apiv1.APICallRc{
			RetCode: apiCallRcError | apiCallRcFailInvldLayerStack,
			Message: "auto_manage_dual_primary requires DRBD (or a shared storage pool)",
			ObjRefs: map[string]string{objRefRscDfn: rdName},
		},
	}
}

// protocolSources is what decides the DRBD protocol of one replica's end
// of a connection, apart from the connection and the definition: the
// replica's own props and the group and controller scopes above the
// definition. Read once before the definition is patched; the definition
// itself is read inside the patch.
type protocolSources struct {
	resource  string // set on the replica itself
	inherited string // from the resource group or the controller
}

func protocolSourcesOf(eff apiv1.EffectiveProperties) protocolSources {
	entry, ok := eff[drbdProtocolProp]
	if !ok {
		return protocolSources{}
	}

	switch entry.Scope {
	case apiv1.EffectivePropScopeResource:
		return protocolSources{resource: entry.Value}
	case apiv1.EffectivePropScopeResourceGroup, apiv1.EffectivePropScopeController:
		return protocolSources{inherited: entry.Value}
	}

	return protocolSources{}
}

// protocolAt answers the protocol of one end: the replica's own setting,
// else the definition's, else the inherited one, else DRBD's default C.
func (p protocolSources) protocolAt(defProps map[string]string) string {
	for _, value := range []string{p.resource, defProps[drbdProtocolProp], p.inherited} {
		if value != "" {
			return value
		}
	}

	return drbdProtocolC
}

// connectionProtocol answers the protocol DRBD runs between source and
// target: the one the connection sets, else whichever end does not run C.
// blockstor renders a replica-level protocol into that replica's resource
// file, so the two ends can differ; either one that is not C needs the
// connection override.
func connectionProtocol(defProps map[string]string, source, target string, ends [2]protocolSources) string {
	if value := defProps[apiv1.ResourceConnectionPropKey(source, target, drbdProtocolProp)]; value != "" {
		return value
	}

	for _, end := range ends {
		if protocol := end.protocolAt(defProps); !strings.EqualFold(protocol, drbdProtocolC) {
			return protocol
		}
	}

	return drbdProtocolC
}

// dualPrimaryPlan is one make-available's request to open the connection
// between source and target to two Primaries.
type dualPrimaryPlan struct {
	rdName       string
	source       string
	target       string
	bothDiskless bool
	ends         [2]protocolSources
}

// apply writes the dual-Primary props into a definition's props and
// answers the messages for the caller. It runs inside the definition
// patch, so the protocol is judged against the props it changes.
func (p *dualPrimaryPlan) apply(props map[string]string) []apiv1.APICallRc {
	scopeKey := func(key string) string {
		return apiv1.ResourceConnectionPropKey(p.source, p.target, key)
	}
	scope := protocolSetOnConnection
	where := fmt.Sprintf("resource connection between nodes '%s' and '%s' of resource '%s'", p.source, p.target, p.rdName)

	if p.bothDiskless {
		// DRBD connects no two diskless peers, so there is no
		// connection of theirs to configure: the definition carries it.
		scopeKey = func(key string) string { return key }
		scope = protocolSetOnDefinition
		where = fmt.Sprintf("resource-definition '%s'", p.rdName)
	}

	var msgs []apiv1.APICallRc

	if protocol := connectionProtocol(props, p.source, p.target, p.ends); !strings.EqualFold(protocol, drbdProtocolC) {
		if prev, ok := props[scopeKey(drbdProtocolProp)]; ok && prev != "" {
			props[liveMigratePrevProtocolProp] = prev
		}

		props[scopeKey(drbdProtocolProp)] = drbdProtocolC
		props[liveMigrateProtocolSetOnProp] = scope

		msgs = append(msgs, apiv1.APICallRc{
			RetCode: maskInfo,
			Message: fmt.Sprintf("DRBD protocol temporarily changed from '%s' to 'C' on %s", protocol, where),
		})
	}

	props[scopeKey(allowTwoPrimariesProp)] = drbdOptionYes
	props[liveMigrateSourceProp] = p.source
	props[liveMigrateTargetProp] = p.target

	return append(msgs, apiv1.APICallRc{
		RetCode: maskInfo,
		Message: fmt.Sprintf("Enabled allow-two-primaries on %s for the live migration from node '%s' to node '%s'. "+
			"Revert with unmake-available.", where, p.source, p.target),
	})
}

// revertDualPrimary takes the dual-Primary props back out of a
// definition's props: allow-two-primaries on the definition, on every
// connection of node and on the connection the marks name; the protocol
// the marks say was changed, back to what it was; and the marks.
//
// It is unconditional on purpose. A definition may still carry
// allow-two-primaries from a CSI driver that set it client-side before
// LINSTOR 1.35, and unmake-available is the call that clears it.
//
// Answers whether anything changed, so the caller can skip the write.
func revertDualPrimary(rdName, node string, props map[string]string) (bool, []apiv1.APICallRc) {
	changed := dropAllowTwoPrimaries(node, props)

	restored, msgs := restoreProtocol(rdName, props)
	changed = changed || restored

	for prop := range props {
		if propInNamespace(prop, liveMigrateNamespace) {
			delete(props, prop)

			changed = true
		}
	}

	return changed, msgs
}

// dropAllowTwoPrimaries removes allow-two-primaries from the definition,
// from every connection of node and from the connection the marks name.
func dropAllowTwoPrimaries(node string, props map[string]string) bool {
	_, changed := props[allowTwoPrimariesProp]
	delete(props, allowTwoPrimariesProp)

	markSource, markTarget := props[liveMigrateSourceProp], props[liveMigrateTargetProp]
	marked := markSource != "" && markTarget != ""

	for prop := range props {
		pair, key, ok := apiv1.ParseResourceConnectionPropKey(prop)
		if !ok || key != allowTwoPrimariesProp {
			continue
		}

		if pair.Has(node) || (marked && pair.Has(markSource) && pair.Has(markTarget)) {
			delete(props, prop)

			changed = true
		}
	}

	return changed
}

// restoreProtocol puts back the protocol the marks say a make-available
// changed: the previous explicit value, or none. A connection the marks
// name but no prop of which is left needs nothing.
func restoreProtocol(rdName string, props map[string]string) (bool, []apiv1.APICallRc) {
	protocolKey := ""

	switch props[liveMigrateProtocolSetOnProp] {
	case protocolSetOnDefinition:
		protocolKey = drbdProtocolProp
	case protocolSetOnConnection:
		// A connection whose replica was deleted took its props
		// along (dropConnectionProps): there is nothing left to put
		// back, and writing the old protocol would bring it back.
		source, target := props[liveMigrateSourceProp], props[liveMigrateTargetProp]
		if source != "" && target != "" && connectionHasProps(props, source, target) {
			protocolKey = apiv1.ResourceConnectionPropKey(source, target, drbdProtocolProp)
		}
	}

	if protocolKey == "" {
		return false, nil
	}

	prev := props[liveMigratePrevProtocolProp]
	if prev == "" {
		_, had := props[protocolKey]
		delete(props, protocolKey)

		return had, nil
	}

	props[protocolKey] = prev

	return true, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: fmt.Sprintf("DRBD protocol of resource '%s' restored to '%s'", rdName, prev),
	}}
}

// connectionHasProps reports whether the connection between a and b
// still holds any prop.
func connectionHasProps(props map[string]string, a, b string) bool {
	prefix := apiv1.ResourceConnectionPropKey(a, b, "")

	for prop := range props {
		if strings.HasPrefix(prop, prefix) {
			return true
		}
	}

	return false
}

// propInNamespace reports whether prop lives in namespace ns.
func propInNamespace(prop, ns string) bool {
	return strings.HasPrefix(prop, ns+"/")
}

// dropConnectionProps removes every resource-connection prop of node from
// a definition's props. A connection lives as long as the replicas on
// both of its ends: once one is deleted, LINSTOR deletes its connections
// with it, and a replica created on that node later starts from none.
func dropConnectionProps(node string, props map[string]string) bool {
	changed := false

	for prop := range props {
		if pair, _, ok := apiv1.ParseResourceConnectionPropKey(prop); ok && pair.Has(node) {
			delete(props, prop)

			changed = true
		}
	}

	return changed
}
