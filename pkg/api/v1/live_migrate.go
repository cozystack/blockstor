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

package v1

import (
	"strings"
)

// The props a live migration writes onto a resource definition, by
// LINSTOR's names (InternalApiConsts, CtrlRscLiveMigrateHelper):
//
//	Internal/Drbd/LiveMigrate/SourceNode     migration source
//	Internal/Drbd/LiveMigrate/TargetNode     migration target
//	Internal/Drbd/LiveMigrate/ProtocolSetOn  RSC_CONN or RSC_DFN, when the protocol was changed
//	Internal/Drbd/LiveMigrate/PrevProtocol   the protocol set explicitly at that scope before
//
// They live here rather than with the REST handlers because the store
// reads them as well: a clone or a restore must not inherit a migration
// that is not running on it.
const (
	LiveMigrateNamespace         = "Internal/Drbd/LiveMigrate"
	LiveMigrateSourceProp        = LiveMigrateNamespace + "/SourceNode"
	LiveMigrateTargetProp        = LiveMigrateNamespace + "/TargetNode"
	LiveMigrateProtocolSetOnProp = LiveMigrateNamespace + "/ProtocolSetOn"
	LiveMigratePrevProtocolProp  = LiveMigrateNamespace + "/PrevProtocol"

	LiveMigrateProtocolSetOnDefinition = "RSC_DFN"
	LiveMigrateProtocolSetOnConnection = "RSC_CONN"

	AllowTwoPrimariesProp = "DrbdOptions/Net/allow-two-primaries"
	DRBDProtocolProp      = "DrbdOptions/Net/protocol"
)

// LiveMigrationMarked reports whether props record a prepared live
// migration.
func LiveMigrationMarked(props map[string]string) bool {
	return props[LiveMigrateSourceProp] != "" && props[LiveMigrateTargetProp] != ""
}

// RevertLiveMigration takes what a live migration wrote back out of a
// definition's props: allow-two-primaries on the definition, on every
// connection of node and on the connection the marks name; the protocol
// the marks say was changed, back to what it was; and the marks. node may
// be empty, for a copy of the props that has no replica to name.
//
// It answers whether anything changed, and the protocol it put back ("" if
// it put none back).
func RevertLiveMigration(node string, props map[string]string) (bool, string) {
	changed := dropAllowTwoPrimaries(node, props)

	restored, prev := restoreLiveMigrationProtocol(props)
	changed = changed || restored

	for prop := range props {
		if strings.HasPrefix(prop, LiveMigrateNamespace+"/") {
			delete(props, prop)

			changed = true
		}
	}

	return changed, prev
}

// dropAllowTwoPrimaries removes allow-two-primaries from the definition,
// from every connection of node and from the connection the marks name.
func dropAllowTwoPrimaries(node string, props map[string]string) bool {
	_, changed := props[AllowTwoPrimariesProp]
	delete(props, AllowTwoPrimariesProp)

	markSource, markTarget := props[LiveMigrateSourceProp], props[LiveMigrateTargetProp]
	marked := LiveMigrationMarked(props)

	for prop := range props {
		pair, key, ok := ParseResourceConnectionPropKey(prop)
		if !ok || key != AllowTwoPrimariesProp {
			continue
		}

		if (node != "" && pair.Has(node)) || (marked && pair.Has(markSource) && pair.Has(markTarget)) {
			delete(props, prop)

			changed = true
		}
	}

	return changed
}

// restoreLiveMigrationProtocol puts back the protocol the marks say a
// make-available changed: the previous explicit value, or none. A
// connection the marks name but no prop of which is left needs nothing:
// its props went with a deleted replica, and writing the old protocol
// would bring the connection back.
func restoreLiveMigrationProtocol(props map[string]string) (bool, string) {
	protocolKey := ""

	switch props[LiveMigrateProtocolSetOnProp] {
	case LiveMigrateProtocolSetOnDefinition:
		protocolKey = DRBDProtocolProp
	case LiveMigrateProtocolSetOnConnection:
		source, target := props[LiveMigrateSourceProp], props[LiveMigrateTargetProp]
		if source != "" && target != "" && connectionHasProps(props, source, target) {
			protocolKey = ResourceConnectionPropKey(source, target, DRBDProtocolProp)
		}
	}

	if protocolKey == "" {
		return false, ""
	}

	prev := props[LiveMigratePrevProtocolProp]
	if prev == "" {
		_, had := props[protocolKey]
		delete(props, protocolKey)

		return had, ""
	}

	props[protocolKey] = prev

	return true, prev
}

// connectionHasProps reports whether the connection between a and b
// still holds any prop.
func connectionHasProps(props map[string]string, a, b string) bool {
	prefix := ResourceConnectionPropKey(a, b, "")

	for prop := range props {
		if strings.HasPrefix(prop, prefix) {
			return true
		}
	}

	return false
}

// DropResourceConnectionProps removes every resource-connection prop of
// node from a definition's props, and answers whether there was any. A
// connection lives as long as the replicas on both of its ends: LINSTOR
// deletes the connections of a deleted resource with it, and a replica
// created on that node later starts from none.
func DropResourceConnectionProps(node string, props map[string]string) bool {
	changed := false

	for prop := range props {
		if pair, _, ok := ParseResourceConnectionPropKey(prop); ok && pair.Has(node) {
			delete(props, prop)

			changed = true
		}
	}

	return changed
}
