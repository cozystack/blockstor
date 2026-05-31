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
	"net/http"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Finding 8 (P2): the `/v1/{kind}/properties/info` endpoints (where
// {kind} ∈ nodes, resource-definitions, resource-groups,
// storage-pool-definitions) were unwired. Two failure modes:
//
//   - `/v1/nodes/properties/info` was swallowed by `GET /v1/nodes/{node}/info`
//     — the router treats "properties" as a node name and falls into
//     the handleNodeInfo NotFound branch, returning 404 with the
//     misleading "node 'properties': object not found" body.
//   - The other three (`resource-definitions`, `resource-groups`,
//     `storage-pool-definitions`) had no registered handler at all and
//     hit the 404 envelope catch-all.
//
// `linstor` CLI's tab completion + `--help` output for property keys
// come from these endpoints. The python CLI calls them on every
// `linstor n lp -i`, `linstor rd lp -i`, etc. Without the routes
// tab-completion is broken; with the routes returning an empty body
// (or a synthesized envelope) the CLI's parser crashes downstream.
//
// Fix:
//
//   - Wire literal-path handlers for all four endpoints. Go 1.22's
//     ServeMux precedence puts literal paths above wildcard ones
//     (`/v1/nodes/properties/info` beats `/v1/nodes/{node}/info`), so
//     a single registration here closes the nodes routing trap too.
//   - Return a hand-curated catalogue covering the prop keys blockstor
//     actually accepts on each object kind. Best-effort: the python
//     CLI only consumes `name` + `info`, so the curated catalogue is
//     enough to keep tab-completion + `--help` working. The catalogue
//     can be expanded in follow-up commits without churning the wire
//     contract — `name` + `info` are stable, additional fields would
//     just enrich the per-key block.
//
// The four catalogues share their core (Aux/*, DrbdOptions/*) because
// every kind accepts those at its scope. Per-kind keys (e.g.
// StorDriver/* for storage pools, autoplacer weights for RGs/RDs/
// controller) layer on top.

// Per-namespace catalogue key constants. The values themselves are
// already-existing prop keys consumed elsewhere; centralising the
// literals here keeps the goconst linter happy and gives a single
// place to add new catalogue entries without re-discovering the keys.
const (
	auxNamespaceKey = "Aux/*"

	drbdNetProtocolKey      = "DrbdOptions/Net/protocol"
	drbdNetPingTimeoutKey   = "DrbdOptions/Net/ping-timeout"
	drbdNetCramHMACAlgKey   = "DrbdOptions/Net/cram-hmac-alg"
	drbdAutoQuorumKey       = "DrbdOptions/auto-quorum"
	storDriverEncryptKey    = "StorDriver/EncryptPassphrase"
	autoplacerThroughputKey = "Autoplacer/MaxThroughput"
	prefNicPropKey          = "PrefNic"
)

// propsInfoEntry is one row of the PropsInfo response. Mirrors
// upstream Java's `JsonGenTypes.PropsInfo` minimal shape: the python
// CLI keys on `name` (the prop key, e.g. "DrbdOptions/Net/protocol")
// and renders `info` as the per-key documentation blurb.
//
// Upstream emits more fields (`type`, `default`, `unit`, `dflt_val`),
// but the python CLI's `linstor n lp -i` parser only requires
// `name` + `info`. Operators that consume PropsInfo via golinstor
// receive the same minimal shape — the SDK's `PropsInfo` struct
// uses `omitempty` on the optional fields, so a partial response
// decodes cleanly. The catalogue is best-effort and can be expanded
// in follow-up without breaking wire compat (additive only).
type propsInfoEntry struct {
	Name string `json:"name"`
	Info string `json:"info"`
}

// registerPropertiesInfo wires the four PropsInfo endpoints onto the
// shared mux. Called from Server.buildMux. Each handler returns the
// per-kind hand-curated catalogue.
//
// The nodes handler also fixes the routing trap (Finding 8 first
// failure mode): registering the literal `GET /v1/nodes/properties/info`
// path makes it more specific than the existing `GET /v1/nodes/{node}/info`
// wildcard, so the literal wins by Go 1.22 ServeMux precedence —
// `/v1/nodes/properties/info` now hits this handler instead of
// `handleNodeInfo`'s NotFound branch.
func (s *Server) registerPropertiesInfo(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/nodes/properties/info", handleNodePropsInfo)
	mux.HandleFunc("GET /v1/resource-definitions/properties/info", handleRDPropsInfo)
	mux.HandleFunc("GET /v1/resource-groups/properties/info", handleRGPropsInfo)
	mux.HandleFunc("GET /v1/storage-pool-definitions/properties/info", handleSPDfnPropsInfo)
}

// commonPropsInfoEntries is the core catalogue every object kind
// accepts at its scope. Aux/* is operator-defined free-form
// metadata (upstream LINSTOR pins this in UG9 §"Properties"). The
// shared DrbdOptions/* keys land here too because every kind that
// owns a DRBD-backed resource (node, RD, RG, SPDfn) lets the
// operator override them.
func commonPropsInfoEntries() []propsInfoEntry {
	return []propsInfoEntry{
		{
			Name: auxNamespaceKey,
			Info: "Operator-defined free-form metadata. Any key under " +
				"the `Aux/` namespace round-trips through the API " +
				"without validation. Use for operator tags " +
				"(e.g. Aux/team, Aux/rack-id).",
		},
		{
			Name: drbdNetProtocolKey,
			Info: "DRBD replication protocol. A (async), B (semi-sync), " +
				"C (sync). Default C.",
		},
		{
			Name: drbdNetPingTimeoutKey,
			Info: "DRBD network ping timeout in deciseconds. Default 500 " +
				"(5 seconds).",
		},
		{
			Name: drbdSharedSecretKey,
			Info: "Shared secret for DRBD per-pair authentication. Redacted " +
				"on read.",
		},
		{
			Name: drbdNetCramHMACAlgKey,
			Info: "HMAC algorithm for DRBD per-pair authentication.",
		},
		{
			Name: drbdEncryptPassphraseKey,
			Info: "Cluster-scope DRBD encryption passphrase. Redacted on " +
				"read; set via `linstor encryption set-passphrase`.",
		},
	}
}

// handleNodePropsInfo serves GET /v1/nodes/properties/info. The
// catalogue lists the per-node prop keys the controller honours —
// operators set these via `linstor n set-property <node> <key> <val>`.
func handleNodePropsInfo(w http.ResponseWriter, _ *http.Request) {
	entries := append(commonPropsInfoEntries(),
		propsInfoEntry{
			Name: prefNicPropKey,
			Info: "Preferred NetInterface name for inbound DRBD traffic. " +
				"Must match a NetInterface registered on the node.",
		},
		propsInfoEntry{
			Name: nodePropDiscoveredVGs,
			Info: "Satellite-stamped comma-separated list of LVM volume " +
				"groups the satellite enumerated on the host. Read-only " +
				"to operators; consumed by storage-pool create pre-flight.",
		},
		propsInfoEntry{
			Name: nodePropDiscoveredZPools,
			Info: "Satellite-stamped comma-separated list of ZFS zpools " +
				"the satellite enumerated on the host. Read-only to " +
				"operators; consumed by storage-pool create pre-flight.",
		},
	)

	writeJSON(w, http.StatusOK, entries)
}

// handleRDPropsInfo serves GET /v1/resource-definitions/properties/info.
// The catalogue lists per-RD prop keys plus the upstream inheritance
// hierarchy notes — RD-scope props override RG-scope, which override
// controller-scope.
func handleRDPropsInfo(w http.ResponseWriter, _ *http.Request) {
	entries := append(commonPropsInfoEntries(),
		propsInfoEntry{
			Name: storPoolPropKey,
			Info: "Default storage pool name new replicas pick up when no " +
				"`--storage-pool` is supplied at resource-create time.",
		},
		propsInfoEntry{
			Name: drbdAutoQuorumKey,
			Info: "Per-RD DRBD quorum mode override. Accepts `off`, " +
				"`suspend-io`, `io-error`.",
		},
		propsInfoEntry{
			Name: apiv1.PropBalanceResourcesEnabled,
			Info: "RD-scope kill-switch for the additive rebalance " +
				"reconciler. Accepts `true` / `false`. Overrides the " +
				"RG and controller-scope values.",
		},
		propsInfoEntry{
			Name: apiv1.PropBalanceResourcesInterval,
			Info: "RD-scope rebalance tick interval in minutes. " +
				"Overrides the RG and controller-scope values.",
		},
		propsInfoEntry{
			Name: apiv1.PropBalanceResourcesGracePeriod,
			Info: "RD-scope grace period in minutes before the " +
				"rebalance reconciler churns replicas on a flapping node. " +
				"Overrides the RG and controller-scope values.",
		},
	)

	writeJSON(w, http.StatusOK, entries)
}

// handleRGPropsInfo serves GET /v1/resource-groups/properties/info.
// Similar to RD-scope but with the rebalance / autoplacer-weight
// knobs that operators typically set group-wide.
func handleRGPropsInfo(w http.ResponseWriter, _ *http.Request) {
	entries := append(commonPropsInfoEntries(),
		propsInfoEntry{
			Name: storPoolPropKey,
			Info: "Default storage pool name child RDs / Resources " +
				"inherit when no explicit pool is supplied.",
		},
		propsInfoEntry{
			Name: apiv1.PropAutoplacerWeightMaxFreeSpace,
			Info: "Scoring weight for the MaxFreeSpace strategy. Default 1.0.",
		},
		propsInfoEntry{
			Name: apiv1.PropAutoplacerWeightMinReservedSpace,
			Info: "Scoring weight for the MinReservedSpace strategy. Default 1.0.",
		},
		propsInfoEntry{
			Name: apiv1.PropAutoplacerWeightMinRscCount,
			Info: "Scoring weight for the MinRscCount strategy. Default 1.0.",
		},
		propsInfoEntry{
			Name: apiv1.PropAutoplacerWeightMaxThroughput,
			Info: "Scoring weight for the MaxThroughput strategy. Default 1.0.",
		},
		propsInfoEntry{
			Name: apiv1.PropBalanceResourcesEnabled,
			Info: "RG-scope kill-switch for the additive rebalance " +
				"reconciler. Accepts `true` / `false`. Overrides the " +
				"controller-scope default.",
		},
		propsInfoEntry{
			Name: apiv1.PropBalanceResourcesInterval,
			Info: "RG-scope rebalance tick interval in minutes. " +
				"Overrides the controller-scope default.",
		},
		propsInfoEntry{
			Name: apiv1.PropBalanceResourcesGracePeriod,
			Info: "RG-scope grace period in minutes before the rebalance " +
				"reconciler churns replicas on a flapping node. Overrides " +
				"the controller-scope default.",
		},
		propsInfoEntry{
			Name: apiv1.PropAllowMixingStoragePoolDriver,
			Info: "Cluster-scope override that opens the LVM_THIN <-> ZFS_THIN " +
				"cell in the same-provider-kind enforcement. Default false.",
		},
	)

	writeJSON(w, http.StatusOK, entries)
}

// handleSPDfnPropsInfo serves
// GET /v1/storage-pool-definitions/properties/info. The catalogue
// lists the per-StoragePool-definition keys operators set via
// `linstor sp-d set-property <name> <key> <val>`.
func handleSPDfnPropsInfo(w http.ResponseWriter, _ *http.Request) {
	entries := append(commonPropsInfoEntries(),
		propsInfoEntry{
			Name: propStorPoolName,
			Info: "Backing VG / zpool / dir name. Provider-agnostic alias; " +
				"the satellite expands it to the kind-specific key " +
				"(StorDriver/LvmVg, StorDriver/ZPool, StorDriver/ZPoolThin, " +
				"StorDriver/FileDir) at create time.",
		},
		propsInfoEntry{
			Name: propLvmVG,
			Info: "LVM volume group name. Required for LVM and LVM_THIN " +
				"provider kinds.",
		},
		propsInfoEntry{
			Name: propThinPool,
			Info: "LVM thin pool name inside the VG. Required for LVM_THIN.",
		},
		propsInfoEntry{
			Name: propZPool,
			Info: "ZFS zpool name. Required for ZFS provider kind.",
		},
		propsInfoEntry{
			Name: propZPoolThin,
			Info: "ZFS zpool name backing a thin dataset. Required for ZFS_THIN.",
		},
		propsInfoEntry{
			Name: propFileDir,
			Info: "Directory path for FILE / FILE_THIN provider kinds.",
		},
		propsInfoEntry{
			Name: storDriverEncryptKey,
			Info: "Per-pool encryption passphrase. Redacted on read.",
		},
		propsInfoEntry{
			Name: propMaxOversubscriptionRatio,
			Info: "Cluster-wide max oversubscription ratio applied to thin " +
				"pools backing this definition. Accepts a floating-point " +
				"value (e.g. 1.5). Default 1.0 (no oversubscription).",
		},
		propsInfoEntry{
			Name: autoplacerThroughputKey,
			Info: "Per-pool advertised maximum throughput in bytes/sec. " +
				"Consumed by the placer's MaxThroughput scoring strategy.",
		},
	)

	writeJSON(w, http.StatusOK, entries)
}
