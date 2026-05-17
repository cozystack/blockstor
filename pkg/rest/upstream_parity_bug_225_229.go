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
	"strings"
)

// This file gathers four upstream-parity endpoints blockstor was
// missing — three GET diagnostics + the SPDef single-item lookup.
// The matching Bug 225 (snapshot-restore-volume-definition) handler
// stays colocated with the resource-restore variant in
// snapshot_restore.go.
//
//   - Bug 226: GET /v1/space-report
//   - Bug 227: GET /v1/resource-definitions/{rd}/sync-status
//   - Bug 228: GET /v1/nodes/{node}/config
//   - Bug 229: GET /v1/storage-pool-definitions/{name} single-item
//
// Each handler mirrors the upstream Java wire shape (see per-handler
// comments) and reuses the existing Store surface — no new Store
// interface methods.

// kibPerMiB renders KiB capacity figures in MiB so the operator-
// facing space-report body stays human-readable for the common
// multi-GiB pool case.
const kibPerMiB = 1024

// registerUpstreamParity225_229 wires the four endpoints onto the
// shared mux. Called from Server.buildMux.
//
//nolint:stylecheck // function name carries bug-range identifier per project convention
func (s *Server) registerUpstreamParity225_229(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/space-report", s.requireStore(s.handleSpaceReport))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/sync-status",
		s.requireStore(s.handleResourceDefinitionSyncStatus))
	mux.HandleFunc("GET /v1/nodes/{node}/config", s.requireStore(s.handleNodeConfig))
	mux.HandleFunc("GET /v1/storage-pool-definitions/{name}",
		s.requireStore(s.handleStoragePoolDefinitionSingle))
}

// spaceReport mirrors upstream Java's JsonSpaceTracking.SpaceReport:
// a single string field `report_text` containing the human-readable
// cluster-wide capacity summary the python CLI's `linstor
// space-reporting query` renders verbatim. Upstream's
// SpaceTrackingService produces a multi-line aggregate; blockstor
// derives the equivalent from the StoragePool table because we don't
// run upstream's SpaceTracking subsystem.
type spaceReport struct {
	ReportText string `json:"report_text"`
}

// handleSpaceReport serves Bug 226. Builds a free/total capacity
// summary by aggregating every registered StoragePool. The body is
// the upstream-exact `{"report_text": "..."}` shape; the python CLI
// only consumes `report_text` and renders it as a plain block.
//
// Format (one pool per line, then a CLUSTER total line so operators
// can answer "how much room is left?" without paging through every
// node):
//
//	Storage pool capacity report
//	  <pool> on <node>: free <X> MiB / total <Y> MiB
//	  ...
//	  CLUSTER TOTAL: free <sum> MiB / total <sum> MiB
//
// Capacity bytes are emitted in MiB (upstream's `free_capacity` is
// in KiB; we render in MiB so the body stays human-readable for the
// common multi-GiB pool case).
func (s *Server) handleSpaceReport(w http.ResponseWriter, r *http.Request) {
	pools, err := s.Store.StoragePools().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	var (
		b         strings.Builder
		totalFree int64
		totalSize int64
	)

	b.WriteString("Storage pool capacity report\n")

	for i := range pools {
		totalFree += pools[i].FreeCapacity
		totalSize += pools[i].TotalCapacity

		fmt.Fprintf(&b,
			"  %s on %s: free %d MiB / total %d MiB\n",
			pools[i].StoragePoolName, pools[i].NodeName,
			pools[i].FreeCapacity/kibPerMiB, pools[i].TotalCapacity/kibPerMiB)
	}

	fmt.Fprintf(&b, "CLUSTER TOTAL: free %d MiB / total %d MiB\n",
		totalFree/kibPerMiB, totalSize/kibPerMiB)

	writeJSON(w, http.StatusOK, spaceReport{ReportText: b.String()})
}

// resourceDefinitionSyncStatus mirrors upstream Java's
// JsonGenTypes.ResourceDefinitionSyncStatus — a single boolean
// `synced_on_all` reflecting whether every replica reports the
// healthy DRBD `UpToDate` state.
type resourceDefinitionSyncStatus struct {
	SyncedOnAll bool `json:"synced_on_all"`
}

// handleResourceDefinitionSyncStatus serves Bug 227. Validates the
// RD exists (404 otherwise), then walks every replica and
// short-circuits on the first non-`UpToDate` DRBD state. An RD with
// no replicas is reported as synced (vacuously true) — matches the
// upstream behaviour where `isResourceSynced` returns true for a
// zero-placement RD.
func (s *Server) handleResourceDefinitionSyncStatus(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	resources, err := s.Store.Resources().ListByDefinition(r.Context(), rdName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	synced := true

	for i := range resources {
		// An empty state means the satellite hasn't reported yet —
		// treat that as not-synced so the snapshot pre-check (which
		// is what `linstor rd sync-status` typically gates on)
		// doesn't run before the satellite has had a chance to
		// report `UpToDate`. Once every replica has reported
		// `UpToDate`, the response flips to true.
		if resources[i].State.DrbdState != diskStateUpToDate {
			synced = false

			break
		}
	}

	writeJSON(w, http.StatusOK, resourceDefinitionSyncStatus{SyncedOnAll: synced})
}

// satelliteConfigNet mirrors upstream Java's
// `JsonGenTypes.SatelliteConfigNet`. Drives the python CLI's
// `linstor node config` Network block.
type satelliteConfigNet struct {
	BindAddress string `json:"bind_address,omitempty"`
	Port        int    `json:"port,omitempty"`
	ComType     string `json:"com_type,omitempty"`
}

// satelliteConfigLog mirrors upstream Java's
// `JsonGenTypes.SatelliteConfigLog`. blockstor doesn't expose a
// per-satellite log-level adjustment yet — the fields surface as
// upstream defaults so the CLI's column rendering doesn't surface
// a blank row.
type satelliteConfigLog struct {
	Level        string `json:"level,omitempty"`
	LevelLinstor string `json:"level_linstor,omitempty"`
}

// satelliteConfig mirrors upstream Java's
// `JsonGenTypes.SatelliteConfig`. blockstor projects what the
// controller knows about the satellite — the node's primary
// NetInterface drives the `net` block; the `log` block carries
// blockstor-wide defaults; `special_satellite` is always false
// (no REMOTE_SPDK / EBS satellites in this build).
type satelliteConfig struct {
	Log              *satelliteConfigLog `json:"log,omitempty"`
	Net              *satelliteConfigNet `json:"net,omitempty"`
	SpecialSatellite bool                `json:"special_satellite,omitempty"`
}

// handleNodeConfig serves Bug 228. Loads the node (404 on miss),
// then projects the per-satellite configuration block the python CLI
// renders for `linstor node config <node>`. We don't run upstream's
// StltConfig push protocol — the `net` block is sourced from the
// node's primary NetInterface, the `log` block is a static blockstor
// default. Matches upstream's `getStltConfig` wire shape so the CLI
// doesn't need translation.
func (s *Server) handleNodeConfig(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	n, err := s.Store.Nodes().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	cfg := satelliteConfig{
		Log: &satelliteConfigLog{
			Level:        logLevelInfo,
			LevelLinstor: logLevelInfo,
		},
	}

	if len(n.NetInterfaces) > 0 {
		nif := n.NetInterfaces[0]
		cfg.Net = &satelliteConfigNet{
			BindAddress: nif.Address,
			Port:        nif.SatellitePort,
			ComType:     nif.SatelliteEncryptionType,
		}
	}

	writeJSON(w, http.StatusOK, cfg)
}

// handleStoragePoolDefinitionSingle serves Bug 229. Walks the
// StoragePool table for entries whose `StoragePoolName` matches the
// requested name (case-insensitive — matches upstream's
// `equalsIgnoreCase` filter), dedups by name, and returns the
// filtered slice in the same wire shape the list endpoint emits.
// 404 on an unknown name so `linstor sp-definition l <name>` surfaces
// a typed error instead of an empty-list confusion.
func (s *Server) handleStoragePoolDefinitionSingle(w http.ResponseWriter, r *http.Request) {
	want := r.PathValue("name")

	pools, err := s.Store.StoragePools().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	type storagePoolDefinition struct {
		StoragePoolName string            `json:"storage_pool_name"`
		Props           map[string]string `json:"props"`
	}

	for i := range pools {
		if !strings.EqualFold(pools[i].StoragePoolName, want) {
			continue
		}

		writeJSON(w, http.StatusOK, []storagePoolDefinition{{
			StoragePoolName: pools[i].StoragePoolName,
			Props:           map[string]string{},
		}})

		return
	}

	// Use the canonical sentinel-shaped 404 surface — the with404Envelope
	// middleware re-routes the bare http.StatusText to the LINSTOR
	// envelope shape; here we emit it directly via writeError so the
	// envelope carries the operator-actionable cause.
	writeError(w, http.StatusNotFound,
		"storage pool definition not found: "+want)
}
