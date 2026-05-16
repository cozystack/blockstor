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

// Resource is a single replica of a ResourceDefinition placed on a node.
// linstor-csi reads this heavily via /v1/view/resources during volume
// reconciliation.
//
// LayerObject is a single layer-stack descriptor — upstream LINSTOR's
// `layer_object` is a SINGLE ResourceLayer, not a list. The Python CLI
// dereferences `rsc.layer_data.layer_stack` unconditionally on
// `linstor r list`, so emitting nothing crashes the CLI with
// AttributeError.
type Resource struct {
	Name        string            `json:"name"`
	NodeName    string            `json:"node_name"`
	Props       map[string]string `json:"props,omitempty"`
	Flags       []string          `json:"flags,omitempty"`
	State       ResourceState     `json:"state,omitzero"`
	UUID        string            `json:"uuid,omitempty"`
	LayerObject *ResourceLayer    `json:"layer_object,omitempty"`

	// Annotations carries the K8s-native metadata.annotations of the
	// backing Resource CRD. Used by Bug 67's `PeerChangedAnnotation`
	// signal: the REST `handleResourceDelete` writer stamps a fresh
	// RFC3339Nano timestamp on every surviving sibling so the satellite
	// reconciler's local-Resource watch fires and re-derives the peer
	// set without the removed replica. Round-tripped through the K8s
	// store via Get/Update so the bump survives the Update merge.
	// Other annotation keys (operator-supplied, CSI metadata) round
	// trip too, but Resource-level annotations are otherwise unused on
	// the wire — Python CLI and golinstor both ignore the field.
	Annotations map[string]string `json:"annotations,omitempty"`

	// ToggleDiskCancel mirrors CRD `Spec.ToggleDiskCancel`. Set by
	// the REST shim when the operator issues `linstor r td --cancel`
	// (upstream LINSTOR shape) — the satellite reconciler observes
	// it and unwinds an in-flight diskless→diskful conversion.
	// Bug 40. The CSI driver never sets this; only the REST surface
	// flips it.
	ToggleDiskCancel bool `json:"toggle_disk_cancel,omitempty"`

	// Volumes is the per-replica volume slice — sourced from
	// Resource.Status.Volumes which the satellite observer writes.
	// Upstream LINSTOR's `Resource` carries volumes inline; the
	// Python CLI's rsc_state derivation reads
	// `rsc.volumes[i].state.disk_state` and gates the Conns column
	// + `--faulty` filter on whether at least one disk_state is
	// present. Without Volumes, every resource appears as "Unknown"
	// and Conns silently hides peer-connection state.
	//
	// Bug 137 follow-up: NO `omitempty` — diskless / TIE_BREAKER
	// replicas have no satellite-written Volumes rows (no local
	// backing storage), and python-linstor's `responses.py` reads
	// `rsc._rest_data['volumes']` unconditionally and walks the
	// entries. The wire contract is: the `volumes` key is ALWAYS
	// present, even if the slice is `[]`. Initial Bug 137 commit
	// kept `omitempty` and relied on slice-init to non-nil for
	// option (B); that doesn't help because Go's `omitempty`
	// considers a zero-length slice "empty" regardless of nil-ness,
	// so a fresh diskful replica before the satellite has reported
	// volumes (empty slice from the in-memory store) still loses
	// the wire key under `omitempty`.
	Volumes []Volume `json:"volumes"`

	// EffectiveProps is the merged Controller→RG→RD→Resource view
	// of the property bag for this replica. Populated on the
	// `/v1/view/resources` aggregate GET path; ignored on writes.
	// Drives the `(R)` inherited-property marker in
	// `linstor r lp <rd> <node>`.
	EffectiveProps EffectiveProperties `json:"effective_props,omitempty"`
}

// ResourceState is the runtime state surface of a Resource.
type ResourceState struct {
	// InUse is intentionally a pointer with no omitempty: upstream
	// LINSTOR (and the Python CLI's Usage column) read it as a
	// tri-state — true=Primary, false=Secondary, unset=satellite
	// hasn't reported yet. Plain bool with omitempty would always
	// serialize as "absent" for Secondary, which the CLI shows as
	// an empty Usage column instead of the expected `Unused`.
	InUse *bool `json:"in_use,omitempty"`

	// DrbdState is the current DRBD role/connection state observed by
	// the satellite via `drbdsetup events2` — `UpToDate`, `Outdated`,
	// `Connected`, `Failed`, etc. Phase 10.2: this lives in Status,
	// not Spec; satellite writes it via the Status subresource so a
	// concurrent Spec mutation (auto-diskful, resize) can't clobber
	// it and vice-versa.
	DrbdState string `json:"drbd_state,omitempty"`

	// ToggleDiskRetries is the satellite-incremented retry counter
	// for the in-flight diskless→diskful conversion on this replica.
	// Surfaces upstream LINSTOR's `Resource.toggle_disk_retries`
	// shape so `linstor r l` users can spot a permanently-stuck
	// toggle. 0 means either no conversion in flight or the last
	// conversion completed. Bug 39.
	ToggleDiskRetries int32 `json:"toggle_disk_retries,omitempty"`

	// Suspended is the per-replica "LUKS-stack blocked on master
	// passphrase" marker the REST /v1/view/resources view stamps
	// when the resource carries a LUKS layer but the controller
	// process has not yet been unlocked (passphrase Secret exists,
	// but the in-memory unlock flag is false — fresh controller
	// restart). The CLI surfaces this as Suspended in the State
	// column; once the operator PATCHes the master passphrase the
	// view flips to Suspended=false, rendered as Available.
	// Scenario 6.W13.
	//
	// Tri-state semantics on the wire (pointer, omitempty):
	//   nil   — no LUKS layer in the stack; field never surfaces.
	//   true  — LUKS layer present, controller still locked.
	//   false — LUKS layer present, controller unlocked.
	Suspended *bool `json:"suspended,omitempty"`
}

// ResourceWithVolumes is the shape `/v1/view/resources` returns — Resource
// plus the per-volume runtime details. linstor-csi expects this exact key
// (`volumes`) on the same level as the embedded Resource fields.
//
// Bug 137 follow-up: NO `omitempty` here either — Go's encoding/json
// picks the outermost tagged field when an embedded struct and the
// outer wrapper declare the same JSON name, so this is the field that
// actually wins on the wire. Mirrors the parent Resource.Volumes
// contract so the `volumes` key is always present (even as `[]`)
// regardless of replica flags or satellite-observation latency.
type ResourceWithVolumes struct {
	Resource

	Volumes []Volume `json:"volumes"`
}

// Volume is a single volume of a placed resource (replica) on a node.
//
// Bug 112: `allocated_size_kib` MUST always be emitted as an int —
// never absent, never null. The python CLI's `vlm.allocated_size`
// property is `_rest_data.get('allocated_size_kib')` (linstor/
// responses.py:1602), so a Go-zero collapsing to omitted under
// `omitempty` returned None to the caller, and
// `SizeCalc.approximate_size_string(None)` crashed `linstor n
// describe`. The wire contract is: present as int, default 0 when the
// satellite hasn't reported usage yet.
type Volume struct {
	VolumeNumber int32             `json:"volume_number"`
	StoragePool  string            `json:"storage_pool_name,omitempty"`
	DevicePath   string            `json:"device_path,omitempty"`
	AllocatedKib int64             `json:"allocated_size_kib"`
	UsableKib    int64             `json:"usable_size_kib,omitempty"`
	Props        map[string]string `json:"props,omitempty"`
	Flags        []string          `json:"flags,omitempty"`
	UUID         string            `json:"uuid,omitempty"`
	State        VolumeState       `json:"state,omitzero"`

	// LayerDataList is the per-volume layer-stack the Python CLI's
	// `volume_expects_disk_state` reads to decide whether the State
	// column should trust the observed `state.disk_state`. Without
	// at least one entry whose `type` is `DRBD`, the CLI falls back
	// to a literal "Created" regardless of what disk_state we set.
	// listMapKey type matches upstream's repeated layer_data shape.
	LayerDataList []VolumeLayerData `json:"layer_data_list,omitempty"`
}

// VolumeLayerData mirrors upstream LINSTOR's per-volume layer
// descriptor: a `type` (DRBD/STORAGE/LUKS/…) plus an opaque
// `data` blob (we leave it empty for now — the CLI only reads
// the type discriminator to gate the disk_state trust path).
type VolumeLayerData struct {
	Type string `json:"type"`
}

// VolumeState is the runtime state surface of a Volume.
type VolumeState struct {
	DiskState string `json:"disk_state,omitempty"`

	// CurrentGi is the DRBD-9 generation identifier reported by
	// `drbdsetup events2 --full` for this replica's local volume.
	// The controller reads it when adding a new replica to skip the
	// full initial-sync (Phase 8.1).
	CurrentGi string `json:"current_gi,omitempty"`

	// OutOfSyncKib is the worst-case "how many KiB this replica is
	// behind any peer" reported by `drbdsetup events2 --statistics`
	// peer-device frames. UI/CLI compute a sync-progress %:
	//   progress = (1 - OutOfSyncKib / VolumeDefinition.SizeKib) * 100
	// 0 means fully in sync.
	OutOfSyncKib int64 `json:"out_of_sync_kib,omitempty"`

	// ReplicationStates is the per-peer replication-state map the
	// Python CLI reads for the `linstor v list` Repl column. Keyed
	// by peer node name. When every peer is `Established`, the CLI
	// renders the column as `Established(N)`; non-uniform states
	// surface per-peer with optional sync-progress percentages.
	ReplicationStates map[string]ReplicationState `json:"replication_states,omitempty"`
}

// ReplicationState is one entry of VolumeState.ReplicationStates:
// the DRBD-9 replication state to a single peer plus an optional
// sync-progress percentage. Mirrors upstream LINSTOR's
// `ReplicationState` REST shape.
type ReplicationState struct {
	// ReplicationState is the DRBD-9 replication-state token:
	// `Established`, `SyncSource`, `SyncTarget`, `PausedSync*`,
	// `VerifyS/T`, `Ahead`, `Behind`, `Off`, `WFBitMap*`, etc.
	ReplicationState string `json:"replication_state,omitempty"`

	// DonePercentage is a float in [0, 100] giving the syncing
	// progress for `SyncTarget` / `PausedSync*` / `VerifyT` states.
	// nil for `Established` (nothing to sync) — the JSON `omitempty`
	// drops it when 0 too, which is fine: the CLI renders "?" when
	// the field is absent.
	DonePercentage *float64 `json:"done_percentage,omitempty"`
}

// VolumeObservation carries per-volume observed state propagated from
// the satellite's events2 observer into the store. Used by SetState
// to update per-volume Status fields without touching Spec.
type VolumeObservation struct {
	VolumeNumber int32
	State        VolumeState
}
