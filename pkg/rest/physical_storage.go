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
	"strings"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Canonical upstream-LINSTOR `DeviceProviderKind` enum values. Mirrors
// `server/generated-src/com/linbit/linstor/storage/kinds/
// DeviceProviderKind.java` and the StoragePool CRD's `providerKind`
// enum. Bug 88 + Bug 73 ride these constants so per-kind dispatch in
// `fillAttachToFromPoolName` and `normalizeProviderKind` can't drift.
const (
	providerKindLVM      = "LVM"
	providerKindLVMThin  = "LVM_THIN"
	providerKindZFS      = "ZFS"
	providerKindZFSThin  = "ZFS_THIN"
	providerKindFile     = "FILE"
	providerKindFileThin = "FILE_THIN"
	providerKindDiskless = "DISKLESS"
)

// physicalStorageCDPRunbookNotice is the wave2 scenario 6.W09 operator-
// runbook advisory: the CDP one-shot runs pvcreate + vgcreate + lvcreate
// --thinpool + LINSTOR pool register, but the OS-level VG / thin LV are
// NOT managed by LINSTOR afterwards — `linstor sp delete` clears only
// the controller record. Operators must `vgremove` / `zpool destroy` /
// `wipefs` on the host themselves before re-using the device. Surfaced
// as an ApiCallRc warning entry in the 202 response body so the python
// CLI prints it in the standard "WARNING:" log line alongside the
// success entry, and so audit-log greppers catch the line under the
// blockstor-convention warn band.
const physicalStorageCDPRunbookNotice = "WARNING: OS-level VG / thin LV are NOT auto-managed; " +
	"`linstor sp delete` will not run vgremove/zpool destroy/wipefs on the host. " +
	"Operator must clean up the backing device before re-using it. " +
	"See operator runbook: day2-storage-pool-physical-create-device-pool."

// registerPhysicalStorage wires the `linstor physical-storage`
// endpoints. Phase 10.7: the GET endpoints now surface
// PhysicalDevice CRDs from the store; the cluster-wide list groups
// devices by (node, size, rotational) per upstream LINSTOR's
// PhysicalStorage shape. The POST create-device-pool path stays
// 501 until the satellite-side reconciler lands — operators can
// already trigger attach by editing PhysicalDevice.Spec.AttachTo
// directly via `kubectl edit`.
func (s *Server) registerPhysicalStorage(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/physical-storage",
		s.requireStore(s.handlePhysicalStorageList))
	mux.HandleFunc("GET /v1/nodes/{node}/physical-storage",
		s.requireStore(s.handlePhysicalStorageListForNode))
	mux.HandleFunc("POST /v1/physical-storage/{node}",
		s.requireStore(s.handlePhysicalStorageCreate))
}

// physicalStorageCreateRequest mirrors upstream golinstor's
// `PhysicalStorageCreate`. We accept only the subset blockstor
// can interpret without VDO / RAID / SED — those upstream knobs
// silently get ignored and the attach falls through to a plain
// pool create. piraeus-operator only sets the simple subset, so
// this lossy mapping is fine in practice.
type physicalStorageCreateRequest struct {
	DevicePaths     []string                          `json:"device_paths"`
	ProviderKind    string                            `json:"provider_kind"`
	PoolName        string                            `json:"pool_name,omitempty"`
	WithStoragePool *physicalStorageCreatePoolDetails `json:"with_storage_pool,omitempty"`
}

// physicalStorageCreatePoolDetails carries optional pool-side
// parameters the operator passes alongside the device list.
type physicalStorageCreatePoolDetails struct {
	Name  string            `json:"name,omitempty"`
	Props map[string]string `json:"props,omitempty"`
}

// handlePhysicalStorageCreate is the Phase 10.7 attach trigger.
// Decodes the upstream-LINSTOR `PhysicalStorageCreate` envelope,
// finds matching PhysicalDevice CRDs on the named node by their
// `Status.DevicePath`, and flips `Spec.AttachTo` so the satellite
// reconciler picks them up on its next pass.
//
// Returns 404 when none of the requested device paths match a
// free PhysicalDevice on this node — surfacing the failure
// rather than silently succeeding lets piraeus-operator retry
// after the discovery loop catches up.
func (s *Server) handlePhysicalStorageCreate(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	req, ok := decodePhysicalStorageCreateRequest(w, r)
	if !ok {
		return
	}

	devs, err := s.Store.PhysicalDevices().ListForNode(r.Context(), node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	target := pickFreeDeviceForAttach(devs, req.DevicePaths)
	if target == nil {
		writeError(w, http.StatusNotFound,
			"no free PhysicalDevice on node "+node+" matches device_paths ["+strings.Join(req.DevicePaths, " ")+"]")

		return
	}

	target.AttachTo = buildAttachTo(&req)

	// Phase 10.7 step 2 (controller-side pool create): if the
	// target StoragePool CRD doesn't exist yet, create it from
	// the request envelope so the satellite's PhysicalDevice
	// reconciler doesn't sit in `PoolMissing` until an operator
	// applies the pool separately. `with_storage_pool.props`
	// carries the provider-specific config (vg name, thin pool
	// name, zpool name) the satellite's NewProviderFromKind
	// reads. Lossy on race with a parallel `kubectl apply -f
	// storagepool.yaml` — store.ErrAlreadyExists is treated as
	// success (the existing CRD wins, the operator's intent
	// rather than the CDP request's wins).
	err = ensureStoragePoolForAttach(r.Context(), s.Store, node, target.AttachTo, &req)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = s.Store.PhysicalDevices().Update(r.Context(), target)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Stamp the PhysicalDevice as an OwnerReference child of the
	// target StoragePool when the apiserver client is wired
	// (production path). Cascade-delete on the StoragePool then
	// reaps orphaned PhysicalDevices via Kubernetes GC — useful
	// when an operator tears down a pool while some device
	// attaches are stalled in `Phase=Failed`. Best-effort: an
	// error here doesn't roll back the AttachTo flip; the
	// satellite's reconciler still completes the attach and the
	// missing OwnerReference is a recoverable papercut. Phase
	// 10.7 cascade-delete contract.
	_ = setStoragePoolOwnership(r.Context(), s.Client, target.Name, target.AttachTo.StoragePoolName)

	writePhysicalStorageCreateAccepted(w, node)
}

// decodePhysicalStorageCreateRequest pulls the upstream-LINSTOR
// `PhysicalStorageCreate` envelope off the request, validates the
// required fields, and normalises `provider_kind` per Bug 73 so every
// CLI variant (lowercase, compressed `lvmthin` / `zfsthin` / `filethin`)
// lands as the canonical uppercase token. Writes the appropriate 400
// response and returns ok=false on the first failure; the caller bails
// without touching the store.
func decodePhysicalStorageCreateRequest(w http.ResponseWriter, r *http.Request) (physicalStorageCreateRequest, bool) {
	var req physicalStorageCreateRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return req, false
	}

	if req.ProviderKind == "" {
		writeError(w, http.StatusBadRequest, "provider_kind is required")

		return req, false
	}

	normalized, ok := normalizeProviderKind(req.ProviderKind)
	if !ok {
		writeError(w, http.StatusBadRequest,
			"provider_kind: unknown value "+req.ProviderKind)

		return req, false
	}

	req.ProviderKind = normalized

	if len(req.DevicePaths) == 0 {
		writeError(w, http.StatusBadRequest, "device_paths is required")

		return req, false
	}

	return req, true
}

// writePhysicalStorageCreateAccepted writes the 202 Accepted reply for
// POST /v1/physical-storage/<node>. The body mirrors upstream's
// `Flux<ApiCallRc>`: a SUCCESS line for the controller-side attach
// record + a WARNING with the wave2 6.W09 operator-runbook note. The
// python-linstor CLI walks the list and prints both, so operators see
// the "OS-level VG / thin LV NOT auto-managed" advisory at the time
// they run `linstor ps cdp` rather than discovering it during teardown.
// Location header points back at the per-node list endpoint so clients
// (golinstor, piraeus-operator) can poll for completion by waiting for
// the matching PhysicalDevice CRD to disappear (success) or report
// `Status.Phase=Failed`. Phase 10.7 + wave2 6.W09.
func writePhysicalStorageCreateAccepted(w http.ResponseWriter, node string) {
	w.Header().Set("Location", "/v1/nodes/"+node+"/physical-storage")
	writeJSON(w, http.StatusAccepted, []apiv1.APICallRc{
		{
			RetCode: maskInfo,
			Message: "physical-storage attach accepted on node '" + node + "'",
		},
		{
			RetCode: maskWarn,
			Message: physicalStorageCDPRunbookNotice,
		},
	})
}

// setStoragePoolOwnership wires a PhysicalDevice CRD as an
// OwnerReference child of its target StoragePool. The
// `client.Client` field on `Server` is populated by `cmd/controller/main.go`
// in production but stays nil in pure-store tests — both this
// helper and the caller handle the nil case as no-op. Looked up
// across the cluster-scoped pool list since we don't yet plumb
// (node, name) → CRD-name through the store API.
func setStoragePoolOwnership(ctx context.Context, c ctrlclient.Client, deviceName, poolName string) error {
	if c == nil || deviceName == "" || poolName == "" {
		return nil
	}

	var dev crdv1alpha1.PhysicalDevice

	err := c.Get(ctx, ctrlclient.ObjectKey{Name: deviceName}, &dev)
	if err != nil {
		return errors.Wrap(err, "get PhysicalDevice for ownership")
	}

	var pools crdv1alpha1.StoragePoolList

	err = c.List(ctx, &pools)
	if err != nil {
		return errors.Wrap(err, "list StoragePool for ownership")
	}

	pool := findStoragePoolByName(&pools, dev.Labels[crdv1alpha1.PhysicalDeviceLabelNode], poolName)
	if pool == nil {
		return errors.Errorf("StoragePool %q not found for ownership", poolName)
	}

	if hasOwnerReference(dev.OwnerReferences, pool.UID) {
		return nil
	}

	dev.OwnerReferences = append(dev.OwnerReferences, metav1.OwnerReference{
		APIVersion: crdv1alpha1.GroupVersion.String(),
		Kind:       "StoragePool",
		Name:       pool.Name,
		UID:        pool.UID,
	})

	err = c.Update(ctx, &dev)
	if err != nil {
		return errors.Wrap(err, "stamp OwnerReference")
	}

	return nil
}

// findStoragePoolByName picks the cluster-scoped StoragePool CRD
// matching (node, pool-name). Phase 10.7 ownership wiring lookup.
func findStoragePoolByName(pools *crdv1alpha1.StoragePoolList, nodeName, poolName string) *crdv1alpha1.StoragePool {
	for i := range pools.Items {
		p := &pools.Items[i]
		if p.Spec.NodeName == nodeName && p.Spec.PoolName == poolName {
			return p
		}
	}

	return nil
}

// hasOwnerReference returns true when refs already contains an
// OwnerReference for the given UID — idempotency probe for
// setStoragePoolOwnership.
func hasOwnerReference(refs []metav1.OwnerReference, uid types.UID) bool {
	for i := range refs {
		if refs[i].UID == uid {
			return true
		}
	}

	return false
}

// pickFreeDeviceForAttach finds the first PhysicalDevice whose
// `Status.DevicePath` (or `Status.CurrentDevPath`, as a fallback
// since some operators pass volatile /dev/sdN paths) appears in
// the requested device_paths list. Skips devices that are already
// being attached or assigned. Returns nil when no match.
//
// Note: this picks the first match deterministically by store
// order (which the in-memory + k8s impls both stable-sort by
// name). Multi-device pool requests trigger one PhysicalDevice
// flip per device — piraeus-operator already POSTs per-device
// in practice.
func pickFreeDeviceForAttach(devs []apiv1.PhysicalDevice, paths []string) *apiv1.PhysicalDevice {
	want := map[string]bool{}
	for _, p := range paths {
		want[p] = true
	}

	for i := range devs {
		dev := &devs[i]
		if dev.AttachTo != nil {
			continue
		}

		if dev.Phase != "" && dev.Phase != crdv1alpha1.PhysicalDevicePhaseAvailable {
			continue
		}

		if want[dev.DevicePath] || want[dev.CurrentDevPath] {
			return dev
		}
	}

	return nil
}

// ensureStoragePoolForAttach creates a StoragePool CRD that
// matches the inbound attach request if one doesn't already
// exist on the named node. Phase 10.7 step 2.
//
// The pool's Spec.Props carries the provider-specific config
// (StorDriver/LvmVg, StorDriver/ThinPool, StorDriver/ZPool,
// StorDriver/FileDir) the satellite's NewProviderFromKind
// reads. We pull those from `with_storage_pool.props`
// (operator-supplied) and fall back to mirroring the AttachTo
// fields so a minimal request without a `with_storage_pool`
// block still produces a working pool.
//
// store.ErrAlreadyExists on Create is treated as success — a
// parallel `kubectl apply -f storagepool.yaml` already won the
// race and the existing CRD takes precedence over the CDP
// request's inferred config.
func ensureStoragePoolForAttach(ctx context.Context, st store.Store, node string, attach *apiv1.PhysicalDeviceAttachTo, req *physicalStorageCreateRequest) error {
	if attach == nil || attach.StoragePoolName == "" {
		return nil
	}

	_, err := st.StoragePools().Get(ctx, node, attach.StoragePoolName)
	if err == nil {
		return nil
	}

	if !errors.Is(err, store.ErrNotFound) {
		return errors.Wrap(err, "lookup StoragePool")
	}

	pool := &apiv1.StoragePool{
		StoragePoolName: attach.StoragePoolName,
		NodeName:        node,
		ProviderKind:    attach.ProviderKind,
		Props:           buildPoolPropsForAttach(attach, req),
	}

	err = st.StoragePools().Create(ctx, pool)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil
		}

		return errors.Wrap(err, "create StoragePool")
	}

	return nil
}

// buildPoolPropsForAttach assembles the per-pool Props bag from
// the operator's `with_storage_pool.props` block, filling in
// provider-specific defaults from `AttachTo` when the operator
// didn't supply them. The satellite's NewProviderFromKind reads
// these keys to instantiate the right provider.
//
// Bug 88: ZFS_THIN reads from `StorDriver/ZPoolThin` (factory.go
// `newZFS`); ZFS reads from `StorDriver/ZPool`. We dispatch on
// `attach.ProviderKind` so the pool_name-fallback inferred in
// `fillAttachToFromPoolName` lands under the kind-specific key the
// satellite probe expects, not the cross-kind alias.
func buildPoolPropsForAttach(attach *apiv1.PhysicalDeviceAttachTo, req *physicalStorageCreateRequest) map[string]string {
	props := map[string]string{}

	if req.WithStoragePool != nil {
		maps.Copy(props, req.WithStoragePool.Props)
	}

	if attach.VGName != "" && props[propLvmVG] == "" {
		props[propLvmVG] = attach.VGName
	}

	if attach.ThinPoolName != "" && props[propThinPool] == "" {
		props[propThinPool] = attach.ThinPoolName
	}

	if attach.ZPoolName != "" {
		zKey := propZPool
		if attach.ProviderKind == providerKindZFSThin {
			zKey = propZPoolThin
		}

		if props[zKey] == "" {
			props[zKey] = attach.ZPoolName
		}
	}

	if attach.Directory != "" && props[propFileDir] == "" {
		props[propFileDir] = attach.Directory
	}

	return props
}

// buildAttachTo turns the upstream PhysicalStorageCreate envelope
// into the typed AttachTo our PhysicalDevice CRD spec carries.
// The pool name comes from `with_storage_pool.name` when present
// (the upstream-shaped payload), falling back to `pool_name` for
// callers that pass it at the top level. Provider-kind-specific
// fields (`vg_name`, `thin_pool`, `zpool`, `directory`) come from
// the props bag the operator may have included.
//
// Bug 88: when the operator runs `linstor ps cdp --pool-name X`
// without `with_storage_pool.props`, the python-linstor CLI passes
// the OS-level pool name as `pool_name` at the top level only — it
// does NOT auto-populate `with_storage_pool.props[StorDriver/*]`.
// Without inferring the kind-specific field from `pool_name`, the
// resulting StoragePool CRD lands with empty Props so the satellite
// provider rejects every reconcile with `attach requires <key>`
// (`ZFS attach requires ZPoolName` etc.), and Status.{FreeCapacity,
// TotalCapacity, SupportsSnapshots} never get probed.
// Mirrors upstream LINSTOR's `deviceProviderToStorPoolProperty`
// (`controller/.../api/rest/v1/PhysicalStorage.java`) which builds
// the same per-kind defaults from `getDevicePoolName(pool_name, ...)`
// when the operator omits `with_storage_pool.props`.
func buildAttachTo(req *physicalStorageCreateRequest) *apiv1.PhysicalDeviceAttachTo {
	out := &apiv1.PhysicalDeviceAttachTo{
		ProviderKind: req.ProviderKind,
	}

	if req.WithStoragePool != nil && req.WithStoragePool.Name != "" {
		out.StoragePoolName = req.WithStoragePool.Name
	} else {
		out.StoragePoolName = req.PoolName
	}

	if req.WithStoragePool != nil {
		out.VGName = req.WithStoragePool.Props["StorDriver/LvmVg"]
		out.ThinPoolName = req.WithStoragePool.Props["StorDriver/ThinPool"]
		out.ZPoolName = req.WithStoragePool.Props["StorDriver/ZPool"]
		out.Directory = req.WithStoragePool.Props["StorDriver/FileDir"]
	}

	// Bug 88: per-kind fallback to req.PoolName when the operator
	// didn't pre-populate `with_storage_pool.props` (the common case
	// for `linstor ps cdp --pool-name X` without a manifest). Treat
	// req.PoolName as the canonical OS-level pool identifier and
	// derive the provider-specific field from it. LVM_THIN parses
	// `vg/thin` and `bare` per upstream `LvmThinDriverKind.VGName`
	// / `LVName` — bare names get a `linstor_` VG prefix.
	fillAttachToFromPoolName(out, req.ProviderKind, req.PoolName)

	return out
}

// fillAttachToFromPoolName populates the kind-specific AttachTo
// fields from the operator-passed `--pool-name` value when
// `with_storage_pool.props` left them empty. Bug 88.
func fillAttachToFromPoolName(out *apiv1.PhysicalDeviceAttachTo, kind, poolName string) {
	if poolName == "" {
		return
	}

	switch kind {
	case providerKindLVM:
		if out.VGName == "" {
			out.VGName = poolName
		}
	case providerKindLVMThin:
		vgName, thinLV := splitLvmThinPoolName(poolName)
		if out.VGName == "" {
			out.VGName = vgName
		}

		if out.ThinPoolName == "" {
			out.ThinPoolName = thinLV
		}
	case providerKindZFS, providerKindZFSThin:
		if out.ZPoolName == "" {
			out.ZPoolName = poolName
		}
	case providerKindFile, providerKindFileThin:
		if out.Directory == "" {
			out.Directory = poolName
		}
	}
}

// splitLvmThinPoolName mirrors upstream LINSTOR's
// `LvmThinDriverKind.VGName` / `LVName`: a `vg/thin`-shaped pool
// name splits on the slash, a bare name gets the `linstor_` VG
// prefix and uses the bare name as the thin LV. Bug 88.
func splitLvmThinPoolName(poolName string) (string, string) {
	if before, after, ok := strings.Cut(poolName, "/"); ok {
		return before, after
	}

	return "linstor_" + poolName, poolName
}

// physicalStorageEntry is the envelope golinstor expects on
// GET /v1/physical-storage — devices grouped by attributes (size,
// rotational), with each group carrying the per-node list of
// devices in that bucket.
type physicalStorageEntry struct {
	Size       int64                                            `json:"size,omitempty"`
	Rotational *bool                                            `json:"rotational,omitempty"`
	Nodes      map[string][]physicalStorageDeviceWireRepetition `json:"nodes,omitempty"`
}

// physicalStorageDeviceWireRepetition mirrors upstream's
// PhysicalStorageDevice (slimmer than our internal apiv1.PhysicalDevice).
type physicalStorageDeviceWireRepetition struct {
	Device string `json:"device,omitempty"`
	Model  string `json:"model,omitempty"`
	Serial string `json:"serial,omitempty"`
	WWN    string `json:"wwn,omitempty"`
}

// handlePhysicalStorageList groups all available PhysicalDevices
// (Phase=Available, AttachTo=nil) by (size, rotational) and
// returns them in the upstream-LINSTOR envelope shape.
func (s *Server) handlePhysicalStorageList(w http.ResponseWriter, r *http.Request) {
	devs, err := s.Store.PhysicalDevices().List(r.Context())
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, groupPhysicalDevices(filterAvailable(devs)))
}

// handlePhysicalStorageListForNode returns the per-node devices
// list — the bucket map for a single node, flattened. piraeus-
// operator uses this to know which devices a satellite has free.
func (s *Server) handlePhysicalStorageListForNode(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	devs, err := s.Store.PhysicalDevices().ListForNode(r.Context(), node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	out := make([]physicalStorageDeviceWireRepetition, 0, len(devs))

	for i := range devs {
		if devs[i].Phase != "" && devs[i].Phase != crdv1alpha1.PhysicalDevicePhaseAvailable {
			continue
		}

		if devs[i].AttachTo != nil {
			continue
		}

		out = append(out, physicalStorageDeviceWireRepetition{
			Device: devs[i].DevicePath,
			Model:  devs[i].Model,
			Serial: devs[i].Serial,
			WWN:    devs[i].StableID,
		})
	}

	writeJSON(w, http.StatusOK, out)
}

// filterAvailable drops PhysicalDevices that are Attaching/Failed
// or already have an AttachTo target. Available + AttachTo=nil is
// the "free for new pool" signal upstream cares about.
func filterAvailable(devs []apiv1.PhysicalDevice) []apiv1.PhysicalDevice {
	out := make([]apiv1.PhysicalDevice, 0, len(devs))

	for i := range devs {
		if devs[i].Phase != "" && devs[i].Phase != crdv1alpha1.PhysicalDevicePhaseAvailable {
			continue
		}

		if devs[i].AttachTo != nil {
			continue
		}

		out = append(out, devs[i])
	}

	return out
}

// groupPhysicalDevices buckets devices by (size, rotational) and
// emits each bucket as a physicalStorageEntry. The shape matches
// upstream's grouping so `linstor physical-storage list` displays
// the same per-bucket counts blockstor users would see in vanilla
// LINSTOR.
func groupPhysicalDevices(devs []apiv1.PhysicalDevice) []physicalStorageEntry {
	type bucketKey struct {
		size       int64
		rotational bool
		hasRotaSet bool
	}

	buckets := map[bucketKey]*physicalStorageEntry{}

	for i := range devs {
		key := bucketKey{size: devs[i].SizeBytes}
		if devs[i].Rotational != nil {
			key.rotational = *devs[i].Rotational
			key.hasRotaSet = true
		}

		entry, ok := buckets[key]
		if !ok {
			entry = &physicalStorageEntry{Size: devs[i].SizeBytes, Nodes: map[string][]physicalStorageDeviceWireRepetition{}}

			if key.hasRotaSet {
				rota := key.rotational
				entry.Rotational = &rota
			}

			buckets[key] = entry
		}

		entry.Nodes[devs[i].NodeName] = append(entry.Nodes[devs[i].NodeName],
			physicalStorageDeviceWireRepetition{
				Device: devs[i].DevicePath,
				Model:  devs[i].Model,
				Serial: devs[i].Serial,
				WWN:    devs[i].StableID,
			})
	}

	out := make([]physicalStorageEntry, 0, len(buckets))
	for _, entry := range buckets {
		out = append(out, *entry)
	}

	return out
}

// normalizeProviderKind maps a CLI-typed provider name to the
// canonical upstream-LINSTOR enum the StoragePool CRD allows.
// Handles every variant the python-linstor CLI emits (lowercase,
// compressed `lvmthin` / `zfsthin` / `filethin`) plus the already-
// canonical uppercase tokens. Returns false for anything else so
// the handler surfaces a 400 instead of letting apiserver reject
// the create with an opaque enum error.
//
// Mirrors upstream's `DeviceProviderKind.valueOfIgnoreCase` —
// shape sourced from linstor-server's
// `controller/src/main/java/com/linbit/linstor/storage/kinds/
// DeviceProviderKind.java` and python-linstor's `consts.py`.
func normalizeProviderKind(raw string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case providerKindLVM:
		return providerKindLVM, true
	case "LVMTHIN", providerKindLVMThin:
		return providerKindLVMThin, true
	case providerKindZFS:
		return providerKindZFS, true
	case "ZFSTHIN", providerKindZFSThin:
		return providerKindZFSThin, true
	case providerKindFile:
		return providerKindFile, true
	case "FILETHIN", providerKindFileThin:
		return providerKindFileThin, true
	case providerKindDiskless:
		return providerKindDiskless, true
	default:
		return "", false
	}
}
