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

package controllers

import (
	"context"
	"regexp"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// zvolKNamePattern matches ZFS volume devices' kernel names
// (zd0, zd16, zd32, …). They surface as TYPE=disk in lsblk
// output but are not pool candidates — they're the block-device
// face of an existing zvol on this node and excluding them keeps
// `linstor ps l` showing only real free disks. Bug 70.
var zvolKNamePattern = regexp.MustCompile(`^zd\d+$`)

// isZVOLKName reports whether the kernel name belongs to a ZFS
// volume device. Used by publishDevice to skip zvols before the
// signature-probe fan-out so they never produce a PhysicalDevice
// CRD. Bug 70.
func isZVOLKName(kname string) bool {
	return zvolKNamePattern.MatchString(kname)
}

// PhysicalDeviceDiscoveryPeriod is the cadence at which the
// satellite re-scans `lsblk` and publishes PhysicalDevice CRDs
// for free block devices on this node. 60s matches the user-facing
// expectation that `linstor ps l` shows freshly wiped disks "within
// a minute" without flooding the apiserver with no-op writes on a
// quiescent host.
const PhysicalDeviceDiscoveryPeriod = 60 * time.Second

// PhysicalDeviceDiscoveryFieldOwner is the SSA field-manager the
// discovery runnable uses for its Status writes. Distinct from
// the satellite-reconciler owner so `kubectl get physicaldevice
// -o yaml` cleanly attributes Status writes between "discovery"
// (filled by lsblk) and "attach" (filled by the reconciler when
// the operator sets Spec.AttachTo).
const PhysicalDeviceDiscoveryFieldOwner = "blockstor-satellite-discovery"

// physicalDeviceDiscoveryConditionFree is the Status.Conditions[type]
// the discovery runnable stamps to record whether the device passed
// the IsDeviceFree probe on the most recent scan. Operators reading
// `kubectl get physicaldevice ... -o yaml` see `Free=True` for
// publishable disks and `Free=False` (with `Reason=SignatureFound`)
// for disks that lsblk says are unused but `wipefs -n` / `pvs` /
// `zpool` / `drbdmeta` say carry a signature.
//
// Re-exported as `blockstoriov1alpha1.PhysicalDeviceConditionFree`
// so the REST layer (Bug 89) can read the same constant when it
// translates the CRD into the wire `apiv1.PhysicalDevice` shape.
const physicalDeviceDiscoveryConditionFree = blockstoriov1alpha1.PhysicalDeviceConditionFree

// PhysicalDeviceDiscoveryRunnable scans local block devices on a
// periodic tick (PhysicalDeviceDiscoveryPeriod) and publishes one
// PhysicalDevice CRD per free `TYPE=disk` row from `lsblk`. The
// CRD's name is `<node>.<stable-id>` per the convention shared
// with Resource / StoragePool / Snapshot — operators can grep
// across kinds by node prefix.
//
// Bug 51: before this runnable existed, `linstor ps l` returned
// an empty list even after `wipefs -a /dev/sdb` because nothing
// on the satellite published PhysicalDevice CRDs. The REST shim
// `pkg/rest/physical_storage.go` already aggregates whatever is
// in the store — this runnable fills the store.
//
// Lifecycle:
//   - Start fires one scan immediately so a freshly-started
//     satellite surfaces free disks within seconds (not one full
//     period later).
//   - Each subsequent tick re-scans and refreshes Status on
//     existing PhysicalDevice CRDs; if `wipefs -n` now reports a
//     signature (e.g. operator just `pvcreate`d the device), the
//     CRD's `Free=False` Condition flips.
//   - Devices that disappear from `lsblk` between ticks (drive
//     physically removed, kname renumbered) get their CRD
//     deleted unless `Spec.AttachTo` is set (an attach is in
//     flight — let the attach-side reconciler own the lifecycle).
//
// Per-node scope: the runnable only touches CRDs labelled with
// its own NodeName (mirrors the per-CRD-reconciler
// `physicalDeviceNodePredicate` filter).
type PhysicalDeviceDiscoveryRunnable struct {
	Client   client.Client
	Exec     storage.Exec
	NodeName string

	// Period overrides PhysicalDeviceDiscoveryPeriod (test-only —
	// production uses the default constant). A zero Period falls
	// back.
	Period time.Duration
}

// NeedLeaderElection reports that this runnable does NOT need
// leader election — every satellite must publish its own local
// PhysicalDevices independently. Leader election would pick one
// pod to enumerate every node's disks which is structurally
// wrong (each host's block devices are opaque to peers).
func (*PhysicalDeviceDiscoveryRunnable) NeedLeaderElection() bool { return false }

// Start runs the discovery loop until ctx cancels. Errors during
// individual scans are logged but never abort the loop — a
// transient apiserver / lsblk hiccup must not take the satellite
// out of service. The first scan fires immediately so a freshly-
// started satellite has its free disks visible within seconds.
func (p *PhysicalDeviceDiscoveryRunnable) Start(ctx context.Context) error {
	period := p.Period
	if period == 0 {
		period = PhysicalDeviceDiscoveryPeriod
	}

	logger := log.FromContext(ctx).WithName("physicaldevice-discovery").WithValues("node", p.NodeName)

	err := p.scanOnce(ctx, logger)
	if err != nil {
		logger.Error(err, "initial PhysicalDevice scan")
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err = p.scanOnce(ctx, logger)
			if err != nil {
				logger.Error(err, "PhysicalDevice scan cycle")
			}
		}
	}
}

// RegisterWithManager adds the runnable to mgr. Symmetrical with
// HeartbeatRunnable.RegisterWithManager so the wiring shape in
// addBackgroundRunnables stays consistent.
func (p *PhysicalDeviceDiscoveryRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(p)
	if err != nil {
		return errors.Wrap(err, "add PhysicalDeviceDiscoveryRunnable")
	}

	return nil
}

// scanOnce performs exactly one discovery cycle:
//  1. `lsblk -Pb` to enumerate every block device on the host.
//  2. For each TYPE=disk row, run IsDeviceFree (which composes
//     lsblk filter + pvs + zpool + drbdmeta + wipefs probes).
//  3. SSA-Apply a PhysicalDevice CRD per discovered device,
//     stamping `Free=True/False` on Status.Conditions.
//  4. Diff against the existing per-node PhysicalDevice list;
//     delete CRDs for devices that no longer appear in lsblk
//     UNLESS Spec.AttachTo is set (attach in flight — the
//     PhysicalDeviceReconciler owns the lifecycle).
//
// Exposed (lowercase) for unit tests pinning a single tick.
func (p *PhysicalDeviceDiscoveryRunnable) scanOnce(ctx context.Context, logger logr.Logger) error {
	rows, err := satellite.Lsblk(ctx, p.Exec)
	if err != nil {
		return errors.Wrap(err, "lsblk")
	}

	// Bug 89: pre-scan the full lsblk output to learn which parent
	// disks have any in-use child (mounted partition, FS-bearing
	// partition, LVM child, MD member). A disk with a mounted
	// `vda4` partition MUST land as Free=False even though `vda`
	// itself has no FSType / no Mountpoint — otherwise `ps cdp`
	// happily attaches the disk and corrupts the running root FS.
	busyChildren := collectBusyChildren(rows)

	discovered := map[string]struct{}{}

	for i := range rows {
		row := rows[i]
		if row.Type != satellite.LsblkTypeDisk {
			continue
		}

		// Bug 72: DRBD devices (kernel major 147) are TYPE=disk
		// with no FSType on the device itself — they pass the
		// signature probes and would be surfaced as "free" for
		// wipe. Exclude them here, mirroring upstream LINSTOR's
		// LsBlkUtils.filterDeviceCandidates.
		if row.Major == satellite.MajorDRBD {
			continue
		}

		free, freeReason, freeMessage, signatureErr := p.probeFree(ctx, &row, busyChildren[row.KName])
		if signatureErr != nil {
			// One device's probe failing shouldn't sink the whole
			// scan — log and move on so the other disks still get
			// surfaced. The next tick re-tries.
			logger.Error(signatureErr, "IsDeviceFree probe", "device", row.KName)

			continue
		}

		name, ok := p.publishDeviceWithReason(ctx, logger, &row, free, freeReason, freeMessage)
		if ok {
			discovered[name] = struct{}{}
		}
	}

	p.pruneDisappeared(ctx, logger, discovered)

	return nil
}

// probeFree runs the standard `IsDeviceFree` cross-checks on the
// row, then applies the Bug 89 parent-child guard: a disk with any
// in-use child (mounted partition / partition with FSType / LVM
// child / md member) is stamped Free=False even when its own
// signature probes come back clean, so `ps l` and `ps cdp` agree
// that the disk isn't safe to wipe.
//
// Returns (free, reason, message, err). Reason/Message map onto
// the Status.Conditions[Free].Reason / Message the REST layer
// quotes verbatim in the 409 envelope on `ps cdp` rejection.
func (p *PhysicalDeviceDiscoveryRunnable) probeFree(ctx context.Context, row, busyChild *satellite.LsblkDevice) (bool, string, string, error) {
	free, err := satellite.IsDeviceFree(ctx, p.Exec, row)
	if err != nil {
		return false, "", "", errors.Wrap(err, "IsDeviceFree")
	}

	if !free {
		return false, discoveryReasonSignatureFound, discoveryMessageSignatureFound, nil
	}

	if busyChild != nil {
		return false, discoveryReasonChildInUse, formatChildInUseMessage(row.KName, busyChild), nil
	}

	return true, discoveryReasonFreeBlockDevice, discoveryMessageFreeBlockDevice, nil
}

// Reason / message strings for the Status.Conditions[Free] entry.
// Surfaced verbatim by the REST shim in the Bug 89 `ps cdp`
// rejection envelope, so operators see the same wording regardless
// of which surface they hit. Centralised here so a future copy
// edit happens in one place rather than every test pinning its
// own substring.
const (
	discoveryReasonFreeBlockDevice  = "FreeBlockDevice"
	discoveryMessageFreeBlockDevice = "device passed lsblk + signature probes"
	discoveryReasonSignatureFound   = "SignatureFound"
	discoveryMessageSignatureFound  = "lsblk / pvs / zpool / drbdmeta / wipefs detected an on-disk signature"
	discoveryReasonChildInUse       = "ChildInUse"
)

// formatChildInUseMessage renders the Bug 89 Status.Conditions[Free].Message
// for a parent disk whose child partition is mounted / formatted /
// LVM-owned. Includes the child kname + the specific in-use
// signal so operators see exactly which child is blocking.
func formatChildInUseMessage(parent string, busyChild *satellite.LsblkDevice) string {
	what := "in use"

	switch {
	case busyChild.Mountpoint != "":
		what = "mounted at " + busyChild.Mountpoint
	case busyChild.FSType != "":
		what = "carries signature " + busyChild.FSType
	}

	return "parent disk /dev/" + parent + " has busy child /dev/" + busyChild.KName + " (" + what + ")"
}

// collectBusyChildren maps parent kname -> first busy child row.
// A child is busy when it has a non-empty Mountpoint or FSType
// (LVM/MD/ZFS members surface as FSType=LVM2_member / md_raid_member /
// zfs_member; ext4/xfs/... show the filesystem). Skips
// PKNAME-less rows (top-level disks) and DRBD children
// (parent already filtered by major 147).
//
// Returns a map keyed by parent KName so the per-disk loop can do
// an O(1) lookup. The first busy child wins per parent — the
// Status message includes a single concrete child path which is
// enough for the operator to identify the problem; the rest of
// the busy children show up under `lsblk /dev/<parent>` anyway.
func collectBusyChildren(rows []satellite.LsblkDevice) map[string]*satellite.LsblkDevice {
	out := map[string]*satellite.LsblkDevice{}

	for i := range rows {
		child := &rows[i]
		if child.PKName == "" {
			continue
		}

		if child.Mountpoint == "" && child.FSType == "" {
			continue
		}

		if _, exists := out[child.PKName]; exists {
			continue
		}

		out[child.PKName] = child
	}

	return out
}

// publishDeviceWithReason creates or updates the CRD for one lsblk row,
// stamping the supplied Reason/Message on Status.Conditions[Free] so the
// REST layer (Bug 89) can quote them verbatim in the `ps cdp` rejection
// envelope. Returns the CRD's metadata.name + ok=true on success so the
// caller can build the discovered-set for prune.
//
// Lifecycle: a missing CRD is Create()d with metadata + Status
// stamped via a follow-up Status().Update. An existing CRD has
// its Status refreshed in place — Spec.AttachTo authored by the
// REST shim / operator is preserved (we never touch Spec here;
// discovery owns Status only).
func (p *PhysicalDeviceDiscoveryRunnable) publishDeviceWithReason(ctx context.Context, logger logr.Logger, row *satellite.LsblkDevice, free bool, reason, message string) (string, bool) {
	// Bug 70: ZFS zvols (KName like zd0, zd16, …) come from an
	// existing zpool — they're already in use as block devices for
	// blockstor's own DRBD volumes (or operator-managed zvols).
	// They MUST NOT appear in `linstor ps l` as candidates for new
	// pools; the operator would otherwise see a list dominated by
	// zvol entries instead of the few real disks they can pool.
	// Upstream LINSTOR doesn't filter them explicitly but their
	// `lsblk --paths` typically renders zvols differently or
	// excludes them through fstype detection — we exclude by kname
	// prefix to avoid wiring up MAJ:MIN parsing.
	if isZVOLKName(row.KName) {
		return "", false
	}

	stableID := satellite.PickStableID(row)
	if stableID == "" {
		// No stable signal at all — virtio without serial /
		// missing kname. Skip silently; re-discovery on a
		// later boot would produce a different (but equally
		// unstable) name and the operator would see ghost
		// CRDs every reboot. Better to drop the row.
		return "", false
	}

	name := k8s.Name(p.NodeName + "." + stableID)
	// Bug 69: operators type `linstor ps cdp ... /dev/vda` and
	// match by kernel-name path. The `/dev/disk/by-id/<stableID>`
	// path is stable across reboots but useless to humans — and
	// for virtio devices without WWN/serial the stableID is the
	// fallback `by-path-<kname>`, producing the nonsensical
	// `/dev/disk/by-id/by-path-vda`. Surface the kernel name as
	// the primary DevicePath; the stable form lives in
	// Status.StableID for CRD-name determinism only.
	devicePath := "/dev/" + row.KName
	currentDevPath := "/dev/" + row.KName

	rotational := row.Rotational

	desiredStatus := buildDiscoveryStatus(p.NodeName, stableID, devicePath, currentDevPath, row, &rotational, free, reason, message)

	var existing blockstoriov1alpha1.PhysicalDevice

	err := p.Client.Get(ctx, client.ObjectKey{Name: name}, &existing)
	switch {
	case apierrors.IsNotFound(err):
		create := &blockstoriov1alpha1.PhysicalDevice{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					blockstoriov1alpha1.PhysicalDeviceLabelNode: p.NodeName,
				},
			},
		}

		err = p.Client.Create(ctx, create)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			logger.Error(err, "create PhysicalDevice", "name", name)

			return "", false
		}

		// Re-fetch for the status update so we have the apiserver-
		// assigned ResourceVersion (Status().Update is an
		// optimistic-concurrency write that needs it).
		err = p.Client.Get(ctx, client.ObjectKey{Name: name}, &existing)
		if err != nil {
			logger.Error(err, "re-get after Create", "name", name)

			return "", false
		}
	case err != nil:
		logger.Error(err, "get PhysicalDevice", "name", name)

		return "", false
	}

	// Ensure the node label is set on existing CRDs that may have
	// been created without it (e.g. operator hand-applied a CRD).
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}

	if existing.Labels[blockstoriov1alpha1.PhysicalDeviceLabelNode] != p.NodeName {
		existing.Labels[blockstoriov1alpha1.PhysicalDeviceLabelNode] = p.NodeName

		err = p.Client.Update(ctx, &existing)
		if err != nil {
			logger.Error(err, "update PhysicalDevice labels", "name", name)

			return "", false
		}
	}

	existing.Status = desiredStatus

	err = p.Client.Status().Update(ctx, &existing)
	if err != nil {
		logger.Error(err, "update PhysicalDevice status", "name", name)

		return "", false
	}

	return name, true
}

// buildDiscoveryStatus assembles the Status subresource the
// discovery runnable writes. Pulled out of publishDevice so the
// function stays under the funlen budget and so the per-field
// mapping (lsblk → Status) is a single readable expression. The
// Reason/Message passed in are surfaced on Status.Conditions[Free]
// so the REST shim (Bug 89) can quote them verbatim when refusing
// a `ps cdp` attach.
func buildDiscoveryStatus(nodeName, stableID, devicePath, currentDevPath string, row *satellite.LsblkDevice, rotational *bool, free bool, reason, message string) blockstoriov1alpha1.PhysicalDeviceStatus {
	condStatus := metav1.ConditionTrue
	if !free {
		condStatus = metav1.ConditionFalse
	}

	return blockstoriov1alpha1.PhysicalDeviceStatus{
		NodeName:       nodeName,
		StableID:       stableID,
		DevicePath:     devicePath,
		CurrentDevPath: currentDevPath,
		SizeBytes:      row.SizeBytes,
		Model:          row.Model,
		Serial:         row.Serial,
		Rotational:     rotational,
		Transport:      row.Transport,
		Phase:          blockstoriov1alpha1.PhysicalDevicePhaseAvailable,
		Conditions: []metav1.Condition{
			{
				Type:               physicalDeviceDiscoveryConditionFree,
				Status:             condStatus,
				LastTransitionTime: metav1.NewTime(time.Now()),
				Reason:             reason,
				Message:            message,
			},
		},
	}
}

// pruneDisappeared deletes PhysicalDevice CRDs for devices that
// no longer appear in lsblk. A device with `Spec.AttachTo` set is
// skipped — an attach is in flight and the PhysicalDeviceReconciler
// owns the lifecycle (it will delete-as-completion on success or
// flip Phase=Failed and own the cleanup on failure).
func (p *PhysicalDeviceDiscoveryRunnable) pruneDisappeared(ctx context.Context, logger logr.Logger, discovered map[string]struct{}) {
	var existing blockstoriov1alpha1.PhysicalDeviceList

	err := p.Client.List(ctx, &existing,
		client.MatchingLabels{blockstoriov1alpha1.PhysicalDeviceLabelNode: p.NodeName})
	if err != nil {
		logger.Error(err, "list PhysicalDevices for prune")

		return
	}

	// Stable iteration order so test assertions on "which CRD
	// did we delete first" are deterministic.
	sort.Slice(existing.Items, func(i, j int) bool {
		return existing.Items[i].Name < existing.Items[j].Name
	})

	for i := range existing.Items {
		dev := &existing.Items[i]
		if _, stillThere := discovered[dev.Name]; stillThere {
			continue
		}

		if dev.Spec.AttachTo != nil {
			// Attach in flight — let the attach reconciler own
			// the lifecycle, not the discovery loop. Otherwise
			// the discovery loop racing the reconciler could
			// delete the CRD out from under an in-progress
			// pvcreate / zpool create.
			continue
		}

		err := p.Client.Delete(ctx, dev)
		if err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "delete disappeared PhysicalDevice", "name", dev.Name)

			continue
		}

		logger.V(1).Info("pruned disappeared PhysicalDevice",
			"name", dev.Name,
			"stableID", dev.Status.StableID)
	}
}

// Compile-time check that we satisfy the runnable contract.
var _ manager.Runnable = (*PhysicalDeviceDiscoveryRunnable)(nil)
