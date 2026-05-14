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
const physicalDeviceDiscoveryConditionFree = "Free"

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

		free, signatureErr := satellite.IsDeviceFree(ctx, p.Exec, &row)
		if signatureErr != nil {
			// One device's probe failing shouldn't sink the whole
			// scan — log and move on so the other disks still get
			// surfaced. The next tick re-tries.
			logger.Error(signatureErr, "IsDeviceFree probe", "device", row.KName)

			continue
		}

		name, ok := p.publishDevice(ctx, logger, &row, free)
		if ok {
			discovered[name] = struct{}{}
		}
	}

	p.pruneDisappeared(ctx, logger, discovered)

	return nil
}

// publishDevice creates or updates the CRD for one lsblk row.
// Returns the CRD's metadata.name + ok=true on success so the
// caller can build the discovered-set for prune.
//
// Lifecycle: a missing CRD is Create()d with metadata + Status
// stamped via a follow-up Status().Update. An existing CRD has
// its Status refreshed in place — Spec.AttachTo authored by the
// REST shim / operator is preserved (we never touch Spec here;
// discovery owns Status only).
func (p *PhysicalDeviceDiscoveryRunnable) publishDevice(ctx context.Context, logger logr.Logger, row *satellite.LsblkDevice, free bool) (string, bool) {
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

	desiredStatus := buildDiscoveryStatus(p.NodeName, stableID, devicePath, currentDevPath, row, &rotational, free)

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
// mapping (lsblk → Status) is a single readable expression.
func buildDiscoveryStatus(nodeName, stableID, devicePath, currentDevPath string, row *satellite.LsblkDevice, rotational *bool, free bool) blockstoriov1alpha1.PhysicalDeviceStatus {
	condReason := "FreeBlockDevice"
	condStatus := metav1.ConditionTrue
	condMessage := "device passed lsblk + signature probes"

	if !free {
		condStatus = metav1.ConditionFalse
		condReason = "SignatureFound"
		condMessage = "lsblk / pvs / zpool / drbdmeta / wipefs detected an on-disk signature"
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
				Reason:             condReason,
				Message:            condMessage,
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
