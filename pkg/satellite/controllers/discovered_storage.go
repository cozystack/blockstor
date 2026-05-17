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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Bug 135 follow-up — satellite-side discovered-storage publisher.
//
// The apiserver's Bug 135 pre-flight (`pkg/rest/storage_pools.go`'s
// refuseUnknownBackingStorage / checkAdvertised) refuses `linstor
// sp c lvmthin <node> <pool> <vg>` when the requested VG / zpool
// isn't in the satellite's advertised set on the Node CRD. The
// reader keys, mirrored here so the two halves stay in lock-step:
//
//   - `Aux/DiscoveredVGs="vg1,vg2"`    — comma-joined VG list.
//   - `Aux/DiscoveredZPools="zp1,zp2"` — comma-joined zpool list.
//
// "Key absent" means "satellite hasn't ticked yet" — the apiserver
// falls through permissive so a fresh cluster can bootstrap. "Key
// present but empty" means "satellite ticked, nothing matches" —
// the apiserver refuses. This runnable stamps the keys on every
// tick (including when the probe returns nothing) so the apiserver
// transitions out of the permissive fall-through within one period
// of satellite startup.

// DiscoveredStoragePeriod is the cadence at which the satellite
// re-scans local VGs / zpools and refreshes the Node CRD props.
// 60s matches PhysicalDeviceDiscoveryPeriod — operators who run
// `vgcreate / vgremove / zpool create / zpool destroy` expect the
// next `linstor sp c` to reflect reality "within a minute" without
// flooding the apiserver with no-op writes on a quiescent host.
const DiscoveredStoragePeriod = 60 * time.Second

// discoveryProbeTimeout bounds each vgs/zpool list invocation
// (Bug 277). Mirrors the 30s ceiling used by the storage providers'
// withProbeTimeout — short enough that the next tick still happens
// within DiscoveredStoragePeriod, long enough that a slow but
// healthy probe completes.
const discoveryProbeTimeout = 30 * time.Second

// Node CRD Props keys the runnable stamps. Mirrors the constants
// the apiserver pre-flight reads in pkg/rest/storage_pools.go —
// kept verbatim here so a grep across the tree pairs writer and
// reader cleanly. Bug 135's `Aux/` namespace matches upstream
// LINSTOR's auxiliary-prop convention.
const (
	NodePropDiscoveredVGs    = "Aux/DiscoveredVGs"
	NodePropDiscoveredZPools = "Aux/DiscoveredZPools"
)

// DiscoveredStorageRunnable is a controller-runtime Runnable that
// keeps the satellite's Node CRD Props in sync with the host's
// enumerated VGs / zpools. The apiserver-side Bug 135 pre-flight
// uses these props as the source of truth for "is this VG / zpool
// real?" so a `linstor sp c` against a garbage backing-storage
// name is refused at create-time rather than admitted and left to
// fail silently on first volume placement.
//
// Per-node scope: one Runnable per satellite pod, owns its own
// NodeName. NeedLeaderElection=false (mirrors HeartbeatRunnable
// and PhysicalDeviceDiscoveryRunnable — each satellite must
// publish its OWN local enumeration).
//
// Lifecycle:
//
//   - Start fires one tick immediately so a freshly-started
//     satellite has its discovery props visible to the apiserver
//     within seconds, not one full period later.
//   - Each subsequent tick re-runs `vgs` + `zpool list`. Tick is
//     idempotent — when the (sorted, joined) lists match the
//     current Node.Spec.Props values the runnable skips the
//     Update entirely to avoid reconcile-loop churn.
//   - Missing-Node is NOT fatal (mirrors HeartbeatRunnable's
//     `node lost` semantic) — the runnable logs and keeps
//     ticking so the next stamp succeeds once the operator
//     re-registers the node.
//   - `vgs` / `zpool list` failures (no LVM installed, no ZFS
//     installed) degrade to an empty list rather than aborting
//     the whole tick. Without this carve-out a single-driver
//     host couldn't publish anything — empty-set is the
//     correct "advertised, nothing here" signal the apiserver
//     pre-flight needs to refuse pools of the absent kind.
type DiscoveredStorageRunnable struct {
	Client   client.Client
	Exec     storage.Exec
	NodeName string

	// Period overrides DiscoveredStoragePeriod (test-only —
	// production uses the default constant). A zero Period falls
	// back.
	Period time.Duration
}

// NeedLeaderElection reports that this runnable does NOT need
// leader election. Each satellite owns its OWN Node CRD's Props —
// leader election would pick one pod to enumerate every node's
// local VGs / zpools, which is structurally impossible (`vgs` /
// `zpool list` on one host doesn't see peer hosts' state).
func (*DiscoveredStorageRunnable) NeedLeaderElection() bool { return false }

// Start runs the discovery loop until ctx cancels. Errors during
// individual ticks are logged but never abort the loop — a
// transient apiserver / vgs / zpool hiccup must not take the
// satellite out of service. The first tick fires immediately so a
// freshly-started satellite has its discovery props visible to the
// apiserver pre-flight within seconds.
func (d *DiscoveredStorageRunnable) Start(ctx context.Context) error {
	period := d.Period
	if period == 0 {
		period = DiscoveredStoragePeriod
	}

	logger := log.FromContext(ctx).WithName("discovered-storage").WithValues("node", d.NodeName)

	err := d.tick(ctx, logger)
	if err != nil {
		logger.Error(err, "initial discovered-storage tick")
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err = d.tick(ctx, logger)
			if err != nil {
				logger.Error(err, "discovered-storage tick")
			}
		}
	}
}

// RegisterWithManager adds the runnable to mgr. Symmetrical with
// HeartbeatRunnable / PhysicalDeviceDiscoveryRunnable so the wiring
// shape in addBackgroundRunnables stays consistent.
func (d *DiscoveredStorageRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(d)
	if err != nil {
		return errors.Wrap(err, "add DiscoveredStorageRunnable")
	}

	return nil
}

// tick performs exactly one discovery cycle:
//  1. enumerate local VGs    (`vgs   --noheadings -o vg_name`)
//  2. enumerate local zpools (`zpool list -H -o name`)
//  3. Get the Node CRD, compare to existing Props.
//  4. If different, Update the Node CRD's Spec.Props with the new
//     comma-joined values. Idempotent on no change.
//
// Single-driver hosts (LVM but no ZFS, or vice versa) degrade to
// an empty list for the missing driver — the apiserver pre-flight
// reads "key present, empty" as "satellite ticked, nothing here"
// which correctly refuses pool creates of that kind. A missing
// driver is NOT a tick failure; the surviving driver's list still
// gets published.
//
// Exposed for tests via DiscoveryTickForTest (export_test).
func (d *DiscoveredStorageRunnable) tick(ctx context.Context, logger logr.Logger) error {
	vgs := probeVGs(ctx, d.Exec, logger)
	zpools := probeZPools(ctx, d.Exec, logger)

	var node blockstoriov1alpha1.Node

	err := d.Client.Get(ctx, types.NamespacedName{Name: d.NodeName}, &node)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Mirrors HeartbeatRunnable's `node lost` carve-out:
			// the satellite does NOT auto-create its own Node CRD.
			// The operator must re-register; the next tick will
			// stamp once the CRD reappears.
			logger.V(1).Info("Node CRD missing; skipping discovered-storage stamp",
				"node", d.NodeName)

			return nil
		}

		return errors.Wrapf(err, "get Node %q", d.NodeName)
	}

	wantVGs := strings.Join(vgs, ",")
	wantZPools := strings.Join(zpools, ",")

	gotVGs, hasVGs := node.Spec.Props[NodePropDiscoveredVGs]
	gotZPools, hasZPools := node.Spec.Props[NodePropDiscoveredZPools]

	// Short-circuit: both keys already match. Avoids burning an
	// Update on every tick of a quiescent host (every 60s × node
	// count would otherwise hammer the apiserver for no gain).
	// The presence check distinguishes "stamped to empty" from
	// "never stamped" — a freshly-bootstrapped Node CRD has
	// neither key, so the first tick always lands an Update even
	// when both probes returned empty.
	if hasVGs && hasZPools && gotVGs == wantVGs && gotZPools == wantZPools {
		return nil
	}

	if node.Spec.Props == nil {
		node.Spec.Props = map[string]string{}
	}

	node.Spec.Props[NodePropDiscoveredVGs] = wantVGs
	node.Spec.Props[NodePropDiscoveredZPools] = wantZPools

	err = d.Client.Update(ctx, &node)
	if err != nil {
		return errors.Wrapf(err, "update Node %q discovered-storage props", d.NodeName)
	}

	logger.V(1).Info("stamped discovered-storage props",
		NodePropDiscoveredVGs, wantVGs,
		NodePropDiscoveredZPools, wantZPools)

	return nil
}

// probeVGs runs `vgs --noheadings -o vg_name` (with the standard
// `--config <ConfigFilter>` guard every LVM shell-out carries) and
// returns the enumerated VG names in `vgs` output order. A non-zero
// exit (no LVM installed, vgs not in PATH, transient lvmetad
// hiccup) degrades to an empty list — the surviving driver's tick
// still completes. The error is logged at V(1) so a quiet host
// without LVM doesn't flood the satellite log.
func probeVGs(ctx context.Context, exec storage.Exec, logger logr.Logger) []string {
	// Bug 277 (P2): discovery ticker runs unattended; bound the
	// vgs call so a suspended VG can't hold the discovery loop
	// indefinitely. Same surface as the storage providers' new
	// withProbeTimeout wrap, kept local to avoid a cross-package
	// dep.
	ctx, cancel := context.WithTimeout(ctx, discoveryProbeTimeout)
	defer cancel()

	out, err := exec.Run(ctx, "vgs", lvm.Args("--noheadings", "-o", "vg_name")...)
	if err != nil {
		// Single-driver host or transient probe failure — empty
		// list is the correct "advertised, nothing here" signal.
		logger.V(1).Info("vgs probe failed; advertising empty VG list",
			"error", err.Error())

		return nil
	}

	return parseDiscoveryLines(out)
}

// probeZPools runs `zpool list -H -o name` and returns the
// enumerated pool names in `zpool` output order. `-H` strips the
// header row (avoids surfacing the literal string "NAME" as a
// fake pool); `-o name` projects only the pool-name column.
// A non-zero exit (no ZFS, no kernel module loaded, zpool not in
// PATH) degrades to an empty list — see probeVGs doc for the
// rationale.
func probeZPools(ctx context.Context, exec storage.Exec, logger logr.Logger) []string {
	// Bug 277 (P2): same rationale as probeVGs above.
	ctx, cancel := context.WithTimeout(ctx, discoveryProbeTimeout)
	defer cancel()

	out, err := exec.Run(ctx, "zpool", "list", "-H", "-o", "name")
	if err != nil {
		logger.V(1).Info("zpool list probe failed; advertising empty zpool list",
			"error", err.Error())

		return nil
	}

	return parseDiscoveryLines(out)
}

// parseDiscoveryLines splits a probe's stdout into one
// whitespace-trimmed name per non-empty line. Tolerates leading
// whitespace (`vgs --noheadings` indents output by default) and
// trailing newlines without per-call sed/awk shims.
func parseDiscoveryLines(out []byte) []string {
	var names []string

	for line := range strings.SplitSeq(string(out), "\n") {
		name := strings.TrimSpace(line)
		if name == "" {
			continue
		}

		names = append(names, name)
	}

	return names
}
