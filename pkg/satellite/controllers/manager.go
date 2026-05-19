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
	"time"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// reconcileTriggerBuffer bounds the observer → reconciler trigger
// channel. The producer is the ObserverRunnable's per-events2 frame
// loop (resource / device / connection / peer-device kinds); the
// consumer is the ResourceReconciler's WatchesRawSource input. A
// reconnect-storm on a multi-peer RD can burst dozens of frames per
// second; 64 leaves comfortable headroom for c-r's per-resource
// debouncer to coalesce them before the channel backs up. Drops on
// full are non-fatal — the observer's 5-second resync ticker is the
// belt-and-braces re-emit, and c-r's RequeueAfter already covers any
// dropped wake-up against the next Status frame.
const reconcileTriggerBuffer = 64

// scheme carries the runtime types this manager understands —
// blockstor CRDs + the core Kubernetes types (Secrets for
// passphrases / shared-secret references the satellite reads
// directly). Package-level state matches controller-runtime's
// own convention (`cmd/controller/main.go` does the same on
// the controller side).
//
//nolint:gochecknoglobals // package-init scheme registry, controller-runtime convention
var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(blockstoriov1alpha1.AddToScheme(scheme))
}

// NewManager constructs a controller-runtime manager wired
// with all four satellite-side reconcilers. The caller is
// expected to call `mgr.Start(ctx)` to begin reconciling.
//
// `restCfg` is typically the in-cluster config the satellite's
// DaemonSet pod gets from its ServiceAccount; for local dev /
// envtest the caller passes the test environment's REST config.
//
// `cfg.Apply` is the existing `pkg/satellite.Reconciler` (the
// one the gRPC `ApplyResources` consumer drives today). The
// satellite-side reconcilers translate the CRD events into
// `Apply.Apply([...DesiredResource])` calls so the storage +
// DRBD + LUKS chain stays a single code path during the
// migration.
//
// Phase 10.1. The reconcilers in this package are scaffolded
// but not yet wired into `agent.Run`; this `NewManager` exists
// so tests can validate the registration + filter-predicate
// plumbing independently of the agent's gRPC-server-still-
// running mainline.
func NewManager(restCfg *rest.Config, cfg Config) (manager.Manager, error) {
	if cfg.NodeName == "" {
		return nil, errors.New("controllers: NodeName is required")
	}

	if cfg.Apply == nil {
		return nil, errors.New("controllers: Apply is required")
	}

	// Bug 285: bound the manager's stop sequence so a wedged
	// reconciler / Runnable can't keep the satellite container
	// alive past the DaemonSet rolling-update grace window.
	// terminationGracePeriodSeconds is 30 s; pre-Stop has already
	// consumed up to 25 s of that running `drbdadm down`, so the
	// manager only has ~5 s of headroom before kubelet escalates
	// to SIGKILL. 10 s is generous for the in-flight Reconciles
	// + informer cache shutdown and still fires well before
	// kubelet would have to SIGKILL the container — a SIGKILL
	// during DaemonSet rollout leaves the next satellite
	// incarnation observing the rolling-upgrade scenario's
	// stuck-state pattern from `memory/blockstor_drbd_stuck_state.md`.
	gracefulShutdownTimeout := 10 * time.Second

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme: scheme,
		// Satellite is per-node; leader election would require
		// a cluster-wide Lease the satellite has no business
		// holding. Skip it; the DaemonSet enforces
		// one-pod-per-node already.
		LeaderElection: false,
		// HealthProbeBindAddress wires the manager's /healthz +
		// /readyz HTTP endpoints. cmd/satellite/main.go sets this
		// from --health-probe-bind-address so the DaemonSet's
		// kubelet probes have something to talk to; an empty
		// string disables the probe server entirely (the
		// controller-runtime default), which is what tests rely on
		// to avoid port-binding races between parallel runs.
		HealthProbeBindAddress:  cfg.HealthProbeBindAddress,
		GracefulShutdownTimeout: &gracefulShutdownTimeout,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create manager")
	}

	// Inject the manager's APIReader so reconcilers can bypass
	// the informer cache for late-stage finalizer re-reads (Bug
	// 65). Falls back gracefully in tests where the field is
	// left nil — see Config.APIReader.
	if cfg.APIReader == nil {
		cfg.APIReader = mgr.GetAPIReader()
	}

	// Phase 11.7: thread the observer-trigger channel through
	// Config so the ObserverRunnable (producer) and the
	// ResourceReconciler (consumer via WatchesRawSource) share
	// the same buffered channel. Production wires it here when
	// the caller left the field nil; unit tests that construct
	// reconcilers directly can supply their own channel or leave
	// the field nil to short-circuit both ends.
	ensureWiredDefaults(&cfg)

	err = (&ResourceReconciler{Config: cfg, Client: mgr.GetClient()}).SetupWithManager(mgr)
	if err != nil {
		return nil, errors.Wrap(err, "setup ResourceReconciler")
	}

	err = (&ResourceDefinitionReconciler{Config: cfg, Client: mgr.GetClient()}).SetupWithManager(mgr)
	if err != nil {
		return nil, errors.Wrap(err, "setup ResourceDefinitionReconciler")
	}

	err = (&SnapshotReconciler{Config: cfg, Client: mgr.GetClient()}).SetupWithManager(mgr)
	if err != nil {
		return nil, errors.Wrap(err, "setup SnapshotReconciler")
	}

	err = (&StoragePoolReconciler{Config: cfg, Client: mgr.GetClient()}).SetupWithManager(mgr)
	if err != nil {
		return nil, errors.Wrap(err, "setup StoragePoolReconciler")
	}

	err = (&PhysicalDeviceReconciler{Config: cfg, Client: mgr.GetClient()}).SetupWithManager(mgr)
	if err != nil {
		return nil, errors.Wrap(err, "setup PhysicalDeviceReconciler")
	}

	err = addBackgroundRunnables(mgr, cfg)
	if err != nil {
		return nil, err
	}

	wireCrossNodeFetcher(mgr, cfg)
	wireConditionStampers(mgr, cfg)

	return mgr, nil
}

// wireConditionStampers injects the satellite-side Status-Condition
// stampers (Phase 11.3) onto the Apply chain. Each stamper owns a
// distinct Condition `type` so SSA's listMap merge keeps the writers
// orthogonal; bundling them in one helper keeps NewManager under
// funlen budget.
//
// Also wires the SkipDiskClearer (Bug 278). Lives here rather than in
// its own helper so NewManager stays under the funlen budget — the
// clearer is the same shape as the Stampers (one apiserver writer
// per satellite-side reconciler hook).
func wireConditionStampers(mgr manager.Manager, cfg Config) {
	wireMetadataCreatedStamper(mgr, cfg)
	wireFilesystemFormattedStamper(mgr, cfg)
	wireSkipDiskClearer(mgr, cfg)
}

// ensureWiredDefaults populates the Config fields the satellite-side
// reconcilers need at runtime when the caller of NewManager left them
// unset. Today that's just the Phase 11.7 ReconcileTrigger channel — a
// buffered `event.GenericEvent` channel the ObserverRunnable
// produces onto and the ResourceReconciler consumes via
// `WatchesRawSource(source.Channel(...))`. The channel is shared so
// observer-driven kernel-state wake-ups land on the same reconcile
// queue as the primary For watch. Unit tests that supply their own
// channel keep it; tests that leave it nil opt both producer and
// consumer out (the observer no-ops on a nil channel and
// SetupWithManager skips the raw-source registration).
func ensureWiredDefaults(cfg *Config) {
	if cfg.ReconcileTrigger == nil {
		cfg.ReconcileTrigger = make(chan event.GenericEvent, reconcileTriggerBuffer)
	}
}

// addBackgroundRunnables wires the per-pod background loops
// (events2 observer, heartbeat, orphan sweeper) into the manager.
// Pulled out of NewManager to keep that function under the funlen
// budget — Scenario 5.34 added the third runnable and the
// inline chain tipped over the limit.
func addBackgroundRunnables(mgr manager.Manager, cfg Config) error {
	err := mgr.Add(&ObserverRunnable{
		Client:   mgr.GetClient(),
		Exec:     cfg.Exec,
		NodeName: cfg.NodeName,
		// Phase 11.7: producer end of the observer-trigger channel.
		// Fires on kernel-state changes (resource lifecycle, role,
		// disk, conn, repl) so the ResourceReconciler wakes even
		// when no apiserver write bumps Generation.
		ReconcileTrigger: cfg.ReconcileTrigger,
	})
	if err != nil {
		return errors.Wrap(err, "add ObserverRunnable")
	}

	err = (&HeartbeatRunnable{Client: mgr.GetClient(), NodeName: cfg.NodeName}).RegisterWithManager(mgr)
	if err != nil {
		return errors.Wrap(err, "register HeartbeatRunnable")
	}

	err = (&OrphanSweeperRunnable{
		Client:   mgr.GetClient(),
		Adm:      drbd.NewAdm(cfg.Exec),
		NodeName: cfg.NodeName,
		StateDir: cfg.Apply.StateDir(),
	}).RegisterWithManager(mgr)
	if err != nil {
		return errors.Wrap(err, "register OrphanSweeperRunnable")
	}

	err = (&StorageOrphanSweeperRunnable{
		Client:    mgr.GetClient(),
		Providers: cfg.Apply.SnapshotProviders,
		NodeName:  cfg.NodeName,
	}).RegisterWithManager(mgr)
	if err != nil {
		return errors.Wrap(err, "register StorageOrphanSweeperRunnable")
	}

	// Bug 341: cfg.UeventListener MUST be threaded into the runnable's
	// Uevent field — without it the discovery loop falls back to
	// pure-polling silently (no log signal in production), and
	// operator-facing `linstor ps l` waits up to PhysicalDeviceDiscoveryPeriod
	// (300 s) to refresh after a manual `wipefs`. The original
	// commit added the field on the runnable + the cfg shape but
	// forgot the assignment here, so the udev fast-path was a
	// no-op in production despite a healthy netlink listener.
	err = (&PhysicalDeviceDiscoveryRunnable{
		Client:   mgr.GetClient(),
		Exec:     cfg.Exec,
		NodeName: cfg.NodeName,
		Uevent:   cfg.UeventListener,
	}).RegisterWithManager(mgr)
	if err != nil {
		return errors.Wrap(err, "register PhysicalDeviceDiscoveryRunnable")
	}

	// Bug 135 follow-up: publish the host's enumerated VGs / zpools
	// onto the Node CRD's Spec.Props so the apiserver's
	// `refuseUnknownBackingStorage` pre-flight has a real list to
	// check against. Without this runnable the apiserver's
	// permissive fall-through admits `sp c` against any garbage
	// backing-storage name. See pkg/rest/storage_pools.go
	// checkAdvertised + the discovered_storage.go doc block.
	err = (&DiscoveredStorageRunnable{
		Client:   mgr.GetClient(),
		Exec:     cfg.Exec,
		NodeName: cfg.NodeName,
	}).RegisterWithManager(mgr)
	if err != nil {
		return errors.Wrap(err, "register DiscoveredStorageRunnable")
	}

	return registerMetadataCreatedBackfill(mgr, cfg)
}

// registerMetadataCreatedBackfill wires the Phase 11.3 Stage 1
// startup backfill runnable. Pulled out of addBackgroundRunnables
// to keep that function under the funlen budget — the bookkeeping
// for one more runnable nudges it over the limit.
func registerMetadataCreatedBackfill(mgr manager.Manager, cfg Config) error {
	err := (&MetadataCreatedBackfillRunnable{
		Client:   mgr.GetClient(),
		Adm:      drbd.NewAdm(cfg.Exec),
		Stamper:  &MetadataCreatedStamper{Client: mgr.GetClient()},
		NodeName: cfg.NodeName,
	}).RegisterWithManager(mgr)
	if err != nil {
		return errors.Wrap(err, "register MetadataCreatedBackfillRunnable")
	}

	return nil
}

// wireCrossNodeFetcher injects the SnapshotFetcher into the Apply
// chain so materializeVolume's no-local-snapshot branch streams from
// a peer satellite instead of falling through to a blank create. The
// fetcher needs the manager's cached client, which is why it ships
// here rather than at NewReconciler time. Pulled out of NewManager
// to keep that function under the funlen budget.
func wireCrossNodeFetcher(mgr manager.Manager, cfg Config) {
	cfg.Apply.SetCrossNodeFetcher(&SnapshotFetcher{
		Client:   mgr.GetClient(),
		NodeName: cfg.NodeName,
	})
}

// wireMetadataCreatedStamper injects the MetadataCreatedStamper into
// the Apply chain so `ensureMetadata` can SSA-patch the
// `MetadataCreated=True` Status Condition after `drbdmeta create-md`
// succeeds. Mirrors `wireCrossNodeFetcher` — the stamper needs the
// manager's cached client, which is why it lands here rather than at
// NewReconciler time. Phase 11.3 Stage 1.
func wireMetadataCreatedStamper(mgr manager.Manager, cfg Config) {
	cfg.Apply.SetMetadataCreatedStamper(&MetadataCreatedStamper{
		Client: mgr.GetClient(),
	})
}

// wireFilesystemFormattedStamper injects the FilesystemFormattedStamper
// into the Apply chain so `runAutoMkfs` can SSA-patch the
// `FilesystemFormatted=True` Status Condition after every diskful
// volume reports a filesystem (freshly mkfs'd or adopted via blkid).
// Mirrors `wireMetadataCreatedStamper`. Phase 11.3 Stage 2.
func wireFilesystemFormattedStamper(mgr manager.Manager, cfg Config) {
	cfg.Apply.SetFilesystemFormattedStamper(&FilesystemFormattedStamper{
		Client: mgr.GetClient(),
	})
}

// wireSkipDiskClearer injects the SkipDiskClearer into the Apply chain
// so `runAdjust` can release the observer's SSA claim on
// Spec.Props[DrbdOptions/SkipDisk] when the kernel re-emerges healthy
// after a defensive stamp (Bug 278: Talos kernel upgrade reattach).
// Mirrors `wireMetadataCreatedStamper` — the clearer needs the
// manager's cached client which doesn't exist at NewReconciler time.
func wireSkipDiskClearer(mgr manager.Manager, cfg Config) {
	cfg.Apply.SetSkipDiskClearer(&SkipDiskClearer{
		Client:   mgr.GetClient(),
		NodeName: cfg.NodeName,
	})
}
