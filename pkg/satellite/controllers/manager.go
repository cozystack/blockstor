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
	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

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

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme: scheme,
		// Satellite is per-node; leader election would require
		// a cluster-wide Lease the satellite has no business
		// holding. Skip it; the DaemonSet enforces
		// one-pod-per-node already.
		LeaderElection: false,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create manager")
	}

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

	return mgr, nil
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

	err = (&PhysicalDeviceDiscoveryRunnable{
		Client:   mgr.GetClient(),
		Exec:     cfg.Exec,
		NodeName: cfg.NodeName,
	}).RegisterWithManager(mgr)
	if err != nil {
		return errors.Wrap(err, "register PhysicalDeviceDiscoveryRunnable")
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
