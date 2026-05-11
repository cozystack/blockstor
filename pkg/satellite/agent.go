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

// Package satellite is the per-node agent runtime. It encapsulates the
// gRPC client connection to the controller, the local state machine that
// owns DRBD/LVM/ZFS bookkeeping, and the observed-state stream that ships
// runtime state back to the controller.
//
// Phase 3 first slice: connect, hello, idle. Reconcile bodies (DRBD .res
// generation, drbdadm, LVM/ZFS provisioning) come in subsequent slices.
package satellite

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/version"
)

// ManagerFactory builds a controller-runtime manager wired with
// the satellite's per-CRD reconcilers. Injected by
// cmd/satellite/main.go so pkg/satellite stays free of an
// import on pkg/satellite/controllers (which itself imports
// pkg/satellite for *Reconciler — direct import would cycle).
type ManagerFactory func(restCfg *rest.Config, nodeName string, rec *Reconciler) (manager.Manager, error)

// Config holds the parameters that come in from the satellite binary's
// command-line flags or its container env.
type Config struct {
	// NodeName is the name this satellite registers under. Required.
	NodeName string

	// ControllerAddr is the gRPC dial address of the blockstor-controller.
	ControllerAddr string

	// ListenAddr is the bind address for the satellite's own gRPC server
	// (the side that the controller dials for ApplyResources, snapshot
	// RPCs, ship). Empty disables the server (useful for unit tests).
	ListenAddr string

	// AdvertisedEndpoint is the host:port the satellite tells the
	// controller to dial back at. Differs from ListenAddr when the
	// satellite binds to 0.0.0.0:7000 but is reachable from the
	// controller as <pod-ip>:7000.
	AdvertisedEndpoint string

	// StateDir is the on-disk directory the satellite uses for DRBD .res
	// files and per-resource state. Required.
	StateDir string

	// Providers maps storage-pool name → provider implementation. The
	// satellite reconciler uses this to resolve which backend a
	// DesiredVolume's StoragePool refers to. Seeded at startup from CLI
	// flags / env (LVM-thin, ZFS, file).
	Providers map[string]storage.Provider

	// DialTimeout caps gRPC connection establishment per attempt.
	DialTimeout time.Duration

	// Logger is the structured logger used by the agent and its sub-loops.
	// Defaults to slog.Default if nil.
	Logger *slog.Logger

	// RESTConfig + ManagerFactory together opt the agent into
	// starting a controller-runtime manager alongside the gRPC
	// server. Both nil keeps the legacy gRPC-only path. Set by
	// cmd/satellite/main.go during in-cluster boot; the factory
	// indirection avoids a pkg/satellite → pkg/satellite/controllers
	// import cycle.
	RESTConfig     *rest.Config
	ManagerFactory ManagerFactory
}

// Agent is the satellite runtime. It is constructed with NewAgent and run
// with Run; Run returns when the parent context is cancelled.
type Agent struct {
	cfg    Config
	logger *slog.Logger
}

// NewAgent constructs an Agent without yet dialling the controller.
//
//nolint:gocritic // value receiver is the ergonomic public API; Config is the binary's flag bundle.
func NewAgent(cfg Config) *Agent {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Agent{cfg: cfg, logger: logger}
}

// Run is the agent's main loop. It dials the controller for the
// observe-stream channel (Resource.Status is pushed via gRPC
// ReportObserved until Phase 10 finishes the satellite-side
// direct-SSA migration), then starts the controller-runtime
// manager so CRD events drive the apply chain. Returns when
// ctx cancels.
func (a *Agent) Run(ctx context.Context) error {
	if a.cfg.NodeName == "" {
		return errors.New("NodeName is required")
	}

	a.logger.Info("agent starting",
		"node", a.cfg.NodeName,
		"blockstor_version", version.Version,
		"controller", a.cfg.ControllerAddr)

	conn, err := a.dial(ctx)
	if err != nil {
		return errors.Wrap(err, "dial controller")
	}

	defer func() { _ = conn.Close() }()

	client := satellitepb.NewControllerClient(conn)

	err = a.hello(ctx, client)
	if err != nil {
		return errors.Wrap(err, "hello")
	}

	// Build the reconciler once so the c-r manager and the gRPC
	// observe/capacity loops share the same apply chain.
	rec := a.newReconciler()

	// When RESTConfig + ManagerFactory are both set, the satellite
	// runs a controller-runtime manager that drives the apply chain
	// from CRD events. Phase 10.6 retired the controller-side gRPC
	// dispatcher; the c-r path is now the only inbound channel.
	if a.cfg.RESTConfig != nil && a.cfg.ManagerFactory != nil {
		err := a.startControllerRuntime(ctx, rec)
		if err != nil {
			return errors.Wrap(err, "start controller-runtime manager")
		}

		a.logger.Info("satellite controller-runtime manager ready")
	}

	// Tail drbdsetup events2 + ship observed state back to the
	// controller via ReportObserved. The supervisor restarts the
	// stream on any error and re-runs Hello first so the controller's
	// satellite registry sees us again after a controller restart.
	go a.superviseObserveLoop(ctx, client)

	// Periodically push pool capacity so /v1/view/storage-pools shows
	// live numbers. Best-effort: errors log and the next tick retries.
	go a.runCapacityLoop(ctx, client)

	<-ctx.Done()

	a.logger.Info("agent stopping", "node", a.cfg.NodeName)

	return ctx.Err() //nolint:wrapcheck // bubbling ctx.Err() unwrapped is the convention
}

// newReconciler builds the satellite's apply-chain Reconciler.
// Pulled out so the gRPC server and the controller-runtime
// manager (when RESTConfig is set) share one instance.
func (a *Agent) newReconciler() *Reconciler {
	return NewReconciler(ReconcilerConfig{
		Providers:    a.cfg.Providers,
		Adm:          drbd.NewAdm(storage.RealExec{}),
		Cryptsetup:   luks.NewCryptsetup(storage.RealExec{}),
		StateDir:     a.cfg.StateDir,
		NodeName:     a.cfg.NodeName,
		LocalAddress: hostFromEndpoint(a.cfg.AdvertisedEndpoint),
	})
}

// startControllerRuntime launches a controller-runtime manager
// that wires the four per-CRD satellite reconcilers (Resource,
// StoragePool, Snapshot, ResourceDefinition) onto the shared
// apply chain `rec`. Manager runs in a goroutine and exits when
// ctx cancels; Serve errors log but do not abort the agent —
// gRPC stays primary until Phase 10.6 retires it.
//
// Returns once `mgr.Start` is in flight. Constructed via the
// `controllers.NewManager` factory so this stays a one-liner if
// the manager's scheme / leader-election story changes later.
func (a *Agent) startControllerRuntime(ctx context.Context, rec *Reconciler) error {
	mgr, err := a.cfg.ManagerFactory(a.cfg.RESTConfig, a.cfg.NodeName, rec)
	if err != nil {
		return errors.Wrap(err, "build manager")
	}

	go func() {
		err := mgr.Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("controller-runtime manager exited", "err", err)
		}
	}()

	return nil
}

// dial opens an insecure gRPC connection to the controller. TLS comes in
// Phase 6 alongside the rest of the encryption work; cluster traffic is
// expected to ride a private k8s network until then.
func (a *Agent) dial(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, a.cfg.DialTimeout)
	defer cancel()

	conn, err := grpc.NewClient(a.cfg.ControllerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrapf(err, "grpc dial %q", a.cfg.ControllerAddr)
	}

	// NewClient is non-blocking; we surface dial-time problems by issuing
	// the first RPC under DialTimeout. Tests rely on this so they fail
	// fast when the server is misconfigured.
	_ = dialCtx

	return conn, nil
}

// superviseObserveLoop wraps runObserveLoop so the satellite
// transparently survives a controller restart (which closes the
// ReportObserved client-stream with EOF). On every retry it re-Hellos
// first — without that, the controller's in-memory satellite registry
// would never learn we exist again and ApplyResources / DeleteResource
// RPCs would all fail with "no SatelliteEndpoint for node X".
func (a *Agent) superviseObserveLoop(ctx context.Context, client satellitepb.ControllerClient) {
	backoff := observeRetryInitial

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := a.hello(ctx, client)
		if err == nil {
			a.runObserveLoop(ctx, client)

			backoff = observeRetryInitial
		} else {
			a.logger.Error("re-hello", "err", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > observeRetryMax {
			backoff = observeRetryMax
		}
	}
}

// runObserveLoop tails `drbdsetup events2` and pushes parsed
// observations to the controller via the ReportObserved
// client-streaming RPC. Returns on any error — superviseObserveLoop
// owns reconnect.
func (a *Agent) runObserveLoop(ctx context.Context, client satellitepb.ControllerClient) {
	watcher, cleanup, err := drbd.StartDrbdsetupEvents2(ctx)
	if err != nil {
		a.logger.Error("start events2", "err", err)

		return
	}
	defer cleanup()

	stream, err := client.ReportObserved(ctx)
	if err != nil {
		a.logger.Error("open ReportObserved stream", "err", err)

		return
	}

	obs := NewObserver(a.cfg.NodeName)
	events := make(chan drbd.Event, observeBuffer)

	go func() {
		watchErr := watcher.Watch(ctx, events)
		if watchErr != nil {
			a.logger.Error("events2 watch", "err", watchErr)
		}
	}()

	adm := drbd.NewAdm(storage.RealExec{})

	for ev := range obs.Translate(events) {
		// Backing-device failure handler: when the kernel reports
		// disk:Failed for a local replica, detach so the lower
		// disk stops getting hammered. Peers stay UpToDate and
		// the consumer keeps doing I/O via the network path. The
		// detach is best-effort — a stale .res or already-Diskless
		// state surfaces as an error we log and move past.
		if ev.GetDrbdState() == "Failed" {
			detachErr := adm.Detach(ctx, ev.GetResourceName())
			if detachErr != nil {
				a.logger.Error("auto-detach on Failed",
					"resource", ev.GetResourceName(),
					"err", detachErr)
			} else {
				a.logger.Info("auto-detached failed replica",
					"resource", ev.GetResourceName())
			}
		}

		err := stream.Send(ev)
		if err != nil {
			a.logger.Error("ReportObserved send", "err", err)

			return
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		a.logger.Error("ReportObserved close", "err", err)
	}
}

// runCapacityLoop walks each registered Provider's PoolStatus on a
// fixed cadence and pushes free/total bytes to the controller via
// ReportPoolCapacity. Best-effort: a failed iteration is logged and
// the next tick retries. Empty Providers map → no-op (the loop still
// runs, but every tick yields a zero-pool request which the
// controller treats as a no-op).
func (a *Agent) runCapacityLoop(ctx context.Context, client satellitepb.ControllerClient) {
	tick := time.NewTicker(capacityInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}

		pools := make([]*satellitepb.PoolCapacity, 0, len(a.cfg.Providers))

		for name, p := range a.cfg.Providers {
			poolStatus, err := p.PoolStatus(ctx)
			if err != nil {
				a.logger.Error("PoolStatus", "pool", name, "err", err)

				continue
			}

			pools = append(pools, &satellitepb.PoolCapacity{
				PoolName:          name,
				FreeCapacityKib:   poolStatus.FreeCapacityKib,
				TotalCapacityKib:  poolStatus.TotalCapacityKib,
				SupportsSnapshots: poolStatus.SupportsSnapshots,
			})
		}

		_, err := client.ReportPoolCapacity(ctx, &satellitepb.ReportPoolCapacityRequest{
			NodeName: a.cfg.NodeName,
			Pools:    pools,
		})
		if err != nil {
			a.logger.Error("ReportPoolCapacity", "err", err)
		}
	}
}

// capacityInterval is the periodic-push cadence. Long enough to keep
// CRD writes cheap, short enough that a freshly-allocated LV shows up
// in /v1/view/storage-pools' free_capacity within ~half a minute.
const capacityInterval = 30 * time.Second

// observeRetryInitial / observeRetryMax bound the reconnect backoff in
// superviseObserveLoop. We want fast pickup after a controller restart
// (300 ms) but no thundering-herd if the controller stays down (cap
// at 30 s, doubling each failure).
const (
	observeRetryInitial = 300 * time.Millisecond
	observeRetryMax     = 30 * time.Second
)

// observeBuffer caps the events2 → Observer in-flight queue. drbd-9
// reconnect storms can burst dozens of events; 256 is a comfortable
// cushion for the satellite-side translation goroutine.
const observeBuffer = 256

// hostFromEndpoint trims the trailing :port off an endpoint string.
// Returns the input unchanged when it has no port (e.g. plain host or
// already host-only) or is empty. We don't use net.SplitHostPort
// because the leniency the helper needs (returning sane fallbacks)
// makes a one-line strings split simpler.
func hostFromEndpoint(endpoint string) string {
	if endpoint == "" {
		return ""
	}

	idx := strings.LastIndex(endpoint, ":")
	if idx <= 0 {
		return endpoint
	}

	return endpoint[:idx]
}

// hello is the registration handshake. The satellite tells the controller
// who it is and what layers / providers it can drive; the controller
// upserts the corresponding Node CRD and replies with the cluster id.
func (a *Agent) hello(ctx context.Context, client satellitepb.ControllerClient) error {
	rpcCtx, cancel := context.WithTimeout(ctx, a.cfg.DialTimeout)
	defer cancel()

	pools := make([]*satellitepb.SatellitePool, 0, len(a.cfg.Providers))
	for name, p := range a.cfg.Providers {
		pools = append(pools, &satellitepb.SatellitePool{
			Name:         name,
			ProviderKind: p.Kind(),
		})
	}

	resp, err := client.Hello(rpcCtx, &satellitepb.HelloRequest{
		NodeName:          a.cfg.NodeName,
		BlockstorVersion:  version.Version,
		LayerKinds:        []string{"DRBD", "STORAGE", "LUKS"},
		ProviderKinds:     []string{"LVM", "LVM_THIN", "ZFS", "ZFS_THIN", "FILE"},
		SatelliteEndpoint: a.cfg.AdvertisedEndpoint,
		Pools:             pools,
	})
	if err != nil {
		return errors.Wrap(err, "Hello RPC")
	}

	a.logger.Info("hello complete",
		"node", a.cfg.NodeName,
		"cluster_id", resp.GetClusterId())

	return nil
}
