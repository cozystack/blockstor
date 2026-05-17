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

// Command apiserver runs the LINSTOR-compatible REST API as a
// standalone Deployment, decoupled from the reconciler binary
// (cmd/controller). Splitting them yields three operational wins:
//
//  1. The apiserver scales horizontally — linstor-csi's view-API
//     polling and `linstor r list --refresh` floods no longer compete
//     for goroutines with reconcile workers.
//
//  2. RBAC narrows. The reconciler still needs CRUD on every
//     blockstor.io CRD; the apiserver only needs read-mostly access
//     plus the few create/delete verbs the upstream LINSTOR REST
//     contract exposes (RD create, Resource create, Snapshot create,
//     and so on).
//
//  3. The reconciler keeps single-leader semantics for write
//     correctness; the apiserver runs N replicas behind a Service
//     for HA reads.
//
// The binary itself is intentionally tiny — it boots a manager
// with no reconcilers (caches only, so the REST server's
// cached-client reads are still cheap), registers the same
// `pkg/rest.Server` the merged binary used to mount, and waits
// for signal. Leader election is OFF: every replica serves
// independently.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/rest"
	"github.com/cozystack/blockstor/pkg/store"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
)

// apiserverFlags collects every CLI knob the binary exposes.
// Lifting them off main keeps the function inside the funlen
// budget and makes the test wiring (later) trivial.
type apiserverFlags struct {
	restAddr            string
	metricsAddr         string
	probeAddr           string
	controllerNamespace string
}

func parseFlags() *apiserverFlags {
	flags := &apiserverFlags{}

	flag.StringVar(&flags.restAddr, "rest-bind-address", ":3370",
		"The address the LINSTOR-compatible REST API binds to.")
	flag.StringVar(&flags.metricsAddr, "metrics-bind-address", "0",
		"The address the metrics endpoint binds to. 0 disables.")
	flag.StringVar(&flags.probeAddr, "health-probe-bind-address", ":8081",
		"The address the probe endpoint binds to.")
	flag.StringVar(&flags.controllerNamespace, "controller-namespace", "",
		"Namespace for the apiserver's own Secrets/ConfigMaps (default $POD_NAMESPACE then blockstor-system).")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Bind slog.Default to the REST package's runtime LevelVar so
	// `controller set-log-level DEBUG` (scenario 7.W06) flips the
	// effective level of every slog.* call without a pod restart.
	// The controller-runtime zap logger keeps its own level for
	// historical reasons; new code paths emitting via slog (e.g.
	// satellite reconcile fan-out) honour this var.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: rest.RuntimeLogLevel(),
	})))

	return flags
}

// newScheme returns a runtime.Scheme with both clientgoscheme
// (core types) and the blockstor CRD group registered. Built
// per-invocation so the scheme is not a package global.
func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(blockstoriov1alpha1.AddToScheme(scheme))

	return scheme
}

// buildManager wires the controller-runtime manager with leader
// election off — every apiserver replica serves reads
// independently. Caches still warm up so the REST server's
// cached-client reads are cheap.
func buildManager(flags *apiserverFlags) (manager.Manager, error) {
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: newScheme(),
		Metrics: metricsserver.Options{
			BindAddress:   flags.metricsAddr,
			SecureServing: false,
			TLSOpts:       []func(*tls.Config){},
		},
		HealthProbeBindAddress: flags.probeAddr,
		LeaderElection:         false,
	})
	if err != nil {
		return nil, errors.Wrap(err, "new manager")
	}

	return mgr, nil
}

// resolveNamespace mirrors the controller's namespace-resolution
// rule: explicit --controller-namespace wins, then $POD_NAMESPACE,
// then the kustomize default `blockstor-system`.
func resolveNamespace(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}

	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	return "blockstor-system"
}

// registerProbesAndREST wires the /healthz + /readyz endpoints
// and registers the REST server as a manager Runnable. /healthz
// stays a liveness probe (manager alive ⇒ 200); /readyz is gated
// by `ready` which flips to 200 only after BOTH the
// controller-runtime cache has completed its initial sync AND the
// REST listener has bound (issue 213).
//
// The cache-sync handoff lives in main() (it needs the manager's
// own ctx); here we only register the gate and the OnReady hook
// on the rest.Server so the bind signal flows through.
func registerProbesAndREST(mgr manager.Manager, st store.Store, flags *apiserverFlags, namespace string, ready *readyState) error {
	err := mgr.AddHealthzCheck("healthz", healthz.Ping)
	if err != nil {
		return errors.Wrap(err, "add healthz check")
	}

	err = mgr.AddReadyzCheck("apiserver-ready", ready.Check)
	if err != nil {
		return errors.Wrap(err, "add readyz check")
	}

	err = mgr.Add(&rest.Server{
		Addr:      flags.restAddr,
		Store:     st,
		Client:    mgr.GetClient(),
		Namespace: namespace,
		OnReady:   ready.MarkBound,
	})
	if err != nil {
		return errors.Wrap(err, "add rest server")
	}

	return nil
}

// waitForCacheSync blocks on the manager's cache sync and then
// flips the cache-sync latch on `ready` exactly once. Run in a
// goroutine off main so the apiserver boot path never blocks on
// cache warmup — production clusters with hundreds of CRDs can
// take seconds to list on a cold apiserver. Context cancellation
// (pod shutting down) suppresses the flip — at that point kubelet
// readiness no longer matters.
func waitForCacheSync(ctx context.Context, mgr manager.Manager, ready *readyState, log func(string)) {
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return
	}

	log("apiserver cache sync complete, marking ready")
	ready.MarkCacheSynced()
}

func main() {
	setupLog := ctrl.Log.WithName("setup")

	flags := parseFlags()
	namespace := resolveNamespace(flags.controllerNamespace)

	mgr, err := buildManager(flags)
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// CRD-backed store is the only supported persistence layer
	// post-Phase-11 — the apiserver/controller split made
	// in-process state pointless across replicas.
	st := storek8s.New(mgr.GetClient())

	ready := newReadyState()

	// Bug 219: `ctrl.SetupSignalHandler` is one-shot — a second call
	// panics with "close of closed channel" because the signal channel
	// is closed on first invocation. Capture the context once and pass
	// the same instance to both the cache-sync watcher and mgr.Start.
	signalCtx := ctrl.SetupSignalHandler()

	go waitForCacheSync(signalCtx, mgr, ready, func(msg string) { setupLog.Info(msg) })

	err = registerProbesAndREST(mgr, st, flags, namespace, ready)
	if err != nil {
		setupLog.Error(err, "Failed to register apiserver components")
		os.Exit(1)
	}

	setupLog.Info("Starting apiserver", "rest", flags.restAddr, "namespace", namespace)

	err = mgr.Start(signalCtx)
	if err != nil {
		setupLog.Error(err, "apiserver failed")
		os.Exit(1)
	}
}
