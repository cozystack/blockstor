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
	"crypto/tls"
	"flag"
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
	storeKind           string
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
	flag.StringVar(&flags.storeKind, "store", "k8s",
		"Persistence backend: 'k8s' (CRD-backed) or 'memory' (in-process).")
	flag.StringVar(&flags.controllerNamespace, "controller-namespace", "",
		"Namespace for the apiserver's own Secrets/ConfigMaps (default $POD_NAMESPACE then blockstor-system).")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

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

// buildStore picks the persistence backend. Same selector the
// controller binary uses so misconfiguration looks identical
// across the two deployments.
func buildStore(kind string, mgr manager.Manager, log func(string)) (store.Store, error) {
	switch kind {
	case "k8s":
		return storek8s.New(mgr.GetClient()), nil
	case "memory":
		log("Using in-memory store; data lost on restart")

		return store.NewInMemory(), nil
	default:
		return nil, errors.Errorf("unknown --store value %q (want k8s or memory)", kind)
	}
}

func registerProbesAndREST(mgr manager.Manager, st store.Store, flags *apiserverFlags, namespace string) error {
	err := mgr.AddHealthzCheck("healthz", healthz.Ping)
	if err != nil {
		return errors.Wrap(err, "add healthz check")
	}

	err = mgr.AddReadyzCheck("readyz", healthz.Ping)
	if err != nil {
		return errors.Wrap(err, "add readyz check")
	}

	err = mgr.Add(&rest.Server{
		Addr:      flags.restAddr,
		Store:     st,
		Client:    mgr.GetClient(),
		Namespace: namespace,
	})
	if err != nil {
		return errors.Wrap(err, "add rest server")
	}

	return nil
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

	st, err := buildStore(flags.storeKind, mgr, func(msg string) { setupLog.Info(msg) })
	if err != nil {
		setupLog.Error(err, "store init", "store", flags.storeKind)
		os.Exit(1)
	}

	err = registerProbesAndREST(mgr, st, flags, namespace)
	if err != nil {
		setupLog.Error(err, "Failed to register apiserver components")
		os.Exit(1)
	}

	setupLog.Info("Starting apiserver", "rest", flags.restAddr, "namespace", namespace)

	err = mgr.Start(ctrl.SetupSignalHandler())
	if err != nil {
		setupLog.Error(err, "apiserver failed")
		os.Exit(1)
	}
}
