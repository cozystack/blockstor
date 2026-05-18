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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/rest"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(blockstoriov1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var restAddr string
	var enableRestAPI bool
	var controllerNamespace string
	var tlsOpts []func(*tls.Config)
	flag.BoolVar(&enableRestAPI, "enable-rest-api", false,
		"Mount the LINSTOR-compatible REST API alongside the reconcilers. Default is OFF — the apiserver runs as a separate Deployment (cmd/apiserver) since Phase 11.x. Set to true only for the legacy single-binary deployment.")
	flag.StringVar(&restAddr, "rest-bind-address", ":3370",
		"The address the LINSTOR-compatible REST API binds to (upstream LINSTOR plain-text port is 3370). Ignored when --enable-rest-api=false.")
	flag.StringVar(&controllerNamespace, "controller-namespace", "",
		"Namespace where the controller's own Secrets/ConfigMaps live (default: $POD_NAMESPACE, then 'blockstor-system').")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	controllerNamespace = resolveControllerNamespace(controllerNamespace)

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime auto-generates
	// self-signed certs for the metrics server. Production deployments
	// should pass --metrics-cert-path / --metrics-cert-name / --metrics-cert-key
	// pointing at cert-manager-issued material instead.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "a3f89427.blockstor.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// Construct the store before reconciler wiring so the
	// NodeReconciler can drive eviction-triggered migration via the
	// shared placer. CRD-backed is the only supported persistence
	// layer since Phase 11.x — the apiserver/controller split makes
	// in-process state pointless across replicas.
	st := storek8s.New(mgr.GetClient())

	if err := (&controller.NodeReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  st,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "node")
		os.Exit(1)
	}
	if err := (&controller.NodeHeartbeatReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "node-heartbeat")
		os.Exit(1)
	}
	if err := (&controller.NodeLabelSyncReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "node-label-sync")
		os.Exit(1)
	}
	if err := (&controller.StoragePoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "storagepool")
		os.Exit(1)
	}
	if err := (&controller.ResourceGroupReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  st,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "resourcegroup")
		os.Exit(1)
	}
	if err := (&controller.RGRebalanceReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  st,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "rg-rebalance")
		os.Exit(1)
	}
	if err := (&controller.ResourceDefinitionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  st,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "resourcedefinition")
		os.Exit(1)
	}
	if err := (&controller.ResourceReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  st,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "resource")
		os.Exit(1)
	}
	if err := (&controller.ResourceMigrationReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "resource-migration")
		os.Exit(1)
	}
	if err := (&controller.ResourceStateProjectionReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "resource-state-projection")
		os.Exit(1)
	}
	if err := (&controller.SnapshotReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "snapshot")
		os.Exit(1)
	}
	if err := (&controller.AutoSnapshotRunnable{
		Client: mgr.GetClient(),
	}).RegisterWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to register runnable", "runnable", "auto-snapshot")
		os.Exit(1)
	}
	if err := (&controller.AutoEvictReconciler{
		Client: mgr.GetClient(),
	}).RegisterWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to register runnable", "runnable", "auto-evict")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}

	// Bug 217: /readyz must gate on real readiness signals — cache
	// sync and (when REST is enabled) REST listener bind — not on
	// healthz.Ping which is true the instant the route is mounted.
	// Same defect Bug 207 closed in cmd/satellite and Bug 213 closed
	// in cmd/apiserver; this is the legacy single-binary path.
	ready := newReadyState()
	if err := mgr.AddReadyzCheck("controller-ready", ready.Check); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	if enableRestAPI {
		ready.ArmREST()

		if err := mgr.Add(&rest.Server{
			Addr:      restAddr,
			Store:     st,
			Client:    mgr.GetClient(),
			Namespace: controllerNamespace,
			OnReady:   ready.MarkBound,
		}); err != nil {
			setupLog.Error(err, "Failed to register REST API server")
			os.Exit(1)
		}
	} else {
		setupLog.Info("REST API mount disabled; run cmd/apiserver as a separate Deployment")
	}

	signalCtx := ctrl.SetupSignalHandler()
	go waitForCacheSync(signalCtx, mgr, ready, func(msg string) { setupLog.Info(msg) })

	setupLog.Info("Starting manager")
	if err := mgr.Start(signalCtx); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

// waitForCacheSync blocks on the manager's cache sync and then flips
// the cache-sync latch on `ready` exactly once. Run in a goroutine
// off main so the controller boot path never blocks on cache warmup
// — production clusters with hundreds of CRDs can take seconds to
// list on a cold apiserver. Context cancellation (pod shutting down)
// suppresses the flip — at that point kubelet readiness no longer
// matters.
func waitForCacheSync(ctx context.Context, mgr manager.Manager, ready *readyState, log func(string)) {
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return
	}

	log("controller cache sync complete, marking ready")
	ready.MarkCacheSynced()
}

// resolveControllerNamespace picks the namespace the controller's
// own Secrets/ConfigMaps live in. Precedence: explicit flag value
// wins, then $POD_NAMESPACE (the standard downward-API env var),
// then the kustomize default `blockstor-system`. Centralised so
// the flag's help text and the runtime default never drift.
func resolveControllerNamespace(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}

	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	return "blockstor-system"
}
