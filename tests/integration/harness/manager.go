//go:build integration

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

package harness

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/rest"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
)

const (
	// managerShutdownBudget caps how long t.Cleanup waits for the
	// manager goroutine to exit before logging the leak.
	managerShutdownBudget = 30 * time.Second

	// healthzPollInterval is how often waitForHealthz retries the
	// /v1/healthz GET while the REST server is coming up.
	healthzPollInterval = 50 * time.Millisecond

	// healthzReadyTimeout caps the whole "wait until REST is up"
	// budget.
	healthzReadyTimeout = 30 * time.Second

	// restNamespace is the namespace pkg/rest.Server uses for the
	// few native-object endpoints (passphrase Secret, etc.).
	// Cluster-scoped CRDs ignore it; we pick `default` because
	// envtest pre-creates that namespace.
	restNamespace = "default"
)

// errHealthzUnready is the sentinel returned by pingHealthz when
// the server answered with a non-200/204 status. Static so err113
// can see it (and so a test can match with errors.Is if it ever
// wants to).
var errHealthzUnready = errors.New("healthz unready")

// Stack is the fully-booted Tier 2 integration target: an envtest
// kube-apiserver, the controller-runtime manager with every
// reconciler wired in, the LINSTOR-compatible REST server, and the
// in-process satellite mock. RestURL is `http://127.0.0.1:<port>` —
// the URL `linstor --controllers` accepts.
type Stack struct {
	Env       *Env
	RestURL   string
	Manager   manager.Manager
	Satellite *Satellite
}

// StartStack composes Env + Manager + REST + Satellite. Layout
// mirrors cmd/controller/main.go's manager bootstrap so the
// production wire-up and the test wire-up cannot drift on which
// reconciler exists.
//
// Concurrency: the manager runs in its own goroutine; the REST
// server is registered as a manager.Runnable so it shuts down with
// the manager. t.Cleanup cancels the root context and waits for
// the manager to exit before tearing down envtest.
func StartStack(t *testing.T) *Stack {
	t.Helper()

	env := Start(t)

	// Pre-flight: blockstor's CRDs install validating webhooks
	// via CEL XValidation rules (see e.g. StoragePool's
	// `metadata.name == poolName.nodeName`). envtest already
	// applied them when Start returned. Nothing extra to do.

	mgr, err := buildIntegrationManager(env)
	if err != nil {
		t.Fatalf("build manager: %v", err)
	}

	st := storek8s.New(mgr.GetClient())

	// Wire every reconciler / runnable cmd/controller/main.go
	// registers. Mirror order exactly so a future split-or-merge
	// of the controller binary cannot surprise these tests.
	err = wireReconcilers(mgr, st)
	if err != nil {
		t.Fatalf("wire reconcilers: %v", err)
	}

	restURL, err := mountREST(mgr, st)
	if err != nil {
		t.Fatalf("mount REST: %v", err)
	}

	sat := NewSatellite(mgr.GetClient())

	err = mgr.Add(sat)
	if err != nil {
		t.Fatalf("add satellite mock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- mgr.Start(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		// Bounded wait so a hung Start can't deadlock the test
		// suite — envtest.Stop runs from Env's Cleanup next and
		// will reclaim the apiserver regardless.
		select {
		case err := <-done:
			if err != nil {
				t.Logf("manager exited: %v", err)
			}
		case <-time.After(managerShutdownBudget):
			t.Logf("manager did not exit within %s", managerShutdownBudget)
		}
	})

	waitForHealthz(t, restURL, healthzReadyTimeout)

	return &Stack{
		Env:       env,
		RestURL:   restURL,
		Manager:   mgr,
		Satellite: sat,
	}
}

// buildIntegrationManager constructs the controller-runtime manager
// the test stack drives. LeaderElection off (every replica is
// authoritative in tests), metrics disabled (no need for a Prometheus
// listener in unit-of-integration tests).
func buildIntegrationManager(env *Env) (manager.Manager, error) {
	scheme := clientgoscheme.Scheme
	utilruntime.Must(blockstoriov1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(env.Cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	})
	if err != nil {
		return nil, fmt.Errorf("new manager: %w", err)
	}

	err = mgr.AddHealthzCheck("healthz", healthz.Ping)
	if err != nil {
		return nil, fmt.Errorf("add healthz: %w", err)
	}

	err = mgr.AddReadyzCheck("readyz", healthz.Ping)
	if err != nil {
		return nil, fmt.Errorf("add readyz: %w", err)
	}

	return mgr, nil
}

// wireReconcilers attaches every reconciler / runnable
// cmd/controller/main.go registers. Kept in lockstep with the
// production main so a new reconciler landing on `main` is one
// edit on each side — not two diff hunks lost in code review.
//
// Split into per-domain helpers (node / storage / resource /
// background) so each one stays under the funlen budget; the
// production main groups them by convention rather than by helper.
func wireReconcilers(mgr manager.Manager, store *storek8s.Store) error {
	err := wireNodeReconcilers(mgr, store)
	if err != nil {
		return err
	}

	err = wireStoragePoolReconciler(mgr)
	if err != nil {
		return err
	}

	err = wireResourceGroupReconcilers(mgr, store)
	if err != nil {
		return err
	}

	err = wireResourceReconcilers(mgr, store)
	if err != nil {
		return err
	}

	return wireBackgroundRunnables(mgr)
}

func wireNodeReconcilers(mgr manager.Manager, store *storek8s.Store) error {
	err := (&controller.NodeReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  store,
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("node: %w", err)
	}

	err = (&controller.NodeHeartbeatReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("node-heartbeat: %w", err)
	}

	err = (&controller.NodeLabelSyncReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("node-label-sync: %w", err)
	}

	return nil
}

func wireStoragePoolReconciler(mgr manager.Manager) error {
	err := (&controller.StoragePoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("storagepool: %w", err)
	}

	return nil
}

func wireResourceGroupReconcilers(mgr manager.Manager, store *storek8s.Store) error {
	err := (&controller.ResourceGroupReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  store,
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("resourcegroup: %w", err)
	}

	err = (&controller.RGRebalanceReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  store,
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("rg-rebalance: %w", err)
	}

	return nil
}

func wireResourceReconcilers(mgr manager.Manager, store *storek8s.Store) error {
	err := (&controller.ResourceDefinitionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  store,
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("resourcedefinition: %w", err)
	}

	err = (&controller.ResourceReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Store:  store,
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("resource: %w", err)
	}

	err = (&controller.ResourceMigrationReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("resource-migration: %w", err)
	}

	err = (&controller.SnapshotReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	return nil
}

func wireBackgroundRunnables(mgr manager.Manager) error {
	err := (&controller.AutoSnapshotRunnable{Client: mgr.GetClient()}).RegisterWithManager(mgr)
	if err != nil {
		return fmt.Errorf("auto-snapshot: %w", err)
	}

	err = (&controller.AutoEvictReconciler{Client: mgr.GetClient()}).RegisterWithManager(mgr)
	if err != nil {
		return fmt.Errorf("auto-evict: %w", err)
	}

	return nil
}

// mountREST picks a free 127.0.0.1 port, hands it to the
// pkg/rest.Server, and returns the resulting http://… URL the
// linstor CLI consumes via --controllers. The free-port-then-bind
// race is documented + accepted (TOCTOU window is microseconds and
// the listener immediately re-binds).
func mountREST(mgr manager.Manager, store *storek8s.Store) (string, error) {
	addr, err := pickFreePort()
	if err != nil {
		return "", fmt.Errorf("pick free port: %w", err)
	}

	err = mgr.Add(&rest.Server{
		Addr:      addr,
		Store:     store,
		Client:    mgr.GetClient(),
		Namespace: restNamespace,
	})
	if err != nil {
		return "", fmt.Errorf("add REST: %w", err)
	}

	return "http://" + addr, nil
}

// pickFreePort asks the kernel for an unused 127.0.0.1 port,
// closes the listener, and returns the host:port string. There
// is a tiny race between Close + the REST server's Listen — in
// practice this is fine for in-process tests; the port is
// exclusively local and nothing else races us in the test process.
func pickFreePort() (string, error) {
	listenCfg := net.ListenConfig{}

	listener, err := listenCfg.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}

	addr := listener.Addr().String()

	err = listener.Close()
	if err != nil {
		return "", fmt.Errorf("close probe listener: %w", err)
	}

	return addr, nil
}

// waitForHealthz polls /v1/healthz until it answers 204 or the
// deadline elapses. The REST server starts as a manager runnable,
// so the listener is up shortly after mgr.Start; this poll keeps
// the test deterministic without a hard sleep.
func waitForHealthz(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	cli := &http.Client{Timeout: time.Second}

	for {
		err := pingHealthz(cli, baseURL)
		if err == nil {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("REST /v1/healthz did not answer within %s (last err=%v)", timeout, err)
		}

		time.Sleep(healthzPollInterval)
	}
}

// pingHealthz issues one GET /v1/healthz request and returns nil
// when the server answered 200 or 204. Extracted from
// waitForHealthz so the polling loop stays readable and so the
// noctx linter's "use Do(*http.Request)" guidance applies in one
// place.
func pingHealthz(cli *http.Client, baseURL string) error {
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodGet, baseURL+"/v1/healthz", http.NoBody)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := cli.Do(req)
	if err != nil {
		return fmt.Errorf("get healthz: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("%w: status %d", errHealthzUnready, resp.StatusCode)
}
