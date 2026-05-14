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

// Package rest exposes the LINSTOR REST API contract on top of the
// controller-runtime manager. It is wired in cmd/controller/main.go via mgr.Add so the
// HTTP server's lifecycle follows the manager's.
package rest

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/version"
)

// Server implements manager.Runnable so it shuts down with the manager.
//
// Store may be nil for endpoints that don't need it (e.g. /v1/controller/version);
// handlers that do need it return 503 if it is nil so the binary stays bootable
// while the persistence backend is still being plumbed in (Phase 2).
//
// Client + Namespace are wired by Phase 10.4 so endpoints that operate
// on native Kubernetes objects (e.g. the cluster passphrase Secret) can
// reach the apiserver directly. Both may be nil/empty in tests that
// only exercise the legacy KV-backed path.
type Server struct {
	Addr      string // e.g. ":3370" — upstream LINSTOR plain-text REST port
	Store     store.Store
	Client    client.Client // controller-runtime client for native-object endpoints
	Namespace string        // namespace where blockstor's own Secrets/ConfigMaps live

	// linstorRemotes is the in-memory registry for LINSTOR remotes —
	// see pkg/rest/remotes.go. Lazy-initialised on the first call to
	// registerRemotes so a zero-value Server keeps working in tests
	// that build the mux without going through the full constructor.
	linstorRemotes *linstorRemoteRegistry

	// (resourceConnections registry from 5.W04 dropped on cherry-pick
	// conflict with wave1 3.7's RD-prop-backed storage of paths —
	// pkg/rest/resource_connections.go stores per-(rd, a, b) state
	// directly on RD.Spec.Props now; no separate registry needed.)

	// errorReports is the in-memory ring buffer that backs
	// `linstor err l` / `GET /v1/error-reports`. Reconcilers and
	// REST handlers call RecordErrorReport to push a structured
	// entry; the LIST handler returns the buffered slice in
	// reverse-chronological order. Lazy-initialised on first push
	// so a zero-value Server keeps working in tests.
	errorReports *errorReportRing

	// passphraseUnlocked is the in-memory "controller process has
	// proof-of-knowledge of the cluster master passphrase" flag
	// that gates LUKS-layer resource provisioning. Scenario 6.W13.
	//
	// Lifecycle: the controller boots zero (false) on every start
	// — that's the whole point of `linstor encryption
	// enter-passphrase`: the master key is intentionally NOT
	// persisted unencrypted, so a controller restart loses the
	// in-memory unlock and the operator has to PATCH it back. A
	// successful POST (create-passphrase) on an empty cluster
	// also flips this on, since the caller demonstrably knows the
	// value they just stamped. A wrong PATCH leaves it untouched.
	//
	// The REST /v1/view/resources handler consults this flag when
	// it walks each replica's LayerObject — if a LUKS layer is
	// present and the flag is false, the synthesized state.suspended
	// is true (rendered `Suspended` by `linstor r l`); when the flag
	// is true, state.suspended is false (rendered `Available`); for
	// non-LUKS replicas the field is left absent.
	//
	// Atomic for the cheap-read fast path: every entry of the view
	// handler reads this flag, but writes happen at most once per
	// `enter-passphrase` PATCH (a low-frequency operator action),
	// so a mutex would be overkill.
	passphraseUnlocked atomic.Bool

	// resolveHost is the DNS-lookup seam used by handleNodeCreate
	// (scenario 4.W01) when the POST body omits a NetInterface
	// address. Tests inject a stub via Server.SetResolveHost to keep
	// the unit suite hermetic — nil means "use the production
	// net.DefaultResolver via defaultResolveHost".
	resolveHost resolveHostFunc
}

// SetResolveHost overrides the DNS-lookup function used by
// handleNodeCreate. Returns the previous value so tests can restore
// it. Production code never calls this; defaultResolveHost is used
// when the field is nil.
func (s *Server) SetResolveHost(fn resolveHostFunc) resolveHostFunc {
	prev := s.resolveHost
	s.resolveHost = fn

	return prev
}

// lookupHost dispatches to s.resolveHost if set, else the package
// default. Hoisted off-handler so the production handler stays
// readable and the test seam is explicit.
func (s *Server) lookupHost(ctx context.Context, host string) ([]string, error) {
	if s.resolveHost != nil {
		return s.resolveHost(ctx, host)
	}

	return defaultResolveHost(ctx, host)
}

// NeedLeaderElection reports whether the server requires leader election.
// REST is read-mostly today and safe to run on every replica; once we
// introduce write-paths that need a single leader we'll change this to true.
func (s *Server) NeedLeaderElection() bool { return false }

// Start runs the HTTP server until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("rest")

	mux := s.buildMux()

	srv := &http.Server{
		Addr:              s.Addr,
		Handler:           withLogging(mux),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(_ net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)

	go func() {
		logger.Info("REST API listening",
			"addr", s.Addr,
			"linstor_contract", version.LinstorVersion,
			"rest_api", version.RestAPIVersion)

		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		return errors.Wrap(srv.Shutdown(shutCtx), "shutdown REST server")
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return errors.Wrap(err, "REST server failed")
	}
}

// buildMux registers every endpoint on a fresh ServeMux. Pulled out
// of Start to keep the latter under the funlen budget — Start now
// only handles lifecycle (listener + shutdown), this owns routing.
func (s *Server) buildMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/controller/version", handleVersion)
	mux.HandleFunc("GET /v1/healthz", handleHealth)
	// /metrics exposes Prometheus exposition-format text on the same
	// listener as the LINSTOR REST API. Scenario 7.W08 (wave2 K8s
	// monitoring stack): a ServiceMonitor targeting the apiserver
	// Service:3370 scrapes this endpoint, picking up the default
	// process_*/go_* collectors registered by client_golang plus any
	// blockstor-specific counters added later. Mounting it on 3370
	// (instead of controller-runtime's separate metrics port) matches
	// upstream LINSTOR's "single REST port" contract — operators don't
	// have to teach Prometheus a second endpoint per replica.
	mux.Handle("GET /metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{},
	))
	s.registerNodes(mux)
	s.registerStoragePools(mux)
	s.registerResourceGroups(mux)
	s.registerResourceDefinitions(mux)
	s.registerVolumeDefinitions(mux)
	s.registerResources(mux)
	s.registerKeyValueStore(mux)
	s.registerSnapshots(mux)
	s.registerSpawn(mux)
	s.registerAutoplace(mux)
	s.registerControllerProperties(mux)
	s.registerAdjust(mux)
	s.registerResourceLifecycle(mux)
	s.registerResourceToggleDisk(mux)
	s.registerStats(mux)
	s.registerErrorReports(mux)
	s.registerPropertiesInfo(mux)
	s.registerSnapshotRestore(mux)
	s.registerEncryption(mux)
	s.registerNodeLifecycle(mux)
	s.registerDRBDProxy(mux)
	s.registerExternalFiles(mux)
	s.registerDRBDPassphrase(mux)
	s.registerRDClone(mux)
	s.registerSOSReport(mux)
	s.registerRemotes(mux)
	s.registerNodeConnections(mux)
	s.registerResourceConnections(mux)
	s.registerSnapshotMulti(mux)
	s.registerQuerySizeInfo(mux)
	s.registerAdvise(mux)
	s.registerPhysicalStorage(mux)
	s.registerResourceGroupExtras(mux)

	return mux
}

// Compile-time check that we satisfy the runnable contract.
var _ manager.Runnable = (*Server)(nil)

func handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, apiv1.ControllerVersion{
		Version:        version.LinstorVersion,
		GitHash:        version.LinstorGitHash,
		BuildTime:      version.LinstorBuildTime,
		RestAPIVersion: version.RestAPIVersion,
	})
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.FromContext(r.Context()).WithName("rest").V(1).Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"dur_ms", time.Since(start).Milliseconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter

	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}
