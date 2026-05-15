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
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"net"
	"net/http"
	"strings"
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
		Handler:           withLogging(with404Envelope(mux)),
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
	s.registerResourceModify(mux)
	s.registerControllerProperties(mux)
	s.registerAdjust(mux)
	s.registerResourceLifecycle(mux)
	s.registerResourceToggleDisk(mux)
	s.registerStats(mux)
	s.registerErrorReports(mux)
	// Bug 126: previously we wired /v1/.../properties/info to a stub
	// that returned a bare `[]`. The CLI consumed the empty array
	// silently with no signal that the catalogue isn't populated. We
	// now leave the routes unregistered so the with404Envelope
	// catch-all (Bug 103) emits the typed LINSTOR "endpoint not
	// implemented" envelope — operators get a real ERROR line, and
	// once a real property catalogue ships the route can be re-wired
	// with the populated payload.
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
	s.registerSchedules(mux)

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

// with404Envelope wraps the inner handler so that a bare 404 OR 405
// reply from the http.ServeMux fallback (plain-text "404 page not
// found\n" or "Method Not Allowed\n") is rewritten to the LINSTOR
// `[]ApiCallRc` JSON envelope. Bug 103 (404) and Bug 109 (405):
// python-linstor's error-decoding path tries JSON, falls back to XML
// on parse failure, and crashes with
// `xml.etree.ElementTree.ParseError: syntax error: line 1, column 0`
// on the plain-text body — instead of surfacing a typed `ERROR:`
// line, the CLI dies with a Python traceback.
//
// Only the http.ServeMux fallback shape is rewritten:
//   - status == 404 with the plain-text "404 page not found" marker
//     (or an empty body), OR
//   - status == 405 with the plain-text "Method Not Allowed" marker
//     (or an empty body).
//
// A previous attempt at the 404 fix used a naked catch-all
// `mux.HandleFunc("/", …)` which broke the per-route 405 dispatch —
// that's why this is implemented as a body-buffering wrapper around
// the mux rather than a route on the mux itself. The 405 path
// preserves both the status code AND the `Allow:` header that
// http.ServeMux populates for wrong-verb requests, so spec-compliant
// HTTP clients can still discover the supported verbs.
//
// Handlers that produce their own 404/405 with a JSON body (e.g.
// `GET /v1/nodes/{name}` on a missing node, which already emits a
// LINSTOR envelope through writeError) are not touched — the body-
// shape sniff catches only the plain-text fallback.
func with404Envelope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := &envelopeBuffer{
			header: http.Header{},
			status: http.StatusOK,
		}

		next.ServeHTTP(buf, r)

		switch {
		case buf.status == http.StatusNotFound && isPlainTextFallbackBody(buf.body.Bytes(), "404 page not found"):
			writeNotFoundEnvelope(w, r)

			return
		case buf.status == http.StatusMethodNotAllowed && isPlainTextFallbackBody(buf.body.Bytes(), "Method Not Allowed"):
			writeMethodNotAllowedEnvelope(w, r, buf.header.Get("Allow"))

			return
		}

		// Pass through unchanged.
		maps.Copy(w.Header(), buf.header)
		w.WriteHeader(buf.status)
		_, _ = w.Write(buf.body.Bytes())
	})
}

// envelopeBuffer is a buffering http.ResponseWriter used by
// with404Envelope. The wrapper needs to inspect the inner handler's
// status AND body before deciding whether to forward bytes to the
// real client — for that the writes must land in a buffer, not on
// the wire. Status code defaults to 200 (matches net/http semantics
// when a handler writes a body without explicitly calling WriteHeader).
type envelopeBuffer struct {
	header      http.Header
	body        bytes.Buffer
	status      int
	wroteHeader bool
}

func (b *envelopeBuffer) Header() http.Header {
	return b.header
}

func (b *envelopeBuffer) Write(p []byte) (int, error) {
	if !b.wroteHeader {
		b.wroteHeader = true
	}

	return b.body.Write(p) //nolint:wrapcheck // bytes.Buffer.Write never returns a non-nil error
}

func (b *envelopeBuffer) WriteHeader(code int) {
	if b.wroteHeader {
		return
	}

	b.status = code
	b.wroteHeader = true
}

// isPlainTextFallbackBody reports whether body looks like an
// http.ServeMux fallback plain-text response — either empty or
// containing the supplied marker (e.g. "404 page not found",
// "Method Not Allowed"). We deliberately accept the empty body case
// too: a handler that calls `http.Error(w, "", http.StatusNotFound)`
// with no message lands here, and the python-linstor crash is
// identical.
//
// Handler-emitted JSON 4xx replies (already-formed `[]ApiCallRc`
// envelopes from writeError) are recognised by the leading `[` —
// they're NOT rewritten, the original body flows through.
func isPlainTextFallbackBody(body []byte, marker string) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return true
	}

	// JSON envelope already — preserve it.
	if trimmed[0] == '[' || trimmed[0] == '{' {
		return false
	}

	return strings.Contains(string(trimmed), marker)
}

// writeNotFoundEnvelope emits the LINSTOR-shaped `[]ApiCallRc` body
// for an unwired endpoint. Bug 103: see with404Envelope.
//
// ret_code: APICallRcMaskError so python-linstor classifies the entry
// as ERROR. We don't OR in a sub-code mask (upstream's
// `FAIL_UNKNOWN_ERROR` is a generic 0 in the error band) — the
// message + cause carry the operator-facing detail.
func writeNotFoundEnvelope(w http.ResponseWriter, r *http.Request) {
	envelope := []apiv1.APICallRc{{
		RetCode: apiv1.APICallRcMaskError,
		Message: "endpoint not implemented",
		Cause: "the path " + r.Method + " " + r.URL.Path +
			" is not registered on this apiserver",
		Correc: "check the blockstor REST API documentation " +
			"or upgrade the controller",
	}}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(envelope)
}

// writeMethodNotAllowedEnvelope emits the LINSTOR-shaped `[]ApiCallRc`
// body for a wrong-verb request against a wired path. Bug 109: see
// with404Envelope. The `Allow` header captured from http.ServeMux's
// fallback response is propagated so HTTP-spec-aware clients can still
// retry with a supported verb.
//
// ret_code: APICallRcMaskError matches the 404 envelope so the python
// CLI classifies the reply as an ERROR (typed `ERROR: method not
// allowed` line) instead of a Python traceback.
func writeMethodNotAllowedEnvelope(w http.ResponseWriter, r *http.Request, allow string) {
	envelope := []apiv1.APICallRc{{
		RetCode: apiv1.APICallRcMaskError,
		Message: "method not allowed",
		Cause: "the path " + r.Method + " " + r.URL.Path +
			" is registered, but not for this verb",
		Correc: "check the LINSTOR REST API documentation; " +
			"the Allow header lists supported methods",
	}}

	if allow != "" {
		w.Header().Set("Allow", allow)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	_ = json.NewEncoder(w).Encode(envelope)
}
