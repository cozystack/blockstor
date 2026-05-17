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
	"io"
	"maps"
	"mime"
	"net"
	"net/http"
	"regexp"
	"strconv"
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

// maxRequestBodyBytes caps an inbound REST request body before any
// decode attempt. Upstream LINSTOR's RD/Resource/snapshot payloads
// are all well under 64 KiB (a populous RG-spawn lands around 8 KiB
// in practice); 1 MiB leaves a generous headroom for clusters with
// hundreds of aux-props per RD while still being well below the K8s
// CRD object cap (~1 MiB request, ~1.5 MiB stored) AND below etcd's
// 1.5 MiB request limit. Bug 146: without this cap an oversized POST
// flowed straight into the persistence backend, which returned the
// raw `etcdserver: request is too large` string in the 500 body —
// leaking the K8s/etcd impl identity AND crashing python-linstor's
// `[]ApiCallRc` decoder.
const maxRequestBodyBytes int64 = 1 << 20

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

	// OnReady, when non-nil, is invoked exactly once after the
	// TCP listener has been successfully bound and BEFORE the
	// HTTP serve loop accepts the first connection. The apiserver
	// uses this to flip its readyz gate (issue 213): kube-proxy
	// must not see the pod Ready until clients can actually
	// connect to s.Addr, otherwise rolling restarts of the
	// apiserver Deployment surface as transient `Connection
	// refused` / 5xx at every CSI / CLI client.
	//
	// Set from outside before mgr.Add(&rest.Server{...}); never
	// mutated by Start. Callbacks must be non-blocking — Start
	// invokes the callback inline on its own goroutine and any
	// blocking work delays the serve loop entry.
	OnReady func()
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

	// Middleware order, outer → inner:
	//   withLogging         — observes the final status code (incl. 4xx
	//                          envelopes the inner layers emit).
	//   instrumentRequests  — Bug 168: emits the blockstor_requests_total
	//                          / blockstor_request_duration_seconds
	//                          series. Sits inside withLogging so the
	//                          status code observation matches the one
	//                          the log line records, but outside the
	//                          envelope/content-type middlewares so 4xx
	//                          replies they emit are still counted.
	//   with404Envelope     — rewrites ServeMux's plain-text 404/405
	//                          fallback bodies to LINSTOR `[]ApiCallRc`
	//                          (Bug 103, Bug 109).
	//   withContentTypeJSON — Bug 147: refuses POST/PUT/PATCH with a
	//                          non-JSON Content-Type before any handler
	//                          tries to decode random bytes as JSON.
	//                          Consults the mux so a wrong-verb request
	//                          (which would 405) is not pre-empted with
	//                          a 415 — the 405 path stays intact.
	//   withBodyLimit       — Bug 146: caps the request body so an
	//                          oversized POST trips a 413 envelope at
	//                          the wire edge instead of leaking the
	//                          etcd/k8s rejection string in a 500.
	srv := &http.Server{
		Addr:              s.Addr,
		Handler:           withLogging(instrumentRequests(mux, withHEADContentLength(with404Envelope(withContentTypeJSON(mux, withBodyLimit(maxRequestBodyBytes, mux)))))),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(_ net.Listener) context.Context { return ctx },
	}

	// Bind the listener up-front so we can signal OnReady BEFORE
	// entering the serve loop. Splitting net.Listen + srv.Serve
	// out of the combined srv.ListenAndServe lets the apiserver
	// hold its /readyz at 503 until clients can actually connect
	// (issue 213). Bind errors are surfaced synchronously — the
	// caller's manager treats Runnable startup failure as fatal,
	// same shape as the pre-split ListenAndServe error path.
	lc := &net.ListenConfig{}

	ln, err := lc.Listen(ctx, "tcp", s.Addr)
	if err != nil {
		return errors.Wrap(err, "REST server listen")
	}

	if s.OnReady != nil {
		s.OnReady()
	}

	errCh := make(chan error, 1)

	go func() {
		logger.Info("REST API listening",
			"addr", s.Addr,
			"linstor_contract", version.LinstorVersion,
			"rest_api", version.RestAPIVersion)

		errCh <- srv.Serve(ln)
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
	s.registerVolumesPerResource(mux)
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
	s.registerUpstreamParity225_229(mux)

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

// with404Envelope wraps the inner handler so that a bare 404 / 405 /
// 3xx-redirect reply from the http.ServeMux fallback (plain-text
// "404 page not found\n" / "Method Not Allowed\n" / HTML
// `<a href="...">Moved Permanently</a>`) is rewritten to the LINSTOR
// `[]ApiCallRc` JSON envelope. Bug 103 (404), Bug 109 (405) and
// Bug 166 (3xx): python-linstor's error-decoding path tries JSON,
// falls back to XML on parse failure, and crashes with
// `xml.etree.ElementTree.ParseError: syntax error: line 1, column 0`
// on a non-JSON body — instead of surfacing a typed `ERROR:` line,
// the CLI dies with a Python traceback.
//
// The http.ServeMux fallback shapes intercepted here:
//   - status == 404 with the plain-text "404 page not found" marker
//     (or an empty body),
//   - status == 405 with the plain-text "Method Not Allowed" marker
//     (or an empty body), and
//   - status ∈ {301, 307, 308} that ServeMux emits on URL
//     pathologies (double slash `//v1/nodes`, parent-relative
//     `/v1/../v1/nodes`, trailing slash on a no-trailing-slash route).
//     These come with `Content-Type: text/html` and an HTML body —
//     the same defect class. We collapse them to a 404 envelope
//     because the LINSTOR REST contract does not need redirects:
//     every path either exists (handler runs) or doesn't (404). The
//     CLI never expected redirects in the first place.
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
		case isServeMuxRedirect(buf.status, buf.header):
			// Bug 166: ServeMux's URL-canonicalisation redirect
			// (//path, /a/../b, /path/ → /path) ships with an HTML
			// body that crashes python-linstor's JSON-only error
			// decoder. The LINSTOR REST API contract has no use
			// for client-side redirects, so we collapse the whole
			// 3xx class to a 404 envelope (same body shape as the
			// "unknown path" case — operators get a typed ERROR).
			writeNotFoundEnvelope(w, r)

			return
		}

		// Pass through unchanged.
		maps.Copy(w.Header(), buf.header)
		w.WriteHeader(buf.status)
		_, _ = w.Write(buf.body.Bytes())
	})
}

// isServeMuxRedirect reports whether the buffered reply looks like an
// http.ServeMux URL-canonicalisation redirect. Three signals must
// align before we rewrite (so we never shadow a legitimate handler-
// emitted 3xx — there isn't one today, but the check stays defensive):
//
//  1. The status is 301, 307 or 308 (the redirect codes net/http's
//     `Redirect` emits; ServeMux currently uses 301 for cleanPath
//     and the same code for the trailing-slash case).
//  2. A `Location` header is present (every legitimate redirect
//     carries one; ServeMux's redirect handler always sets it).
//  3. The Content-Type is text/html OR absent (a handler that
//     produced its own JSON 3xx would set application/json; we
//     don't want to clobber such a body even though no current
//     route emits one).
func isServeMuxRedirect(status int, header http.Header) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
	default:
		return false
	}

	if header.Get("Location") == "" {
		return false
	}

	ct := header.Get("Content-Type")
	if ct == "" {
		return true
	}

	return strings.HasPrefix(ct, "text/html")
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

// withBodyLimit caps every inbound request body at max bytes. Bug 146:
// without this cap, an oversized POST (a misbehaving client, a stuck
// uploader, a malicious peer) sailed past every wire-edge check and
// landed in the persistence backend, which on the K8s/etcd path
// returned the raw `etcdserver: request is too large` string in the
// 500 body. Three things broke:
//
//   - operators saw the etcd error string and learned the backend
//     identity from a single curl, defeating the apiserver's
//     abstraction;
//   - python-linstor's `[]ApiCallRc` decoder crashed because the body
//     wasn't a JSON array; and
//   - the controller burned cycles serialising a CRD it was about to
//     drop on the floor anyway.
//
// The middleware uses two complementary mechanisms:
//
//  1. If the client advertised a Content-Length larger than the cap,
//     reject immediately with 413 — no point burning bytes off the
//     wire. This is the common case for honest clients that just sent
//     too much; it also covers payloads that aren't valid JSON
//     (without this short-circuit a 2MB stream of `x` characters
//     would trip the json decoder's SyntaxError on byte 1 long before
//     the MaxBytesReader fired, surfacing as a 400 instead of 413).
//
//  2. http.MaxBytesReader on the request body — catches chunked
//     transfers and lying Content-Length headers. When that reader
//     trips, json.Decode returns `*http.MaxBytesError`; the decode-
//     error path (writeDecodeError) maps the sentinel to a 413 +
//     LINSTOR envelope.
func withBodyLimit(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxBytes {
			writeError(w, http.StatusRequestEntityTooLarge,
				"request body too large (limit "+formatBytes(maxBytes)+")")

			return
		}

		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		}

		next.ServeHTTP(w, r)
	})
}

// withContentTypeJSON gates POST/PUT/PATCH requests so egregious
// non-JSON bodies are refused at the wire edge before any handler
// tries to decode random bytes as JSON.
//
// Bug 147 originally made this strict: only `application/json` (with
// or without parameters) was accepted, everything else was 415. That
// turned out to be too narrow — Bug 157 (P0 regression): stock
// `python-linstor 1.27.1` does NOT set Content-Type, so Python's
// http.client.HTTPConnection.request auto-fills
// `application/x-www-form-urlencoded` whenever there is a request
// body. Every blockstor CLI write op (`rd c`, `vd c`, `r c`, `sp c`,
// `n c`, `kvs`, encryption, …) was rejected with 415, leaving the
// deployed CLI unusable against the apiserver.
//
// The compromise: accept the media types real-world LINSTOR clients
// actually send AND let the JSON decoder be the final validator on
// the body shape. The Bug 147 protection survives only against
// egregious mismatches that no legitimate JSON client sends.
//
// Accepted (next.ServeHTTP runs, body decoder takes over):
//   - missing Content-Type (python-linstor default before
//     http.client's fallback kicks in)
//   - `application/json` (with or without parameters)
//   - `application/x-www-form-urlencoded` (python-linstor's actual
//     wire shape after http.client's auto-fill)
//   - any other `application/*` (covers application/vnd...+json,
//     application/cbor, application/yaml — narrow JSON encoders that
//     a future client might use; the body decoder will reject if the
//     bytes aren't JSON)
//
// Rejected with 415 + LINSTOR envelope:
//   - `text/*`, `image/*`, `multipart/*`, `video/*`, `audio/*` and
//     anything else that's clearly NOT a structured-data media type
//
// Other rules unchanged from the Bug 147 wiring:
//   - GET / HEAD / DELETE / OPTIONS pass through unconditionally.
//   - Requests that the mux can't dispatch (unknown path → 404 /
//     wrong verb → 405) also pass through so the with404Envelope
//     wrapper can still emit the correct typed reply.
//   - Requests with an empty body (Content-Length 0 AND not chunked)
//     pass through — verb-only mutation endpoints carry no JSON.
//
// The mux argument is the actual http.ServeMux so we can peek at
// whether the request would dispatch — needed for the 404/405
// passthrough rule above.
func withContentTypeJSON(mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !methodCarriesBody(r.Method) {
			next.ServeHTTP(w, r)

			return
		}

		// mux.Handler returns the std-lib's NotFoundHandler / a
		// method-mismatch handler with an empty registered pattern
		// when the route isn't wired for this (method, path) combo.
		// In that case let the request through so with404Envelope
		// emits the correct LINSTOR envelope.
		if _, pattern := mux.Handler(r); pattern == "" {
			next.ServeHTTP(w, r)

			return
		}

		// Empty-body verbs (e.g. `PUT /v1/nodes/{n}/reconnect`) carry
		// no JSON; the Content-Type gate doesn't apply. We detect by
		// Content-Length == 0 with no chunked transfer encoding.
		if requestHasNoBody(r) {
			next.ServeHTTP(w, r)

			return
		}

		ct := r.Header.Get("Content-Type")
		if !isAcceptableBodyContentType(ct) {
			writeUnsupportedMediaTypeEnvelope(w, r, ct)

			return
		}

		next.ServeHTTP(w, r)
	})
}

// requestHasNoBody reports whether the incoming request advertised an
// empty body — Content-Length == 0 AND no chunked transfer encoding.
// Used by withContentTypeJSON to skip the JSON content-type gate on
// verb-only endpoints like `PUT /v1/nodes/{n}/reconnect`.
func requestHasNoBody(r *http.Request) bool {
	if r.ContentLength != 0 {
		return false
	}

	for _, te := range r.TransferEncoding {
		if strings.EqualFold(te, "chunked") {
			return false
		}
	}

	return true
}

// methodCarriesBody reports whether the HTTP method conventionally
// carries a request body that the apiserver will try to JSON-decode.
// GET/HEAD/DELETE/OPTIONS are body-less in this REST contract — the
// Content-Type gate doesn't apply.
func methodCarriesBody(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return true
	default:
		return false
	}
}

// withHEADContentLength wraps the handler chain so HEAD requests
// return the same headers as the GET counterpart — including a
// Content-Length matching the would-be body byte count — without
// writing the body itself. Bug 160: net/http's default chunked-
// stripping behaviour on HEAD removes the body but does not
// substitute Content-Length, violating RFC 9110 §9.3.2 and breaking
// curl -I, some LB health checks, and oncall scripts that lean on
// the response size.
//
// Implementation: for HEAD, replace the ResponseWriter with a
// buffering recorder; run the inner handler; on completion set
// Content-Length from the buffered size, flush the recorded
// headers + status to the real writer, and drop the body.
func withHEADContentLength(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			next.ServeHTTP(w, r)

			return
		}

		// The mux is registered for GET routes; ServeMux serves
		// HEAD via the GET handler natively, but it doesn't add
		// Content-Length. Run the handler as GET against a
		// recorder, then replay headers + Content-Length to the
		// real writer.
		rec := &headRecorder{header: http.Header{}, status: http.StatusOK}
		// Rewrite Method to GET so handlers that branch on
		// Method (e.g. some bug-pattern envelopes) treat this
		// like a normal GET; the recorder swallows the body.
		probe := r.Clone(r.Context())
		probe.Method = http.MethodGet
		next.ServeHTTP(rec, probe)

		for key, vals := range rec.header {
			for _, val := range vals {
				w.Header().Add(key, val)
			}
		}

		w.Header().Set("Content-Length", strconv.Itoa(rec.body.Len()))
		w.WriteHeader(rec.status)
	})
}

// headRecorder is a write-once buffer used by withHEADContentLength
// to capture the GET-shaped response so we can stamp Content-Length
// before dropping the body. The body byte count is the only payload
// metric Bug 160 needs.
type headRecorder struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (h *headRecorder) Header() http.Header {
	return h.header
}

func (h *headRecorder) Write(b []byte) (int, error) {
	return h.body.Write(b)
}

func (h *headRecorder) WriteHeader(code int) {
	h.status = code
}

// isAcceptableBodyContentType reports whether ct names a media type
// that withContentTypeJSON should let through to the body decoder.
//
// Bug 157 (the Bug 147 follow-up): the original Bug 147 implementation
// only accepted `application/json`. That broke stock python-linstor,
// which never sets Content-Type — Python's http.client then auto-fills
// `application/x-www-form-urlencoded` whenever there is a request
// body, and every blockstor CLI write op started returning 415.
//
// Relaxed rule:
//
//   - Empty string → true. Missing Content-Type is exactly what
//     python-linstor produces before http.client's fallback fires,
//     and is the most common case for hand-rolled curl probes. The
//     body decoder is the actual JSON-validity gate; the wire-edge
//     gate is just here to refuse obviously-wrong media types.
//   - Any `application/*` media type → true. Covers the legitimate
//     shapes blockstor sees in the wild: `application/json`,
//     `application/json; charset=utf-8`, `application/x-www-form-
//     urlencoded` (python-linstor's actual wire shape), and
//     `application/vnd.foo+json` (vendor JSON profiles).
//   - Anything else (text/*, image/*, multipart/*, video/*, audio/*,
//     and other major types) → false. These are clearly not
//     structured-data JSON bodies, so we refuse at the wire edge
//     instead of forcing the body decoder to surface a confusing
//     error.
//   - A Content-Type header that won't parse → false. A malformed
//     header is a programming bug at the caller, not "implicit
//     JSON".
func isAcceptableBodyContentType(ct string) bool {
	if ct == "" {
		return true
	}

	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}

	return strings.HasPrefix(strings.ToLower(mediaType), "application/")
}

// writeUnsupportedMediaTypeEnvelope emits the LINSTOR-shaped 415 reply
// for the Bug 147 Content-Type gate. ret_code carries MASK_ERROR so
// python-linstor classifies the entry as ERROR rather than crashing
// on a plain-text body.
func writeUnsupportedMediaTypeEnvelope(w http.ResponseWriter, r *http.Request, got string) {
	cause := "this endpoint accepts application/json bodies only"
	if got != "" {
		cause += "; received Content-Type: " + got
	} else {
		cause += "; no Content-Type header was sent"
	}

	envelope := []apiv1.APICallRc{{
		RetCode: apiv1.APICallRcMaskError,
		Message: "unsupported media type",
		Cause:   cause,
		Correc: "retry with `Content-Type: application/json`" +
			" (charset parameter optional) — see " + r.Method + " " + r.URL.Path,
	}}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnsupportedMediaType)
	_ = json.NewEncoder(w).Encode(envelope)
}

// decodeJSON is the typed-envelope JSON decode helper used by every
// `r.Body`-consuming REST handler in this package. It enforces the
// LINSTOR `[]ApiCallRc` envelope invariant on every failure mode and
// gates inbound bodies against unknown fields (Bug 161).
//
// Wire contract (Bug 158 — typed envelope invariant):
//
//   - empty body            → 400 + envelope ("request body is empty")
//   - oversized body        → 413 + envelope (Bug 146 short-circuit;
//     `*http.MaxBytesError` routed through the
//     cap-byte-count message handler)
//   - malformed JSON        → 400 + envelope ("request body is not
//     valid JSON"); the std-lib decoder's
//     "invalid character '\x1f' …" /
//     "literal null" strings DO NOT reach the
//     wire — they leak the JSON internals.
//   - wrong JSON shape      → 400 + envelope citing the offending
//     wire-side field name (NOT the Go-side
//     type name; `v1.Node` is an internal
//     API identifier).
//   - unknown field         → 400 + envelope citing the offending
//     wire-side field name (Bug 161 —
//     DisallowUnknownFields).
//   - any other decode err  → 400 + envelope with a scrubbed message
//     (etcd / k8s impl-detail strings are
//     stripped before they hit the wire).
//
// On success returns true; on failure writes the envelope and returns
// false — the caller must `return` immediately.
//
// Operators see an `ERROR:` line with a stable, actionable message;
// python-linstor's `[]ApiCallRc` decoder no longer crashes on the
// plain-text leak. The Go-side type name is never on the wire.
func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	dec := json.NewDecoder(r.Body)
	// Bug 161: refuse unknown top-level (and recursively, nested)
	// fields so a stray `{"props":…}` at the wrong nesting level
	// doesn't sail past the Bug 117/118 SP-existence gate. Without
	// this gate a buggy client puts `props` at the top level instead
	// of inside `resource`, the SP gate never fires (wrong field
	// carries the pool reference) and the Resource lands in Unknown.
	dec.DisallowUnknownFields()

	err := dec.Decode(target)
	if err != nil {
		writeDecodeError(w, err)

		return false
	}

	// Bug 203: refuse residual bytes after the primary JSON value.
	// The std-lib decoder is happy once it has parsed one complete
	// JSON value; `{"valid":"json"}garbage` returns nil here and the
	// handler then runs with the partially-decoded value. dec.More()
	// reports whether the buffered Reader still has non-whitespace
	// bytes — if it does, treat the body as malformed and surface the
	// same Bug 158 envelope shape every other decode-failure mode
	// emits ("trailing JSON data after the request body"). A
	// streaming endpoint that legitimately wants multiple JSON values
	// on one connection would NOT route through decodeJSON in the
	// first place; every caller of this helper expects exactly one
	// top-level value.
	if dec.More() {
		writeDecodeError(w, errTrailingJSONData)

		return false
	}

	return true
}

// errTrailingJSONData signals the Bug 203 decode-failure mode (residual
// bytes after the primary JSON value). Module-scoped sentinel so
// `writeDecodeError` can errors.Is against it without parsing message
// strings; pre-Bug-203 there was no value to thread through here at all.
var errTrailingJSONData = errors.New("trailing JSON data after the request body")

// writeDecodeError is the handler-side bridge for the Bug 146 body
// limit AND the Bug 158 typed-envelope invariant. Handlers that
// decode a JSON body via decodeJSON route through here on failure;
// the routing is:
//
//   - `*http.MaxBytesError`     → 413 + envelope ("request body too
//     large"); the cap byte-count is
//     surfaced so operators know the
//     contract.
//   - `io.EOF` (empty body)     → 400 + envelope ("request body is
//     empty"); pre-fix the wire surfaced
//     the literal string `EOF`.
//   - `*json.SyntaxError`       → 400 + envelope ("request body is
//     not valid JSON"); the std-lib's
//     internal "invalid character …" /
//     "literal null" / "\x1f" strings
//     DO NOT reach the wire.
//   - `*json.UnmarshalTypeError` → 400 + envelope citing the wire-side
//     field name only — the Go-side type
//     (`v1.Node`, `apiv1.Resource`) is
//     scrubbed.
//   - unknown field (Bug 161)   → 400 + envelope citing the field
//     name; the helper detects the
//     std-lib's `json: unknown field
//     "<name>"` shape.
//   - any other decode err      → 400 + envelope with a scrubbed
//     message (etcd / k8s impl-detail
//     strings are stripped before they
//     hit the wire).
func writeDecodeError(w http.ResponseWriter, err error) {
	// Bug 168: mint the decode-failure metric BEFORE the per-shape
	// branches so a regression in the message-formatting code (a
	// reordering, an early return) can't lose the observation.
	observeDecodeFailure(err)

	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeError(w, http.StatusRequestEntityTooLarge,
			"request body too large (limit "+formatBytes(maxErr.Limit)+")")

		return
	}

	if writeDecodeBodyShapeError(w, err) {
		return
	}

	// Unknown field (Bug 161). DisallowUnknownFields emits a plain
	// `*errors.errorString` (not a sentinel value), so we match on the
	// std-lib's stable prefix `json: unknown field "<name>"`. The
	// field name is operator-actionable — it tells the caller exactly
	// which key to remove (or move to the right nesting level).
	if name, ok := unknownFieldName(err); ok {
		writeError(w, http.StatusBadRequest,
			`unknown field "`+name+`" in request body: this endpoint does not `+
				`accept that key; check the LINSTOR REST API documentation`)

		return
	}

	// Wrong JSON shape. UnmarshalTypeError carries the Go-side type
	// name (e.g. `v1.Node`) — that's an internal identifier; the wire
	// must surface only the JSON field name (which the operator
	// controls) plus a generic "wrong type" cue. Top-level decodes
	// have Field == ""; in that case fall back to the generic message.
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		msg := "wrong JSON shape: request body has the wrong type for this endpoint"
		if typeErr.Field != "" {
			msg = `wrong JSON shape: field "` + typeErr.Field + `" has the wrong type`
		}

		writeError(w, http.StatusBadRequest, msg)

		return
	}

	// Malformed JSON — bad bytes, gzip body, unterminated string, etc.
	// SyntaxError messages name the offending byte ("invalid character
	// '\x1f' …"), which leaks the JSON internals AND, in the gzip
	// case, the magic-byte fingerprint of the wrong content encoding.
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		writeError(w, http.StatusBadRequest,
			"request body is not valid JSON: send `application/json` "+
				"content; verify the body parses with `jq .`")

		return
	}

	writeError(w, http.StatusBadRequest, scrubImplDetails(err.Error()))
}

// writeDecodeBodyShapeError handles the body-shape-level decode
// failure modes that don't depend on JSON-internal types: empty body
// (io.EOF), truncated body (io.ErrUnexpectedEOF), and trailing data
// after the primary value (Bug 203 sentinel). Pulled out of
// writeDecodeError so the parent function stays under the linter's
// funlen budget while still emitting per-shape operator-facing cues.
// Returns true when the branch fired (caller must stop); false
// otherwise (caller continues to the JSON-typed branches).
func writeDecodeBodyShapeError(w http.ResponseWriter, err error) bool {
	// Empty body. The std-lib decoder returns io.EOF when called on a
	// zero-byte stream; the message is literally "EOF" — operators
	// see a wire reply of `[{"message":"EOF"}]`, python-linstor's
	// CLI surfaces "ERROR: EOF" with no hint that the body was empty.
	if errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest,
			"request body is empty: send the JSON payload this endpoint expects")

		return true
	}

	// Truncated body — the decoder consumed something but ran out of
	// bytes mid-structure. Same operator-facing shape as empty body.
	if errors.Is(err, io.ErrUnexpectedEOF) {
		writeError(w, http.StatusBadRequest,
			"request body is truncated: re-send the complete JSON payload")

		return true
	}

	// Bug 203: trailing bytes after the primary JSON value. The
	// std-lib decoder is happy after one complete value, so
	// `{"valid":"json"}garbage` decoded cleanly and the handler ran
	// with the partial value. `decodeJSON` now checks `dec.More()`
	// after the primary Decode and routes that case through this
	// branch with the operator-actionable cue "trailing JSON data".
	if errors.Is(err, errTrailingJSONData) {
		writeError(w, http.StatusBadRequest,
			"trailing JSON data after the request body: send exactly one "+
				"top-level JSON value per request")

		return true
	}

	return false
}

// unknownFieldName extracts the offending key from Go's
// DisallowUnknownFields error. The std-lib emits a plain
// `*errors.errorString` with the message `json: unknown field "<name>"`
// — there's no typed sentinel to errors.As against, so we parse the
// message. Returns (name, true) on a match; (empty, false) otherwise.
//
// Stable since Go 1.10 (DisallowUnknownFields' introduction); the
// upstream error is constructed by `encoding/json/decode.go`'s
// `Decoder.Decode` via fmt.Errorf with this exact format string.
func unknownFieldName(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	const prefix = `json: unknown field "`

	msg := err.Error()
	if !strings.HasPrefix(msg, prefix) {
		return "", false
	}

	name, _, ok := strings.Cut(msg[len(prefix):], `"`)
	if !ok {
		return "", false
	}

	return name, true
}

// scrubImplDetails strips backend-implementation identifiers from a
// decode/error message before it goes on the wire. Bug 146 sibling:
// even when the body is in-cap, a malformed JSON payload or a
// store-layer error can carry the etcd/k8s identity through to the
// REST envelope. The python CLI doesn't care about the strings, but
// operators do — surfacing "etcdserver:" tells anyone running curl
// against the apiserver exactly which persistence backend is in
// play, which is a small but real abstraction leak.
//
// We replace the offending substrings with a stable, opaque token
// rather than dropping them — keeps the message length sensible and
// the error class identifiable for log greps without leaking the
// backend.
func scrubImplDetails(msg string) string {
	const (
		opaque       = "<backend>"
		etcdServer   = "etcdserver"
		etcdShort    = "etcd"
		apimachinery = "apimachinery"
		k8sPrefix    = "k8s.io"
	)

	replacements := []string{
		etcdServer + ":", opaque + ":",
		etcdServer, opaque,
		etcdShort, opaque,
		k8sPrefix + "/" + apimachinery, opaque,
		apimachinery, opaque,
		k8sPrefix, opaque,
	}

	out := strings.NewReplacer(replacements...).Replace(msg)

	// Bug 162: apimachinery's status messages embed the
	// GroupResource — e.g. "controllerconfigs.blockstor.io" or
	// "controllerconfigs.blockstor.io.blockstor.io" (resource name
	// suffixed with group, then printed as <resource>.<group>). The
	// CRD plural names ARE the persistence backend's identity from
	// the operator's perspective — the wire surface speaks LINSTOR,
	// not K8s. Strip them with a regex that matches the
	// `<word>.blockstor.io[.suffix...]` shape.
	return blockstorGroupRefRE.ReplaceAllString(out, opaque)
}

// blockstorGroupRefRE matches an apimachinery-style group/resource
// reference rooted at our project's API group. Two shapes occur in
// the wild:
//
//	controllerconfigs.blockstor.io
//	controllerconfigs.blockstor.io.blockstor.io
//
// The first is the GroupResource.String() output; the second appears
// when the CRD's plural itself carries the group suffix
// (controllerconfigs.blockstor.io is the literal resource name) and
// apimachinery re-appends the group. We greedily consume any number
// of trailing ".blockstor.io" segments so both shapes collapse to a
// single <backend> token.
var blockstorGroupRefRE = regexp.MustCompile(`[a-zA-Z0-9-]+\.blockstor\.io(\.blockstor\.io)*`)

// formatBytes renders n as a short human-readable size string for
// operator-facing error messages. Stays in MiB/KiB granularity so
// tests can match exact byte values when the cap is well-known.
func formatBytes(n int64) string {
	const (
		kib = int64(1024)
		mib = kib * kib
	)

	switch {
	case n >= mib && n%mib == 0:
		return formatInt(n/mib) + " MiB"
	case n >= kib && n%kib == 0:
		return formatInt(n/kib) + " KiB"
	default:
		return formatInt(n) + " bytes"
	}
}

// formatInt renders a non-negative int64 in base 10. Stays in the
// std lib without importing strconv at the top — keeps the diff
// scoped to wire-edge concerns.
func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}

	var (
		buf [20]byte
		i   = len(buf)
	)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}
