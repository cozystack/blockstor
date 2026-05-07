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
// controller-runtime manager. It is wired in cmd/main.go via mgr.Add so the
// HTTP server's lifecycle follows the manager's.
package rest

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
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
type Server struct {
	Addr  string // e.g. ":3370" — upstream LINSTOR plain-text REST port
	Store store.Store
}

// NeedLeaderElection reports whether the server requires leader election.
// REST is read-mostly today and safe to run on every replica; once we
// introduce write-paths that need a single leader we'll change this to true.
func (s *Server) NeedLeaderElection() bool { return false }

// Start runs the HTTP server until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("rest")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/controller/version", handleVersion)
	mux.HandleFunc("GET /v1/healthz", handleHealth)
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
