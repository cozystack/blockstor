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

package stream

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// readHeaderTimeout caps the slowloris window — clients have this
// long to push headers before we drop the connection. The body
// itself (the snapshot stream) is unbounded and can take minutes.
const readHeaderTimeout = 10 * time.Second

// PoolResolver returns the storage.Provider for a (rd, vol) pair on
// this satellite. The satellite's Reconciler keeps a resource→pool
// map populated by Apply; this is the gate that lets the stream
// server reach into it without taking a direct dependency on the
// Reconciler type.
type PoolResolver interface {
	// ProviderForResource resolves the provider backing the given
	// resource definition. Returns nil + an error when the resource
	// is unknown to this satellite (e.g. wrong-node lookup).
	ProviderForResource(rdName string) (storage.Provider, error)
}

// Server is the HTTP handler for satellite-to-satellite snapshot
// streaming. Construct via NewServer and mount via Register on the
// satellite agent's mux. The server holds no goroutines — each
// request runs in the caller's goroutine.
type Server struct {
	pools PoolResolver
}

// NewServer wires a Server against the satellite's pool resolver.
func NewServer(pools PoolResolver) *Server {
	return &Server{pools: pools}
}

// Register mounts the snapshot-stream handler on mux. Idempotent
// across calls only when mux is fresh — http.ServeMux panics on a
// duplicate pattern, so callers should not Register twice.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET "+PathPrefix+"/{rd}/{snap}/{vol}", s.handleSnapshot)
}

// handleSnapshot streams the named (rd, snap, vol) snapshot to the
// peer. Returns 404 when the satellite doesn't host the snapshot,
// 501 when the backing provider can't ship (legacy FILE driver
// pre-Phase-11 etc.), 500 on any other failure.
func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	snapName := r.PathValue("snap")

	volNum, err := strconv.Atoi(r.PathValue("vol"))
	if err != nil {
		http.Error(w, fmt.Sprintf("vol must be int: %v", err), http.StatusBadRequest)

		return
	}

	provider, err := s.pools.ProviderForResource(rd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	shipper, ok := provider.(storage.SnapshotShipper)
	if !ok {
		http.Error(w, fmt.Sprintf("provider %q has no SnapshotShipper capability", provider.Kind()),
			http.StatusNotImplemented)

		return
	}

	body, err := shipper.SendSnapshot(r.Context(), storage.Snapshot{
		ResourceName: rd,
		SnapshotName: snapName,
		// VolumeNumber is encoded in the URL but Snapshot.VolumeNumber
		// doesn't exist on the type — provider implementations key
		// off ResourceName + SnapshotName + (volume from layout
		// elsewhere). Passing volNum here is reserved for future
		// per-volume backends; the parsed value is asserted to be a
		// valid int above.
	})
	if err != nil {
		if stderrors.Is(err, storage.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)

			return
		}

		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	defer closeQuietly(body)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	// Stream straight from the provider into the response. The
	// receiver consumes via io.Copy on the response body — context
	// cancellation on either side propagates through the underlying
	// process (RealExec passes its context to exec.CommandContext).
	// A late io.Copy error after headers are already on the wire
	// can't be reported; the receiver detects a short read.
	_, _ = io.Copy(w, body)

	// volNum is reserved for per-volume backends; today's providers
	// key snapshots off (rd, snap) alone.
	_ = volNum
}

// closeQuietly closes c, swallowing the error. Used after a copy
// loop — a late close error doesn't help the receiver and would
// just spam logs.
func closeQuietly(c io.Closer) {
	_ = c.Close()
}

// ListenAndServe binds an HTTP server on addr serving the snapshot
// stream endpoint. Caller is responsible for cancelling ctx to stop
// the server. The Server type doesn't manage its own goroutine —
// this helper is the canonical wiring for cmd/satellite.
//
// Returns http.ErrServerClosed on a clean ctx-cancel shutdown so
// callers can distinguish that from a real bind error.
func ListenAndServe(ctx context.Context, addr string, s *Server) error {
	mux := http.NewServeMux()
	s.Register(mux)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout, // CWE-400: bound the slowloris window
	}

	var (
		serveErr error
		wg       sync.WaitGroup
	)

	wg.Go(func() {
		serveErr = srv.ListenAndServe()
	})

	<-ctx.Done()

	// Shutdown blocks until in-flight requests finish or the
	// shutdown context expires; the per-request handlers already
	// observe r.Context() so an in-flight zfs-send pipeline gets
	// torn down too.
	_ = srv.Close()

	wg.Wait()

	return errors.Wrap(serveErr, "stream server")
}
