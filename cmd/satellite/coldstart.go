// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
)

// startColdStartLiveness answers the kubelet's liveness probe on addr until
// the returned stop is called, which the satellite does just before the
// controller-runtime manager takes the address over.
//
// The cold start waits for the API server before the manager exists, and on
// a node whose service network is still coming up that wait outlasts the
// probe's budget: with nothing listening the kubelet restarts the container,
// and each restart backs off for longer, so the node rejoins later than if
// the satellite had simply waited. The process is alive and making progress
// all that time, which is what liveness asks. /readyz is not served, so the
// pod stays unready until the manager's own probe says otherwise.
func startColdStartLiveness(logger *slog.Logger, addr string) func() {
	if addr == "" || addr == "0" {
		return func() {}
	}

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		logger.Warn("cold-start: cannot answer the liveness probe while starting", "addr", addr, "err", err.Error())

		return func() {}
	}

	stop, _ := serveColdStartLiveness(logger, lis)

	return stop
}

// serveColdStartLiveness serves /healthz on lis. stop returns once the
// listener is closed, so the address is free for the manager.
func serveColdStartLiveness(logger *slog.Logger, lis net.Listener) (func(), net.Addr) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	const headerTimeout = 5 * time.Second

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: headerTimeout}
	done := make(chan struct{})

	go func() {
		defer close(done)

		err := srv.Serve(lis)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("cold-start liveness server", "err", err.Error())
		}
	}()

	stop := func() {
		const shutdownTimeout = 5 * time.Second

		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		err := srv.Shutdown(ctx)
		if err != nil {
			_ = srv.Close()
		}

		<-done
	}

	return stop, lis.Addr()
}

// coldStartCleanup takes down blockstor's orphaned kernel resources and the
// .res files nothing in the kernel uses any more. owned is nil when it is
// not known which resources are blockstor's; the kernel is then left alone.
//
// Both steps work from one probe of which kernel resources are healthy. A
// probe that did not finish says nothing about them, and reading it as "none
// is healthy" would delete the .res of every live resource, which a DRBD
// handler such as before-resync-target needs. So when the probe fails, or a
// shutdown arrives before the cleanup starts, nothing is cleaned.
func coldStartCleanup(
	ctx context.Context, logger *slog.Logger, stateDir string, owned map[string]struct{},
	probe func(context.Context, *slog.Logger) (map[string]struct{}, error),
) {
	healthy, err := probe(ctx, logger)
	if err != nil {
		logger.Warn("cold-start: cannot tell which DRBD resources are healthy; leaving the kernel and the .res files as they are",
			"err", err.Error())

		return
	}

	if ctx.Err() != nil {
		return
	}

	// Tear down only the kernel resources that are NOT in the healthy
	// set. Without this, a reconcile that re-allocates a node-id
	// (e.g. the dispatcher picked a different id after a peer joined /
	// left) would hit "peer node id cannot be my own node id" on a
	// genuinely orphaned slot — but a live replica's node-id is
	// reconciled by `drbdadm adjust`, not by a destructive down.
	cleanKernelState(ctx, logger, healthy, owned)

	// Wipe stale .res files, but PRESERVE the ones backing a healthy
	// kernel resource. The c-r reconciler re-renders every Resource
	// CRD on this node shortly after startup, but a surviving kernel
	// resource needs its .res in place until that first render so an
	// interim `drbdadm adjust` has something to read. Orphan .res
	// files (no matching live kernel resource) are still wiped.
	cleanStateDir(stateDir, healthy, logger)
}
