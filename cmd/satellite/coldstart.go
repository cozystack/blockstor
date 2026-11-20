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
