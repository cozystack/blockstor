// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// apiServerAfter returns an address that refuses connections until delay has
// passed and then forwards every connection to target, the way an API server
// restarting while the pod starts looks from the pod.
func apiServerAfter(t *testing.T, target string, delay time.Duration) string {
	t.Helper()

	reserve, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}

	addr := reserve.Addr().String()
	_ = reserve.Close()

	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })

	go func() {
		select {
		case <-time.After(delay):
		case <-stop:
			return
		}

		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
		if err != nil {
			t.Errorf("listen on %s after the delay: %v", addr, err)

			return
		}

		go func() {
			<-stop

			_ = listener.Close()
		}()

		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go forward(conn, target)
		}
	}()

	return addr
}

func forward(conn net.Conn, target string) {
	defer func() { _ = conn.Close() }()

	upstream, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", target)
	if err != nil {
		return
	}

	defer func() { _ = upstream.Close() }()

	go func() { _, _ = io.Copy(upstream, conn) }()

	_, _ = io.Copy(conn, upstream)
}

func managerOptions() ctrl.Options {
	return ctrl.Options{
		Scheme:                 fixture.client.Scheme(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	}
}

// Both server binaries exit when NewManager fails, and registering the field
// indexes asks the API server for each indexed kind's REST mapping before the
// manager starts. An API server that is unreachable for a moment while the pod
// starts has to be waited for, not turned into a crash loop.
func TestNewManagerWaitsForAnAPIServerThatComesUp(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest not available")
	}

	t.Parallel()

	served, err := url.Parse(fixture.env.Config.Host)
	if err != nil || served.Host == "" {
		t.Fatalf("parse the envtest API server address %q: %v", fixture.env.Config.Host, err)
	}

	late := rest.CopyConfig(fixture.env.Config)
	late.Host = "https://" + apiServerAfter(t, served.Host, 2*time.Second)

	mgr, err := k8s.NewIndexedManagerWithin(late, managerOptions(), 30*time.Second)
	if err != nil {
		t.Fatalf("NewManager against an API server that came up 2s later: %v", err)
	}

	if mgr == nil {
		t.Fatal("NewManager returned no error and no manager")
	}
}

// The wait is bounded: an API server that never comes back is reported, so the
// pod is restarted by its own exit rather than by a liveness probe it cannot
// answer while it is still constructing.
func TestNewManagerGivesUpWhenTheBudgetIsSpent(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest not available")
	}

	t.Parallel()

	gone := rest.CopyConfig(fixture.env.Config)
	gone.Host = "https://127.0.0.1:1"

	started := time.Now()

	_, err := k8s.NewIndexedManagerWithin(gone, managerOptions(), time.Second)
	if err == nil {
		t.Fatal("NewManager against an API server that never answers returned no error")
	}

	if elapsed := time.Since(started); elapsed > 15*time.Second {
		t.Errorf("NewManager took %s to give up on a 1s budget", elapsed)
	}

	var netErr net.Error
	if !errors.As(err, &netErr) && !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("the error does not say the API server was unreachable: %v", err)
	}
}
