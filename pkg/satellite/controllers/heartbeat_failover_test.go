// SPDX-License-Identifier: Apache-2.0

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

package controllers_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
)

// silentProxy forwards TCP to an API server until kill, after which
// every connection it carried goes silent: bytes are swallowed both
// ways and nothing is closed, which is what a client sees of a node
// that was hard reset. Connections made after kill are forwarded, as
// the Service then sends them to an API server that is still up.
type silentProxy struct {
	ln      net.Listener
	backend string

	mu    sync.Mutex
	alive []*atomic.Bool
	conns []net.Conn
}

func newSilentProxy(t *testing.T, backend string) *silentProxy {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	p := &silentProxy{ln: ln, backend: backend}

	go p.serve(t.Context())

	t.Cleanup(p.close)

	return p
}

func (p *silentProxy) serve(ctx context.Context) {
	for {
		down, err := p.ln.Accept()
		if err != nil {
			return
		}

		up, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.backend)
		if err != nil {
			_ = down.Close()

			continue
		}

		alive := &atomic.Bool{}
		alive.Store(true)

		p.mu.Lock()
		p.alive = append(p.alive, alive)
		p.conns = append(p.conns, down, up)
		p.mu.Unlock()

		go pipe(up, down, alive)
		go pipe(down, up, alive)
	}
}

func pipe(dst io.Writer, src io.Reader, alive *atomic.Bool) {
	buf := make([]byte, 32*1024)

	for {
		n, err := src.Read(buf)
		if n > 0 && alive.Load() {
			_, werr := dst.Write(buf[:n])
			if werr != nil {
				return
			}
		}

		if err != nil {
			return
		}
	}
}

func (p *silentProxy) kill() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, a := range p.alive {
		a.Store(false)
	}
}

func (p *silentProxy) close() {
	_ = p.ln.Close()

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.conns {
		_ = c.Close()
	}
}

// TestHeartbeatSurvivesAPIServerDyingSilently: the API server the
// heartbeat's connection sits on goes silent. The next stamps must
// reach an API server that is up within seconds, not after the
// minute TCP or the HTTP/2 health check take to drop the connection,
// or the controller marks a node that is running fine OFFLINE.
func TestHeartbeatSurvivesAPIServerDyingSilently(t *testing.T) {
	t.Parallel()

	var stamps atomic.Int64

	api := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/apis/blockstor.org/v1alpha1/nodes/worker-1/status" {
			http.NotFound(w, r)

			return
		}

		stamps.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"blockstor.org/v1alpha1","kind":"Node","metadata":{"name":"worker-1"}}`))
	}))
	api.EnableHTTP2 = true
	api.StartTLS()
	t.Cleanup(api.Close)

	proxy := newSilentProxy(t, api.Listener.Addr().String())

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(blockstoriov1alpha1.GroupVersion.WithKind("Node"), meta.RESTScopeRoot)

	cli, reset, err := controllers.NewHeartbeatClient(&rest.Config{
		Host:            "https://" + proxy.ln.Addr().String(),
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
	}, scheme, mapper)
	if err != nil {
		t.Fatalf("NewHeartbeatClient: %v", err)
	}

	hb := &controllers.HeartbeatRunnable{
		Client:           cli,
		NodeName:         "worker-1",
		Period:           100 * time.Millisecond,
		Timeout:          300 * time.Millisecond,
		RetryPeriod:      50 * time.Millisecond,
		ResetConnections: reset,
	}

	done := make(chan error, 1)

	go func() { done <- hb.Start(t.Context()) }()

	waitForStamps(t, &stamps, 1, 3*time.Second, "first stamp")

	// A stamp already on its way when the API server goes silent can
	// still land and be counted; only the second one after the count
	// is certain to have been sent after the kill.
	before := stamps.Load()

	proxy.kill()

	waitForStamps(t, &stamps, before+2, 3*time.Second,
		"a stamp after the API server went silent")

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, t.Context().Err()) {
			t.Fatalf("Start: %v", err)
		}
	default:
	}
}

func waitForStamps(t *testing.T, stamps *atomic.Int64, want int64, within time.Duration, what string) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if stamps.Load() >= want {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("%s did not arrive within %s (%d stamps so far)", what, within, stamps.Load())
}
