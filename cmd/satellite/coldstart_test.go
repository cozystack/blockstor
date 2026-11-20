// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// An answer the API server will give again however long the satellite waits
// ends the wait at once: a satellite that never starts because RBAC lagged
// behind an upgrade, or the CRD is missing, is worse than one that starts
// without cleaning the kernel.
func TestColdStartDoesNotWaitOutARefusal(t *testing.T) {
	t.Parallel()

	resources := schema.GroupResource{Group: blockstoriov1alpha1.GroupVersion.Group, Resource: "resources"}

	for name, refusal := range map[string]error{
		"forbidden": apierrors.NewForbidden(resources, "", errors.New("RBAC: access denied")),
		"not found": apierrors.NewNotFound(resources, ""),
		"no kind":   &meta.NoKindMatchError{GroupKind: resources.WithVersion("").GroupVersion().WithKind("Resource").GroupKind()},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("scheme: %v", err)
			}

			var reads int

			c := fake.NewClientBuilder().WithScheme(scheme).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
						reads++

						return refusal
					},
				}).
				Build()

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()

			retry := ownedRetry{attempt: time.Second, first: time.Millisecond, longest: time.Millisecond}

			owned, err := waitOwnedOnNode(ctx, slog.New(slog.DiscardHandler), c, "srv1", retry)
			if err == nil || owned != nil {
				t.Fatalf("waitOwnedOnNode = %v, %v; want no list and an error", owned, err)
			}

			if ctx.Err() != nil || reads != 1 {
				t.Errorf("read %d times before giving up (deadline hit: %v), want once", reads, ctx.Err() != nil)
			}
		})
	}
}

// While the satellite waits for the API server the kubelet keeps probing
// liveness, and with nothing listening it restarts the container into a
// growing back-off. The cold start answers the probe until the manager takes
// the address over, and then frees it.
func TestColdStartAnswersLivenessUntilTheManagerStarts(t *testing.T) {
	t.Parallel()

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	stop, addr := serveColdStartLiveness(slog.New(slog.DiscardHandler), lis)
	url := "http://" + addr.String()

	if code := probe(t, url+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz during the cold start = %d, want 200", code)
	}

	if code := probe(t, url+"/readyz"); code == http.StatusOK {
		t.Error("/readyz answered 200 before the manager synced its cache")
	}

	stop()

	again, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", addr.String())
	if err != nil {
		t.Fatalf("the probe address is still taken after stop: %v", err)
	}

	_ = again.Close()
}

// The address the satellite is configured with is the one served.
func TestStartColdStartLivenessServesTheProbeAddress(t *testing.T) {
	t.Parallel()

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := lis.Addr().String()
	_ = lis.Close()

	stop := startColdStartLiveness(slog.New(slog.DiscardHandler), addr)
	defer stop()

	if code := probe(t, "http://"+addr+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz on the probe address = %d, want 200", code)
	}
}

func probe(t *testing.T, url string) int {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	return resp.StatusCode
}

var errStatusFailed = errors.New("drbdsetup status: signal: killed")

// writeRes puts a .res for each name into a fresh state dir.
func writeRes(t *testing.T, names ...string) string {
	t.Helper()

	dir := t.TempDir()
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name+".res"), []byte("resource "+name+" {}\n"), 0o600); err != nil {
			t.Fatalf("write %s.res: %v", name, err)
		}
	}

	return dir
}

func resLeft(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name+".res"))

	return err == nil
}

// A health probe that did not finish says nothing about the kernel. Read as
// "nothing is healthy" it deleted the .res of every live resource, which a
// DRBD handler needs, and a SIGTERM during the cold start ends the probe
// exactly that way.
func TestColdStartCleansNothingWhenTheHealthProbeFails(t *testing.T) {
	t.Parallel()

	dir := writeRes(t, "pvc-live")
	failed := func(context.Context, *slog.Logger) (map[string]struct{}, error) {
		return nil, errStatusFailed
	}

	coldStartCleanup(t.Context(), slog.New(slog.DiscardHandler), dir, nil, failed)

	if !resLeft(dir, "pvc-live") {
		t.Error("a failed health probe deleted the .res of a live resource")
	}
}

// A shutdown that arrives once the probe has answered stops the cleanup too.
func TestColdStartCleansNothingOnShutdown(t *testing.T) {
	t.Parallel()

	dir := writeRes(t, "pvc-live")
	ctx, cancel := context.WithCancel(t.Context())

	shutDown := func(context.Context, *slog.Logger) (map[string]struct{}, error) {
		cancel()

		return map[string]struct{}{}, nil
	}

	coldStartCleanup(ctx, slog.New(slog.DiscardHandler), dir, nil, shutDown)

	if !resLeft(dir, "pvc-live") {
		t.Error("a shutdown during the cold start deleted the .res of a live resource")
	}
}

// The positive control: an empty kernel, as after a reboot, leaves no .res
// worth keeping, and a healthy resource keeps its own.
func TestColdStartCleansStaleResFiles(t *testing.T) {
	t.Parallel()

	dir := writeRes(t, "pvc-live", "pvc-stale")
	probed := func(context.Context, *slog.Logger) (map[string]struct{}, error) {
		return map[string]struct{}{"pvc-live": {}}, nil
	}

	coldStartCleanup(t.Context(), slog.New(slog.DiscardHandler), dir, nil, probed)

	if !resLeft(dir, "pvc-live") {
		t.Error("the .res of a healthy resource was deleted")
	}

	if resLeft(dir, "pvc-stale") {
		t.Error("a stale .res was kept")
	}
}
