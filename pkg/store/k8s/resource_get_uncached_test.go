// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"context"
	"errors"
	"testing"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// replicaCacheIsBlind is a client whose cache has not seen any replica.
type replicaCacheIsBlind struct{ ctrlclient.Client }

func (b replicaCacheIsBlind) Get(
	ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption,
) error {
	if _, ok := obj.(*crdv1alpha1.Resource); ok {
		return b.Client.Get(ctx, ctrlclient.ObjectKey{Name: "no-such-" + key.Name}, obj, opts...) //nolint:wrapcheck // pass-through
	}

	return b.Client.Get(ctx, key, obj, opts...) //nolint:wrapcheck // pass-through
}

// A Create that collided with a replica decides on whether that replica is
// still there and whether it is going, so the read behind it is the API
// server's, not a cache's.
func TestResourceGetUncachedReadsPastTheCache(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	ctx := t.Context()
	seed := k8s.New(fixture.client)

	if err := seed.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-res-uncached"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	t.Cleanup(func() { _ = seed.ResourceDefinitions().Delete(context.Background(), "pvc-res-uncached") })

	if err := seed.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-res-uncached", NodeName: "node-x"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	t.Cleanup(func() { _ = seed.Resources().Delete(context.Background(), "pvc-res-uncached", "node-x") })

	st := k8s.NewWithAPIReader(replicaCacheIsBlind{fixture.client}, fixture.client)

	if _, err := st.Resources().Get(ctx, "pvc-res-uncached", "node-x"); err == nil {
		t.Fatal("fixture: the cached read was supposed to miss the replica")
	}

	direct, ok := st.Resources().(interface {
		GetUncached(ctx context.Context, rdName, node string) (apiv1.Resource, error)
	})
	if !ok {
		t.Fatal("the Kubernetes resource store has no GetUncached")
	}

	if _, err := direct.GetUncached(ctx, "pvc-res-uncached", "node-x"); err != nil {
		t.Errorf("GetUncached missed a replica the API server holds: %v", err)
	}
}

var errLiveReadRefused = errors.New("apiserver refused the live read")

// liveReadRefused is an API reader that cannot be reached.
type liveReadRefused struct{ ctrlclient.Reader }

func (liveReadRefused) Get(context.Context, ctrlclient.ObjectKey, ctrlclient.Object, ...ctrlclient.GetOption) error {
	return errLiveReadRefused
}

// The live read is what a collision decides on, so a failed one is an error,
// never "not found" and never the cache's answer in its place.
func TestResourceGetUncachedReportsAFailedLiveRead(t *testing.T) {
	t.Parallel()

	st := k8s.NewWithAPIReader(bug201NewFakeClient(t), liveReadRefused{})

	direct, ok := st.Resources().(interface {
		GetUncached(ctx context.Context, rdName, node string) (apiv1.Resource, error)
	})
	if !ok {
		t.Fatal("the Kubernetes resource store has no GetUncached")
	}

	_, err := direct.GetUncached(t.Context(), "pvc-live-refused", "node-x")
	if !errors.Is(err, errLiveReadRefused) || errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetUncached over a failed live read = %v, want that error and not NotFound", err)
	}
}
