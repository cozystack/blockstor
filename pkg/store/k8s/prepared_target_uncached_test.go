// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"context"
	"testing"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// cacheBehindTheTarget is a client whose cache has seen neither the target's
// volumes nor its replicas: the definition reads back without its inline
// volume definitions, and no replica is listed.
type cacheBehindTheTarget struct{ ctrlclient.Client }

func (c cacheBehindTheTarget) Get(
	ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption,
) error {
	err := c.Client.Get(ctx, key, obj, opts...)
	if rd, ok := obj.(*crdv1alpha1.ResourceDefinition); ok && err == nil {
		rd.Spec.VolumeDefinitions = nil
	}

	return err //nolint:wrapcheck // pass-through
}

func (c cacheBehindTheTarget) List(ctx context.Context, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
	if _, ok := list.(*crdv1alpha1.ResourceList); ok {
		return nil
	}

	return c.Client.List(ctx, list, opts...) //nolint:wrapcheck // pass-through
}

// The prepared-target decision reads a definition's volumes and replicas past
// the cache: the volume restore and the resource restore after it can be
// served by different replicas of the server.
func TestVolumeAndReplicaListingsReadPastTheCache(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	ctx := t.Context()
	seed := k8s.New(fixture.client)

	if err := seed.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-prep-uncached"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	t.Cleanup(func() { _ = seed.ResourceDefinitions().Delete(context.Background(), "pvc-prep-uncached") })

	if err := seed.VolumeDefinitions().Create(ctx, "pvc-prep-uncached",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20}); err != nil {
		t.Fatalf("seed volume: %v", err)
	}

	if err := seed.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-prep-uncached", NodeName: "node-p"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	t.Cleanup(func() { _ = seed.Resources().Delete(context.Background(), "pvc-prep-uncached", "node-p") })

	st := k8s.NewWithAPIReader(cacheBehindTheTarget{fixture.client}, fixture.client)

	if vds, _ := st.VolumeDefinitions().List(ctx, "pvc-prep-uncached"); len(vds) != 0 {
		t.Fatalf("fixture: the cached volume listing was supposed to be empty, got %d", len(vds))
	}

	vols, ok := st.VolumeDefinitions().(interface {
		ListUncached(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error)
	})
	if !ok {
		t.Fatal("the Kubernetes volume-definition store has no ListUncached")
	}

	if vds, err := vols.ListUncached(ctx, "pvc-prep-uncached"); err != nil || len(vds) != 1 {
		t.Errorf("ListUncached = %d volume(s) (%v), want the one the API server holds", len(vds), err)
	}

	if replicas, _ := st.Resources().ListByDefinition(ctx, "pvc-prep-uncached"); len(replicas) != 0 {
		t.Fatalf("fixture: the cached replica listing was supposed to be empty, got %d", len(replicas))
	}

	reps, ok := st.Resources().(interface {
		ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error)
	})
	if !ok {
		t.Fatal("the Kubernetes resource store has no ListByDefinitionUncached")
	}

	if replicas, err := reps.ListByDefinitionUncached(ctx, "pvc-prep-uncached"); err != nil || len(replicas) != 1 {
		t.Errorf("ListByDefinitionUncached = %d replica(s) (%v), want the one the API server holds", len(replicas), err)
	}
}
