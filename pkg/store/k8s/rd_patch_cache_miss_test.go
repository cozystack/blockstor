// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// definitionCacheTrails is a client whose cache has not seen any definition
// yet: every Get of one answers NotFound, the way an informer does right after
// another client created it. Writes go through.
type definitionCacheTrails struct{ ctrlclient.Client }

func (d definitionCacheTrails) Get(
	ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption,
) error {
	if _, ok := obj.(*crdv1alpha1.ResourceDefinition); ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "resourcedefinitions"}, key.Name)
	}

	return d.Client.Get(ctx, key, obj, opts...) //nolint:wrapcheck // pass-through test double
}

// A patch written moments after the definition was created reads it at the
// peak of the informer's lag. Taking the cache's NotFound there dropped the
// write as if the definition were gone: the abandoned-rollback mark, written
// first thing in a compensation, did not land, and a rollback that then died
// left a partial clone the replay answered 201 over.
func TestResourceDefinitionPatchLandsOnADefinitionTheCacheHasNotSeen(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	ctx := t.Context()
	seed := k8s.New(fixture.client)

	if err := seed.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-patch-lag"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	t.Cleanup(func() { _ = seed.ResourceDefinitions().Delete(context.Background(), "pvc-patch-lag") })

	st := k8s.NewWithAPIReader(definitionCacheTrails{fixture.client}, fixture.client)

	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "pvc-patch-lag",
		func(rd *apiv1.ResourceDefinition) error {
			if rd.Props == nil {
				rd.Props = map[string]string{}
			}

			rd.Props["Aux/patched"] = "yes"

			return nil
		})
	if err != nil {
		t.Fatalf("patch of a definition the cache has not seen: %v", err)
	}

	got, err := seed.ResourceDefinitions().Get(ctx, "pvc-patch-lag")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if got.Props["Aux/patched"] != "yes" {
		t.Errorf("the patch did not land: props %v", got.Props)
	}
}

// definitionCacheIsStale holds every definition as it was before its props
// were written.
type definitionCacheIsStale struct{ ctrlclient.Client }

func (d definitionCacheIsStale) Get(
	ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption,
) error {
	err := d.Client.Get(ctx, key, obj, opts...)
	if rd, ok := obj.(*crdv1alpha1.ResourceDefinition); ok && err == nil {
		rd.Spec = crdv1alpha1.ResourceDefinitionSpec{}
		rd.Annotations = nil
	}

	return err //nolint:wrapcheck // pass-through test double
}

// GetUncached exists for the decision a cache that holds the object stale gets
// wrong: Get falls back to the API server only when the cache has nothing.
func TestResourceDefinitionGetUncachedReadsPastAStaleCache(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	ctx := t.Context()
	seed := k8s.New(fixture.client)

	if err := seed.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-stale-cache", Props: map[string]string{"Aux/fresh": "yes"},
	}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	t.Cleanup(func() { _ = seed.ResourceDefinitions().Delete(context.Background(), "pvc-stale-cache") })

	st := k8s.NewWithAPIReader(definitionCacheIsStale{fixture.client}, fixture.client)

	if cached, err := st.ResourceDefinitions().Get(ctx, "pvc-stale-cache"); err != nil || cached.Props["Aux/fresh"] != "" {
		t.Fatalf("fixture: the cached read was supposed to be stale: %v %v", cached.Props, err)
	}

	live, err := st.ResourceDefinitions().GetUncached(ctx, "pvc-stale-cache")
	if err != nil {
		t.Fatalf("GetUncached: %v", err)
	}

	if live.Props["Aux/fresh"] != "yes" {
		t.Errorf("GetUncached answered from the stale cache: props %v", live.Props)
	}
}
