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

// blindToDefinitions is a client whose cache has seen no definition at all.
type blindToDefinitions struct{ ctrlclient.Client }

func (b blindToDefinitions) List(ctx context.Context, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
	if _, ok := list.(*crdv1alpha1.ResourceDefinitionList); ok {
		return nil
	}

	return b.Client.List(ctx, list, opts...) //nolint:wrapcheck // pass-through
}

// ListUncached exists for a decision that destroys something when the answer
// comes back short, so it has to reach past a cache that trails.
func TestResourceDefinitionListUncachedReadsPastTheCache(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	ctx := t.Context()

	if err := k8s.New(fixture.client).ResourceDefinitions().Create(ctx,
		&apiv1.ResourceDefinition{Name: "pvc-uncached-list"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	t.Cleanup(func() {
		_ = k8s.New(fixture.client).ResourceDefinitions().Delete(context.Background(), "pvc-uncached-list")
	})

	st := k8s.NewWithAPIReader(blindToDefinitions{fixture.client}, fixture.client)

	cached, err := st.ResourceDefinitions().List(ctx)
	if err != nil || len(cached) != 0 {
		t.Fatalf("fixture: the cached list saw %d definition(s) (%v), want none", len(cached), err)
	}

	live, err := st.ResourceDefinitions().ListUncached(ctx)
	if err != nil {
		t.Fatalf("ListUncached: %v", err)
	}

	found := false

	for i := range live {
		if live[i].Name == "pvc-uncached-list" {
			found = true
		}
	}

	if !found {
		t.Errorf("ListUncached missed a definition the API server holds: %d returned", len(live))
	}
}
