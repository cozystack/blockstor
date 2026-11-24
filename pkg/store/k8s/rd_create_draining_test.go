// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"context"
	"strings"
	"testing"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A replica drains the way production drains it: its satellite's finalizer
// holds the object after the delete, and nothing writes a DELETE flag into its
// spec; the flag the refusal reads is projected from the deletion timestamp.
// The contract test seeds the flag directly and so never covered that.
func TestRDCreateRefusedOverAFinalizerHeldReplica(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	const (
		rdName    = "pvc-drain-fin"
		finalizer = "blockstor.org/satellite-resource"
	)

	ctx := t.Context()
	st := k8s.NewWithAPIReader(fixture.client, fixture.client)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("create definition: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rdName, NodeName: "node-down"}); err != nil {
		t.Fatalf("create replica: %v", err)
	}

	crd := replicaCRD(ctx, t, rdName)

	release := func() {
		cur := replicaCRD(context.Background(), t, rdName)
		if cur == nil {
			return
		}

		cur.Finalizers = nil
		_ = fixture.client.Update(context.Background(), cur)
	}
	t.Cleanup(release)

	crd.Finalizers = append(crd.Finalizers, finalizer)
	if err := fixture.client.Update(ctx, crd); err != nil {
		t.Fatalf("hold the replica with the satellite finalizer: %v", err)
	}

	if err := fixture.client.Delete(ctx, crd); err != nil {
		t.Fatalf("delete the replica: %v", err)
	}

	if held := replicaCRD(ctx, t, rdName); held == nil || held.DeletionTimestamp.IsZero() ||
		len(held.Spec.Flags) != 0 {
		t.Fatalf("fixture: want a finalizer-held replica with a deletion timestamp and no spec flags, got %+v", held)
	}

	if err := st.ResourceDefinitions().Delete(ctx, rdName); err != nil {
		t.Fatalf("delete definition: %v", err)
	}

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if !store.IsReplicasDraining(err) || !strings.Contains(err.Error(), "node-down") {
		t.Fatalf("create over the finalizer-held replica = %v, want the draining refusal naming node-down", err)
	}

	release()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("create once the replica is gone: %v", err)
	}

	t.Cleanup(func() { _ = st.ResourceDefinitions().Delete(context.Background(), rdName) })
}

// replicaCRD reads the one Resource object under rdName from the API server,
// or nil when there is none.
func replicaCRD(ctx context.Context, t *testing.T, rdName string) *crdv1alpha1.Resource {
	t.Helper()

	var list crdv1alpha1.ResourceList
	if err := fixture.client.List(ctx, &list); err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName == rdName {
			return &list.Items[i]
		}
	}

	return nil
}
