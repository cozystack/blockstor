//go:build integration

// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// `rd d PVC-X` over a definition stored as `pvc-x`: the snapshot gate and the
// cascade listed under the typed spelling and found nothing, and the folded
// Delete then took the definition, leaving its replicas and snapshots behind.
// This needs the real API server, since only its field selector compares
// the stored spelling exactly.
func TestRDDeleteInAnotherCaseSeesSnapshotsAndReplicas(t *testing.T) {
	stack := harness.StartStack(t)
	ctx := context.Background()
	st := k8sstore.New(stack.Env.Client)

	const rd = "pvc-mixcase"

	status, body := b434HTTP(t, http.MethodPost, stack.RestURL+"/v1/resource-definitions",
		map[string]any{"resource_definition": map[string]any{"name": rd}})
	if status != http.StatusCreated {
		t.Fatalf("rd create: %d %s", status, body)
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rd + ".node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: "node-1"},
	}
	if err := stack.Env.Client.Create(ctx, res); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: rd + ".snap-1"},
		Spec:       blockstoriov1alpha1.SnapshotSpec{ResourceDefinitionName: rd, SnapshotName: "snap-1"},
	}
	if err := stack.Env.Client.Create(ctx, snap); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	waitForMixedCase(t, "the cache to see the replica and the snapshot", func() bool {
		replicas, rErr := st.Resources().ListByDefinition(ctx, rd)
		snaps, sErr := st.Snapshots().ListByDefinition(ctx, rd)

		return rErr == nil && sErr == nil && len(replicas) == 1 && len(snaps) == 1
	})

	// With a snapshot on it, the delete is refused in any spelling.
	resp := rgcHTTPDelete(t, stack.RestURL+"/v1/resource-definitions/"+strings.ToUpper(rd))
	_ = resp.Body.Close()

	if resp.StatusCode < 400 {
		t.Fatalf("rd d %s answered %d over a definition with a snapshot", strings.ToUpper(rd), resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, rd); err != nil {
		t.Fatalf("the refused delete took the definition anyway: %v", err)
	}

	// Without the snapshot, the delete in another case takes the replica too.
	if err := stack.Env.Client.Delete(ctx, snap); err != nil {
		t.Fatalf("drop snapshot: %v", err)
	}

	waitForMixedCase(t, "the cache to drop the snapshot", func() bool {
		snaps, err := st.Snapshots().ListByDefinition(ctx, rd)

		return err == nil && len(snaps) == 0
	})

	resp = rgcHTTPDelete(t, stack.RestURL+"/v1/resource-definitions/"+strings.ToUpper(rd))
	_ = resp.Body.Close()

	if resp.StatusCode >= 400 {
		t.Fatalf("rd d %s: %d", strings.ToUpper(rd), resp.StatusCode)
	}

	waitForMixedCase(t, "the replica to be reaped with its definition", func() bool {
		var live blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: rd + ".node-1"}, &live)

		return err != nil || live.DeletionTimestamp != nil
	})
}

func waitForMixedCase(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}
