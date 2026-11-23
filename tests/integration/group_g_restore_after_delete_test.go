// SPDX-License-Identifier: Apache-2.0

//go:build integration

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

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// takeSnapshotStandingIn creates snap of rd over REST and drives it through
// the controller's suspend, take and resume, writing what each targeted
// satellite writes. pool is what a satellite records as the pool holding its
// copy; "" stands in for a satellite from before that record.
func takeSnapshotStandingIn(t *testing.T, stack *harness.Stack, rd, snap, pool string) {
	t.Helper()

	status, body := httpPostJSON(t, stack.RestURL+"/v1/resource-definitions/"+rd+"/snapshots",
		map[string]any{"name": snap})
	if status/100 != 2 {
		t.Fatalf("snapshot create: status %d: %s", status, body)
	}

	waitForSnapshotCRD(t, stack, rd, snap)

	ctx := context.Background()
	key := types.NamespacedName{Name: rd + "." + snap}

	getSnap := func() blockstoriov1alpha1.Snapshot {
		var got blockstoriov1alpha1.Snapshot
		if err := stack.Env.Client.Get(ctx, key, &got); err != nil {
			t.Fatalf("get Snapshot: %v", err)
		}

		return got
	}

	stampNodes := func(acked, ready bool) {
		err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			got := getSnap()
			got.Status.NodeStatus = nil

			for _, node := range got.Spec.Nodes {
				entry := blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: node, SuspendIOAcked: acked, Ready: ready}
				if ready {
					entry.CreateTimestamp = time.Now().UnixMilli()
					entry.StoragePool = pool
				}

				got.Status.NodeStatus = append(got.Status.NodeStatus, entry)
			}

			return stack.Env.Client.Status().Update(ctx, &got)
		})
		if err != nil {
			t.Fatalf("stamp per-node status: %v", err)
		}
	}

	harness.Eventually(t, groupGTimeout, func() bool {
		got := getSnap()

		return got.Spec.SuspendIO && len(got.Spec.Nodes) == len(harness.FixtureNodes())
	}, "snapshot did not enter the suspend phase on every replica's node")

	stampNodes(true, false)

	harness.Eventually(t, groupGTimeout, func() bool { return getSnap().Spec.TakeSnapshot },
		"controller did not order the take once every node suspended")

	stampNodes(true, true)

	harness.Eventually(t, groupGTimeout, func() bool {
		got := getSnap()

		return !got.Spec.SuspendIO && !got.Spec.TakeSnapshot
	}, "controller did not resume I/O once every node took the snapshot")

	stampNodes(false, true)
}

// deleteEveryReplica removes the source volume's replicas and leaves its
// definition and snapshots, which is what a volume delete leaves behind.
func deleteEveryReplica(t *testing.T, stack *harness.Stack, rd string) {
	t.Helper()

	for _, res := range listResourcesByRD(t, stack, rd) {
		if err := stack.Env.Client.Delete(context.Background(), &res); err != nil {
			t.Fatalf("delete Resource %s: %v", res.Name, err)
		}
	}

	stripResourceFinalizers(t, stack, rd)

	harness.Eventually(t, groupGTimeout, func() bool { return len(listResourcesByRD(t, stack, rd)) == 0 },
		"source replicas were not deleted")
}

// testGroupGSnapRestoreAfterSourceDeleted restores the way linstor-csi does,
// onto one node, after every replica of the source is gone. The restored
// replica lands in the pool its node recorded for the snapshot, read back
// through the CRD store; before that record it was stamped with no pool and
// looped on `unknown storage pool ""` on its satellite.
func testGroupGSnapRestoreAfterSourceDeleted(t *testing.T, stack *harness.Stack) {
	src := seedRDWithVolume(t, stack, "gone-src")
	stack.Satellite.SimulateSnapshotsByHand(src)
	takeSnapshotStandingIn(t, stack, src, "snap-1", "zfs-thin")
	deleteEveryReplica(t, stack, src)

	dst := groupGRDName("gone-dst")

	status, body := httpPostJSON(t,
		stack.RestURL+"/v1/resource-definitions/"+src+"/snapshot-restore-resource/snap-1",
		map[string]any{"to_resource": dst, "nodes": []string{harness.NodeWorker2}})
	if status != http.StatusCreated {
		t.Fatalf("restore after the source went: status %d: %s", status, body)
	}

	restored := getResource(t, stack, dst, harness.NodeWorker2)
	if restored == nil {
		t.Fatalf("no restored replica on %s", harness.NodeWorker2)
	}

	if restored.Spec.StoragePool != "zfs-thin" {
		t.Errorf("restored replica pool = %q, want zfs-thin, the pool its node recorded for the snapshot",
			restored.Spec.StoragePool)
	}
}

// testGroupGSnapRestoreAfterSourceDeletedNeedsAPool is the same restore of a
// snapshot whose satellites recorded no pool. Nothing names one any more,
// and the restore is refused before it writes anything.
func testGroupGSnapRestoreAfterSourceDeletedNeedsAPool(t *testing.T, stack *harness.Stack) {
	src := seedRDWithVolume(t, stack, "nopool-src")
	stack.Satellite.SimulateSnapshotsByHand(src)
	takeSnapshotStandingIn(t, stack, src, "snap-1", "")
	deleteEveryReplica(t, stack, src)

	dst := groupGRDName("nopool-dst")

	status, body := httpPostJSON(t,
		stack.RestURL+"/v1/resource-definitions/"+src+"/snapshot-restore-resource/snap-1",
		map[string]any{"to_resource": dst, "nodes": []string{harness.NodeWorker2}})
	if status != http.StatusBadRequest {
		t.Fatalf("restore with no pool to restore into: status %d, want 400: %s", status, body)
	}

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(body, &rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal %s: %v", body, err)
	}

	const failNotFoundStorPool = 550
	if rcs[0].RetCode&failNotFoundStorPool != failNotFoundStorPool {
		t.Errorf("refusal ret_code %#x lacks FAIL_NOT_FOUND_STOR_POOL", rcs[0].RetCode)
	}

	if got := listResourcesByRD(t, stack, dst); len(got) != 0 {
		t.Errorf("the refused restore placed replicas: %d", len(got))
	}
}
