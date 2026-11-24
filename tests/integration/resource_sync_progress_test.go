//go:build integration

// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/cli"
	"github.com/cozystack/blockstor/pkg/store"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// A replica that finished a small resync from one peer and then ran a full
// one from another: the satellite's volume-wide out-of-sync count is the
// finished peer's (1% of the volume), DRBD's `done` for the running resync
// is 50.31. `r l --faulty` showed `Inconsistent(99%)`.
//
// LINSTOR answers with the running resync: `replication_states[peer]
// .done_percentage` is the peer's `done`, and the State column reads
// `SyncTarget(50.31%)`. Asserted on the wire the python client reads, and
// through the blockstor CLI, which reads the store directly.
func TestResourceListShowsTheRunningResyncsDone(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	linstor := &harness.CLI{URL: stack.RestURL}

	const (
		rdName = "pvc-resync"
		target = harness.NodeWorker3
		pool   = "lvm-thin"
	)

	linstor.Run(t, "resource-definition", "create", rdName)
	linstor.Run(t, "volume-definition", "create", rdName, "1G")

	for _, node := range []string{harness.NodeWorker1, harness.NodeWorker2, target} {
		linstor.Run(t, "resource", "create", "--storage-pool="+pool, node, rdName)
	}

	stack.Satellite.SimulateDRBDState(rdName, target, "Inconsistent")
	harness.WaitForDRBDState(t, stack, rdName, target, "Inconsistent")

	stampResync(t, stack, rdName+"."+target)

	const want = "SyncTarget(50.31%)"

	harness.Eventually(t, 15*time.Second, func() bool {
		return wireDiskState(t, linstor, rdName, target) == want
	}, "view/resources disk_state of the SyncTarget replica != "+want)

	done := wireDone(t, linstor, rdName, target, harness.NodeWorker1)
	if done == nil || *done != 50.31 {
		t.Errorf("replication_states[%s].done_percentage = %v, want 50.31", harness.NodeWorker1, done)
	}

	if table := blockstorTable(t, stack, "resource", "list", "--faulty"); !strings.Contains(table, want) {
		t.Errorf("blockstor r l --faulty does not show %s:\n%s", want, table)
	}
}

// stampResync writes what the satellite observer writes for the scenario:
// the local volume Inconsistent with the finished peer's out-of-sync count,
// one peer in SyncTarget with its `done`, the other Established without one.
func stampResync(t *testing.T, stack *harness.Stack, name string) {
	t.Helper()

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var res blockstoriov1alpha1.Resource
		if err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: name}, &res); err != nil {
			return err
		}

		res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
			{VolumeNumber: 0, DiskState: "Inconsistent", OutOfSyncKib: 10486},
		}
		res.Status.Connections = []blockstoriov1alpha1.ResourceConnectionStatus{
			{
				PeerNodeName:     harness.NodeWorker1,
				Connected:        true,
				Message:          "Connected",
				ReplicationState: "SyncTarget",
				PeerVolumes: []blockstoriov1alpha1.PeerVolumeStatus{
					{VolumeNumber: 0, PeerDiskState: "UpToDate", Done: "50.31"},
				},
			},
			{
				PeerNodeName:     harness.NodeWorker2,
				Connected:        true,
				Message:          "Connected",
				ReplicationState: "Established",
				PeerVolumes: []blockstoriov1alpha1.PeerVolumeStatus{
					{VolumeNumber: 0, PeerDiskState: "UpToDate"},
				},
			},
		}

		return stack.Env.Client.Status().Update(context.Background(), &res)
	})
	if err != nil {
		t.Fatalf("stamp resync status on %s: %v", name, err)
	}
}

func wireVolumeState(t *testing.T, linstor *harness.CLI, rdName, node string) map[string]any {
	t.Helper()

	for _, row := range linstor.JSON(t, "resource", "list-volumes", "--resources", rdName) {
		if row["node_name"] != node {
			continue
		}

		vols, _ := row["volumes"].([]any)
		if len(vols) == 0 {
			return nil
		}

		vol, _ := vols[0].(map[string]any)
		state, _ := vol["state"].(map[string]any)

		return state
	}

	return nil
}

func wireDiskState(t *testing.T, linstor *harness.CLI, rdName, node string) string {
	t.Helper()

	state := wireVolumeState(t, linstor, rdName, node)
	disk, _ := state["disk_state"].(string)

	return disk
}

func wireDone(t *testing.T, linstor *harness.CLI, rdName, node, peer string) *float64 {
	t.Helper()

	state := wireVolumeState(t, linstor, rdName, node)
	repl, _ := state["replication_states"].(map[string]any)
	entry, _ := repl[peer].(map[string]any)

	done, ok := entry["done_percentage"].(float64)
	if !ok {
		return nil
	}

	return &done
}

func blockstorTable(t *testing.T, stack *harness.Stack, args ...string) string {
	t.Helper()

	var out, errBuf bytes.Buffer

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return k8sstore.New(stack.Env.Client), nil
		},
	}

	if code := app.Run(context.Background(), args); code != 0 {
		t.Fatalf("blockstor %v: exit %d: %s", args, code, errBuf.String())
	}

	return out.String()
}
