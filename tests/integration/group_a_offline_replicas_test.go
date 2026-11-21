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
	"os/exec"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// TestGroupAOfflineNodeReplicasReadUnknown: while a node's satellite is
// OFFLINE, LINSTOR answers that node's replicas with no satellite state,
// and linstor-client shows them as a yellow Unknown that `r l --faulty`
// lists. blockstor kept answering the dead satellite's last report, a
// connected UpToDate Secondary, so neither listing showed them.
func TestGroupAOfflineNodeReplicasReadUnknown(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	ctx := context.Background()

	const rdName = "rd-offline"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: harness.FixtureDefaultRG},
	}

	err := stack.Env.Client.Create(ctx, rd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("seed RD: %v", err)
	}

	cli := &harness.CLI{URL: stack.RestURL}
	cli.Run(t, "volume-definition", "create", rdName, "10M")

	peers := map[string]string{harness.NodeWorker1: harness.NodeWorker2, harness.NodeWorker2: harness.NodeWorker1}
	for node := range peers {
		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rdName, NodeName: node},
		}

		err = stack.Env.Client.Create(ctx, res)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("seed Resource on %s: %v", node, err)
		}
	}

	// What a satellite reports for a healthy Secondary; the mock
	// stamps only the role.
	for node, peer := range peers {
		reportHealthySecondary(t, stack, rdName+"."+node, peer)
	}

	stack.Satellite.SimulateNodeOffline(harness.NodeWorker1)
	waitForNodeConnectionStatus(t, stack, harness.NodeWorker1, blockstoriov1alpha1.NodeConnectionStatusOffline)

	for _, row := range cli.JSON(t, "resource", "list", "-r", rdName) {
		node, _ := row["node_name"].(string)
		if _, ours := peers[node]; !ours {
			continue // the tie-breaker the controller adds
		}

		vols, _ := row["volumes"].([]any)
		if len(vols) == 0 {
			t.Fatalf("%s: no volumes on the wire: %v", node, row)
		}

		vol, _ := vols[0].(map[string]any)
		state, _ := vol["state"].(map[string]any)
		disk, _ := state["disk_state"].(string)

		switch node {
		case harness.NodeWorker1:
			if disk != "" {
				t.Errorf("replica on the OFFLINE node answers the stale disk state %q", disk)
			}
		case harness.NodeWorker2:
			if disk != "UpToDate" {
				t.Errorf("replica on the ONLINE node answers %q, want UpToDate", disk)
			}
		}
	}

	// The table linstor-client renders from that answer. Run directly:
	// the harness CLI always asks for the machine-readable form.
	table := faultyTable(t, stack, rdName)

	row := ""

	for line := range strings.SplitSeq(table, "\n") {
		if strings.Contains(line, harness.NodeWorker1) {
			row = line
		}
	}

	if !strings.Contains(row, "Unknown") {
		t.Errorf("`r l --faulty` does not list the OFFLINE node's replica as Unknown:\n%s", table)
	}

	if strings.Contains(table, harness.NodeWorker2) {
		t.Errorf("`r l --faulty` lists the healthy replica on the ONLINE node:\n%s", table)
	}
}

// TestGroupARebootedNodeReplicasReadUnknown: a node back ONLINE
// after a reboot runs a new satellite, which brings its replicas up one
// by one. Until it reports a replica the store holds the report from
// before the reboot, a connected UpToDate Secondary. LINSTOR shows state
// only from the satellite it is connected to now, so linstor-client
// reads that replica Unknown and `r l --faulty` lists it, until the new
// satellite reports it.
func TestGroupARebootedNodeReplicasReadUnknown(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	ctx := context.Background()

	const rdName = "rd-restarted"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: harness.FixtureDefaultRG},
	}

	err := stack.Env.Client.Create(ctx, rd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("seed RD: %v", err)
	}

	cli := &harness.CLI{URL: stack.RestURL}
	cli.Run(t, "volume-definition", "create", rdName, "10M")

	peers := map[string]string{harness.NodeWorker1: harness.NodeWorker2, harness.NodeWorker2: harness.NodeWorker1}
	for node := range peers {
		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rdName, NodeName: node},
		}

		err = stack.Env.Client.Create(ctx, res)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("seed Resource on %s: %v", node, err)
		}
	}

	for node, peer := range peers {
		reportHealthySecondary(t, stack, rdName+"."+node, peer)
	}

	// worker-1's satellite before the reboot reported its replica;
	// the one running there now has not yet.
	stampReplicaIncarnation(t, stack, rdName+"."+harness.NodeWorker1, "inc-1")
	stampNodeIncarnation(t, stack, harness.NodeWorker1, "inc-2")

	waitForNodeConnectionStatus(t, stack, harness.NodeWorker1, blockstoriov1alpha1.NodeConnectionStatusOnline)

	table := faultyTable(t, stack, rdName)
	if !strings.Contains(faultyRow(table, harness.NodeWorker1), "Unknown") {
		t.Errorf("`r l --faulty` does not list the replica the rebooted node's satellite has not reported as Unknown:\n%s", table)
	}

	if strings.Contains(table, harness.NodeWorker2) {
		t.Errorf("`r l --faulty` lists the healthy replica on the other node:\n%s", table)
	}

	// The new satellite reports the replica: its state reads again.
	stampReplicaIncarnation(t, stack, rdName+"."+harness.NodeWorker1, "inc-2")

	table = faultyTable(t, stack, rdName)
	if strings.Contains(table, harness.NodeWorker1) {
		t.Errorf("`r l --faulty` still lists the replica the rebooted node's satellite has reported:\n%s", table)
	}
}

func faultyRow(table, node string) string {
	row := ""

	for line := range strings.SplitSeq(table, "\n") {
		if strings.Contains(line, node) {
			row = line
		}
	}

	return row
}

// stampReplicaIncarnation marks the replica's status as reported by
// the satellite on the kernel boot named incarnation.
func stampReplicaIncarnation(t *testing.T, stack *harness.Stack, name, incarnation string) {
	t.Helper()

	harness.Eventually(t, 10*time.Second, func() bool {
		var res blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: name}, &res)
		if err != nil {
			return false
		}

		res.Status.SatelliteIncarnation = incarnation

		return stack.Env.Client.Status().Update(context.Background(), &res) == nil
	}, "stamp "+name+" with "+incarnation)
}

// stampNodeIncarnation names the kernel boot node runs now, as the
// satellite heartbeat does.
func stampNodeIncarnation(t *testing.T, stack *harness.Stack, node, incarnation string) {
	t.Helper()

	harness.Eventually(t, 10*time.Second, func() bool {
		var n blockstoriov1alpha1.Node

		err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: node}, &n)
		if err != nil {
			return false
		}

		n.Status.SatelliteIncarnation = incarnation

		return stack.Env.Client.Status().Update(context.Background(), &n) == nil
	}, "stamp node "+node+" with "+incarnation)
}

// reportHealthySecondary writes the status a satellite reports for a
// connected UpToDate Secondary, retrying over the mock's own writes.
func reportHealthySecondary(t *testing.T, stack *harness.Stack, name, peer string) {
	t.Helper()

	harness.Eventually(t, 10*time.Second, func() bool {
		var res blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: name}, &res)
		if err != nil || res.Status.DrbdState != "UpToDate" {
			return false
		}

		res.Status.InUse = false
		res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, DiskState: "UpToDate"}}
		res.Status.Connections = []blockstoriov1alpha1.ResourceConnectionStatus{
			{PeerNodeName: peer, Connected: true, Message: "Connected"},
		}

		return stack.Env.Client.Status().Update(context.Background(), &res) == nil
	}, "report "+name+" as a healthy Secondary")
}

// faultyTable renders `r l --faulty` for the two diskful replicas. The
// tie-breaker the controller adds is left out: the mock satellite never
// reports it, so its DRBD layer carries no data and linstor-client's
// table crashes on it with a KeyError once the controller has placed
// it, which it does at an unpredictable moment during the test.
func faultyTable(t *testing.T, stack *harness.Stack, rdName string) string {
	t.Helper()

	return runLinstorTable(t, stack.RestURL, "resource", "list", "-r", rdName,
		"-n", harness.NodeWorker1, harness.NodeWorker2, "--faulty")
}

// runLinstorTable runs linstor-client for its human-readable table.
func runLinstorTable(t *testing.T, url string, args ...string) string {
	t.Helper()

	full := append([]string{"--controllers", url, "--no-color", "--no-utf8"}, args...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "linstor", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("linstor %s: %v\n%s", strings.Join(args, " "), err, out)
	}

	return string(out)
}
