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
	"k8s.io/apimachinery/pkg/api/meta"
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

// TestGroupARestartedSatelliteReplicasReadUnknown: a node back ONLINE
// after a reboot runs a new satellite, which brings its replicas up one
// by one. Until it reports a replica the store holds the report from
// before the crash, a connected UpToDate Secondary. LINSTOR shows state
// only from the satellite it is connected to now, so linstor-client
// reads that replica Unknown and `r l --faulty` lists it, until the new
// satellite reports it.
func TestGroupARestartedSatelliteReplicasReadUnknown(t *testing.T) {
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
		t.Errorf("`r l --faulty` does not list the replica the restarted satellite has not reported as Unknown:\n%s", table)
	}

	if strings.Contains(table, harness.NodeWorker2) {
		t.Errorf("`r l --faulty` lists the healthy replica on the other node:\n%s", table)
	}

	// The new satellite reports the replica: its state reads again.
	stampReplicaIncarnation(t, stack, rdName+"."+harness.NodeWorker1, "inc-2")

	table = faultyTable(t, stack, rdName)
	if strings.Contains(table, harness.NodeWorker1) {
		t.Errorf("`r l --faulty` still lists the replica the restarted satellite has reported:\n%s", table)
	}
}

// TestGroupADestroyedKernelResourceReadsUnknown: while a replica's DRBD
// resource is gone from the kernel of a node that stays ONLINE after
// it had been up (`drbdsetup down`), the LINSTOR satellite has closed
// that resource's state stream, so the controller answers it with no
// state and linstor-client shows a yellow Unknown with a blank Usage
// that `r l --faulty` lists. blockstor answered the status the destroy
// frames leave, no disk state or peer link but a role that still reads
// Secondary: the row read Unused, a healthy-looking replica that holds
// no data and no peer link.
func TestGroupADestroyedKernelResourceReadsUnknown(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	ctx := context.Background()

	const rdName = "rd-kernel-down"

	// The test reports the replicas' volumes itself; the harness satellite
	// would overwrite them with its own healthy view on its next tick.
	stack.Satellite.SimulateVolumesUnreported(rdName)

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

	reportKernelResourceDestroyed(t, stack, rdName+"."+harness.NodeWorker1)

	table := faultyTable(t, stack, rdName)

	row := faultyRow(table, harness.NodeWorker1)
	if !strings.Contains(row, "Unknown") || strings.Contains(row, "Unused") {
		t.Errorf("`r l --faulty` does not list the replica with no kernel resource as Unknown with no usage:\n%s", table)
	}

	if strings.Contains(table, harness.NodeWorker2) {
		t.Errorf("`r l --faulty` lists the healthy replica on the other node:\n%s", table)
	}
}

// TestGroupAStaleKernelLoadedFalseReadsHealthy: the KernelLoaded
// Condition is written only from events2 resource frames, so a write
// lost when a downed replica came back leaves it False while the
// observer's resync keeps reporting the replica UpToDate and connected.
// That replica is up, and `r l --faulty` must not list it as Unknown.
func TestGroupAStaleKernelLoadedFalseReadsHealthy(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	ctx := context.Background()

	const rdName = "rd-kernel-stale"

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

	markKernelLoadedFalse(t, stack, rdName+"."+harness.NodeWorker1)

	table := faultyTable(t, stack, rdName)
	if strings.Contains(table, harness.NodeWorker1) {
		t.Errorf("`r l --faulty` lists a replica still reported UpToDate and connected:\n%s", table)
	}
}

// TestGroupARecreatedWithoutDiskReadsFaulty: once the satellite has
// brought a downed replica's kernel resource back but its adjust keeps
// failing, the kernel holds the resource with no disk and its peer
// links StandAlone. LINSTOR answers that state as reported, and
// linstor-client shows a red Diskless, since the replica is not flagged
// DISKLESS, with the StandAlone peers in Conns, and lists it under
// `--faulty`. A provoked-failure test read this row as healthy; this
// pins that the view gives linstor-client what it needs to flag it.
func TestGroupARecreatedWithoutDiskReadsFaulty(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	ctx := context.Background()

	const rdName = "rd-kernel-diskless"

	// The test reports the replicas' volumes itself; the harness satellite
	// would overwrite them with its own healthy view on its next tick.
	stack.Satellite.SimulateVolumesUnreported(rdName)

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

	reportRecreatedWithoutDisk(t, stack, rdName, harness.NodeWorker1, harness.NodeWorker2)

	table := faultyTable(t, stack, rdName)

	row := faultyRow(table, harness.NodeWorker1)
	if !strings.Contains(row, "Diskless") || !strings.Contains(row, "StandAlone("+harness.NodeWorker2+")") {
		t.Errorf("`r l --faulty` does not list the replica with no disk and a StandAlone peer as such:\n%s", table)
	}
}

// reportRecreatedWithoutDisk writes what the satellite reports for a
// kernel resource brought back without its disk whose peer link stays
// StandAlone: a Diskless volume, the link down, KernelLoaded=True.
// The mock satellite projects its DRBD state onto every volume and
// rewrites a disk state that differs, so it is told the same state.
func reportRecreatedWithoutDisk(t *testing.T, stack *harness.Stack, rdName, node, peer string) {
	t.Helper()

	stack.Satellite.SimulateDRBDState(rdName, node, "Diskless")

	name := rdName + "." + node

	harness.Eventually(t, 10*time.Second, func() bool {
		var res blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: name}, &res)
		if err != nil {
			return false
		}

		res.Status.InUse = false
		res.Status.DrbdState = "Diskless"
		res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, DiskState: "Diskless"}}
		res.Status.Connections = []blockstoriov1alpha1.ResourceConnectionStatus{
			{PeerNodeName: peer, Connected: false, Message: "StandAlone"},
		}
		meta.SetStatusCondition(&res.Status.Conditions, metav1.Condition{
			Type:   blockstoriov1alpha1.ConditionKernelLoaded,
			Status: metav1.ConditionTrue,
			Reason: "KernelSlotCreated",
		})

		return stack.Env.Client.Status().Update(context.Background(), &res) == nil
	}, "report "+name+" brought back without its disk")
}

// markKernelLoadedFalse sets KernelLoaded=False on a replica and leaves
// the rest of its status as reported.
func markKernelLoadedFalse(t *testing.T, stack *harness.Stack, name string) {
	t.Helper()

	harness.Eventually(t, 10*time.Second, func() bool {
		var res blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: name}, &res)
		if err != nil {
			return false
		}

		meta.SetStatusCondition(&res.Status.Conditions, metav1.Condition{
			Type:   blockstoriov1alpha1.ConditionKernelLoaded,
			Status: metav1.ConditionFalse,
			Reason: "KernelSlotDestroyed",
		})

		return stack.Env.Client.Status().Update(context.Background(), &res) == nil
	}, "mark KernelLoaded False on "+name)
}

// reportKernelResourceDestroyed writes what the satellite's observer
// leaves on a replica whose kernel resource it saw destroyed: no
// observed volume or peer link, the role it cannot unset, and
// KernelLoaded=False.
func reportKernelResourceDestroyed(t *testing.T, stack *harness.Stack, name string) {
	t.Helper()

	harness.Eventually(t, 10*time.Second, func() bool {
		var res blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: name}, &res)
		if err != nil {
			return false
		}

		res.Status.InUse = false
		res.Status.Volumes = nil
		res.Status.Connections = nil
		meta.SetStatusCondition(&res.Status.Conditions, metav1.Condition{
			Type:   blockstoriov1alpha1.ConditionKernelLoaded,
			Status: metav1.ConditionFalse,
			Reason: "KernelSlotDestroyed",
		})

		return stack.Env.Client.Status().Update(context.Background(), &res) == nil
	}, "report the kernel resource of "+name+" destroyed")
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
// the satellite process named incarnation.
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

// stampNodeIncarnation names the satellite process running on node, as
// the satellite heartbeat does.
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
