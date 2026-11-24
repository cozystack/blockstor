// SPDX-License-Identifier: Apache-2.0

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

package controllers

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

const (
	statusTestRD      = "pvc-status"
	statusTestNode    = "n1"
	statusTestBacking = "/dev/vg/pvc-status_00000"
	statusTestGI      = "6E3A5D0B2C1F4A96"
)

// statusTestExec answers get-gi for the test volume, as DRBD 9.3 has
// to be asked: its events2 frames carry no current-uuid.
func statusTestExec() *storage.FakeExec {
	fx := storage.NewFakeExec()
	fx.Expect("drbdmeta --force - v09 "+statusTestBacking+" internal get-gi --node-id 0",
		storage.FakeResponse{Stdout: []byte(statusTestGI + ":0:0:0\n")})

	return fx
}

// startStatusEnvtest runs a real apiserver: only it implements the
// server-side apply rule these tests are about, that a field manager's
// apply which leaves out a field the manager set before removes it.
func startStatusEnvtest(t *testing.T) client.WithWatch {
	t.Helper()

	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	_, thisFile, _, _ := runtime.Caller(0) //nolint:dogsled // only the file path is useful
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(repoRoot, "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest: %v", err)
	}

	t.Cleanup(func() { _ = env.Stop() })

	scheme := k8sruntime.NewScheme()

	err = blockstoriov1alpha1.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("scheme: %v", err)
	}

	cli, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	return cli
}

// createStatusReplica creates the replica and stamps the device path
// the way the reconciler does, under its own field manager.
func createStatusReplica(t *testing.T, cli client.Client) string {
	t.Helper()

	name := k8s.ResourceCRName(statusTestRD, statusTestNode)

	err := cli.Create(t.Context(), &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               statusTestNode,
			ResourceDefinitionName: statusTestRD,
			StoragePool:            "thin1",
		},
	})
	if err != nil {
		t.Fatalf("create replica: %v", err)
	}

	stamp := &blockstoriov1alpha1.Resource{
		TypeMeta:   metav1.TypeMeta{Kind: resourceKind, APIVersion: blockstoriov1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: blockstoriov1alpha1.ResourceStatus{Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{{
			VolumeNumber: 0, StoragePool: "thin1", DevicePath: "/dev/drbd1000",
		}}},
	}

	err = cli.Status().Patch(t.Context(), stamp,
		client.Apply, //nolint:staticcheck // SA1019: same call the reconciler makes
		client.FieldOwner(volumeStatusFieldOwner))
	if err != nil {
		t.Fatalf("stamp device path: %v", err)
	}

	return name
}

func getStatusReplica(t *testing.T, cli client.Client, name string) blockstoriov1alpha1.ResourceStatus {
	t.Helper()

	var res blockstoriov1alpha1.Resource

	err := cli.Get(t.Context(), client.ObjectKey{Name: name}, &res)
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	return res.Status
}

func deviceUpToDate() *observation {
	return &observation{
		ResourceName: statusTestRD,
		DrbdState:    "UpToDate",
		Volumes: []volumeObservation{{
			VolumeNumber: 0, DiskState: "UpToDate", Quorum: true, HasQuorum: true, BackingDev: statusTestBacking,
		}},
	}
}

func peerConnected(peer string) *observation {
	return &observation{
		ResourceName: statusTestRD,
		Connections:  []connectionObservation{{PeerNodeName: peer, Connected: true, Message: "Connected"}},
	}
}

func peerEstablished(peer string) *observation {
	return &observation{
		ResourceName: statusTestRD,
		Connections: []connectionObservation{{
			PeerNodeName:     peer,
			ReplicationState: "Established",
			PeerVolumes:      []peerVolumeObservation{{VolumeNumber: 0, PeerDiskState: "UpToDate"}},
		}},
	}
}

// A frame about one part of the replica writes the observer's view of
// all of it. On a busy healthy replica the kernel sends connection and
// role frames that carry no volume, and statistics frames that change
// nothing on the volume; an apply that left the volumes out made the
// apiserver drop the disk state, the GI and the quorum the observer had
// set, and `r l` showed the replica Unknown until the next resync.
func TestObserverFrameKeepsTheRestOfTheReplicaState(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	o := &ObserverRunnable{Client: cli, Exec: statusTestExec(), NodeName: statusTestNode}
	adm := drbd.NewAdm(o.Exec)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())
	o.handleObservation(ctx, adm, peerConnected("n2"))
	o.handleObservation(ctx, adm, peerEstablished("n2"))

	status := getStatusReplica(t, cli, name)
	if len(status.Volumes) != 1 || status.Volumes[0].DiskState != "UpToDate" || !status.Volumes[0].Quorum {
		t.Fatalf("after connection frames the volume lost the observer's state: %+v", status.Volumes)
	}

	if status.Volumes[0].CurrentGI != statusTestGI {
		t.Fatalf("after connection frames the volume lost the GI read from disk: %+v", status.Volumes)
	}

	if status.Volumes[0].DevicePath != "/dev/drbd1000" {
		t.Fatalf("the reconciler's device path went: %+v", status.Volumes)
	}

	// A role change carries neither volumes nor connections.
	o.handleObservation(ctx, adm, &observation{ResourceName: statusTestRD, HasResource: true, Role: "Primary", InUse: true})

	status = getStatusReplica(t, cli, name)
	if len(status.Volumes) != 1 || status.Volumes[0].DiskState != "UpToDate" || status.Volumes[0].CurrentGI != statusTestGI {
		t.Fatalf("after a role frame the volume lost its disk state: %+v", status.Volumes)
	}

	if len(status.Connections) != 1 || status.Connections[0].ReplicationState != "Established" ||
		status.Connections[0].Message != "Connected" {
		t.Fatalf("after a role frame the connection went: %+v", status.Connections)
	}

	if status.Role != "Primary" || !status.InUse {
		t.Fatalf("role frame not applied: role=%q inUse=%v", status.Role, status.InUse)
	}
}

// A resync pass snapshots a replica, reads its GI from disk and only
// then writes. A frame handled in between must not be undone by that
// older snapshot: it was how a finished resync could stay on the
// replica as SyncTarget until some later frame happened to come.
func TestObserverResyncDoesNotWriteAnOlderSnapshot(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	o := &ObserverRunnable{Client: cli, Exec: statusTestExec(), NodeName: statusTestNode}
	adm := drbd.NewAdm(o.Exec)
	ctx := t.Context()

	o.handleObservation(ctx, adm, deviceUpToDate())
	o.handleObservation(ctx, adm, peerConnected("n2"))
	o.handleObservation(ctx, adm, &observation{
		ResourceName: statusTestRD,
		Connections: []connectionObservation{{
			PeerNodeName: "n2", ReplicationState: "SyncSource",
			PeerVolumes: []peerVolumeObservation{{VolumeNumber: 0, PeerDiskState: "Inconsistent"}},
		}},
	})

	stale := o.snapshotFor(statusTestRD)

	o.handleObservation(ctx, adm, peerEstablished("n2"))

	err := o.writeStatus(ctx, &stale)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	status := getStatusReplica(t, cli, name)
	if len(status.Connections) != 1 || status.Connections[0].ReplicationState != "Established" ||
		len(status.Connections[0].PeerVolumes) != 1 || status.Connections[0].PeerVolumes[0].PeerDiskState != "UpToDate" {
		t.Fatalf("an older snapshot went over the newer state: %+v", status.Connections)
	}
}
