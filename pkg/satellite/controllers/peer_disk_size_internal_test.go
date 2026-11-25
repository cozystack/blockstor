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
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// sizedReplica is a replica of pvc-split on node whose volume 0 reports
// a device of deviceKib in diskState, with its connections to peers in
// the states given (true: Connected).
func sizedReplica(node string, deviceKib int64, diskState string, conns map[string]bool) *blockstoriov1alpha1.Resource {
	res := makeToggledResource("pvc-split", node, "pool-a")
	res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DeviceSizeKib: deviceKib, DiskState: diskState},
	}

	for peer, connected := range conns {
		message := "StandAlone"
		if connected {
			message = "Connected"
		}

		res.Status.Connections = append(res.Status.Connections,
			blockstoriov1alpha1.ResourceConnectionStatus{PeerNodeName: peer, Connected: connected, Message: message})
	}

	return res
}

// TestPeerSizeRefusals: the replica that holds data on a device larger
// than a peer's disk is the one whose kernel refuses that peer, as long
// as neither side shows the connection up. The shape is the one a
// spawn left: the first replica UpToDate on 65564 KiB, StandAlone
// toward one peer and never connected to the other, both of them
// Inconsistent on 65548 KiB.
func TestPeerSizeRefusals(t *testing.T) {
	t.Parallel()

	larger := sizedReplica("plo", 65564, "UpToDate", map[string]bool{"luc": false})
	luc := sizedReplica("luc", 65548, "Inconsistent", map[string]bool{"plo": false, "gld": true})
	gld := sizedReplica("gld", 65548, "Inconsistent", map[string]bool{"plo": false, "luc": true})

	for name, tc := range map[string]struct {
		self  *blockstoriov1alpha1.Resource
		peers []*blockstoriov1alpha1.Resource
		want  []string
	}{
		"larger replica holding data":    {larger, []*blockstoriov1alpha1.Resource{larger, luc, gld}, []string{"luc", "gld"}},
		"smaller replica":                {luc, []*blockstoriov1alpha1.Resource{larger, luc, gld}, nil},
		"larger replica without data":    {sizedReplica("plo", 65564, "Inconsistent", nil), []*blockstoriov1alpha1.Resource{luc}, nil},
		"connected despite the sizes":    {sizedReplica("plo", 65564, "UpToDate", map[string]bool{"luc": true}), []*blockstoriov1alpha1.Resource{luc}, nil},
		"peer's record still connected":  {larger, []*blockstoriov1alpha1.Resource{sizedReplica("luc", 65548, "UpToDate", map[string]bool{"plo": true})}, nil},
		"equal sizes":                    {larger, []*blockstoriov1alpha1.Resource{sizedReplica("luc", 65564, "Inconsistent", nil)}, nil},
		"peer that reported no size yet": {larger, []*blockstoriov1alpha1.Resource{sizedReplica("luc", 0, "", nil)}, nil},
		"diskless peer":                  {larger, []*blockstoriov1alpha1.Resource{disklessPeer(luc)}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			peers := make([]blockstoriov1alpha1.Resource, 0, len(tc.peers))
			for _, peer := range tc.peers {
				peers = append(peers, *peer.DeepCopy())
			}

			var got []string
			for _, refusal := range peerSizeRefusals(tc.self, peers) {
				got = append(got, refusal.peer)
			}

			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("refused peers %v, want %v", got, tc.want)
			}
		})
	}
}

func disklessPeer(res *blockstoriov1alpha1.Resource) *blockstoriov1alpha1.Resource {
	out := res.DeepCopy()
	out.Spec.Flags = []string{apiv1.ResourceFlagDiskless}

	return out
}

// TestAnApplyOverPeersTooSmallReportsTheRefusal: a replica whose kernel
// refuses its peers for their disk size reported a successful apply on
// every pass, with no Event, while two of its three replicas stayed
// Inconsistent for good. The pass now fails with the refusal, as a
// PeerDiskTooSmall Warning and on Status.ApplyFailure, naming the peers
// and the sizes.
func TestAnApplyOverPeersTooSmallReportsTheRefusal(t *testing.T) {
	t.Parallel()

	self := sizedReplica("plo", 65564, "UpToDate", map[string]bool{"luc": false})
	peer := sizedReplica("luc", 65548, "Inconsistent", map[string]bool{"plo": false})

	cli := fake.NewClientBuilder().
		WithScheme(newToggleDiskTestScheme(t)).
		WithObjects(makeStorageOnlyRD("pvc-split"), self, peer).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	recorder := events.NewFakeRecorder(8)

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName: "plo",
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{"pool-a": &scriptedCreateProvider{}},
				NodeName:  "plo",
			}),
			Exec:     storage.NewFakeExec(),
			Recorder: recorder,
		},
	}

	_, err := reconciler.runApply(t.Context(), self.DeepCopy(), logr.Discard())
	if err != nil {
		t.Fatalf("runApply: %v", err)
	}

	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, peerDiskTooSmallReason) || !strings.Contains(ev, "luc") {
			t.Errorf("event %q, want a %s Warning naming luc", ev, peerDiskTooSmallReason)
		}
	default:
		t.Errorf("no Event: the refusal passes as a successful apply")
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(t.Context(), client.ObjectKey{Name: self.Name}, &after)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if after.Status.ApplyFailure == nil || !strings.Contains(after.Status.ApplyFailure.Message, "65548 KiB") {
		t.Errorf("Status.ApplyFailure = %+v, want the refusal with the peer's size", after.Status.ApplyFailure)
	}
}

// TestRefusalKeepsAFailedApplysOwnError: an apply that failed on its
// own keeps its error; the refusal is reported once the apply goes
// through.
func TestRefusalKeepsAFailedApplysOwnError(t *testing.T) {
	t.Parallel()

	self := sizedReplica("plo", 65564, "UpToDate", nil)
	peers := []blockstoriov1alpha1.Resource{*self, *sizedReplica("luc", 65548, "Inconsistent", nil)}
	results := []*intent.ResourceApplyResult{{Ok: false, Message: "drbdadm adjust: exit 10"}}

	refusePeersTooSmall(self, peers, results)

	if results[0].GetMessage() != "drbdadm adjust: exit 10" {
		t.Errorf("message %q, want the apply's own", results[0].GetMessage())
	}
}
