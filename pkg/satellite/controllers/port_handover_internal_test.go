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
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// addressReuseRefusal is the refusal drbdadm gave the .res of a new
// replica of pvc-new whose port the .res of pvc-old, still on the node
// while its teardown retried, held.
const addressReuseRefusal = "new .res for pvc-new.res not installed: drbdadm refuses /etc/drbd.d/pvc-new.res.tmp: " +
	"drbdadm --config-to-test /etc/drbd.d/pvc-new.res.tmp --config-to-exclude /etc/drbd.d/pvc-new.res sh-nop: " +
	"/etc/drbd.d/pvc-new.res.tmp:53: in resource pvc-new\n" +
	"    ipv4:10.0.0.3:20055 is also used /etc/drbd.d/pvc-old.res:49 (resource pvc-old)\n" +
	"command sh-nop exited with code 10: exit status 10"

func TestHeldAddressOwner(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		message, rd, want string
	}{
		"the other resource holds it":  {addressReuseRefusal, "pvc-new", "pvc-old"},
		"reported from the other side": {addressReuseRefusal, "pvc-old", "pvc-new"},
		"any other failure":            {"drbdadm adjust: exit 10", "pvc-new", ""},
		"a resource conflicting with itself": {
			"in resource pvc-new\n    ipv4:10.0.0.3:20055 is also used /etc/drbd.d/pvc-new.res:4 (resource pvc-new)", "pvc-new", "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := heldAddressOwner(tc.message, tc.rd); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAPortHandoverIsAWaitNotAFailedApply: a new replica's port that the
// .res of a replica still being torn down on the node holds is refused by
// drbdadm until that teardown removes it; the next pass goes through.
// Reported as ApplyFailed Warnings, the handover read as a fault. While
// the previous owner is a live replica here, the refusal is a real
// conflict and stays one.
func TestAPortHandoverIsAWaitNotAFailedApply(t *testing.T) {
	t.Parallel()

	deleting := metav1.NewTime(time.Unix(1, 0))

	for name, tc := range map[string]struct {
		owner     *blockstoriov1alpha1.Resource
		wantEvent bool
	}{
		"previous owner gone":    {nil, false},
		"previous owner deleted": {ownerReplica(&deleting), false},
		"previous owner live":    {ownerReplica(nil), true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := makeToggledResource("pvc-new", "node-c", "pool-a")
			objects := []client.Object{makeStorageOnlyRD("pvc-new"), res}

			if tc.owner != nil {
				objects = append(objects, tc.owner)
			}

			recorder := events.NewFakeRecorder(8)

			reconciler := &ResourceReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(newToggleDiskTestScheme(t)).
					WithObjects(objects...).
					WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
					Build(),
				Config: Config{
					NodeName: "node-c",
					Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
						Providers: map[string]storage.Provider{"pool-a": &scriptedCreateProvider{script: []string{addressReuseRefusal}}},
						NodeName:  "node-c",
					}),
					Exec:     storage.NewFakeExec(),
					Recorder: recorder,
				},
			}

			result, err := reconciler.runApply(t.Context(), res.DeepCopy(), logr.Discard())
			if err != nil {
				t.Fatalf("runApply: %v", err)
			}

			var event string

			select {
			case event = <-recorder.Events:
			default:
			}

			if (event != "") != tc.wantEvent {
				t.Errorf("event %q, want one: %v", event, tc.wantEvent)
			}

			if !tc.wantEvent && result.RequeueAfter != applyFailureRequeue {
				t.Errorf("retried after %v, want the wait's %v", result.RequeueAfter, applyFailureRequeue)
			}
		})
	}
}

// ownerReplica is pvc-old's replica on node-c, deleted at deleted.
func ownerReplica(deleted *metav1.Time) *blockstoriov1alpha1.Resource {
	owner := makeToggledResource("pvc-old", "node-c", "pool-a")
	owner.DeletionTimestamp = deleted

	return owner
}
