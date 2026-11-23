// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// refusedMessage is the apply message of a create-md drbdmeta refused, in
// the shape the satellite reports it.
const refusedMessage = "fsm dispatch createMd: create-md pvc-1/0: drbdadm create-md: " +
	"Found ext4 filesystem ... Device size would be truncated ... Operation refused. " +
	"(exit status 40): "

// drbdmeta refuses to write metadata over the data on a lower disk the
// same way on every attempt. Before, the satellite logged an Info line and
// retried every 5 seconds forever; the refusal is now a Warning Event on
// the Resource, carrying drbdmeta's reason.
func TestARefusedCreateMDIsAWarningEventOnTheResource(t *testing.T) {
	t.Parallel()

	res := &blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.node-a"}}
	recorder := events.NewFakeRecorder(4)
	r := &ResourceReconciler{Config: Config{Recorder: recorder}}

	message := refusedMessage + drbd.ErrCreateMDRefused.Error()
	if !r.recordPerResourceFailures(res, []*intent.ResourceApplyResult{{Name: "pvc-1", Message: message}}, logr.Discard()) {
		t.Fatal("a failed result was not reported as a failure")
	}

	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "Warning "+createMDRefusedReason) || !strings.Contains(ev, "Device size would be truncated") {
			t.Errorf("Event = %q, want a Warning %s with drbdmeta's reason", ev, createMDRefusedReason)
		}
	default:
		t.Error("no Event for a refused create-md")
	}
}

// An apply whose only failure is a refused create-md is not retried on the
// 5 second failure timer: nothing about the lower disk changes between two
// attempts. Any other failure in the same apply keeps the short retry.
func TestARefusedCreateMDIsNotRetriedOnTheFailureTimer(t *testing.T) {
	t.Parallel()

	refused := &intent.ResourceApplyResult{Name: "pvc-1", Message: refusedMessage + drbd.ErrCreateMDRefused.Error()}
	other := &intent.ResourceApplyResult{Name: "pvc-2", Message: "drbdadm adjust pvc-2: exit status 10"}
	ok := &intent.ResourceApplyResult{Name: "pvc-3", Ok: true}

	for name, tc := range map[string]struct {
		results []*intent.ResourceApplyResult
		want    time.Duration
	}{
		"refused only":      {results: []*intent.ResourceApplyResult{refused, ok}, want: refusedApplyRequeue},
		"refused and other": {results: []*intent.ResourceApplyResult{refused, other}, want: applyFailureRequeue},
		"other only":        {results: []*intent.ResourceApplyResult{other}, want: applyFailureRequeue},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := failureRequeue(tc.results); got != tc.want {
				t.Errorf("failureRequeue = %v, want %v", got, tc.want)
			}
		})
	}
}

// uncleanUnrecordedMessage is the apply message of a volume the satellite
// would not re-create metadata on, in the shape it reports it.
const uncleanUnrecordedMessage = "fsm dispatch createMd: dump-md pvc-1/0: its superblock was written " +
	"for node id 3, and this replica is node id 0; it may hold the only up-to-date copy of the data: " +
	"run `drbdadm apply-al pvc-1/0` to keep its metadata, or `drbdadm wipe-md pvc-1/0` to have it " +
	"re-created and resynced; the satellite retries every 5 minutes, or at once on any change to the " +
	"Resource or its ResourceDefinition: "

// Metadata under an unapplied activity log on a volume with no recorded GI
// is no longer re-created; the replica stays down until an operator acts.
// That has to reach the operator as a Warning Event carrying the commands
// to act with, and stay off the 5 second failure timer, which only repeats
// the same refusal.
func TestAnUncleanVolumeWithNoRecordedGIIsAWarningEventOnTheResource(t *testing.T) {
	t.Parallel()

	res := &blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.node-a"}}
	recorder := events.NewFakeRecorder(4)
	r := &ResourceReconciler{Config: Config{Recorder: recorder}}

	result := &intent.ResourceApplyResult{
		Name: "pvc-1", Message: uncleanUnrecordedMessage + drbd.ErrUncleanMetadataUnrecorded.Error(),
	}
	r.recordPerResourceFailures(res, []*intent.ResourceApplyResult{result}, logr.Discard())

	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "Warning "+uncleanMetadataRefusedReason) || !strings.Contains(ev, "drbdadm apply-al pvc-1/0") {
			t.Errorf("Event = %q, want a Warning %s naming the command that keeps the metadata", ev, uncleanMetadataRefusedReason)
		}
	default:
		t.Error("no Event for refused metadata under an unapplied activity log")
	}

	if got := failureRequeue([]*intent.ResourceApplyResult{result}); got != refusedApplyRequeue {
		t.Errorf("failureRequeue = %v, want %v", got, refusedApplyRequeue)
	}
}
