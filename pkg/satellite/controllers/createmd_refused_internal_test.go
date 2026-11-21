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
	if failed, _ := r.recordPerResourceFailures(res, []*intent.ResourceApplyResult{{Name: "pvc-1", Message: message}}, logr.Discard()); !failed {
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
