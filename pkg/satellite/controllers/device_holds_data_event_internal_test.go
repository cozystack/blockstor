// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"strings"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/luks"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// A LUKS format refused over a device that holds data leaves the volume down
// for good, so the refusal reaches the Resource as a Warning Event and not
// only the satellite log. Any other failure stays a log line.
func TestAFormatRefusedOverDataIsAWarningEventOnTheResource(t *testing.T) {
	t.Parallel()

	res := &blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.node-a"}}

	for _, tc := range []struct {
		name, message string
		event         bool
	}{
		{name: "holds-data", message: "luks format /dev/vg/pvc-1_00000: carries TYPE=ext4; refusing luksFormat: " + luks.ErrDeviceHoldsData.Error(), event: true},
		{name: "other", message: "drbdadm adjust pvc-1: exit status 10", event: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := events.NewFakeRecorder(4)
			r := &ResourceReconciler{Config: Config{Recorder: recorder}}

			failed := r.recordPerResourceFailures(res, []*intent.ResourceApplyResult{{Name: "pvc-1", Ok: false, Message: tc.message}}, logr.Discard())
			if !failed {
				t.Fatal("a failed result was not reported as a failure")
			}

			select {
			case ev := <-recorder.Events:
				if !tc.event {
					t.Errorf("unexpected Event %q", ev)
				} else if !strings.Contains(ev, "Warning "+deviceHoldsDataReason) {
					t.Errorf("Event = %q, want a Warning %s", ev, deviceHoldsDataReason)
				}
			default:
				if tc.event {
					t.Error("no Event for a format refused over data")
				}
			}
		})
	}
}
