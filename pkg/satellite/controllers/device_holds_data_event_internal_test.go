// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/luks"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// A failed apply leaves the replica down or StandAlone, so it reaches the
// Resource as a Warning Event and not only the satellite log: a LUKS format
// refused over a device that holds data under its own reason, any other
// failure, a drbdadm adjust refused over a port in use say, as ApplyFailed.
func TestAFormatRefusedOverDataIsAWarningEventOnTheResource(t *testing.T) {
	t.Parallel()

	res := &blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.node-a"}}

	for _, tc := range []struct {
		name, message, reason string
	}{
		{name: "holds-data", message: "luks format /dev/vg/pvc-1_00000: carries TYPE=ext4; refusing luksFormat: " + luks.ErrDeviceHoldsData.Error(), reason: deviceHoldsDataReason},
		{name: "other", message: "adjust pvc-1: drbdadm adjust pvc-1: Failure: (103) Remote address(port) already in use", reason: applyFailedReason},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := events.NewFakeRecorder(4)
			r := &ResourceReconciler{Config: Config{Recorder: recorder}}

			failed, _ := r.recordPerResourceFailures(res, []*intent.ResourceApplyResult{{Name: "pvc-1", Ok: false, Message: tc.message}}, logr.Discard())
			if !failed {
				t.Fatal("a failed result was not reported as a failure")
			}

			select {
			case ev := <-recorder.Events:
				if !strings.Contains(ev, "Warning "+tc.reason) || !strings.Contains(ev, tc.message) {
					t.Errorf("Event = %q, want a Warning %s carrying the failure", ev, tc.reason)
				}

				select {
				case extra := <-recorder.Events:
					t.Errorf("a second Event %q for one failure", extra)
				default:
				}
			default:
				t.Errorf("no Event for a failed apply (%s)", tc.name)
			}
		})
	}
}

// A drbdadm failure carries the whole command line and stderr, past the
// 1 KiB the apiserver accepts in an events.k8s.io/v1 note with a few
// peers. The note is cut to fit, on a rune boundary, rather than the
// Event being rejected and the failure going unreported.
func TestAFailedApplyEventNoteFitsTheAPILimit(t *testing.T) {
	t.Parallel()

	message := "adjust pvc-1: drbdadm adjust pvc-1: Failure: (103) Remote address(port) already in use. " +
		strings.Repeat("Command 'drbdsetup new-path pvc-1 0 ipv4:10.0.0.1:7001 ipv4:10.0.0.3:7003' ü ", 40)

	for _, prefix := range []string{"", luks.ErrDeviceHoldsData.Error() + " "} {
		_, note := applyFailureEvent(prefix + message)

		if len(note) > eventNoteLimit {
			t.Errorf("note is %d bytes, the apiserver takes at most %d", len(note), eventNoteLimit)
		}

		if !utf8.ValidString(note) {
			t.Errorf("note cut inside a rune: %q", note[len(note)-8:])
		}

		if !strings.Contains(note, "(103) Remote address(port) already in use") {
			t.Errorf("the cut note lost the failure: %q", note)
		}
	}

	// Two-byte runes from offset 0 put the cut inside one.
	if note := eventNote(strings.Repeat("ü", eventNoteLimit)); len(note) > eventNoteLimit || !utf8.ValidString(note) {
		t.Errorf("a note of two-byte runes was cut to %d bytes, valid UTF-8 %v", len(note), utf8.ValidString(note))
	}

	if _, note := applyFailureEvent("short"); strings.HasSuffix(note, eventNoteEllipsis) {
		t.Errorf("a note within the limit was cut: %q", note)
	}
}
