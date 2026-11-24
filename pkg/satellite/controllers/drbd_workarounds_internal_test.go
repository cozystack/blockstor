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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

const workaroundNode = "n3"

// fakeWorkaroundAdm hands out one kernel status per pass and records the
// actions taken. A peer it disconnected shows StandAlone in every status
// it hands out until it connects that peer again, as the kernel's does.
type fakeWorkaroundAdm struct {
	status  string
	reads   int
	actions []string

	// next, when set, are the statuses of the next reads, each becoming
	// status as it is read.
	next []string

	// errs fails the next calls of an action ("up pvc-x"), in order.
	errs map[string][]error

	// away holds the links ("pvc-x:n2") this fake disconnected.
	away map[string]bool
}

func (f *fakeWorkaroundAdm) KernelStatus(context.Context) ([]drbd.KernelResource, error) {
	f.reads++

	if len(f.next) > 0 {
		f.status, f.next = f.next[0], f.next[1:]
	}

	var out []drbd.KernelResource

	err := json.Unmarshal([]byte(f.status), &out)
	if err != nil {
		return nil, fmt.Errorf("fake kernel status: %w", err)
	}

	for i := range out {
		for j := range out[i].Connections {
			conn := &out[i].Connections[j]
			if f.away[out[i].Name+":"+conn.Name] {
				conn.ConnectionState, conn.PeerRole = "StandAlone", "Unknown"
			}
		}
	}

	return out, nil
}

func (f *fakeWorkaroundAdm) DisconnectPeer(_ context.Context, res, peer string) error {
	err := f.act("disconnect " + res + ":" + peer)
	if err == nil {
		if f.away == nil {
			f.away = map[string]bool{}
		}

		f.away[res+":"+peer] = true
	}

	return err
}

func (f *fakeWorkaroundAdm) ConnectPeer(_ context.Context, res, peer string) error {
	err := f.act("connect " + res + ":" + peer)
	if err == nil {
		delete(f.away, res+":"+peer)
	}

	return err
}

func (f *fakeWorkaroundAdm) Secondary(_ context.Context, res string) error {
	return f.act("secondary " + res)
}

func (f *fakeWorkaroundAdm) Down(_ context.Context, res string) error {
	return f.act("down " + res)
}

func (f *fakeWorkaroundAdm) Up(_ context.Context, res string) error {
	return f.act("up " + res)
}

func (f *fakeWorkaroundAdm) Adjust(_ context.Context, res string) error {
	return f.act("adjust " + res)
}

// act records action and returns the next error queued for it.
func (f *fakeWorkaroundAdm) act(action string) error {
	f.actions = append(f.actions, action)

	queued := f.errs[action]
	if len(queued) == 0 {
		return nil
	}

	f.errs[action] = queued[1:]

	return queued[0]
}

// workaroundReader has a blockstor replica on this node of each of rds.
func workaroundReader(t *testing.T, rds ...string) client.Reader {
	t.Helper()

	builder := fake.NewClientBuilder().WithScheme(scheme)

	for _, rd := range rds {
		builder = builder.WithObjects(&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName(rd, workaroundNode)},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: workaroundNode},
		})
	}

	return builder.Build()
}

func newTestWorkarounds(t *testing.T, adm *fakeWorkaroundAdm, cfg WorkaroundsConfig, rds ...string) (*DRBDWorkaroundsRunnable, *events.FakeRecorder) {
	t.Helper()

	recorder := events.NewFakeRecorder(16)

	// Every look at the clock is the next pass, 30 s on.
	now := time.Date(2026, 11, 22, 22, 0, 0, 0, time.UTC)

	return &DRBDWorkaroundsRunnable{
		Reader:   workaroundReader(t, rds...),
		Adm:      adm,
		Recorder: recorder,
		NodeName: workaroundNode,
		Config:   cfg,
		Clock: func() time.Time {
			now = now.Add(30 * time.Second)

			return now
		},
	}, recorder
}

// resyncSample is what the stalled replica reports of its resync from
// the active peer n1 on one pass: out-of-sync and received in KiB, and
// the requests of that link not completed yet.
type resyncSample struct {
	oos, received, outstanding int64
}

// resyncStatus is a replica, Secondary, of a 50 GiB volume resyncing
// from n1 while its resync from n2 waits on it
// (resync-suspended:dependency), the shape of the field stalls; n2Role
// is n2's role. percent-in-sync follows out-of-sync, as the kernel's
// does.
func resyncStatus(s resyncSample, n2Role string) string {
	return stallShape{n1Role: "Secondary", n2Role: n2Role, n2Disk: "UpToDate"}.status(s)
}

// stallShape is the resyncStatus shape with the roles of both peers and
// the paused peer's disk state set.
type stallShape struct {
	n1Role, n2Role, n2Disk string
}

func (shape stallShape) status(s resyncSample) string {
	const sizeKiB = 50 << 20

	percent := 100 * float64(sizeKiB-s.oos) / sizeKiB

	return fmt.Sprintf(`[{"name":"pvc-stall","role":"Secondary",
 "devices":[{"volume":0,"disk-state":"Inconsistent","open":false}],
 "connections":[
  {"name":"n1","connection-state":"Connected","peer-role":%q,"peer_devices":[{"volume":0,
   "replication-state":"SyncTarget","peer-disk-state":"UpToDate","resync-suspended":"no",
   "received":%d,"out-of-sync":%d,"pending":0,"unacked":%d,"percent-in-sync":%.4f}]},
  {"name":"n2","connection-state":"Connected","peer-role":%q,"peer_devices":[{"volume":0,
   "replication-state":"PausedSyncT","peer-disk-state":%q,"resync-suspended":"dependency",
   "received":11840,"out-of-sync":2122972,"pending":0,"unacked":70,"percent-in-sync":0.58}]}]}]`,
		shape.n1Role, s.received, s.oos, s.outstanding, percent, shape.n2Role, shape.n2Disk)
}

// stalledStatus is the stall frozen: the same counts on every pass.
func stalledStatus(string) string {
	return resyncStatus(resyncSample{oos: 14592, received: 113448, outstanding: 542}, "Secondary")
}

// stallConfig is the stalled-resync check with its action opted in.
func stallConfig() WorkaroundsConfig {
	return WorkaroundsConfig{Interval: 1, StalledResync: true, StalledResyncPasses: 4, StalledResyncAct: true}
}

// afterStall is the replica once the active resync from n1 is done,
// UpToDate, with n2 Connected to a Secondary unless the fake dropped it;
// n1 is the state and role of the link to n1.
func afterStall(n1 string) string {
	return fmt.Sprintf(`[{"name":"pvc-stall","role":"Secondary","devices":[{"volume":0,"disk-state":"UpToDate"}],
 "connections":[{"name":"n1",%s,"peer_devices":[{"volume":0,
  "replication-state":"Established","peer-disk-state":"UpToDate","resync-suspended":"no","out-of-sync":0}]},
  {"name":"n2","connection-state":"Connected","peer-role":"Secondary","peer_devices":[{"volume":0,
  "replication-state":"Established","peer-disk-state":"UpToDate","resync-suspended":"no","out-of-sync":0}]}]}]`, n1)
}

// n1Secondary is the link to n1 Connected to a Secondary.
const n1Secondary = `"connection-state":"Connected","peer-role":"Secondary"`

func drainEvents(recorder *events.FakeRecorder) []string {
	var out []string

	for {
		select {
		case ev := <-recorder.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

// runResync runs one pass per sample and returns the actions taken.
func runResync(t *testing.T, w *DRBDWorkaroundsRunnable, adm *fakeWorkaroundAdm, n2Role string, samples ...resyncSample) []string {
	t.Helper()

	for _, sample := range samples {
		adm.status = resyncStatus(sample, n2Role)
		w.pass(t.Context(), logr.Discard())
	}

	return adm.actions
}

// ramp is n samples, the one of each pass from sample.
func ramp(n int64, sample func(pass int64) resyncSample) []resyncSample {
	samples := make([]resyncSample, 0, n)

	for pass := range n {
		samples = append(samples, sample(pass))
	}

	return samples
}

// frozen is n samples of the alpha.15 freeze: nothing moves.
func frozen(n int) []resyncSample {
	return slices.Repeat([]resyncSample{{oos: 14592, received: 113448, outstanding: 542}}, n)
}

// The field stalls (alpha.9, alpha.14, alpha.15): the resync from n1 is
// frozen with its requests held while n2 waits on it. After four stalled
// passes, compared with the one before each, the paused peer n2 is
// disconnected, not the active n1; the next pass connects it again.
func TestStalledResyncDisconnectsThePausedPeer(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	if got := runResync(t, w, adm, "Secondary", frozen(4)...); len(got) != 0 {
		t.Fatalf("acted before four stalled passes: %v", got)
	}

	got := runResync(t, w, adm, "Secondary", frozen(1)...)
	if want := []string{"disconnect pvc-stall:n2"}; !slices.Equal(got, want) {
		t.Fatalf("actions = %v, want %v", got, want)
	}

	evs := drainEvents(recorder)
	if len(evs) != 1 || !strings.Contains(evs[0], "Warning DRBDResyncStalled") || !strings.Contains(evs[0], "disconnecting n2") {
		t.Errorf("events = %v, want one Warning DRBDResyncStalled naming n2", evs)
	}

	adm.actions = nil

	// The active resync finishes while n2 is away; the next pass
	// connects n2 again.
	adm.status = afterStall(n1Secondary)
	w.pass(t.Context(), logr.Discard())

	if want := []string{"connect pvc-stall:n2"}; !slices.Equal(adm.actions, want) {
		t.Errorf("pass after the disconnect: actions = %v, want %v", adm.actions, want)
	}

	adm.actions = nil
	w.pass(t.Context(), logr.Discard())

	if len(adm.actions) != 0 {
		t.Errorf("connected again after it went through: %v", adm.actions)
	}
}

var errAdjustBusy = errors.New("exit status 10: pvc-stall: Failure: (162) Invalid configuration request")

// The connect of the paused peer is retried until it goes through.
func TestStalledResyncRetriesTheReconnectOfThePausedPeer(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{errs: map[string][]error{"connect pvc-stall:n2": {errAdjustBusy}}}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	runResync(t, w, adm, "Secondary", frozen(5)...)

	adm.status = afterStall(n1Secondary)
	adm.actions = nil

	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())

	if want := []string{"connect pvc-stall:n2", "connect pvc-stall:n2"}; !slices.Equal(adm.actions, want) {
		t.Errorf("actions = %v, want a failed connect retried once", adm.actions)
	}
}

// By default the check only reports: one Warning Event, and the same
// line in the log, with the commands that would do what it used to do,
// for the node it runs on.
func TestStalledResyncReportsOnlyByDefault(t *testing.T) {
	t.Parallel()

	cfg := stallConfig()
	cfg.StalledResyncAct = false

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, cfg, "pvc-stall")

	if got := runResync(t, w, adm, "Secondary", frozen(5)...); len(got) != 0 {
		t.Errorf("acted with --drbd-workaround-stalled-resync-act off: %v", got)
	}

	evs := drainEvents(recorder)
	if len(evs) != 1 || !strings.Contains(evs[0], "Warning DRBDResyncStalled") ||
		!strings.Contains(evs[0], "report-only") ||
		!strings.Contains(evs[0], "by hand on n3: `drbdadm disconnect pvc-stall:n2`, "+
			"then once the resync from n1 is done `drbdadm connect pvc-stall:n2`") {
		t.Errorf("events = %v, want one report-only Warning with the manual commands", evs)
	}

	if DefaultWorkaroundsConfig().StalledResyncAct {
		t.Error("the default config acts on a stalled resync")
	}
}

// alpha.17 reboot test: the paused peer A was a Secondary and the active
// one the Primary; after the disconnect of A and the reconnect, the
// returning node became A's SyncSource while the Primary wrote, and A
// ended up UpToDate with different data. With a Primary at either end,
// here, or a peer that is not connected and could be one, the check
// reports and does nothing, even opted in; the report names the hazard.
func TestStalledResyncNeverActsWithAPrimaryInSight(t *testing.T) {
	t.Parallel()

	down := `,{"name":"n4","connection-state":"Connecting","peer-role":"Unknown","peer_devices":[{"volume":0,
   "replication-state":"Off","peer-disk-state":"DUnknown","resync-suspended":"no"}]}]}]`
	stall := frozen(1)[0]

	for name, status := range map[string]string{
		"active Primary":     stallShape{n1Role: "Primary", n2Role: "Secondary", n2Disk: "UpToDate"}.status(stall),
		"paused Primary":     resyncStatus(stall, "Primary"),
		"local Primary":      strings.Replace(resyncStatus(stall, "Secondary"), `"role":"Secondary"`, `"role":"Primary"`, 1),
		"peer not connected": strings.TrimSuffix(resyncStatus(stall, "Secondary"), "]}]") + down,
	} {
		adm := &fakeWorkaroundAdm{status: status}
		w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

		for range 9 {
			w.pass(t.Context(), logr.Discard())
		}

		if len(adm.actions) != 0 {
			t.Errorf("%s: actions = %v, want none", name, adm.actions)
		}

		evs := drainEvents(recorder)
		if len(evs) != 2 || !strings.Contains(evs[0], "not acting, a Primary is in sight") ||
			!strings.Contains(evs[0], "LINBIT/drbd#153") {
			t.Errorf("%s: events = %v, want the stall reported on each count with the hazard", name, evs)
		}
	}
}

// alpha.17, issue 02: an initial sync from the importing Primary, the
// paused peer as empty as this node. The active peer holds the only
// UpToDate copy, and the check leaves it alone even with no Primary in
// sight.
func TestStalledResyncLeavesAnInitialSyncAlone(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	shape := stallShape{n1Role: "Secondary", n2Role: "Secondary", n2Disk: "Inconsistent"}

	for _, sample := range frozen(9) {
		adm.status = shape.status(sample)
		w.pass(t.Context(), logr.Discard())
	}

	if len(adm.actions) != 0 {
		t.Errorf("acted on an initial sync: %v", adm.actions)
	}

	evs := drainEvents(recorder)
	if len(evs) != 2 || !strings.Contains(evs[0], "n1 holds the only UpToDate copy in sight, as in an initial sync") {
		t.Errorf("events = %v, want the initial sync reported and left alone", evs)
	}
}

// A paused peer the check disconnected is connected again only while no
// Primary is in sight: a Primary that came up meanwhile is reported once
// and waited out, and the reconnect follows once it is gone.
func TestStalledResyncReconnectsThePausedPeerOnlyWithNoPrimaryInSight(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	if got := runResync(t, w, adm, "Secondary", frozen(5)...); !slices.Equal(got, []string{"disconnect pvc-stall:n2"}) {
		t.Fatalf("actions = %v, want n2 disconnected", got)
	}

	drainEvents(recorder)

	adm.actions = nil
	adm.status = `[{"name":"pvc-stall","role":"Secondary","devices":[{"volume":0,"disk-state":"UpToDate"}],
 "connections":[{"name":"n1","connection-state":"Connected","peer-role":"Primary","peer_devices":[{"volume":0,
  "replication-state":"Established","peer-disk-state":"UpToDate","resync-suspended":"no","out-of-sync":0}]},
  {"name":"n2","connection-state":"StandAlone","peer-role":"Unknown","peer_devices":[{"volume":0,
  "replication-state":"Off","peer-disk-state":"DUnknown","resync-suspended":"no"}]}]}]`

	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())

	if len(adm.actions) != 0 {
		t.Errorf("connected n2 again with n1 Primary: %v", adm.actions)
	}

	evs := drainEvents(recorder)
	if len(evs) != 1 || !strings.Contains(evs[0], "while a Primary is in sight") ||
		!strings.Contains(evs[0], "`drbdadm connect pvc-stall:n2`") {
		t.Errorf("events = %v, want the wait reported once with the manual command", evs)
	}

	adm.status = strings.Replace(adm.status, `"peer-role":"Primary"`, `"peer-role":"Secondary"`, 1)
	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())

	if want := []string{"connect pvc-stall:n2"}; !slices.Equal(adm.actions, want) {
		t.Errorf("actions once the Primary is gone = %v, want %v", adm.actions, want)
	}
}

// A link to a third node that is down or still connecting may lead to a
// Primary, so the paused peer is not connected again until that link is
// back and shows a Secondary.
func TestStalledResyncReconnectWaitsForAPrimaryToBeRuledOut(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	if got := runResync(t, w, adm, "Secondary", frozen(5)...); !slices.Equal(got, []string{"disconnect pvc-stall:n2"}) {
		t.Fatalf("actions = %v, want n2 disconnected", got)
	}

	drainEvents(recorder)

	adm.actions = nil
	adm.status = `[{"name":"pvc-stall","role":"Secondary","devices":[{"volume":0,"disk-state":"UpToDate"}],
 "connections":[{"name":"n1","connection-state":"Connecting","peer-role":"Unknown","peer_devices":[{"volume":0,
  "replication-state":"Off","peer-disk-state":"DUnknown","resync-suspended":"no"}]},
  {"name":"n2","connection-state":"StandAlone","peer-role":"Unknown","peer_devices":[{"volume":0,
  "replication-state":"Off","peer-disk-state":"DUnknown","resync-suspended":"no"}]}]}]`

	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())

	if len(adm.actions) != 0 {
		t.Errorf("connected n2 again while n1 was connecting: %v", adm.actions)
	}

	if evs := drainEvents(recorder); len(evs) != 1 || !strings.Contains(evs[0], "not ruled out") {
		t.Errorf("events = %v, want the wait reported once", evs)
	}

	adm.status = strings.Replace(adm.status,
		`"connection-state":"Connecting","peer-role":"Unknown"`, `"connection-state":"Connected","peer-role":"Secondary"`, 1)
	w.pass(t.Context(), logr.Discard())

	if want := []string{"connect pvc-stall:n2"}; !slices.Equal(adm.actions, want) {
		t.Errorf("actions once n1 is a connected Secondary = %v, want %v", adm.actions, want)
	}
}

// stallDisconnected runs w until the stall made it disconnect n2, and
// drops what that pass recorded.
func stallDisconnected(t *testing.T, w *DRBDWorkaroundsRunnable, adm *fakeWorkaroundAdm, recorder *events.FakeRecorder) {
	t.Helper()

	if got := runResync(t, w, adm, "Secondary", frozen(5)...); !slices.Equal(got, []string{"disconnect pvc-stall:n2"}) {
		t.Fatalf("actions = %v, want n2 disconnected", got)
	}

	drainEvents(recorder)

	adm.actions = nil
}

// Only the link the check dropped is skipped when a Primary is ruled out
// for its reconnect: a second StandAlone link, dropped by a split brain
// or by hand, may lead to a Primary as well, and `drbdadm adjust` would
// have connected both at once. Nothing is connected while it is down.
func TestStalledResyncReconnectWaitsOutAnotherStandAloneLink(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	stallDisconnected(t, w, adm, recorder)

	adm.status = afterStall(`"connection-state":"StandAlone","peer-role":"Unknown"`)

	for range 3 {
		w.pass(t.Context(), logr.Discard())
	}

	if len(adm.actions) != 0 {
		t.Errorf("connected n2 again with the link to n1 StandAlone: %v", adm.actions)
	}

	if evs := drainEvents(recorder); len(evs) != 1 || !strings.Contains(evs[0], "`drbdadm connect pvc-stall:n2`") {
		t.Errorf("events = %v, want the wait reported once with the manual command", evs)
	}
}

// A resource that is down when its reconnect is owed, as after a reboot,
// is left to the apply, whose adjust brings it up connected to every
// peer: this check runs nothing for it, and keeps the debt for a peer
// that is StandAlone once the resource is up.
func TestStalledResyncReconnectLeavesADownResourceToTheApply(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")
	w.StateDir = dir

	stallDisconnected(t, w, adm, recorder)

	adm.status = `[]`

	for range 3 {
		w.pass(t.Context(), logr.Discard())
	}

	if len(adm.actions) != 0 {
		t.Errorf("acted on a resource that is down: %v", adm.actions)
	}

	if _, err := os.Stat(filepath.Join(dir, "pvc-stall"+satellite.ReconnectPendingSuffix)); err != nil {
		t.Errorf("the debt of a resource that is down was dropped: %v", err)
	}

	adm.status = afterStall(n1Secondary)
	w.pass(t.Context(), logr.Discard())

	if want := []string{"connect pvc-stall:n2"}; !slices.Equal(adm.actions, want) {
		t.Errorf("actions once the resource is up with n2 StandAlone = %v, want %v", adm.actions, want)
	}
}

// The status is read again right before the connect: a Primary that
// turned up since the pass read it holds the connect off as well.
func TestStalledResyncReconnectRereadsTheStatusBeforeConnecting(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	stallDisconnected(t, w, adm, recorder)

	primary := afterStall(`"connection-state":"Connected","peer-role":"Primary"`)
	adm.next = []string{afterStall(n1Secondary), primary}

	w.pass(t.Context(), logr.Discard())

	if len(adm.actions) != 0 {
		t.Errorf("connected n2 again with n1 Primary on the second read: %v", adm.actions)
	}

	if evs := drainEvents(recorder); len(evs) != 1 || !strings.Contains(evs[0], "not ruled out") {
		t.Errorf("events = %v, want the wait reported", evs)
	}
}

// A reconnect kept waiting is reported again every reconnectReportEvery,
// not once for good: the link stays down all that while, and an Event
// expires.
func TestStalledResyncReconnectWaitIsReportedAgain(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	stallDisconnected(t, w, adm, recorder)

	adm.status = afterStall(`"connection-state":"Connected","peer-role":"Primary"`)

	// Every pass looks at the test clock once, 30 s on.
	passes := int(reconnectReportEvery/(30*time.Second)) + 1
	for range passes {
		w.pass(t.Context(), logr.Discard())
	}

	if evs := drainEvents(recorder); len(evs) != 2 {
		t.Errorf("events over %d passes = %v, want the wait reported twice", passes, evs)
	}

	if len(adm.actions) != 0 {
		t.Errorf("connected n2 again with n1 Primary: %v", adm.actions)
	}
}

// A record an earlier satellite wrote without naming the peer is owed to
// the one StandAlone peer, under the same gate; with two StandAlone
// peers neither is connected.
func TestStalledResyncReconnectReadsARecordNamingNoPeer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		n1   string
		want []string
	}{
		{n1: n1Secondary, want: []string{"connect pvc-stall:n2"}},
		{n1: `"connection-state":"StandAlone","peer-role":"Unknown"`, want: nil},
	} {
		dir := t.TempDir()

		err := os.WriteFile(filepath.Join(dir, "pvc-stall"+satellite.ReconnectPendingSuffix), nil, 0o600)
		if err != nil {
			t.Fatal(err)
		}

		adm := &fakeWorkaroundAdm{status: afterStall(tc.n1), away: map[string]bool{"pvc-stall:n2": true}}
		w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")
		w.StateDir = dir

		w.pass(t.Context(), logr.Discard())
		w.pass(t.Context(), logr.Discard())

		if !slices.Equal(adm.actions, tc.want) {
			t.Errorf("n1 %s: actions = %v, want %v", tc.n1, adm.actions, tc.want)
		}
	}
}

// alpha.15 before the freeze: the active resync ran at 100-150 KiB/s for
// 14 minutes, percent still moving. Too slow is stalled too.
func TestStalledResyncCatchesATrickle(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	// 130 KiB/s over 30 s passes; outstanding moves both ways.
	samples := ramp(5, func(pass int64) resyncSample {
		return resyncSample{oos: 120000 - pass*3900, received: 100000 + pass*3900, outstanding: 200 + (pass%2)*300}
	})

	got := runResync(t, w, adm, "Secondary", samples...)
	if want := []string{"disconnect pvc-stall:n2"}; !slices.Equal(got, want) {
		t.Errorf("actions = %v, want %v", got, want)
	}
}

// alpha.17, issue 02: a writer outran the resync, so out-of-sync grew
// for minutes while the resync was healthy, resync data coming in all
// along. A pass on which it grew that way is no stall, and it starts the
// count over: three stalled passes, a growing one, three more stalled
// ones are no four in a row.
func TestStalledResyncLeavesAGrowingOutOfSyncAlone(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	growing := ramp(9, func(pass int64) resyncSample {
		return resyncSample{oos: 59_370_316 + pass*400_000, received: pass * 10_000, outstanding: 1328}
	})

	if got := runResync(t, w, adm, "Secondary", growing...); len(got) != 0 {
		t.Errorf("acted while out-of-sync grew: %v", got)
	}

	adm.actions = nil

	samples := frozen(4)
	samples = append(samples, resyncSample{oos: 15592, received: 513448, outstanding: 542})
	samples = append(samples, slices.Repeat([]resyncSample{{oos: 15592, received: 513448, outstanding: 542}}, 3)...)

	if got := runResync(t, w, adm, "Secondary", samples...); len(got) != 0 {
		t.Errorf("acted although out-of-sync grew in between: %v", got)
	}

	if evs := drainEvents(recorder); len(evs) != 0 {
		t.Errorf("reported a growing out-of-sync as a stall: %v", evs)
	}
}

// alpha.19, issue 14: the resync from the Primary n1 stopped with its
// replies held, received and the outstanding count fixed, while the
// guest kept writing a little: every pass grew out-of-sync. That is a
// stall all the same, and it is reported.
func TestStalledResyncCatchesAStallThePrimaryWritesInto(t *testing.T) {
	t.Parallel()

	cfg := stallConfig()
	cfg.StalledResyncAct = false

	adm := &fakeWorkaroundAdm{}
	w, recorder := newTestWorkarounds(t, adm, cfg, "pvc-stall")

	shape := stallShape{n1Role: "Primary", n2Role: "Secondary", n2Disk: "UpToDate"}

	for _, sample := range ramp(5, func(pass int64) resyncSample {
		return resyncSample{oos: 23340 + pass*44, received: 349988, outstanding: 1436}
	}) {
		adm.status = shape.status(sample)
		w.pass(t.Context(), logr.Discard())
	}

	if len(adm.actions) != 0 {
		t.Errorf("acted in report-only mode: %v", adm.actions)
	}

	evs := drainEvents(recorder)
	if len(evs) != 1 || !strings.Contains(evs[0], "Warning DRBDResyncStalled") ||
		!strings.Contains(evs[0], "resync of volume 0 from n1 stalled for 4 passes") {
		t.Errorf("events = %v, want one Warning reporting the stall", evs)
	}
}

// Writes that clear out-of-sync blocks can make the count fall while the
// resync itself is frozen: its requests stay outstanding, no fewer, and
// hardly any resync data comes in.
func TestStalledResyncCatchesHeldRequests(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	samples := ramp(5, func(pass int64) resyncSample {
		return resyncSample{oos: 900000 - pass*20000, received: 5000 + pass*100, outstanding: 431}
	})

	got := runResync(t, w, adm, "Secondary", samples...)
	if want := []string{"disconnect pvc-stall:n2"}; !slices.Equal(got, want) {
		t.Errorf("actions = %v, want %v", got, want)
	}
}

// A cluster with a lower c-min-rate or a low fixed resync-rate sets a
// lower floor, and a slow but healthy resync there is left alone; at the
// default floor, the same resync counts as stalled.
func TestStalledResyncMinRateFollowsTheConfig(t *testing.T) {
	t.Parallel()

	// 120 KiB/s, a resync-rate of 120k.
	samples := ramp(10, func(pass int64) resyncSample {
		return resyncSample{oos: 100000 - pass*3600, received: pass * 3600, outstanding: 16 + pass%2}
	})

	for _, tc := range []struct {
		minRate int
		acted   bool
	}{
		{minRate: 100, acted: false},
		{minRate: 0, acted: true},
	} {
		cfg := stallConfig()
		cfg.StalledResyncMinRate = tc.minRate

		adm := &fakeWorkaroundAdm{}
		w, _ := newTestWorkarounds(t, adm, cfg, "pvc-stall")

		if got := runResync(t, w, adm, "Secondary", samples...); (len(got) != 0) != tc.acted {
			t.Errorf("min rate %d: actions = %v, want acted %v", tc.minRate, got, tc.acted)
		}
	}
}

// A resync that moves is left alone, however long it takes, even with a
// steady number of requests in flight; one moving pass starts the count
// over.
func TestStalledResyncLeavesAMovingResyncAlone(t *testing.T) {
	t.Parallel()

	t.Run("fast", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{}
		w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

		// ~90 MB/s, the normal rate on the test cluster.
		samples := ramp(10, func(pass int64) resyncSample {
			return resyncSample{oos: 40_000_000 - pass*2_700_000, received: pass * 2_700_000, outstanding: 2048}
		})

		if got := runResync(t, w, adm, "Secondary", samples...); len(got) != 0 {
			t.Errorf("a moving resync was acted on: %v", got)
		}
	})

	t.Run("slow but above the floor", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{}
		w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

		// c-min-rate, 250 KiB/s.
		samples := ramp(10, func(pass int64) resyncSample {
			return resyncSample{oos: 100000 - pass*7500, received: pass * 7500, outstanding: 64}
		})

		if got := runResync(t, w, adm, "Secondary", samples...); len(got) != 0 {
			t.Errorf("a resync at c-min-rate was acted on: %v", got)
		}
	})

	// A thin or csums resync clears ranges without receiving data
	// (P_RS_DEALLOCATED, P_RS_IS_IN_SYNC), so received hardly moves
	// while its pipeline stays full.
	t.Run("thin", func(t *testing.T) {
		t.Parallel()

		for _, outstanding := range []func(pass int64) int64{
			func(int64) int64 { return 2048 },
			func(pass int64) int64 { return 2048 + pass%3 },
		} {
			adm := &fakeWorkaroundAdm{}
			w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

			samples := ramp(10, func(pass int64) resyncSample {
				return resyncSample{oos: 40_000_000 - pass*300_000, received: 5000 + pass*100, outstanding: outstanding(pass)}
			})

			if got := runResync(t, w, adm, "Secondary", samples...); len(got) != 0 {
				t.Errorf("a thin resync clearing 10 MiB/s was acted on: %v", got)
			}
		}
	})

	// A pipeline whose count moves between passes is working, even when
	// it never drops below the last pass.
	t.Run("pipeline count moves", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{}
		w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

		samples := ramp(10, func(pass int64) resyncSample {
			return resyncSample{oos: 900000 - pass*20000, received: 5000 + pass*100, outstanding: 400 + pass}
		})

		if got := runResync(t, w, adm, "Secondary", samples...); len(got) != 0 {
			t.Errorf("a moving pipeline was acted on: %v", got)
		}
	})

	t.Run("moving pass resets", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{}
		w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

		samples := frozen(4)
		samples = append(samples, resyncSample{oos: 4592, received: 123448, outstanding: 10})
		samples = append(samples, slices.Repeat([]resyncSample{{oos: 4592, received: 123448, outstanding: 10}}, 3)...)

		if got := runResync(t, w, adm, "Secondary", samples...); len(got) != 0 {
			t.Errorf("acted although a pass in between moved: %v", got)
		}
	})
}

// The kernel zeroes received when the link to the active peer connects
// again, which makes the pass after it look like no data came in. That
// link is a new one, so the count starts over there.
func TestStalledResyncCountsAfreshWhenReceivedDrops(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	held := ramp(4, func(pass int64) resyncSample {
		return resyncSample{oos: 900000 - pass*10000, received: 1_000_000 + pass*100, outstanding: 300}
	})
	reconnected := resyncSample{oos: 860000, received: 50, outstanding: 300}

	if got := runResync(t, w, adm, "Secondary", append(held, reconnected)...); len(got) != 0 {
		t.Errorf("counted the pass after a reconnect as stalled: %v", got)
	}
}

// When disconnecting the paused peer did not get the resync moving, the
// next action is the plunger's: disconnect and connect the active peer,
// which also cleared a stall on the test cluster (alpha.7).
func TestStalledResyncFallsBackToTheActivePeer(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	runResync(t, w, adm, "Secondary", frozen(5)...)

	// n2 is connected back, and the same stall shows again.
	adm.actions = nil

	got := runResync(t, w, adm, "Secondary", frozen(4)...)
	want := []string{"connect pvc-stall:n2", "disconnect pvc-stall:n1", "connect pvc-stall:n1"}

	if !slices.Equal(got, want) {
		t.Errorf("actions = %v, want %v", got, want)
	}
}

// A disconnect of the paused peer that failed may still have gone
// through, so its connect is owed all the same; here it did not go
// through, the peer is still connected, and the debt settles with no
// command. The next stall then tries the paused peer again before the
// active one.
func TestStalledResyncRetriesAFailedDisconnectOfThePausedPeer(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{errs: map[string][]error{"disconnect pvc-stall:n2": {errAdjustBusy}}}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	runResync(t, w, adm, "Secondary", frozen(5)...)

	adm.actions = nil

	got := runResync(t, w, adm, "Secondary", frozen(4)...)
	if want := []string{"disconnect pvc-stall:n2"}; !slices.Equal(got, want) {
		t.Errorf("actions after a failed disconnect = %v, want %v", got, want)
	}
}

// A reconnect of the active peer whose connect failed after the
// disconnect went through leaves that peer StandAlone, and the apply
// keeps a StandAlone link down: its connect is owed, and runs on the
// next pass with a Primary ruled out on the other links.
func TestStalledResyncOwesTheConnectOfTheActivePeer(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{errs: map[string][]error{"connect pvc-stall:n1": {errAdjustBusy}}}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	runResync(t, w, adm, "Secondary", frozen(5)...)

	got := runResync(t, w, adm, "Secondary", frozen(4)...)
	if want := []string{"disconnect pvc-stall:n2", "connect pvc-stall:n2", "disconnect pvc-stall:n1", "connect pvc-stall:n1"}; !slices.Equal(got, want) {
		t.Fatalf("actions = %v, want the fallback with its connect failing", got)
	}

	adm.status = afterStall(n1Secondary)
	adm.actions = nil

	w.pass(t.Context(), logr.Discard())

	if want := []string{"connect pvc-stall:n1"}; !slices.Equal(adm.actions, want) {
		t.Errorf("pass after a failed connect: actions = %v, want %v", adm.actions, want)
	}
}

// The reconnect owed after a disconnect outlives a satellite restart:
// the next satellite reads it from the state dir, with the peer it names,
// and connects that peer again; the record goes once the connect went
// through.
func TestStalledResyncOwedReconnectOutlivesARestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	record := filepath.Join(dir, "pvc-stall"+satellite.ReconnectPendingSuffix)

	adm := &fakeWorkaroundAdm{}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")
	w.StateDir = dir

	if got := runResync(t, w, adm, "Secondary", frozen(5)...); !slices.Equal(got, []string{"disconnect pvc-stall:n2"}) {
		t.Fatalf("actions = %v, want the paused peer disconnected", got)
	}

	if got, err := os.ReadFile(record); err != nil || string(got) != "n2\n" {
		t.Fatalf("the owed reconnect was not recorded with its peer: %q, %v", got, err)
	}

	restarted := &fakeWorkaroundAdm{status: afterStall(n1Secondary), away: map[string]bool{"pvc-stall:n2": true}}
	w2, _ := newTestWorkarounds(t, restarted, stallConfig(), "pvc-stall")
	w2.StateDir = dir

	w2.pass(t.Context(), logr.Discard())

	if want := []string{"connect pvc-stall:n2"}; !slices.Equal(restarted.actions, want) {
		t.Errorf("first pass after a restart: actions = %v, want %v", restarted.actions, want)
	}

	if _, err := os.Stat(record); !os.IsNotExist(err) {
		t.Errorf("the record of a settled reconnect is still there: %v", err)
	}
}

// lateAddStall is the stall on volume 1 of a resource whose volume 0 is
// UpToDate here, the shape of a volume added later; n2Role is n2's role.
func lateAddStall(n2Role string) string {
	return fmt.Sprintf(`[{"name":"pvc-stall","role":"Secondary",
 "devices":[{"volume":0,"disk-state":"UpToDate"},{"volume":1,"disk-state":"Inconsistent"}],
 "connections":[
  {"name":"n1","connection-state":"Connected","peer-role":"Secondary","peer_devices":[
   {"volume":0,"replication-state":"Established","peer-disk-state":"UpToDate","resync-suspended":"no","out-of-sync":0},
   {"volume":1,"replication-state":"SyncTarget","peer-disk-state":"UpToDate","resync-suspended":"no","received":1,"out-of-sync":14592,"unacked":542}]},
  {"name":"n2","connection-state":"Connected","peer-role":%q,"peer_devices":[
   {"volume":0,"replication-state":"Established","peer-disk-state":"UpToDate","resync-suspended":"no","out-of-sync":0},
   {"volume":1,"replication-state":"PausedSyncT","peer-disk-state":"UpToDate","resync-suspended":"dependency","out-of-sync":8}]}]}]`, n2Role)
}

// On a volume added later, a reconnect of the active peer can end its
// resync with the volume still Inconsistent, which only the late-add
// kick recovers, and the kick waits while a replica is Primary. With a
// Primary in sight nothing is done; without one the fallback runs.
func TestStalledResyncLeavesTheLateAddKickItsCase(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		n2Role string
		want   []string
	}{
		{n2Role: "Primary", want: nil},
		{n2Role: "Secondary", want: []string{"disconnect pvc-stall:n2", "connect pvc-stall:n2", "disconnect pvc-stall:n1", "connect pvc-stall:n1"}},
	} {
		adm := &fakeWorkaroundAdm{status: lateAddStall(tc.n2Role)}
		w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

		for range 9 {
			w.pass(t.Context(), logr.Discard())
		}

		if !slices.Equal(adm.actions, tc.want) {
			t.Errorf("n2 %s: actions = %v, want %v", tc.n2Role, adm.actions, tc.want)
		}

		evs := drainEvents(recorder)
		if left := slices.ContainsFunc(evs, func(ev string) bool { return strings.Contains(ev, "not acting") }); left != (tc.n2Role == "Primary") {
			t.Errorf("n2 %s: events = %v", tc.n2Role, evs)
		}
	}
}

// The fallback is remembered across the passes the paused peer is away
// while the replica stays Inconsistent, and forgotten once it is in
// sync: a later stall tries the paused peer first again.
func TestStalledResyncRemembersThePausedPeerWasTried(t *testing.T) {
	t.Parallel()

	away := `[{"name":"pvc-stall","role":"Secondary","devices":[{"volume":0,"disk-state":"Inconsistent"}],
 "connections":[{"name":"n1","connection-state":"Connected","peer-role":"Secondary","peer_devices":[{"volume":0,
  "replication-state":"SyncTarget","peer-disk-state":"UpToDate","resync-suspended":"no","out-of-sync":14592,"unacked":542}]},
  {"name":"n2","connection-state":"StandAlone","peer-role":"Unknown"}]}]`
	inSync := afterStall(n1Secondary)

	for _, tc := range []struct {
		between string
		want    string
	}{
		{between: away, want: "disconnect pvc-stall:n1"},
		{between: inSync, want: "disconnect pvc-stall:n2"},
	} {
		adm := &fakeWorkaroundAdm{}
		w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

		runResync(t, w, adm, "Secondary", frozen(5)...)

		adm.status = tc.between
		w.pass(t.Context(), logr.Discard())

		adm.actions = nil

		got := runResync(t, w, adm, "Secondary", frozen(5)...)
		if len(got) == 0 || got[0] != tc.want {
			t.Errorf("after a pass with %s: actions = %v, want %s first", tc.between, got, tc.want)
		}
	}
}

// The workarounds touch only resources blockstor has a replica of on this
// node, and none an apply is changing.
func TestStalledResyncSkipsForeignAndBusyResources(t *testing.T) {
	t.Parallel()

	t.Run("foreign", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{status: stalledStatus("42.17")}
		w, _ := newTestWorkarounds(t, adm, stallConfig())

		for range 6 {
			w.pass(t.Context(), logr.Discard())
		}

		if len(adm.actions) != 0 {
			t.Errorf("acted on a resource blockstor has no replica of: %v", adm.actions)
		}
	})

	t.Run("busy", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{status: stalledStatus("42.17")}
		w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

		w.Hold = func(string) (func(), bool) { return nil, false }

		for range 6 {
			w.pass(t.Context(), logr.Discard())
		}

		if len(adm.actions) != 0 {
			t.Errorf("acted on a resource an apply is changing: %v", adm.actions)
		}
	})
}

// With nothing in the observer's cache a check acts on, a pass runs no
// command at all; with the check off, its shape does not count.
func TestWorkaroundPassReadsTheKernelOnlyOnAHint(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{status: stalledStatus("42.17")}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	hints := WorkaroundHints{}
	w.Hints = func() WorkaroundHints { return hints }

	w.pass(t.Context(), logr.Discard())

	if adm.reads != 0 {
		t.Fatalf("idle pass read the kernel status %d times", adm.reads)
	}

	hints.Inconsistent = true
	w.pass(t.Context(), logr.Discard())

	if adm.reads != 1 {
		t.Fatalf("pass with an Inconsistent volume read the kernel status %d times, want 1", adm.reads)
	}

	w.Config.StalledResync = false
	w.pass(t.Context(), logr.Discard())

	if adm.reads != 1 {
		t.Errorf("an Inconsistent volume read the kernel status with the stall check off")
	}
}

// The observer's cache gives the hints, ignoring resources whose kernel
// slot is gone.
func TestObserverWorkaroundHints(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{}
	if o.WorkaroundHints() != (WorkaroundHints{}) {
		t.Fatal("an empty cache gives hints")
	}

	o.mergeResource(&observation{
		ResourceName: "a", HasResource: true, Role: drbdRolePrimary,
		HasKernelLoaded: true, KernelLoaded: true,
	})
	o.mergeVolumes(&observation{ResourceName: "a", Volumes: []volumeObservation{{VolumeNumber: 0, DiskState: "UpToDate", Open: "no"}}})
	o.mergeResource(&observation{
		ResourceName: "b", HasResource: true, Role: "Secondary", ForceIOFailures: true,
		HasKernelLoaded: true, KernelLoaded: true,
	})
	o.mergeVolumes(&observation{ResourceName: "c", Volumes: []volumeObservation{{VolumeNumber: 0, DiskState: "Inconsistent"}}})

	if got := o.WorkaroundHints(); got != (WorkaroundHints{IdlePrimary: true, Inconsistent: true, ForcedIOFailures: true}) {
		t.Errorf("hints = %+v, want all set", got)
	}

	for _, name := range []string{"a", "b", "c"} {
		o.mergeResource(&observation{ResourceName: name, HasResource: true, HasKernelLoaded: true, KernelLoaded: false})
	}

	if got := o.WorkaroundHints(); got != (WorkaroundHints{}) {
		t.Errorf("hints of destroyed resources = %+v, want none", got)
	}
}

// A Primary in use, as on every node running a VM, gives no hint, so it
// costs the pass no status read; nor does one whose open state the
// kernel does not report. The open state comes from events2 device
// frames.
func TestObserverIdlePrimaryHintFollowsOpen(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{}
	o.mergeResource(&observation{
		ResourceName: "vm", HasResource: true, Role: drbdRolePrimary,
		HasKernelLoaded: true, KernelLoaded: true,
	})

	device := func(open string) {
		obs, ok := translateDeviceEvent(drbd.Event{
			Action: "change", Kind: "device",
			Fields: map[string]string{"name": "vm", "volume": "1", "disk": "UpToDate", "open": open},
		})
		if !ok {
			t.Fatal("device frame not translated")
		}

		o.mergeVolumes(&obs)
		o.mergeVolumes(&observation{ResourceName: "vm", Volumes: []volumeObservation{{VolumeNumber: 0, DiskState: "UpToDate", Open: "no"}}})
	}

	device("yes")

	if o.WorkaroundHints().IdlePrimary {
		t.Error("a Primary with a volume open hints an idle Primary")
	}

	// A peer-device frame carries no open: and must not reset it.
	o.mergeVolumes(&observation{ResourceName: "vm", Volumes: []volumeObservation{{VolumeNumber: 1, OutOfSyncKib: 0, HasSync: true}}})

	if o.WorkaroundHints().IdlePrimary {
		t.Error("a frame without open: made an open Primary look idle")
	}

	device("unknown")

	if o.WorkaroundHints().IdlePrimary || !o.OpenUnknown("vm") {
		t.Errorf("open:unknown: hint %v, unknown %v; want no hint, unknown", o.WorkaroundHints().IdlePrimary, o.OpenUnknown("vm"))
	}

	device("no")

	if !o.WorkaroundHints().IdlePrimary || o.OpenUnknown("vm") {
		t.Errorf("nothing open: hint %v, unknown %v; want a hint, known", o.WorkaroundHints().IdlePrimary, o.OpenUnknown("vm"))
	}
}

// `drbdsetup status --json` prints a volume whose open state the kernel
// does not report as not open; the checks that act on an unused replica
// leave such a resource alone.
func TestUnusedChecksSkipAnUnknownOpenState(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		status string
		cfg    WorkaroundsConfig
		rd     string
	}{
		{"idle primary", idlePrimaryStatus, idleConfig(), "pvc-idle"},
		{"failing io", failingIOStatus("Secondary", false), failingIOConfig(), "pvc-failio"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adm := &fakeWorkaroundAdm{status: tc.status}
			w, _ := newTestWorkarounds(t, adm, tc.cfg, tc.rd)
			w.OpenUnknown = func(string) bool { return true }

			for range 4 {
				w.pass(t.Context(), logr.Discard())
			}

			if len(adm.actions) != 0 {
				t.Errorf("acted on a replica whose open state is unknown: %v", adm.actions)
			}
		})
	}
}

// translateResourceEvent reads force-io-failures off resource frames.
func TestTranslateResourceEventReadsForcedIOFailures(t *testing.T) {
	t.Parallel()

	obs, ok := translateResourceEvent(drbd.Event{
		Action: "change", Kind: "resource",
		Fields: map[string]string{"name": "r", "role": "Secondary", "force-io-failures": "yes"},
	})
	if !ok || !obs.ForceIOFailures {
		t.Errorf("force-io-failures:yes not read: %+v", obs)
	}
}

func idleConfig() WorkaroundsConfig {
	return WorkaroundsConfig{Interval: 1, IdlePrimary: true}
}

const idlePrimaryStatus = `[{"name":"pvc-idle","role":"Primary",
 "devices":[{"volume":0,"disk-state":"UpToDate","open":false},{"volume":1,"disk-state":"UpToDate","open":false}]}]`

// piraeus-operator#942, and alpha.15 issue 01 had the kernel side been at
// fault: a Primary nobody has open is demoted on the second pass in a row.
func TestIdlePrimaryIsDemotedOnTheSecondPass(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{status: idlePrimaryStatus}
	w, recorder := newTestWorkarounds(t, adm, idleConfig(), "pvc-idle")

	w.pass(t.Context(), logr.Discard())

	if len(adm.actions) != 0 {
		t.Fatalf("demoted on the first pass: %v", adm.actions)
	}

	w.pass(t.Context(), logr.Discard())

	if strings.Join(adm.actions, ",") != "secondary pvc-idle" {
		t.Fatalf("actions = %v, want secondary pvc-idle", adm.actions)
	}

	evs := drainEvents(recorder)
	if len(evs) != 1 || !strings.Contains(evs[0], "Warning DRBDIdlePrimary") {
		t.Errorf("events = %v, want one Warning DRBDIdlePrimary", evs)
	}
}

// A Primary with a volume open, one that was opened in between, and one
// an apply is changing (an auto-promote before its mkfs) are not demoted.
func TestIdlePrimaryLeavesUsedPrimariesAlone(t *testing.T) {
	t.Parallel()

	t.Run("open", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{status: `[{"name":"pvc-idle","role":"Primary",
 "devices":[{"volume":0,"disk-state":"UpToDate","open":false},{"volume":1,"disk-state":"UpToDate","open":true}]}]`}
		w, _ := newTestWorkarounds(t, adm, idleConfig(), "pvc-idle")

		for range 4 {
			w.pass(t.Context(), logr.Discard())
		}

		if len(adm.actions) != 0 {
			t.Errorf("demoted a Primary in use: %v", adm.actions)
		}
	})

	t.Run("opened-between", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{}
		w, _ := newTestWorkarounds(t, adm, idleConfig(), "pvc-idle")

		for _, status := range []string{
			idlePrimaryStatus,
			`[{"name":"pvc-idle","role":"Primary","devices":[{"volume":0,"disk-state":"UpToDate","open":true}]}]`,
			idlePrimaryStatus,
		} {
			adm.status = status
			w.pass(t.Context(), logr.Discard())
		}

		if len(adm.actions) != 0 {
			t.Errorf("demoted a Primary that was not idle two passes in a row: %v", adm.actions)
		}
	})

	t.Run("busy", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{status: idlePrimaryStatus}
		w, _ := newTestWorkarounds(t, adm, idleConfig(), "pvc-idle")
		w.Hold = func(string) (func(), bool) { return nil, false }

		for range 4 {
			w.pass(t.Context(), logr.Discard())
		}

		if len(adm.actions) != 0 {
			t.Errorf("demoted a Primary an apply is changing: %v", adm.actions)
		}
	})

	t.Run("off", func(t *testing.T) {
		t.Parallel()

		adm := &fakeWorkaroundAdm{status: idlePrimaryStatus}
		w, _ := newTestWorkarounds(t, adm, WorkaroundsConfig{Interval: 1, StalledResync: true, StalledResyncPasses: 4}, "pvc-idle")
		hints := WorkaroundHints{IdlePrimary: true}
		w.Hints = func() WorkaroundHints { return hints }

		for range 4 {
			w.pass(t.Context(), logr.Discard())
		}

		if len(adm.actions) != 0 || adm.reads != 0 {
			t.Errorf("check off, only a Primary: actions=%v reads=%d, want none", adm.actions, adm.reads)
		}

		// Another check reads the status: the idle Primary in it is
		// still left alone.
		hints.Inconsistent = true

		for range 4 {
			w.pass(t.Context(), logr.Discard())
		}

		if len(adm.actions) != 0 || adm.reads == 0 {
			t.Errorf("check off, status read: actions=%v reads=%d, want none and some", adm.actions, adm.reads)
		}
	})
}

// A check turned off leaves its shape alone even when another check has
// the kernel status read.
func TestStalledResyncOffLeavesAStallAlone(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{status: `[` + strings.Trim(stalledStatus("42.17"), "[]") + `,` +
		strings.Trim(idlePrimaryStatus, "[]") + `]`}
	w, _ := newTestWorkarounds(t, adm, idleConfig(), "pvc-stall")

	for range 6 {
		w.pass(t.Context(), logr.Discard())
	}

	if adm.reads == 0 || len(adm.actions) != 0 {
		t.Errorf("stall check off: reads=%d actions=%v, want some and none", adm.reads, adm.actions)
	}
}

func failingIOConfig() WorkaroundsConfig {
	return WorkaroundsConfig{Interval: 1, ForcedIOFailures: true}
}

func failingIOStatus(role string, open bool) string {
	return fmt.Sprintf(`[{"name":"pvc-failio","role":%q,"force-io-failures":true,
 "devices":[{"volume":0,"disk-state":"Outdated","open":%v}]}]`, role, open)
}

// A Secondary nobody has open that still forces I/O failures is taken
// down and up on the second pass in a row.
func TestForcedIOFailuresSecondaryIsRestarted(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{status: failingIOStatus("Secondary", false)}
	w, recorder := newTestWorkarounds(t, adm, failingIOConfig(), "pvc-failio")

	w.pass(t.Context(), logr.Discard())

	if len(adm.actions) != 0 {
		t.Fatalf("acted on the first pass: %v", adm.actions)
	}

	w.pass(t.Context(), logr.Discard())

	if strings.Join(adm.actions, ",") != "down pvc-failio,up pvc-failio" {
		t.Fatalf("actions = %v, want down and up", adm.actions)
	}

	evs := drainEvents(recorder)
	if len(evs) != 1 || !strings.Contains(evs[0], "Warning DRBDForcedIOFailures") {
		t.Errorf("events = %v, want one Warning DRBDForcedIOFailures", evs)
	}
}

// Never a Primary, never a replica still open, never with the check off.
func TestForcedIOFailuresLeavesOthersAlone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		status string
		cfg    WorkaroundsConfig
	}{
		{"primary", failingIOStatus("Primary", false), failingIOConfig()},
		{"open", failingIOStatus("Secondary", true), failingIOConfig()},
		{"off", failingIOStatus("Secondary", false), WorkaroundsConfig{Interval: 1, IdlePrimary: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adm := &fakeWorkaroundAdm{status: tc.status}
			w, _ := newTestWorkarounds(t, adm, tc.cfg, "pvc-failio")

			for range 4 {
				w.pass(t.Context(), logr.Discard())
			}

			for _, action := range adm.actions {
				if strings.HasPrefix(action, "down") || strings.HasPrefix(action, "up") {
					t.Errorf("took down a replica it must not: %v", adm.actions)
				}
			}
		})
	}
}

// The observer's force-io-failures hint has the status read.
func TestForcedIOFailuresHintReadsTheStatus(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{status: failingIOStatus("Secondary", false)}
	w, _ := newTestWorkarounds(t, adm, failingIOConfig(), "pvc-failio")
	w.Hints = func() WorkaroundHints { return WorkaroundHints{ForcedIOFailures: true} }

	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())

	if adm.reads != 2 || len(adm.actions) != 2 {
		t.Errorf("reads=%d actions=%v, want 2 reads and down/up", adm.reads, adm.actions)
	}
}

// autoclearMark at the end of a fakeSysBlock backing file marks the loop
// device to go away on its last close.
const autoclearMark = "+autoclear"

// fakeSysBlock lays out /sys/block with a loop device per entry of
// backing (name → backing_file content); an empty content leaves the
// loop device unattached (no loop/ directory).
func fakeSysBlock(t *testing.T, backing map[string]string) string {
	t.Helper()

	root := t.TempDir()

	for name, file := range backing {
		dir := filepath.Join(root, name)
		if file != "" {
			dir = filepath.Join(dir, "loop")
		}

		err := os.MkdirAll(dir, 0o755)
		if err != nil {
			t.Fatal(err)
		}

		if file == "" {
			continue
		}

		autoclearFlag := "0"

		file, marked := strings.CutSuffix(file, autoclearMark)
		if marked {
			autoclearFlag = "1"
		}

		err = os.WriteFile(filepath.Join(dir, "backing_file"), []byte(file+"\n"), 0o600)
		if err != nil {
			t.Fatal(err)
		}

		err = os.WriteFile(filepath.Join(dir, "autoclear"), []byte(autoclearFlag+"\n"), 0o600)
		if err != nil {
			t.Fatal(err)
		}
	}

	return root
}

// A loop device whose backing file reads "/" or "/ (deleted)" is
// detached, and the action recorded on this satellite's Node; loop
// devices with a real backing file, unattached ones, and ones a detach
// already marked to go away on their last close are left alone.
// No kernel status is read for it.
func TestStaleLoopDevicesAreDetached(t *testing.T) {
	t.Parallel()

	exec := storage.NewFakeExec()
	adm := &fakeWorkaroundAdm{}
	recorder := events.NewFakeRecorder(16)

	w := &DRBDWorkaroundsRunnable{
		Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(&blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: workaroundNode},
		}).Build(),
		Adm:      adm,
		Exec:     exec,
		Recorder: recorder,
		NodeName: workaroundNode,
		Config:   WorkaroundsConfig{Interval: 1, StaleLoopDevices: true},
		Hints:    func() WorkaroundHints { return WorkaroundHints{} },
		SysBlock: fakeSysBlock(t, map[string]string{
			"loop0": "/",
			"loop1": "/ (deleted)",
			"loop2": "/var/lib/blockstor/pool/pvc-a_00000.img",
			"loop3": "",
			"loop4": "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/pvc-b/dev/0b5c (deleted)",
			// Detached on an earlier pass while still open.
			"loop5": "/" + autoclearMark,
		}),
	}

	w.pass(t.Context(), logr.Discard())

	got := exec.CommandLines()
	slices.Sort(got)

	want := []string{"losetup --detach /dev/loop0", "losetup --detach /dev/loop1"}
	if !slices.Equal(got, want) {
		t.Errorf("commands = %q, want %q", got, want)
	}

	if adm.reads != 0 {
		t.Errorf("read the kernel status %d times for loop devices", adm.reads)
	}

	evs := drainEvents(recorder)
	if len(evs) != 2 || !strings.Contains(evs[0], "Warning StaleLoopDevice") {
		t.Errorf("events = %v, want two Warning StaleLoopDevice", evs)
	}
}

func TestStaleLoopDevicesOffDetachesNothing(t *testing.T) {
	t.Parallel()

	exec := storage.NewFakeExec()
	w := &DRBDWorkaroundsRunnable{
		Reader:   workaroundReader(t),
		Adm:      &fakeWorkaroundAdm{},
		Exec:     exec,
		NodeName: workaroundNode,
		Config:   WorkaroundsConfig{Interval: 1, IdlePrimary: true},
		Hints:    func() WorkaroundHints { return WorkaroundHints{} },
		SysBlock: fakeSysBlock(t, map[string]string{"loop0": "/"}),
	}

	w.pass(t.Context(), logr.Discard())

	if got := exec.CommandLines(); len(got) != 0 {
		t.Errorf("check off ran %q", got)
	}
}

// The manager's runnable reads its checks from the satellite's config,
// its hints from the observer and its busy resources from the apply.
func TestNewDRBDWorkaroundsRunnableWiresConfigHintsAndHold(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		NodeName:    workaroundNode,
		Apply:       satellite.NewReconciler(satellite.ReconcilerConfig{NodeName: workaroundNode}),
		Exec:        storage.NewFakeExec(),
		Workarounds: DefaultWorkaroundsConfig(),
	}
	observer := &ObserverRunnable{}
	observer.mergeResource(&observation{ResourceName: "a", HasResource: true, Role: drbdRolePrimary})

	w := newDRBDWorkaroundsRunnable(workaroundReader(t).(client.Client), cfg, observer)

	if w.Config != DefaultWorkaroundsConfig() || w.NodeName != workaroundNode || w.Exec == nil || w.Adm == nil {
		t.Errorf("runnable = %+v, want the satellite's config", w)
	}

	if w.Hints == nil || !w.Hints().IdlePrimary {
		t.Error("hints are not the observer's")
	}

	observer.mergeVolumes(&observation{ResourceName: "a", Volumes: []volumeObservation{{VolumeNumber: 0, Open: drbdOpenUnknown}}})

	if w.OpenUnknown == nil || !w.OpenUnknown("a") {
		t.Error("the open-unknown lookup is not the observer's")
	}

	if w.Hold == nil {
		t.Fatal("no hold")
	}

	release, ok := w.Hold("a")
	if !ok {
		t.Fatal("hold of an idle resource refused")
	}

	if _, again := cfg.Apply.Hold("a"); again {
		t.Error("hold is not the apply's: the apply does not see it")
	}

	release()

	// A zero interval registers nothing.
	mgr := &logManager{}

	off := &DRBDWorkaroundsRunnable{}
	if err := off.RegisterWithManager(mgr); err != nil {
		t.Errorf("zero interval: %v", err)
	}

	if len(mgr.added) != 0 {
		t.Errorf("zero interval added %d runnables", len(mgr.added))
	}
}

// logManager is a manager that records the runnables added to it and the
// lines logged through its logger; every other method panics. Its logger
// keeps Info and drops V(1), as the satellite's slog sink at LevelInfo
// does, so a line logged only at V(1) is missing here as in production.
type logManager struct {
	manager.Manager

	added []manager.Runnable
	lines []string
}

func (m *logManager) Add(r manager.Runnable) error {
	m.added = append(m.added, r)

	return nil
}

func (m *logManager) GetLogger() logr.Logger {
	return funcr.New(func(prefix, args string) {
		m.lines = append(m.lines, prefix+" "+args)
	}, funcr.Options{Verbosity: 0})
}

// The satellite says at start which DRBD workarounds it runs and how
// often, since a check logs nothing until it acts.
func TestWorkaroundsLogTheirStateAtStart(t *testing.T) {
	t.Parallel()

	cfg := DefaultWorkaroundsConfig()
	cfg.IdlePrimary = false

	for _, tc := range []struct {
		name string
		cfg  WorkaroundsConfig
		want string
		runs int
	}{
		{
			name: "enabled",
			cfg:  cfg,
			want: `"msg"="DRBD workarounds enabled" "interval"="30s" "stalledResync"=true "stalledResyncPasses"=4 ` +
				`"stalledResyncMinRate"=200 "stalledResyncAct"=false "idlePrimary"=false "forcedIOFailures"=true "staleLoopDevices"=true`,
			runs: 1,
		},
		{
			name: "disabled",
			cfg:  WorkaroundsConfig{},
			want: `"msg"="DRBD workarounds disabled" "interval"="0s" "reason"="interval is 0"`,
		},
		{
			// An interval with every check off would tick a pass that
			// does nothing and log "enabled" with every check false.
			name: "no check on",
			cfg:  WorkaroundsConfig{Interval: defaultWorkaroundInterval, StalledResyncPasses: 4},
			want: `"msg"="DRBD workarounds disabled" "interval"="30s" "reason"="no check enabled"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mgr := &logManager{}

			err := (&DRBDWorkaroundsRunnable{Config: tc.cfg}).RegisterWithManager(mgr)
			if err != nil {
				t.Fatal(err)
			}

			if len(mgr.added) != tc.runs {
				t.Errorf("added %d runnables, want %d", len(mgr.added), tc.runs)
			}

			if len(mgr.lines) != 1 || !strings.HasPrefix(mgr.lines[0], "drbd-workarounds ") ||
				!strings.Contains(mgr.lines[0], tc.want) {
				t.Errorf("logged %q, want one drbd-workarounds line with %s", mgr.lines, tc.want)
			}
		})
	}
}

// A pass says at Info whether it read the kernel status, on the first
// pass and then only when that changes, so an idle node running no
// command is visible in the satellite's Info-level log without a line
// every interval.
func TestWorkaroundPassLogsWhetherItReadTheKernel(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{status: stalledStatus("42.17")}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	hints := WorkaroundHints{}
	w.Hints = func() WorkaroundHints { return hints }

	mgr := &logManager{}

	for _, inconsistent := range []bool{false, false, true, true, false} {
		hints.Inconsistent = inconsistent
		w.pass(t.Context(), mgr.GetLogger())
	}

	var passes []string

	for _, line := range mgr.lines {
		if strings.Contains(line, `"msg"="DRBD workarounds pass"`) {
			passes = append(passes, line)
		}
	}

	want := []string{`"kernelStatusRead"=false`, `"kernelStatusRead"=true`, `"kernelStatusRead"=false`}
	if len(passes) != len(want) {
		t.Fatalf("pass lines = %q, want one per change: %q", passes, want)
	}

	for i := range want {
		if !strings.Contains(passes[i], want[i]) || !strings.Contains(passes[i], `"level"=0`) {
			t.Errorf("pass line %d = %q, want an Info line with %s", i, passes[i], want[i])
		}
	}
}

// A replica whose Resource is being deleted is left to the satellite's
// delete: no check demotes or restarts it in the meantime.
func TestWorkaroundsSkipAReplicaBeingDeleted(t *testing.T) {
	t.Parallel()

	now := metav1.Now()
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              k8s.ResourceCRName("pvc-idle", workaroundNode),
			DeletionTimestamp: &now,
			Finalizers:        []string{"blockstor.io/test"},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-idle", NodeName: workaroundNode},
	}).Build()

	adm := &fakeWorkaroundAdm{status: idlePrimaryStatus}
	w, _ := newTestWorkarounds(t, adm, idleConfig())
	w.Reader = reader

	for range 4 {
		w.pass(t.Context(), logr.Discard())
	}

	if len(adm.actions) != 0 {
		t.Errorf("acted on a replica being deleted: %v", adm.actions)
	}
}

// errUp is a `drbdadm up` that did not go through.
var errUp = errors.New("exit status 10: pvc-failio: Failure: (114) Lower device is already claimed")

// A failed `drbdadm up` after the down leaves the replica down: the
// following passes bring it up with adjust until that goes through, even
// with nothing in the observer's cache to read the status for, and the
// failure is recorded as an Event of its own.
func TestForcedIOFailuresRetriesAFailedUp(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{
		status: failingIOStatus("Secondary", false),
		errs: map[string][]error{
			"up pvc-failio":     {errUp},
			"adjust pvc-failio": {errUp},
		},
	}
	w, recorder := newTestWorkarounds(t, adm, failingIOConfig(), "pvc-failio")

	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())

	evs := drainEvents(recorder)
	if len(evs) != 2 || !strings.Contains(evs[1], "Warning DRBDWorkaroundUpFailed") {
		t.Fatalf("events = %v, want DRBDForcedIOFailures then DRBDWorkaroundUpFailed", evs)
	}

	// The replica is down: the kernel no longer lists it and the
	// observer gives no hint.
	adm.status = "[]"
	w.Hints = func() WorkaroundHints { return WorkaroundHints{} }
	adm.actions = nil

	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())
	w.pass(t.Context(), logr.Discard())

	if got := strings.Join(adm.actions, ","); got != "adjust pvc-failio,adjust pvc-failio" {
		t.Errorf("actions = %q, want one failed adjust, one that went through, then nothing", got)
	}
}

// A Secondary failing I/O that a diskless Primary on another node reads
// from, as its only UpToDate disk, is left up while that lasts, and
// restarted on the first pass it no longer is.
func TestForcedIOFailuresLeavesTheSourceOfAPrimaryPeerUp(t *testing.T) {
	t.Parallel()

	const sole = `[{"name":"pvc-failio","role":"Secondary","force-io-failures":true,
 "devices":[{"volume":0,"disk-state":"UpToDate","open":false}],
 "connections":[{"name":"n1","connection-state":"Connected","peer-role":"Primary",
  "peer_devices":[{"volume":0,"peer-disk-state":"Diskless"}]}]}]`

	adm := &fakeWorkaroundAdm{status: sole}
	w, _ := newTestWorkarounds(t, adm, failingIOConfig(), "pvc-failio")

	for range 4 {
		w.pass(t.Context(), logr.Discard())
	}

	if len(adm.actions) != 0 {
		t.Fatalf("took down the only UpToDate disk of a Primary peer: %v", adm.actions)
	}

	adm.status = strings.Replace(sole, `"peer-role":"Primary"`, `"peer-role":"Secondary"`, 1)
	w.pass(t.Context(), logr.Discard())

	if got := strings.Join(adm.actions, ","); got != "down pvc-failio,up pvc-failio" {
		t.Errorf("actions once the peer demoted = %q, want down and up", got)
	}
}

// A record of two peers settles the one that is connected already and
// connects the other.
func TestStalledResyncReconnectSettlesEachPeerOnItsOwn(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	record := filepath.Join(dir, "pvc-stall"+satellite.ReconnectPendingSuffix)

	err := os.WriteFile(record, []byte("n1\nn2\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	adm := &fakeWorkaroundAdm{status: afterStall(n1Secondary), away: map[string]bool{"pvc-stall:n2": true}}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")
	w.StateDir = dir

	w.pass(t.Context(), logr.Discard())

	if want := []string{"connect pvc-stall:n2"}; !slices.Equal(adm.actions, want) {
		t.Errorf("actions = %v, want %v", adm.actions, want)
	}

	if _, err := os.Stat(record); !os.IsNotExist(err) {
		t.Errorf("the record of a settled reconnect is still there: %v", err)
	}
}
