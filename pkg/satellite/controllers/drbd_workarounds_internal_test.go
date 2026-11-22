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

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

const workaroundNode = "n3"

// fakeWorkaroundAdm hands out one kernel status per pass and records the
// actions taken.
type fakeWorkaroundAdm struct {
	status  string
	reads   int
	actions []string

	// errs fails the next calls of an action ("up pvc-x"), in order.
	errs map[string][]error
}

func (f *fakeWorkaroundAdm) KernelStatus(context.Context) ([]drbd.KernelResource, error) {
	f.reads++

	var out []drbd.KernelResource

	err := json.Unmarshal([]byte(f.status), &out)
	if err != nil {
		return nil, fmt.Errorf("fake kernel status: %w", err)
	}

	return out, nil
}

func (f *fakeWorkaroundAdm) DisconnectPeer(_ context.Context, res, peer string) error {
	return f.act("disconnect " + res + ":" + peer)
}

func (f *fakeWorkaroundAdm) ConnectPeer(_ context.Context, res, peer string) error {
	return f.act("connect " + res + ":" + peer)
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

	return &DRBDWorkaroundsRunnable{
		Reader:   workaroundReader(t, rds...),
		Adm:      adm,
		Recorder: recorder,
		NodeName: workaroundNode,
		Config:   cfg,
	}, recorder
}

// stalledStatus is a replica resyncing from n1 at percent while its resync
// from n2 waits on that one.
func stalledStatus(percent string) string {
	return `[{"name":"pvc-stall","role":"Secondary",
 "devices":[{"volume":0,"disk-state":"Inconsistent","open":false}],
 "connections":[
  {"name":"n1","peer_devices":[{"volume":0,"replication-state":"SyncTarget","resync-suspended":"no","percent-in-sync":` + percent + `}]},
  {"name":"n2","peer_devices":[{"volume":0,"replication-state":"PausedSyncT","resync-suspended":"dependency","percent-in-sync":` + percent + `}]}]}]`
}

func stallConfig() WorkaroundsConfig {
	return WorkaroundsConfig{Interval: 1, StalledResync: true, StalledResyncPasses: 4}
}

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

// LINBIT/drbd#149: the resync from n1 sits at the same percent while n2
// waits on it. The fourth pass in a row reconnects n1, and not before.
func TestStalledResyncReconnectsTheStuckSourceAfterFourPasses(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{status: stalledStatus("42.17")}
	w, recorder := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	for pass := 1; pass <= 3; pass++ {
		w.pass(t.Context(), logr.Discard())

		if len(adm.actions) != 0 {
			t.Fatalf("pass %d acted early: %v", pass, adm.actions)
		}
	}

	w.pass(t.Context(), logr.Discard())

	want := []string{"disconnect pvc-stall:n1", "connect pvc-stall:n1"}
	if strings.Join(adm.actions, ",") != strings.Join(want, ",") {
		t.Fatalf("actions = %v, want %v", adm.actions, want)
	}

	evs := drainEvents(recorder)
	if len(evs) != 1 || !strings.Contains(evs[0], "Warning DRBDResyncStalled") {
		t.Errorf("events = %v, want one Warning DRBDResyncStalled", evs)
	}

	// The reconnect starts the count over.
	adm.actions = nil

	for range 3 {
		w.pass(t.Context(), logr.Discard())
	}

	if len(adm.actions) != 0 {
		t.Errorf("reconnected again before four more passes: %v", adm.actions)
	}
}

// A resync that moves is left alone, however long it takes.
func TestStalledResyncLeavesAMovingResyncAlone(t *testing.T) {
	t.Parallel()

	adm := &fakeWorkaroundAdm{}
	w, _ := newTestWorkarounds(t, adm, stallConfig(), "pvc-stall")

	for _, percent := range []string{"10.00", "10.00", "10.00", "11.50", "11.50", "11.50", "12.00"} {
		adm.status = stalledStatus(percent)
		w.pass(t.Context(), logr.Discard())
	}

	if len(adm.actions) != 0 {
		t.Errorf("a moving resync was reconnected: %v", adm.actions)
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
	o.mergeResource(&observation{
		ResourceName: "b", HasResource: true, Role: "Secondary", ForceIOFailures: true,
		HasKernelLoaded: true, KernelLoaded: true,
	})
	o.mergeVolumes(&observation{ResourceName: "c", Volumes: []volumeObservation{{VolumeNumber: 0, DiskState: "Inconsistent"}}})

	if got := o.WorkaroundHints(); got != (WorkaroundHints{Primary: true, Inconsistent: true, ForcedIOFailures: true}) {
		t.Errorf("hints = %+v, want all set", got)
	}

	for _, name := range []string{"a", "b", "c"} {
		o.mergeResource(&observation{ResourceName: name, HasResource: true, HasKernelLoaded: true, KernelLoaded: false})
	}

	if got := o.WorkaroundHints(); got != (WorkaroundHints{}) {
		t.Errorf("hints of destroyed resources = %+v, want none", got)
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
		hints := WorkaroundHints{Primary: true}
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

		err = os.WriteFile(filepath.Join(dir, "backing_file"), []byte(file+"\n"), 0o600)
		if err != nil {
			t.Fatal(err)
		}
	}

	return root
}

// A loop device whose backing file reads "/" or "/ (deleted)" is
// detached, and the action recorded on this satellite's Node; loop
// devices with a real backing file, and unattached ones, are left alone.
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

	if w.Hints == nil || !w.Hints().Primary {
		t.Error("hints are not the observer's")
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

	// A zero interval registers nothing (and touches no manager).
	off := &DRBDWorkaroundsRunnable{}
	if err := off.RegisterWithManager(nil); err != nil {
		t.Errorf("zero interval: %v", err)
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
