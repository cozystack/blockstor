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
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

const (
	windowTestResource = "pvc-1"
	windowTestSnapshot = "pvc-1.snap-1"
	windowTestMinor    = 1000
)

var (
	errFakeResume      = errors.New("resume-io: device busy")
	errNoSuchResource  = errors.New("No such resource")
	errNotInYourConfig = errors.New("not defined in your config")
)

// fakeKernel is a DRBD kernel and a shell in one. It keeps each
// resource's suspended-user flag the way the kernel does, and it makes
// every command that writes back to a suspended device wait for the
// resume, which is what deadlocked a global sync on the Primary. Such a
// command returns only when its context ends, and is counted.
type fakeKernel struct {
	mu sync.Mutex

	calls []string
	res   map[string]*fakeKernelResource
	role  string

	// blockedWhileSuspended counts writeback commands issued while the
	// device was suspended.
	blockedWhileSuspended int
	// resumeFailures makes the next resume-io calls fail.
	resumeFailures int
	// resumeIgnored makes resume-io exit 0 without resuming.
	resumeIgnored bool
	// resFileGone makes drbdadm fail as it does once the .res is gone.
	resFileGone bool
	// suspendLandsLate makes suspend-io fail at once, as one killed at
	// its timeout does, and the suspend reach the kernel this much later.
	suspendLandsLate  time.Duration
	lateSuspendLanded bool
	// resumeHang makes resume-io of these resources wait for its context.
	resumeHang map[string]bool
}

type fakeKernelResource struct {
	suspended bool
	minors    []int32
}

func newFakeKernel() *fakeKernel {
	return &fakeKernel{
		role:       "Primary",
		res:        map[string]*fakeKernelResource{windowTestResource: {minors: []int32{windowTestMinor}}},
		resumeHang: map[string]bool{},
	}
}

func (k *fakeKernel) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return k.RunWithStdin(ctx, nil, name, args...)
}

func (k *fakeKernel) RunWithStdin(ctx context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	line := strings.Join(append([]string{name}, args...), " ")

	k.mu.Lock()
	k.calls = append(k.calls, line)
	k.mu.Unlock()

	switch {
	case name == "sync" || strings.HasPrefix(line, "blockdev --flushbufs "):
		if k.writebackBlocked(line) {
			k.mu.Lock()
			k.blockedWhileSuspended++
			k.mu.Unlock()

			<-ctx.Done()

			return nil, errors.Wrap(ctx.Err(), line)
		}

		return nil, nil
	case line == "drbdsetup status --json":
		return k.status(""), nil
	}

	switch {
	case len(args) == 2 && name == "drbdadm" && args[0] == "suspend-io":
		return nil, k.suspend(args[1])
	case len(args) == 2 && name == "drbdadm" && args[0] == "resume-io":
		if k.resFileGone {
			return nil, errors.Wrapf(errNotInYourConfig, "'%s'", args[1])
		}

		return nil, k.resume(ctx, args[1])
	case len(args) == 2 && name == "drbdsetup" && args[0] == "resume-io":
		return nil, k.resume(ctx, k.byMinor(args[1]))
	case len(args) == 3 && name == "drbdsetup" && args[0] == "status" && args[2] == "--json":
		out := k.status(args[1])
		if out == nil {
			return nil, errors.Wrap(errNoSuchResource, args[1])
		}

		return out, nil
	}

	return nil, nil
}

// addResource loads another resource into the kernel.
func (k *fakeKernel) addResource(name string, minors ...int32) {
	k.set(func() { k.res[name] = &fakeKernelResource{minors: minors} })
}

// writebackBlocked reports whether a writeback command targets a
// suspended device: a global sync targets every device, a flush the
// one it names.
func (k *fakeKernel) writebackBlocked(line string) bool {
	device, isFlush := strings.CutPrefix(line, "blockdev --flushbufs ")
	owner := ""

	if minor, ok := strings.CutPrefix(device, "/dev/drbd"); ok && isFlush {
		owner = k.byMinor(minor)
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	for name, r := range k.res {
		if !r.suspended {
			continue
		}

		if !isFlush || name == owner || strings.Contains(device, "/"+name+"_") {
			return true
		}
	}

	return false
}

// status renders `drbdsetup status --json` for one resource, or for
// every resource when name is empty; nil when name is not loaded.
func (k *fakeKernel) status(name string) []byte {
	k.mu.Lock()
	defer k.mu.Unlock()

	names := slices.Sorted(maps.Keys(k.res))
	if name != "" {
		if k.res[name] == nil {
			return nil
		}

		names = []string{name}
	}

	entries := make([]string, 0, len(names))

	for _, n := range names {
		r := k.res[n]

		devices := make([]string, 0, len(r.minors))
		for vol, minor := range r.minors {
			devices = append(devices, fmt.Sprintf(`{"volume":%d,"minor":%d}`, vol, minor))
		}

		entries = append(entries, fmt.Sprintf(`{"name":%q,"role":%q,"suspended-user":%t,"devices":[%s]}`,
			n, k.role, r.suspended, strings.Join(devices, ",")))
	}

	return []byte("[" + strings.Join(entries, ",") + "]")
}

func (k *fakeKernel) byMinor(minor string) string {
	k.mu.Lock()
	defer k.mu.Unlock()

	for name, r := range k.res {
		for _, m := range r.minors {
			if strconv.Itoa(int(m)) == minor {
				return name
			}
		}
	}

	return ""
}

func (k *fakeKernel) suspend(name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	r := k.res[name]
	if r == nil {
		return errors.Wrap(errNoSuchResource, name)
	}

	if k.suspendLandsLate > 0 {
		time.AfterFunc(k.suspendLandsLate, func() {
			k.set(func() {
				r.suspended = true
				k.lateSuspendLanded = true
			})
		})

		return errors.New("drbdadm suspend-io: signal: killed")
	}

	r.suspended = true

	return nil
}

func (k *fakeKernel) resume(ctx context.Context, name string) error {
	k.mu.Lock()
	hang := k.resumeHang[name]
	k.mu.Unlock()

	if hang {
		<-ctx.Done()

		return errors.Wrap(ctx.Err(), "resume-io")
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	r := k.res[name]
	if r == nil {
		return errors.Wrap(errNoSuchResource, name)
	}

	if k.resumeFailures > 0 {
		k.resumeFailures--

		return errFakeResume
	}

	if !k.resumeIgnored {
		r.suspended = false
	}

	return nil
}

func (k *fakeKernel) set(fn func()) {
	k.mu.Lock()
	defer k.mu.Unlock()

	fn()
}

func (k *fakeKernel) isSuspended() bool { return k.resourceSuspended(windowTestResource) }

func (k *fakeKernel) resourceSuspended(name string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.res[name] != nil && k.res[name].suspended
}

// suspendResource suspends name behind the satellite's back, as an
// older satellite or an operator would.
func (k *fakeKernel) suspendResource(name string) {
	k.set(func() { k.res[name].suspended = true })
}

func (k *fakeKernel) count(cmd string) int {
	n := 0

	for _, c := range k.commands() {
		if c == cmd {
			n++
		}
	}

	return n
}

func (k *fakeKernel) blocked() int {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.blockedWhileSuspended
}

func (k *fakeKernel) commands() []string {
	k.mu.Lock()
	defer k.mu.Unlock()

	return slices.Clone(k.calls)
}

// fakeProvider is a storage provider whose take records whether the
// device was suspended at the time, and can hang the way a take stuck
// in the kernel does: deaf to its context until released.
type fakeProvider struct {
	kernel *fakeKernel

	mu            sync.Mutex
	takes         int
	takeSuspended []bool
	takeErr       error
	hang          chan struct{}
	deletes       int
}

func (*fakeProvider) Kind() string { return "ZFS_THIN" }

func (*fakeProvider) PoolStatus(context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{}, nil
}

func (*fakeProvider) CreateVolume(context.Context, storage.Volume) error { return nil }
func (*fakeProvider) DeleteVolume(context.Context, storage.Volume) error { return nil }
func (*fakeProvider) ResizeVolume(context.Context, storage.Volume) error { return nil }

func (*fakeProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{DevicePath: "/dev/zvol/pool/" + vol.ResourceName + "_00000"}, nil
}

func (p *fakeProvider) CreateSnapshot(context.Context, storage.Snapshot) error {
	p.mu.Lock()
	p.takes++
	p.takeSuspended = append(p.takeSuspended, p.kernel.isSuspended())
	hang, err := p.hang, p.takeErr
	p.mu.Unlock()

	if hang != nil {
		<-hang
	}

	return err
}

func (p *fakeProvider) DeleteSnapshot(context.Context, storage.Snapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.deletes++

	return nil
}

func (*fakeProvider) RestoreVolumeFromSnapshot(context.Context, storage.Volume, storage.Snapshot) error {
	return nil
}

func (p *fakeProvider) deleteCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.deletes
}

func (p *fakeProvider) takeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.takes
}

// windowRig is one satellite with a fake kernel under it and a
// Snapshot CR in Phase 1.
type windowRig struct {
	t        *testing.T
	r        *SnapshotReconciler
	cli      client.Client
	kernel   *fakeKernel
	provider *fakeProvider
	apply    *satellite.Reconciler
	recorder *events.FakeRecorder
	stateDir string
}

func newWindowRig(t *testing.T) *windowRig {
	t.Helper()

	return newWindowRigIn(t, t.TempDir(), newFakeKernel(), nil)
}

// newWindowRigIn builds a satellite over an existing state directory,
// kernel and API, as a restarted satellite finds them.
func newWindowRigIn(t *testing.T, stateDir string, kernel *fakeKernel, cli client.Client) *windowRig {
	t.Helper()

	if cli == nil {
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}

		if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}

		cli = fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
			WithObjects(windowSnapshot(windowTestSnapshot, windowTestResource)).Build()
	}

	provider := &fakeProvider{kernel: kernel}
	apply := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool": provider},
		Adm:       drbd.NewAdm(kernel),
		Exec:      kernel,
		StateDir:  stateDir,
		NodeName:  "n1",
	})

	_, err := apply.Apply(t.Context(), []*intent.DesiredResource{{
		Name: windowTestResource, NodeName: "n1", LayerStack: []string{"STORAGE"},
		Volumes: []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024, StoragePool: "pool"}},
	}})
	if err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	recorder := events.NewFakeRecorder(100)

	return &windowRig{
		t: t,
		r: &SnapshotReconciler{
			Client: cli,
			Config: Config{NodeName: "n1", Apply: apply, Exec: kernel, Recorder: recorder},
			timing: snapshotWindowTiming{
				flushTimeout:   200 * time.Millisecond,
				windowDeadline: 2 * time.Second,
				watchdogTick:   10 * time.Millisecond,
				commandTimeout: 500 * time.Millisecond,
				unownedSweep:   20 * time.Millisecond,
			},
		},
		cli:      cli,
		kernel:   kernel,
		provider: provider,
		apply:    apply,
		recorder: recorder,
		stateDir: stateDir,
	}
}

// windowSnapshot is a Snapshot CR of resource in Phase 1 on n1.
func windowSnapshot(name, resource string) *blockstoriov1alpha1.Snapshot {
	return &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name, Finalizers: []string{SatelliteSnapshotFinalizer}},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: resource,
			SnapshotName:           strings.TrimPrefix(name, resource+"."),
			Nodes:                  []string{"n1"},
			SuspendIO:              true,
			VolumeDefinitions:      []blockstoriov1alpha1.SnapshotVolumeRef{{VolumeNumber: 0}},
		},
	}
}

// addResource loads another resource on the satellite and in the
// kernel, and a Snapshot of it named snapshot.
func (rig *windowRig) addResource(resource string, minor int32, snapshot string) {
	rig.t.Helper()

	rig.kernel.addResource(resource, minor)

	_, err := rig.apply.Apply(rig.t.Context(), []*intent.DesiredResource{{
		Name: resource, NodeName: "n1", LayerStack: []string{"STORAGE"},
		Volumes: []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024, StoragePool: "pool"}},
	}})
	if err != nil {
		rig.t.Fatalf("seed resource %s: %v", resource, err)
	}

	if snapshot != "" {
		rig.create(windowSnapshot(snapshot, resource))
	}
}

func (rig *windowRig) create(obj client.Object) {
	rig.t.Helper()

	err := rig.cli.Create(context.Background(), obj)
	if err != nil {
		rig.t.Fatalf("create %s: %v", obj.GetName(), err)
	}
}

func (rig *windowRig) reconcile() {
	rig.t.Helper()

	rig.reconcileName(windowTestSnapshot)
}

func (rig *windowRig) reconcileName(name string) ctrl.Result {
	rig.t.Helper()

	res, err := rig.r.Reconcile(rig.t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		rig.t.Fatalf("Reconcile %s: %v", name, err)
	}

	return res
}

// startWatchdog runs the watchdog until the returned stop is called.
func (rig *windowRig) startWatchdog() func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = (&SnapshotSuspendWatchdog{Reconciler: rig.r}).Start(ctx)
	}()

	stop := func() {
		cancel()
		<-done
	}

	rig.t.Cleanup(stop)

	return func() {
		cancel()
		<-done
	}
}

func (rig *windowRig) snapshot() blockstoriov1alpha1.Snapshot {
	rig.t.Helper()

	var snap blockstoriov1alpha1.Snapshot

	err := rig.cli.Get(context.Background(), types.NamespacedName{Name: windowTestSnapshot}, &snap)
	if err != nil {
		rig.t.Fatalf("get snapshot: %v", err)
	}

	return snap
}

// controllerSets flips the Spec flags the way the controller does.
func (rig *windowRig) controllerSets(suspendIO, takeSnapshot bool) {
	rig.t.Helper()

	rig.controllerSetsOn(windowTestSnapshot, suspendIO, takeSnapshot)
}

func (rig *windowRig) controllerSetsOn(name string, suspendIO, takeSnapshot bool) {
	rig.t.Helper()

	var snap blockstoriov1alpha1.Snapshot

	err := rig.cli.Get(context.Background(), types.NamespacedName{Name: name}, &snap)
	if err != nil {
		rig.t.Fatalf("get snapshot %s: %v", name, err)
	}

	snap.Spec.SuspendIO = suspendIO
	snap.Spec.TakeSnapshot = takeSnapshot

	err = rig.cli.Update(context.Background(), &snap)
	if err != nil {
		rig.t.Fatalf("update snapshot spec: %v", err)
	}
}

func (rig *windowRig) node() blockstoriov1alpha1.SnapshotPerNodeStatus {
	snap := rig.snapshot()
	for _, entry := range snap.Status.NodeStatus {
		if entry.NodeName == "n1" {
			return entry
		}
	}

	return blockstoriov1alpha1.SnapshotPerNodeStatus{}
}

func (rig *windowRig) markerExists() bool {
	_, err := os.Stat(filepath.Join(rig.stateDir, windowTestSnapshot+suspendMarkerSuffix))

	return err == nil
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// The take of an in-use volume flushes the Primary's device before the
// suspend and nothing at all while suspended. A global sync after the
// suspend waited for writeback to the suspended device forever, so the
// take never ran and the resume after it never came.
func TestSnapshotWindowFlushesBeforeSuspendAndNeverWhileSuspended(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.startWatchdog()

	rig.reconcile()

	if !rig.kernel.isSuspended() || !rig.node().SuspendIOAcked {
		t.Fatalf("suspend phase: suspended=%t acked=%t", rig.kernel.isSuspended(), rig.node().SuspendIOAcked)
	}

	rig.controllerSets(true, true)
	rig.reconcile()
	eventually(t, "Ready", func() bool { return rig.node().Ready })

	rig.controllerSets(false, false)
	rig.reconcile()
	eventually(t, "ack cleared", func() bool { return !rig.node().SuspendIOAcked })

	if n := rig.kernel.blocked(); n != 0 {
		t.Errorf("%d writeback command(s) ran while the device was suspended: %v", n, rig.kernel.commands())
	}

	cmds := rig.kernel.commands()
	flush := slices.Index(cmds, fmt.Sprintf("blockdev --flushbufs /dev/drbd%d", windowTestMinor))
	suspend := slices.Index(cmds, "drbdadm suspend-io "+windowTestResource)

	if flush < 0 || suspend < 0 || flush > suspend {
		t.Errorf("want the Primary's DRBD device flushed before suspend-io: %v", cmds)
	}

	if slices.Contains(cmds, "sync") {
		t.Errorf("a global sync ran: %v", cmds)
	}

	if rig.provider.takeCount() != 1 || !rig.provider.takeSuspended[0] {
		t.Errorf("want one take while suspended; takes=%d suspended=%v", rig.provider.takeCount(), rig.provider.takeSuspended)
	}

	if rig.kernel.isSuspended() || rig.markerExists() {
		t.Errorf("after the window: suspended=%t marker=%t", rig.kernel.isSuspended(), rig.markerExists())
	}
}

// A Secondary has no writer above DRBD and nothing to flush.
func TestSnapshotWindowDoesNotFlushASecondary(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.kernel.set(func() { rig.kernel.role = "Secondary" })

	rig.reconcile()

	for _, cmd := range rig.kernel.commands() {
		if strings.HasPrefix(cmd, "blockdev --flushbufs ") {
			t.Errorf("flushed a Secondary: %v", rig.kernel.commands())
		}
	}
}

// A take that fails for good ends the window at once on this node.
func TestSnapshotWindowResumesWhenTheTakeFails(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	// Far past the wait below, so only the failed take can end the window.
	rig.r.timing.windowDeadline = time.Minute
	rig.provider.takeErr = storage.ErrTerminal
	rig.startWatchdog()

	rig.reconcile()
	rig.controllerSets(true, true)

	eventually(t, "resume after a failed take", func() bool {
		return !rig.kernel.isSuspended() && rig.node().Failed && !rig.node().SuspendIOAcked
	})
}

// A take that hangs in the kernel, deaf to its context, cannot hold the
// window open: the deadline resumes I/O, fails the snapshot, and the
// take's late success is not reported.
func TestSnapshotWindowResumesAtTheDeadlineWhenTheTakeHangs(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.r.timing.windowDeadline = 300 * time.Millisecond
	rig.provider.hang = make(chan struct{})
	rig.startWatchdog()

	rig.reconcile()
	rig.controllerSets(true, true)

	start := time.Now()

	rig.reconcile()

	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("Reconcile waited %v on a hanging take", took)
	}

	eventually(t, "resume at the deadline", func() bool {
		return !rig.kernel.isSuspended() && rig.node().Failed && !rig.node().SuspendIOAcked
	})

	close(rig.provider.hang)
	time.Sleep(50 * time.Millisecond)

	if rig.node().Ready {
		t.Errorf("a take that returned after the window closed was reported Ready")
	}

	// The node's delete is not held up by the take once it has returned.
	if rig.r.takeInFlight(windowTestSnapshot) {
		t.Errorf("take still in flight after it returned")
	}
}

// A stopping satellite resumes what it suspended.
func TestSnapshotWindowResumesWhenTheSatelliteStops(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	stop := rig.startWatchdog()

	rig.reconcile()

	if !rig.kernel.isSuspended() {
		t.Fatal("not suspended")
	}

	stop()

	if rig.kernel.isSuspended() || rig.markerExists() {
		t.Errorf("after stop: suspended=%t marker=%t", rig.kernel.isSuspended(), rig.markerExists())
	}
}

// A satellite that restarts inside the window, after its .res was wiped
// on startup, resumes by minor from the marker and fails the snapshot.
func TestSnapshotWindowIsResumedByARestartedSatellite(t *testing.T) {
	t.Parallel()

	first := newWindowRig(t)
	first.reconcile()

	if !first.kernel.isSuspended() || !first.markerExists() {
		t.Fatalf("first satellite: suspended=%t marker=%t", first.kernel.isSuspended(), first.markerExists())
	}

	// The first process dies here: no watchdog, no resume.
	first.kernel.set(func() { first.kernel.resFileGone = true })

	second := newWindowRigIn(t, first.stateDir, first.kernel, first.cli)
	second.startWatchdog()

	eventually(t, "resume by the restarted satellite", func() bool {
		return !second.kernel.isSuspended() && !second.markerExists()
	})

	if !slices.Contains(second.kernel.commands(), fmt.Sprintf("drbdsetup resume-io %d", windowTestMinor)) {
		t.Errorf("no resume by minor: %v", second.kernel.commands())
	}

	eventually(t, "Failed and ack cleared", func() bool {
		return second.node().Failed && !second.node().SuspendIOAcked
	})

	// The controller asking for the take now does not reopen anything.
	second.controllerSets(true, true)
	second.reconcile()

	if second.kernel.isSuspended() || second.provider.takeCount() != 0 {
		t.Errorf("restarted satellite took or suspended: suspended=%t takes=%d",
			second.kernel.isSuspended(), second.provider.takeCount())
	}
}

// The window is driven by the watchdog alone. With the Snapshot queue
// never reaching this snapshot again after the suspend, as when it is
// busy with deletes, the take still runs and the resume still comes.
func TestSnapshotWindowDoesNotWaitForTheQueue(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.startWatchdog()

	rig.reconcile()

	rig.controllerSets(true, true)
	eventually(t, "take without a reconcile", func() bool { return rig.node().Ready })

	rig.controllerSets(false, false)
	eventually(t, "resume without a reconcile", func() bool {
		return !rig.kernel.isSuspended() && !rig.node().SuspendIOAcked
	})
}

// The ack is the status of the kernel, not of the command: a resume-io
// that exits 0 without resuming keeps it, a failing one is retried, and
// both reach the Snapshot's events.
func TestSnapshotWindowReportsAResumeOnlyOnceTheKernelConfirmsIt(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.reconcile()
	rig.kernel.set(func() {
		rig.kernel.resumeIgnored = true
	})

	rig.controllerSets(false, false)

	res, err := rig.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: windowTestSnapshot}})
	if err != nil {
		t.Fatal(err)
	}

	if !rig.node().SuspendIOAcked {
		t.Errorf("ack cleared while the kernel still reports the device suspended")
	}

	if res.RequeueAfter == 0 {
		t.Errorf("an unconfirmed resume is not retried: %+v", res)
	}

	select {
	case ev := <-rig.recorder.Events:
		if !strings.Contains(ev, "ResumeIOFailed") {
			t.Errorf("event %q", ev)
		}
	default:
		t.Errorf("no event for an unconfirmed resume")
	}

	rig.kernel.set(func() {
		rig.kernel.resumeIgnored = false
		rig.kernel.resumeFailures = 2
	})

	rig.startWatchdog()
	eventually(t, "resume after retries", func() bool {
		return !rig.kernel.isSuspended() && !rig.node().SuspendIOAcked
	})
}
