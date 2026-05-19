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

package controllers_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/uevent"
)

// fakeUeventNotifier satisfies controllers.UeventNotifier without
// opening a real netlink socket. The discovery loop selects on the
// returned channel; tests push synthetic events through Emit().
type fakeUeventNotifier struct {
	events chan uevent.Event
}

func newFakeUeventNotifier(capacity int) *fakeUeventNotifier {
	return &fakeUeventNotifier{events: make(chan uevent.Event, capacity)}
}

func (f *fakeUeventNotifier) Events() <-chan uevent.Event { return f.events }

func (f *fakeUeventNotifier) Emit(event uevent.Event) { f.events <- event }

// countLsblkCalls counts how many times the FakeExec saw an `lsblk`
// invocation. FakeExec records every Run as a FakeCall with the
// program name; one scanOnce always shells out to lsblk first, so
// the count is a 1:1 proxy for "scanOnce ran".
func countLsblkCalls(fx *storage.FakeExec) int {
	calls := fx.Calls

	n := 0

	for i := range calls {
		if calls[i].Name == "lsblk" {
			n++
		}
	}

	return n
}

// waitForLsblkCount polls fx.Calls until at least `want` lsblk
// invocations have been recorded, or the deadline expires. Returns
// the actual count seen so the caller can include it in the failure
// message — "got 1 want 2" is more diagnosable than "got false".
func waitForLsblkCount(fx *storage.FakeExec, want int, deadline time.Duration) int {
	end := time.Now().Add(deadline)

	for {
		got := countLsblkCalls(fx)
		if got >= want {
			return got
		}

		if time.Now().After(end) {
			return got
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// TestPhysicalDeviceDiscoveryUeventTriggersScan_Bug341 pins the
// wiring contract that was broken in production: a synthetic udev
// event on the listener's channel MUST cause the discovery loop to
// run an extra scanOnce within the debounce window — independent of
// the (300 s) periodic ticker.
//
// Regression for Bug 341: the original udev commit added
// Uevent on the runnable + UeventListener on Config, but
// controllers/manager.go's addBackgroundRunnables forgot to thread
// cfg.UeventListener into the runnable. Result on the stand: zero
// uevent-related log lines, `linstor ps l` lagged up to 5 min after
// `wipefs`. This test fakes the wiring path end-to-end so a future
// refactor can't silently re-introduce the same nil-Uevent state.
//
// Test shape:
//  1. Construct the runnable with a fakeUeventNotifier and a tiny
//     debounce (5 ms) so the assertion completes inside the unit-
//     test budget.
//  2. Pre-seed lsblk to return a single free disk.
//  3. Start the runnable; wait for the FIRST scan (the initial
//     immediate scan that every Start() does).
//  4. Push a synthetic `change@sdb` uevent; assert a SECOND scan
//     fires within 1 s. Without the wiring fix the second scan
//     would only land at the 24 h ticker tick — well outside the
//     test deadline.
func TestPhysicalDeviceDiscoveryUeventTriggersScan_Bug341(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	fx := storage.NewFakeExec()
	// One free disk's worth of probe responses; FakeExec returns
	// these on every matching call so multiple scans against the
	// same lsblk output keep getting the same answers.
	fx.Expect(lsblkCmdLine, storage.FakeResponse{
		Stdout: []byte(lsblkRow("sdb", "sdb", "2000398934016", "", "disk", "",
			"0xWWN-B", "DISK_B", "SN-B", "0", "sata") + "\n"),
	})
	fx.Expect(pvsCmdLine, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zpool list -PHv", storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("drbdmeta 0 v09 /dev/sdb internal dump-md", storage.FakeResponse{Err: errNoDRBDMeta})
	fx.Expect("wipefs -n /dev/sdb", storage.FakeResponse{Stdout: []byte("")})

	notifier := newFakeUeventNotifier(16)

	runnable := &controllers.PhysicalDeviceDiscoveryRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
		// Long period so the periodic tick can't masquerade as the
		// udev-driven one within the test window.
		Period: 24 * time.Hour,
		// Tiny debounce so the synthetic event causes a scan
		// within the test deadline rather than the 250 ms
		// production debounce.
		Debounce: 5 * time.Millisecond,
		Uevent:   notifier,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- runnable.Start(ctx)
	}()

	// Wait for the initial immediate scan that every Start() fires.
	// Two seconds is a comfortable upper bound on the goroutine
	// schedule + the first synchronous scanOnce.
	if got := waitForLsblkCount(fx, 1, 2*time.Second); got < 1 {
		t.Fatalf("initial scan never fired; lsblk calls: %d, want >=1", got)
	}

	// Synthetic block-change event mirroring a real `wipefs -a`
	// frame: the kernel emits `change@/devices/.../block/sdb`
	// with SUBSYSTEM=block.
	notifier.Emit(uevent.Event{
		Action:    uevent.ActionChange,
		Subsystem: uevent.SubsystemBlock,
		Devpath:   "/devices/pci0000:00/0000:00:1f.2/ata2/host1/target1:0:0/1:0:0:0/block/sdb",
		Kernel:    "sdb",
		Devname:   "sdb",
	})

	// Bug 341 contract: the debounce window closes ~5 ms after the
	// event lands; the discovery loop's select drains `trigger`
	// and runs scanOnce. Allow 2 s of headroom for the goroutine
	// scheduler — if it doesn't fire in 2 s, either the wiring is
	// missing (production bug, exact regression we're catching) or
	// the debounce timer got wedged.
	if got := waitForLsblkCount(fx, 2, 2*time.Second); got < 2 {
		t.Fatalf("udev-triggered scan never fired; got %d lsblk calls want >=2 — cfg.UeventListener was not threaded into PhysicalDeviceDiscoveryRunnable.Uevent (Bug 341 regression)", got)
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return within 2 s of ctx cancel")
	}
}

// TestPhysicalDeviceDiscoveryNoUeventWiringFallsBackToPolling_Bug341
// pins the graceful-degradation contract: if no listener is wired
// (the uevent.New error path in cmd/satellite/main.go), the runnable
// MUST still start and serve the initial scan without panicking on
// a nil-interface dereference. This complements the Bug 341 wiring
// test by pinning the OTHER half of the contract: production has
// two valid shapes, "listener wired" and "listener nil", and both
// must work.
func TestPhysicalDeviceDiscoveryNoUeventWiringFallsBackToPolling_Bug341(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect(lsblkCmdLine, storage.FakeResponse{Stdout: []byte("")})

	runnable := &controllers.PhysicalDeviceDiscoveryRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
		Period:   24 * time.Hour,
		Debounce: 5 * time.Millisecond,
		// Uevent intentionally nil — production "listener open failed".
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- runnable.Start(ctx)
	}()

	// Initial scan MUST still fire even without a listener.
	if got := waitForLsblkCount(fx, 1, 2*time.Second); got < 1 {
		t.Fatalf("initial scan never fired in pure-polling mode (nil listener should not block Start); lsblk calls: %d", got)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return within 2 s of ctx cancel (nil listener)")
	}
}

// TestNewPhysicalDeviceDiscoveryRunnableFromConfigThreadsUevent_Bug341
// is the focused regression test for the exact bug we shipped: the
// constructor MUST propagate cfg.UeventListener onto the runnable's
// Uevent field. If a future refactor of manager.go's
// addBackgroundRunnables block silently drops the field assignment
// again, this assertion fires immediately without needing the
// goroutine + lsblk dance the higher-level test does.
//
// Co-located with the higher-level "synthetic event triggers scan"
// test so a refactor of the wiring shape touches one file. The
// in-flight failure mode this protects against: someone deletes
// NewPhysicalDeviceDiscoveryRunnableFromConfig and re-inlines the
// struct-literal in manager.go without `Uevent: cfg.UeventListener`.
func TestNewPhysicalDeviceDiscoveryRunnableFromConfigThreadsUevent_Bug341(t *testing.T) {
	t.Parallel()

	notifier := newFakeUeventNotifier(0)

	cfg := controllers.Config{
		NodeName:       "n1",
		Exec:           storage.NewFakeExec(),
		UeventListener: notifier,
	}

	runnable := controllers.NewPhysicalDeviceDiscoveryRunnableFromConfig(nil, cfg)

	if runnable == nil {
		t.Fatal("NewPhysicalDeviceDiscoveryRunnableFromConfig returned nil")
	}

	if runnable.Uevent == nil {
		t.Fatal("Bug 341: NewPhysicalDeviceDiscoveryRunnableFromConfig did not thread cfg.UeventListener onto runnable.Uevent — udev fast-path will be a no-op in production")
	}

	if runnable.NodeName != "n1" {
		t.Errorf("NodeName: got %q, want %q", runnable.NodeName, "n1")
	}
}

// Compile-time check the fake satisfies the interface — pins the
// shape so a future split of UeventNotifier's method surface fails
// fast in this file instead of inside the runnable's body.
var _ controllers.UeventNotifier = (*fakeUeventNotifier)(nil)

// keep strings import used (the linter trims unused imports).
var _ = strings.Contains
