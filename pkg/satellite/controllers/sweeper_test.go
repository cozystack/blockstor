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
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// sweeperFixture wires up the bits every sweeper test needs:
// a fake k8s client preloaded with a set of Resource CRDs, a
// FakeExec that returns canned `drbdsetup status` output, and a
// constructed OrphanSweeperRunnable ready to call sweepOnce on.
//
// Centralises the test boilerplate so the assertions in each
// TestSweeperX function focus on the behaviour under test.
func sweeperFixture(t *testing.T, nodeName string, kernelStatusOut string, resources []*blockstoriov1alpha1.Resource) (*OrphanSweeperRunnable, *storage.FakeExec) {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	objs := make([]client.Object, 0, len(resources)+1)
	// Always include a Node CRD for the satellite so shouldSkip's
	// Get round-trip succeeds. Tests that want the missing-Node
	// path build their own fixture.
	objs = append(objs, &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	})

	for _, r := range resources {
		objs = append(objs, r)
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status", storage.FakeResponse{Stdout: []byte(kernelStatusOut)})

	sweeper := &OrphanSweeperRunnable{
		Client:   cli,
		Adm:      drbd.NewAdm(fx),
		NodeName: nodeName,
	}

	return sweeper, fx
}

// TestSweeperLeavesMatchingResourceAlone pins the core invariant:
// when the kernel has a resource AND a matching Resource CRD
// exists on this node, the sweeper MUST NOT issue a down call.
// A regression here would tear down healthy replicas every sweep
// cycle — the worst possible failure mode for this code.
func TestSweeperLeavesMatchingResourceAlone(t *testing.T) {
	t.Parallel()

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-aaa.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "n1",
			ResourceDefinitionName: "pvc-aaa",
		},
	}

	sweeper, fx := sweeperFixture(t, "n1",
		"pvc-aaa role:Primary\n  volume:0 disk:UpToDate\n",
		[]*blockstoriov1alpha1.Resource{res})

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm down") || strings.HasPrefix(line, "drbdsetup down") {
			t.Errorf("sweeper tore down matching resource: %s; calls=%v", line, fx.CommandLines())
		}
	}
}

// TestSweeperDownsOrphan pins the load-bearing case: kernel
// reports a resource for which no Resource CRD exists on this
// node → `drbdsetup down <rsc>` MUST run. Without this, the
// force-strip aftermath documented in the blockstor_drbd_stuck_state
// recovery skill never resolves automatically.
//
// Issue 288: use `drbdsetup down` (kernel-direct, no .res lookup)
// rather than `drbdadm down`. The sweeper exists to clean up the
// .res-less aftermath; calling drbdadm-down with no .res in
// /etc/drbd.d fails with "not defined in your config" and the
// kernel slot leaks forever, pinning the minor and blocking
// re-creation of any RD that lands on that minor.
func TestSweeperDownsOrphan(t *testing.T) {
	t.Parallel()

	// No Resource CRD for pvc-orphan — sweeper should tear it down.
	sweeper, fx := sweeperFixture(t, "n1",
		"pvc-orphan role:Secondary\n  volume:0 disk:Diskless\n",
		nil)

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	want := "drbdsetup down pvc-orphan"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("sweeper did not down orphan; want %q in %v", want, fx.CommandLines())
	}

	// Regression guard: drbdadm down requires the .res file and
	// fails on the .res-less orphan we're trying to clean up.
	// Calling it here would have leaked the kernel slot forever.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm down") {
			t.Errorf("sweeper used drbdadm down on orphan (needs .res file, would leak slot): %s",
				line)
		}
	}
}

// TestSweeperOnlyConsidersLocalResources pins the per-node scope:
// a Resource CRD for pvc-xxx that lives on node n2 must NOT
// protect the kernel-resident pvc-xxx on node n1 from being
// swept. The DRBD kernel state is per-node — a foreign Resource
// CRD says nothing about whether this node's kernel should still
// hold the resource. A regression that pooled CRDs across nodes
// would leave stuck resources unswept indefinitely after a
// cross-node migration left kernel state behind.
func TestSweeperOnlyConsidersLocalResources(t *testing.T) {
	t.Parallel()

	// pvc-xxx exists as a CRD, but for node n2. From n1's
	// perspective it is an orphan.
	foreign := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-xxx.n2"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "n2",
			ResourceDefinitionName: "pvc-xxx",
		},
	}

	sweeper, fx := sweeperFixture(t, "n1",
		"pvc-xxx role:Secondary\n  volume:0 disk:Diskless\n",
		[]*blockstoriov1alpha1.Resource{foreign})

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	want := "drbdsetup down pvc-xxx"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("sweeper did not down foreign-CRD orphan; want %q in %v", want, fx.CommandLines())
	}
}

// TestSweeperRespectsRateLimit pins the bound on per-cycle
// destruction: with 20 kernel orphans and MaxDownPerCycle=2 the
// sweeper MUST stop after 2 downs and defer the rest to the next
// tick. The bound exists so a pathological state (orphans
// produced faster than they can be cleaned) doesn't burn the
// whole satellite's tick budget on drbdadm and doesn't mask the
// upstream bug producing orphans by silently cleaning up all
// evidence in one pass.
func TestSweeperRespectsRateLimit(t *testing.T) {
	t.Parallel()

	var b strings.Builder

	for i := range 20 {
		fmt.Fprintf(&b, "pvc-orphan-%02d role:Secondary\n  volume:0 disk:Diskless\n\n", i)
	}

	sweeper, fx := sweeperFixture(t, "n1", b.String(), nil)
	sweeper.MaxDownPerCycle = 2

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	var downCount int

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdsetup down ") {
			downCount++
		}
	}

	if downCount != 2 {
		t.Errorf("sweeper rate-limit: got %d downs, want 2; calls=%v", downCount, fx.CommandLines())
	}
}

// TestSweeperSkipAnnotationDisablesSweep pins the operator
// escape hatch: setting the SweeperSkipAnnotation on the local
// Node CRD makes one sweep cycle a no-op even with orphans
// present. Required for manual recovery (Bug 4 scenario) where
// the operator wants the kernel state preserved while they
// export GI / bitmap evidence.
func TestSweeperSkipAnnotationDisablesSweep(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "n1",
			Annotations: map[string]string{SweeperSkipAnnotation: sweeperSkipValue},
		},
		Spec: blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status",
		storage.FakeResponse{Stdout: []byte("pvc-orphan role:Secondary\n")})

	sweeper := &OrphanSweeperRunnable{
		Client:   cli,
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	}

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	// Skip annotation should prevent drbdsetup status from even
	// running (we short-circuit before the kernel-side call) —
	// but the load-bearing assertion is that no down command was
	// issued.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm down") || strings.HasPrefix(line, "drbdsetup down") {
			t.Errorf("sweeper ignored skip annotation: %s", line)
		}
	}
}

// TestSweeperBoundsPerResourceSetupDown pins the load-bearing
// Bug 290 invariant: when one orphan's `drbdsetup down` wedges
// (the DRBD-stuck-state pattern where the kernel netlink call
// hangs forever on a gone peer), the sweeper MUST move past it
// within SetupDownTimeout and try the NEXT orphan rather than
// burning the whole tick budget on the one stuck slot.
//
// The setupDownFn hook simulates the wedge: the first call to
// `pvc-stuck` blocks until its own ctx fires (which the sweeper's
// per-resource timeout will cancel within SetupDownTimeout), then
// returns ctx.Err(). The second orphan `pvc-other` must still get
// a tear-down attempt inside the same sweep cycle.
func TestSweeperBoundsPerResourceSetupDown(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "n1"},
			Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		}).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status", storage.FakeResponse{
		Stdout: []byte("pvc-stuck role:Secondary\n  volume:0 disk:UpToDate\n\n" +
			"pvc-other role:Secondary\n  volume:0 disk:Diskless\n"),
	})

	var stuckCalls, otherCalls atomic.Int32

	sweeper := &OrphanSweeperRunnable{
		Client:           cli,
		Adm:              drbd.NewAdm(fx),
		NodeName:         "n1",
		SetupDownTimeout: 50 * time.Millisecond,
		// Negative RDGrace disables the grace window so the
		// table-driven orphan list lands in tear-down directly.
		RDGrace: -1,
		setupDownFn: func(ctx context.Context, resource string) error {
			switch resource {
			case "pvc-stuck":
				stuckCalls.Add(1)
				// Block until ctx fires — simulates the kernel-stuck
				// hang. The per-resource WithTimeout in callSetupDown
				// must cancel us within SetupDownTimeout.
				<-ctx.Done()

				return ctx.Err()
			case "pvc-other":
				otherCalls.Add(1)

				return nil
			}

			return nil
		},
	}

	start := time.Now()

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	elapsed := time.Since(start)

	if stuckCalls.Load() != 1 {
		t.Errorf("pvc-stuck call count = %d; want 1", stuckCalls.Load())
	}

	if otherCalls.Load() != 1 {
		t.Errorf("pvc-other never got a chance after pvc-stuck wedged; "+
			"the per-resource bound is not being applied (Bug 290 regression). "+
			"otherCalls=%d, elapsed=%s", otherCalls.Load(), elapsed)
	}

	// 50ms timeout + 50ms slack should be plenty. If the test
	// takes more than ~500ms, the per-resource bound is broken
	// (probably reverted to passing the whole tick ctx in).
	if elapsed > 500*time.Millisecond {
		t.Errorf("sweepOnce took %s for one stuck orphan + one healthy one; "+
			"per-resource bound is not being honoured (Bug 290 regression)", elapsed)
	}
}

// TestSweeperRunsImmediatelyOnStart pins the second half of
// Bug 290: the sweeper Start() must fire its first sweep
// immediately rather than after one full Period. On a satellite
// restart that left a leaked kernel slot behind, every extra
// second of latency on the first sweep is a second the next RD
// wanting that minor stays Inconsistent / fails create-md.
func TestSweeperRunsImmediatelyOnStart(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "n1"},
			Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
		}).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status",
		storage.FakeResponse{Stdout: []byte("pvc-orphan role:Secondary\n  volume:0 disk:Diskless\n")})

	swept := make(chan string, 1)

	sweeper := &OrphanSweeperRunnable{
		Client: cli,
		Adm:    drbd.NewAdm(fx),
		// 1h Period: if the first sweep waited for the ticker
		// we'd time out the test long before it fired. The
		// immediate-sweep semantics is the only path that can
		// satisfy the assertion below.
		Period:   time.Hour,
		NodeName: "n1",
		RDGrace:  -1,
		setupDownFn: func(_ context.Context, resource string) error {
			select {
			case swept <- resource:
			default:
			}

			return nil
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})

	go func() {
		_ = sweeper.Start(ctx)
		close(done)
	}()

	select {
	case got := <-swept:
		if got != "pvc-orphan" {
			t.Errorf("first sweep tore down %q; want pvc-orphan", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start() did not run an immediate sweep; ticker-only Start is a Bug 290 regression")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start() did not return after ctx cancel")
	}
}
