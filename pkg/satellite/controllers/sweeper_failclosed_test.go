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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// The tests in this file pin one rule: the DRBD orphan sweeper tears a
// kernel slot down only on positive proof that blockstor owns it and
// that nobody asked it to stop. Anything it cannot establish (a Node
// it cannot read, an RD list it cannot read, a marker it cannot probe)
// leaves the slot alone. On a node that also runs another DRBD
// manager, a wrong "orphan" verdict downs a live resource of that
// manager; a wrong "keep" verdict only delays cleanup by a cycle.

var errAPIUnavailable = errors.New("apiserver unavailable")

// failClosedSweeper builds a sweeper whose kernel reports one slot,
// pvc-orphan, with no Resource CRD on this node and its `.owned`
// marker in StateDir: a genuine orphan the sweeper downs unless a
// fail-closed guard stops it. StateDir also holds a stale
// `pvc-stale.res` the .res GC would remove, so a test can tell whether
// the cycle ran at all. node == nil leaves the Node CRD out.
func failClosedSweeper(t *testing.T, node *blockstoriov1alpha1.Node, funcs interceptor.Funcs) (*OrphanSweeperRunnable, *storage.FakeExec, string) {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	objs := []client.Object{}
	if node != nil {
		objs = append(objs, node)
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status",
		storage.FakeResponse{Stdout: []byte("pvc-orphan role:Secondary\n  volume:0 disk:Diskless\n")})

	stateDir := ownedStateDir(t, "pvc-orphan")

	stalePath := filepath.Join(stateDir, "pvc-stale.res")

	err := os.WriteFile(stalePath, []byte("resource pvc-stale { }\n"), 0o600)
	if err != nil {
		t.Fatalf("seed stale .res: %v", err)
	}

	sweeper := &OrphanSweeperRunnable{
		Client:   cli,
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
		StateDir: stateDir,
		RDGrace:  -1,
		isLoadedFn: func(_ context.Context, _ string) (bool, error) {
			return false, nil
		},
	}

	return sweeper, fx, stalePath
}

func satelliteNode(annotations map[string]string) *blockstoriov1alpha1.Node {
	return &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1", Annotations: annotations},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}
}

func downCalls(fx *storage.FakeExec) []string {
	var out []string

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdsetup down") || strings.HasPrefix(line, "drbdadm down") {
			out = append(out, line)
		}
	}

	return out
}

// assertCycleSkipped checks that sweepOnce reported an error and did
// nothing: no kernel call of any kind, and the stale .res still there.
func assertCycleSkipped(t *testing.T, sweeper *OrphanSweeperRunnable, fx *storage.FakeExec, stalePath string) {
	t.Helper()

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err == nil {
		t.Errorf("sweepOnce returned nil; a skipped cycle must be reported")
	}

	if calls := fx.CommandLines(); len(calls) != 0 {
		t.Errorf("skipped cycle still ran kernel commands: %v", calls)
	}

	_, statErr := os.Stat(stalePath)
	if statErr != nil {
		t.Errorf("skipped cycle still ran the .res GC: stat %s: %v", stalePath, statErr)
	}
}

// TestSweeperSkipsCycleWhenNodeUnreadable: the opt-out annotation
// lives on the Node, so a Node read error means the opt-out is
// unknown. The sweeper used to log and sweep anyway.
func TestSweeperSkipsCycleWhenNodeUnreadable(t *testing.T) {
	t.Parallel()

	sweeper, fx, stalePath := failClosedSweeper(t,
		satelliteNode(map[string]string{SweeperSkipAnnotation: sweeperSkipValue}),
		interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*blockstoriov1alpha1.Node); ok {
					return errAPIUnavailable
				}

				return c.Get(ctx, key, obj, opts...)
			},
		})

	assertCycleSkipped(t, sweeper, fx, stalePath)
}

// TestSweeperSkipsCycleWhenNodeMissing: a Node that does not exist
// is as unreadable as one the apiserver failed to return; the
// sweeper cannot see an opt-out on it either way.
func TestSweeperSkipsCycleWhenNodeMissing(t *testing.T) {
	t.Parallel()

	sweeper, fx, stalePath := failClosedSweeper(t, nil, interceptor.Funcs{})

	assertCycleSkipped(t, sweeper, fx, stalePath)
}

// TestSweeperSkipAnnotationValues pins how the opt-out value is read:
// every spelling of true skips, false sweeps, and a value that is
// neither is an opt-out the sweeper cannot interpret, so it skips and
// reports instead of reading it as "not opted out".
func TestSweeperSkipAnnotationValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		value     string
		wantDown  bool
		wantError bool
	}{
		{value: "true"},
		{value: "True"},
		{value: "1"},
		{value: "yes", wantError: true},
		{value: "", wantError: true},
		{value: "false", wantDown: true},
	}

	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			t.Parallel()

			sweeper, fx, _ := failClosedSweeper(t,
				satelliteNode(map[string]string{SweeperSkipAnnotation: tc.value}),
				interceptor.Funcs{})

			err := sweeper.sweepOnce(t.Context(), logr.Discard())
			if gotError := err != nil; gotError != tc.wantError {
				t.Errorf("sweepOnce error = %v; want error %v", err, tc.wantError)
			}

			gotDown := slices.Contains(fx.CommandLines(), "drbdsetup down pvc-orphan")
			if gotDown != tc.wantDown {
				t.Errorf("orphan torn down = %v; want %v (calls %v)", gotDown, tc.wantDown, fx.CommandLines())
			}
		})
	}
}

// TestSweeperSkipsCycleWhenRDListFails: without the RD list the grace
// window cannot be applied, and the sweeper used to proceed as if no
// RD were inside it, downing slots the controller was still creating.
func TestSweeperSkipsCycleWhenRDListFails(t *testing.T) {
	t.Parallel()

	sweeper, fx, stalePath := failClosedSweeper(t, satelliteNode(nil), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*blockstoriov1alpha1.ResourceDefinitionList); ok {
				return errAPIUnavailable
			}

			return c.List(ctx, list, opts...)
		},
	})

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err == nil {
		t.Errorf("sweepOnce returned nil; a skipped cycle must be reported")
	}

	if got := downCalls(fx); len(got) != 0 {
		t.Errorf("sweeper downed a slot without the RD grace data: %v", got)
	}

	_, statErr := os.Stat(stalePath)
	if statErr != nil {
		t.Errorf("sweeper ran the .res GC without the RD grace data: stat %s: %v", stalePath, statErr)
	}
}

// TestSweeperDownsGenuineOrphan is the control for the tests above:
// with the Node and the RD list readable, no opt-out and the marker
// present, the same fixture does get its orphan torn down and its
// stale .res collected. Without it every skip test would pass on a
// fixture that never sweeps.
func TestSweeperDownsGenuineOrphan(t *testing.T) {
	t.Parallel()

	sweeper, fx, stalePath := failClosedSweeper(t, satelliteNode(nil), interceptor.Funcs{})

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if !slices.Contains(fx.CommandLines(), "drbdsetup down pvc-orphan") {
		t.Errorf("genuine orphan not torn down; calls %v", fx.CommandLines())
	}

	_, statErr := os.Stat(stalePath)
	if !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("stale .res not collected: stat err %v", statErr)
	}
}
