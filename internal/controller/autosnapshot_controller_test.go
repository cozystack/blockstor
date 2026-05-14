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

package controller_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// stubClock is a deterministic clock for the auto-snapshot tests.
// Advance the time by reassigning the embedded time.Time — no
// concurrency guards because every test is single-goroutined.
type stubClock struct {
	t time.Time
}

func (s *stubClock) Now() time.Time { return s.t }

func (s *stubClock) advance(d time.Duration) {
	s.t = s.t.Add(d)
}

// makeRDWithAutoSnapshot is the common fixture builder — a single
// RD with `AutoSnapshot/RunEvery` set to the given minutes value and
// (optionally) a Keep override.
func makeRDWithAutoSnapshot(t *testing.T, name string, runEveryMinutes int, keep string) *blockstoriov1alpha1.ResourceDefinition {
	t.Helper()

	props := map[string]string{
		controllerpkg.PropAutoSnapshotRunEvery: fmt.Sprintf("%d", runEveryMinutes),
	}

	if keep != "" {
		props[controllerpkg.PropAutoSnapshotKeep] = keep
	}

	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: props,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1048576},
			},
		},
	}
}

// listAutoSnapshotsByRD returns the auto-snapshot CRDs labelled for
// the given RD, sorted by snapshot name for stable assertions.
func listAutoSnapshotsByRD(t *testing.T, cli client.Client, rdName string) []blockstoriov1alpha1.Snapshot {
	t.Helper()

	var snaps blockstoriov1alpha1.SnapshotList

	err := cli.List(context.Background(), &snaps, client.MatchingLabels{
		"blockstor.io/resource-definition": rdName,
		controllerpkg.LabelAutoSnapshot:    "true",
	})
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}

	sort.Slice(snaps.Items, func(i, j int) bool {
		return snaps.Items[i].Spec.SnapshotName < snaps.Items[j].Spec.SnapshotName
	})

	return snaps.Items
}

// TestAutoSnapshotFirstTickCreatesOne: RunEvery=15, no existing
// state — the first Tick allocates id=1 and creates `auto-snap-00001`.
// Pins the cadence semantic the scenario doc spells out ("set
// RunEvery=15, advance past one interval → one snapshot").
func TestAutoSnapshotFirstTickCreatesOne(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-w05-first", 15, "")

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rd).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	snaps := listAutoSnapshotsByRD(t, cli, "pvc-w05-first")
	if len(snaps) != 1 {
		t.Fatalf("expected 1 auto-snapshot, got %d", len(snaps))
	}

	if got := snaps[0].Spec.SnapshotName; got != "auto-snap-00001" {
		t.Errorf("snapshot name = %q, want auto-snap-00001", got)
	}

	if got := snaps[0].Spec.ResourceDefinitionName; got != "pvc-w05-first" {
		t.Errorf("snapshot RD = %q, want pvc-w05-first", got)
	}

	if got := snaps[0].Labels[controllerpkg.LabelAutoSnapshot]; got != "true" {
		t.Errorf("auto-snapshot label = %q, want true", got)
	}

	// RD bookkeeping should advance: NextAutoId=2, last-at stamped.
	var updated blockstoriov1alpha1.ResourceDefinition
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-w05-first"}, &updated); err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if got := updated.Spec.Props[controllerpkg.PropAutoSnapshotNextID]; got != "2" {
		t.Errorf("NextAutoId = %q, want 2", got)
	}

	if updated.Annotations[controllerpkg.AnnotationAutoSnapshotLastAt] == "" {
		t.Errorf("last-at annotation not stamped")
	}
}

// TestAutoSnapshotFiveIntervalsProduceFiveSnapshots: the scenario
// doc's integration assertion — RunEvery=15, advance past 5
// intervals, expect 5 auto-snapshots with sequential ids.
func TestAutoSnapshotFiveIntervalsProduceFiveSnapshots(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-w05-five", 15, "")

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rd).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	for i := 0; i < 5; i++ {
		err := r.Tick(context.Background())
		if err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}
		// Advance just past 15 minutes so the next tick is due.
		clk.advance(15*time.Minute + time.Second)
	}

	snaps := listAutoSnapshotsByRD(t, cli, "pvc-w05-five")
	if len(snaps) != 5 {
		t.Fatalf("expected 5 auto-snapshots, got %d", len(snaps))
	}

	for i, snap := range snaps {
		want := fmt.Sprintf("auto-snap-%05d", i+1)
		if snap.Spec.SnapshotName != want {
			t.Errorf("snap[%d].name = %q, want %q", i, snap.Spec.SnapshotName, want)
		}
	}
}

// TestAutoSnapshotTickBeforeIntervalIsNoOp: when the last-at
// annotation is fresh enough, the next Tick does nothing — pins the
// cadence gate so a 1-minute poll loop doesn't burst-create on every
// pass.
func TestAutoSnapshotTickBeforeIntervalIsNoOp(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-w05-noop", 15, "")

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rd).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	// First Tick fires once.
	if err := r.Tick(context.Background()); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}

	// Advance only 5 minutes — still inside the 15-minute window.
	clk.advance(5 * time.Minute)

	if err := r.Tick(context.Background()); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}

	snaps := listAutoSnapshotsByRD(t, cli, "pvc-w05-noop")
	if len(snaps) != 1 {
		t.Errorf("expected 1 auto-snapshot, got %d", len(snaps))
	}
}

// TestAutoSnapshotKeepPrunesOldestBeyondBudget: the scenario doc's
// retention invariant — RunEvery=15, Keep=3, advance past 5
// intervals. The 5 auto-snapshots get created, the oldest 2 get
// pruned, leaving the 3 newest.
func TestAutoSnapshotKeepPrunesOldestBeyondBudget(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-w05-keep", 15, "3")

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rd).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	for i := 0; i < 5; i++ {
		if err := r.Tick(context.Background()); err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}

		clk.advance(15*time.Minute + time.Second)
	}

	snaps := listAutoSnapshotsByRD(t, cli, "pvc-w05-keep")
	if len(snaps) != 3 {
		t.Fatalf("expected 3 auto-snapshots after prune, got %d", len(snaps))
	}

	// Survivors should be the three newest ids: 3, 4, 5.
	wantNames := []string{"auto-snap-00003", "auto-snap-00004", "auto-snap-00005"}

	gotNames := make([]string, 0, len(snaps))
	for _, snap := range snaps {
		gotNames = append(gotNames, snap.Spec.SnapshotName)
	}

	for i := range wantNames {
		if gotNames[i] != wantNames[i] {
			t.Errorf("survivor[%d] = %q, want %q", i, gotNames[i], wantNames[i])
		}
	}
}

// TestAutoSnapshotManualSnapshotsNotPruned: scenario doc explicitly
// states "Manually-created snapshots NOT counted against the keep
// budget". Verify by seeding three manual snapshots (no
// auto-snapshot label) and letting auto-snapshots accumulate beyond
// the Keep=2 budget — the manual ones survive.
func TestAutoSnapshotManualSnapshotsNotPruned(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-w05-manual", 15, "2")

	manual := func(suffix string) *blockstoriov1alpha1.Snapshot {
		return &blockstoriov1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pvc-w05-manual." + suffix,
				Labels: map[string]string{
					"blockstor.io/resource-definition": "pvc-w05-manual",
					"blockstor.io/snapshot-name":       suffix,
					// Intentionally NO LabelAutoSnapshot.
				},
			},
			Spec: blockstoriov1alpha1.SnapshotSpec{
				ResourceDefinitionName: "pvc-w05-manual",
				SnapshotName:           suffix,
			},
		}
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rd, manual("nightly"), manual("preupgrade")).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	for i := 0; i < 4; i++ {
		if err := r.Tick(context.Background()); err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}

		clk.advance(15*time.Minute + time.Second)
	}

	// Auto-snapshots: Keep=2 → only 2 survivors.
	auto := listAutoSnapshotsByRD(t, cli, "pvc-w05-manual")
	if len(auto) != 2 {
		t.Errorf("expected 2 auto-snapshots after prune, got %d", len(auto))
	}

	// Manual snapshots should still both be present — list them by
	// their distinct snapshot-name label.
	var allSnaps blockstoriov1alpha1.SnapshotList
	if err := cli.List(context.Background(), &allSnaps, client.MatchingLabels{
		"blockstor.io/resource-definition": "pvc-w05-manual",
	}); err != nil {
		t.Fatalf("list all snaps: %v", err)
	}

	manualSurvivors := 0
	for _, snap := range allSnaps.Items {
		if _, isAuto := snap.Labels[controllerpkg.LabelAutoSnapshot]; isAuto {
			continue
		}

		manualSurvivors++
	}

	if manualSurvivors != 2 {
		t.Errorf("manual snapshot survivors = %d, want 2", manualSurvivors)
	}
}

// TestAutoSnapshotRunEveryDisabledSkipsRD: RunEvery absent (or <=0
// per the OpenAPI doc) means "disabled" — the runnable must not
// touch the RD. Pins the per-RD opt-in semantic so unrelated RDs in
// the cluster don't suddenly start accreting snapshots.
func TestAutoSnapshotRunEveryDisabledSkipsRD(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	// No AutoSnapshot/RunEvery at all.
	plainRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-w05-plain"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{"Foo": "bar"},
		},
	}

	// RunEvery=0 → upstream OpenAPI maps to "disabled".
	zeroRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-w05-zero"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				controllerpkg.PropAutoSnapshotRunEvery: "0",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(plainRD, zeroRD).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	for i := 0; i < 3; i++ {
		if err := r.Tick(context.Background()); err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}

		clk.advance(time.Hour)
	}

	var allSnaps blockstoriov1alpha1.SnapshotList
	if err := cli.List(context.Background(), &allSnaps); err != nil {
		t.Fatalf("list snaps: %v", err)
	}

	if len(allSnaps.Items) != 0 {
		t.Errorf("expected no snapshots on disabled RDs, got %d", len(allSnaps.Items))
	}
}

// TestAutoSnapshotKeepZeroDisablesPrune: the OpenAPI doc says
// "Removing this property or having a value <= 0 disables
// auto-cleanup, all auto-snapshots will be kept". Pin that
// invariant — Keep=0 means never prune.
func TestAutoSnapshotKeepZeroDisablesPrune(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-w05-keep0", 15, "0")

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rd).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	// 15 ticks across 15 intervals — well past the default Keep=10.
	for i := 0; i < 15; i++ {
		if err := r.Tick(context.Background()); err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}

		clk.advance(15*time.Minute + time.Second)
	}

	snaps := listAutoSnapshotsByRD(t, cli, "pvc-w05-keep0")
	if len(snaps) != 15 {
		t.Errorf("expected 15 auto-snapshots (cleanup disabled), got %d", len(snaps))
	}
}

// TestAutoSnapshotDefaultKeepIs10: when Keep is absent on the RD,
// the runnable defaults to 10 — matches upstream's
// `DFLT_AUTO_SNAPSHOT_KEEP="10"` constant.
func TestAutoSnapshotDefaultKeepIs10(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-w05-default", 15, "")

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rd).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	// 12 ticks → 12 created, 2 pruned, 10 retained.
	for i := 0; i < 12; i++ {
		if err := r.Tick(context.Background()); err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}

		clk.advance(15*time.Minute + time.Second)
	}

	snaps := listAutoSnapshotsByRD(t, cli, "pvc-w05-default")
	if len(snaps) != controllerpkg.DefaultAutoSnapshotKeep {
		t.Errorf("expected %d auto-snapshots, got %d",
			controllerpkg.DefaultAutoSnapshotKeep, len(snaps))
	}
}

// TestAutoSnapshotSkipsDeletingRD: an RD with a DeletionTimestamp
// is on its way out — creating a fresh snapshot on it would block
// the delete on the new snapshot's own finalizer chain. Pin the
// skip so operator-initiated deletes complete cleanly.
func TestAutoSnapshotSkipsDeletingRD(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rd := makeRDWithAutoSnapshot(t, "pvc-w05-deleting", 15, "")
	// Stamp a DeletionTimestamp. Need a finalizer for the fake
	// client to honour the deletion timestamp (otherwise the
	// builder collapses Delete on creation).
	now := metav1.Now()
	rd.DeletionTimestamp = &now
	rd.Finalizers = []string{"test/keep"}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rd).
		Build()

	clk := &stubClock{t: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	if err := r.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	snaps := listAutoSnapshotsByRD(t, cli, "pvc-w05-deleting")
	if len(snaps) != 0 {
		t.Errorf("expected 0 auto-snapshots on deleting RD, got %d", len(snaps))
	}
}
