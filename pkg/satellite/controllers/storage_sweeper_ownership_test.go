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
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Issue 264: after a LINSTOR migration the storage sweeper destroyed
// every pvc-* zvol in the pool that no Resource claimed, LINSTOR's
// leftovers and their snapshots included. These tests pin the decision
// the sweeper now makes for a volume no Resource claims:
//
//	marker  snapshots   verdict
//	yes     none        reap
//	no      any         keep, report UnownedStorageVolume
//	yes     some        keep, report OrphanStorageVolumeHasSnapshots

var errInspect = errors.New("zfs get: pool suspended")

// sweepWithRecorder runs one cycle with a fake recorder and returns the
// Events it raised.
func sweepWithRecorder(t *testing.T, sweeper *StorageOrphanSweeperRunnable) []string {
	t.Helper()

	recorder := events.NewFakeRecorder(64)
	sweeper.Recorder = recorder

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	return drainEvents(recorder)
}

func drainEvents(recorder *events.FakeRecorder) []string {
	var got []string

	for {
		select {
		case e := <-recorder.Events:
			got = append(got, e)
		default:
			return got
		}
	}
}

// eventReasons keeps the "<type> <reason>" head of each Event.
func eventReasons(evs []string) []string {
	out := make([]string, 0, len(evs))

	for _, e := range evs {
		fields := strings.Fields(e)
		if len(fields) >= 2 {
			out = append(out, fields[0]+" "+fields[1])
		}
	}

	return out
}

func TestStorageSweeperOwnershipDecisionTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		marked     bool
		snapshots  []string
		wantReaped bool
		wantEvents []string
	}{
		{name: "marked, no snapshots: reaped", marked: true, wantReaped: true},
		{
			name:       "unmarked: kept and reported",
			wantEvents: []string{"Warning " + StorageSweeperReasonUnowned},
		},
		{
			name:       "unmarked with snapshots: kept and reported as unowned",
			snapshots:  []string{"pre-migration"},
			wantEvents: []string{"Warning " + StorageSweeperReasonUnowned},
		},
		{
			name:       "marked with untracked snapshots: kept and reported",
			marked:     true,
			snapshots:  []string{"pre-migration"},
			wantEvents: []string{"Warning " + StorageSweeperReasonSnapshots},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			provider := &fakeListerProvider{
				volumes:   []storage.VolumeRef{{ResourceName: "pvc-leak", VolumeNumber: 0}},
				marked:    map[string]bool{"pvc-leak": tc.marked},
				snapshots: map[string][]string{"pvc-leak": tc.snapshots},
			}

			evs := sweepWithRecorder(t, storageSweeperFixture(t, "n1", provider, nil, nil))

			if reaped := len(provider.snapshotDeleted()) == 1; reaped != tc.wantReaped {
				t.Errorf("reaped=%v, want %v", reaped, tc.wantReaped)
			}

			if got := eventReasons(evs); !slices.Equal(got, tc.wantEvents) {
				t.Errorf("events=%v, want %v", evs, tc.wantEvents)
			}

			if provider.deleteVolumeCalls != 0 {
				t.Errorf("the sweeper called DeleteVolume, which deletes recursively; it must reap through ReapVolume")
			}
		})
	}
}

// The scenario from the issue: a pool full of pvc-* zvols LINSTOR no
// longer referenced, no opt-out annotation on the Node. Not one may go.
func TestStorageSweeperIssue264AdoptedPoolLeftovers(t *testing.T) {
	t.Parallel()

	vols := make([]storage.VolumeRef, 0, 17)

	for i := range 17 {
		vols = append(vols, storage.VolumeRef{
			ResourceName: fmt.Sprintf("pvc-%08d-0000-0000-0000-000000000000", i),
			VolumeNumber: 0,
		})
	}

	provider := &fakeListerProvider{volumes: vols}
	sweeper := storageSweeperFixture(t, "n1", provider, nil, nil)
	sweeper.MaxDeletePerCycle = -1

	evs := sweepWithRecorder(t, sweeper)

	if got := provider.snapshotDeleted(); len(got) != 0 {
		t.Fatalf("sweeper destroyed %d volumes it has no proof it made: %+v", len(got), got)
	}

	if len(evs) != len(vols) {
		t.Errorf("want one report per volume, got %d: %v", len(evs), evs)
	}
}

func TestStorageSweeperReportsOncePerVolume(t *testing.T) {
	t.Parallel()

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{{ResourceName: "pvc-leftover", VolumeNumber: 0}},
	}
	sweeper := storageSweeperFixture(t, "n1", provider, nil, nil)

	first := sweepWithRecorder(t, sweeper)
	second := sweepWithRecorder(t, sweeper)

	if len(first) != 1 || len(second) != 0 {
		t.Errorf("want one Event over two cycles, got %v then %v", first, second)
	}

	// A new reason for the same volume is news.
	provider.marked = map[string]bool{"pvc-leftover": true}
	provider.snapshots = map[string][]string{"pvc-leftover": {"s1"}}

	third := sweepWithRecorder(t, sweeper)
	if got := eventReasons(third); !slices.Equal(got, []string{"Warning " + StorageSweeperReasonSnapshots}) {
		t.Errorf("want the snapshot report, got %v", third)
	}
}

// The reap rechecks at deletion time; a refusal there is reported
// under the reason it names, and does not count against the budget.
func TestStorageSweeperReportsReapRefusals(t *testing.T) {
	t.Parallel()

	cases := map[error]string{
		storage.ErrNotOwned:     StorageSweeperReasonUnowned,
		storage.ErrHasSnapshots: StorageSweeperReasonSnapshots,
	}

	for refusal, reason := range cases {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()

			provider := &fakeListerProvider{
				volumes: []storage.VolumeRef{{ResourceName: "pvc-leak", VolumeNumber: 0}},
				marked:  map[string]bool{"pvc-leak": true},
				delErr:  errors.Wrap(refusal, "recheck"),
			}

			evs := sweepWithRecorder(t, storageSweeperFixture(t, "n1", provider, nil, nil))

			if got := eventReasons(evs); !slices.Equal(got, []string{"Warning " + reason}) {
				t.Errorf("events=%v, want %s", evs, reason)
			}
		})
	}
}

// A probe that fails is not a verdict: nothing is reaped and nothing
// is reported as unowned on the strength of it.
func TestStorageSweeperInspectFailureKeepsTheVolume(t *testing.T) {
	t.Parallel()

	provider := &fakeListerProvider{
		volumes:    []storage.VolumeRef{{ResourceName: "pvc-leak", VolumeNumber: 0}},
		marked:     map[string]bool{"pvc-leak": true},
		inspectErr: errInspect,
	}

	evs := sweepWithRecorder(t, storageSweeperFixture(t, "n1", provider, nil, nil))

	if got := provider.snapshotDeleted(); len(got) != 0 {
		t.Errorf("reaped on a failed inspection: %+v", got)
	}

	if len(evs) != 0 {
		t.Errorf("reported on a failed inspection: %v", evs)
	}
}

// listOnlyProvider lists volumes but cannot show an ownership marker.
type listOnlyProvider struct {
	storage.Provider

	lister storage.VolumeLister
}

func (p listOnlyProvider) ListVolumeNames(ctx context.Context) ([]storage.VolumeRef, error) {
	return p.lister.ListVolumeNames(ctx) //nolint:wrapcheck // test delegate
}

func TestStorageSweeperNeverReapsWithoutAnOwnershipCheck(t *testing.T) {
	t.Parallel()

	inner := &fakeListerProvider{
		volumes: []storage.VolumeRef{{ResourceName: "pvc-leak", VolumeNumber: 0}},
		marked:  map[string]bool{"pvc-leak": true},
	}

	sweeper := storageSweeperFixture(t, "n1", inner, nil, nil)
	sweeper.Providers = func() map[string]storage.Provider {
		return map[string]storage.Provider{"pool0": listOnlyProvider{Provider: inner, lister: inner}}
	}

	evs := sweepWithRecorder(t, sweeper)

	if got := inner.snapshotDeleted(); len(got) != 0 || inner.deleteVolumeCalls != 0 {
		t.Errorf("reaped through a provider that cannot show the marker: %+v", got)
	}

	if got := eventReasons(evs); !slices.Equal(got, []string{"Warning " + StorageSweeperReasonUnowned}) {
		t.Errorf("events=%v, want %s", evs, StorageSweeperReasonUnowned)
	}
}

// The satellite sweeps before its Node exists on a cold start. A Node
// it cannot read is one whose opt-out it cannot see, so the cycle is
// skipped, even for a volume that would be reaped.
func TestStorageSweeperSkipsWithoutALocalNode(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{{ResourceName: "pvc-leak", VolumeNumber: 0}},
		marked:  map[string]bool{"pvc-leak": true},
	}

	sweeper := &StorageOrphanSweeperRunnable{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Providers: func() map[string]storage.Provider {
			return map[string]storage.Provider{"pool0": provider}
		},
		NodeName: "n1",
	}

	evs := sweepWithRecorder(t, sweeper)

	if got := provider.snapshotDeleted(); len(got) != 0 {
		t.Errorf("swept without a readable Node: %+v", got)
	}

	if len(evs) != 0 {
		t.Errorf("reported without a readable Node: %v", evs)
	}
}

func TestClassifyStorageOrphan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		found storage.VolumeOwnership
		want  storageOrphanVerdict
	}{
		{found: storage.VolumeOwnership{Owned: true}, want: storageReap},
		{found: storage.VolumeOwnership{}, want: storageKeepUnowned},
		{found: storage.VolumeOwnership{Snapshots: []string{"s"}}, want: storageKeepUnowned},
		{found: storage.VolumeOwnership{Owned: true, Snapshots: []string{"s"}}, want: storageKeepSnapshots},
	}

	for _, tc := range cases {
		if got := classifyStorageOrphan(tc.found); got != tc.want {
			t.Errorf("classifyStorageOrphan(%+v)=%v, want %v", tc.found, got, tc.want)
		}
	}
}
