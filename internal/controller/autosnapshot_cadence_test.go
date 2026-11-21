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

package controller_test

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestAutoSnapshotRunEveryOneTakesOnePerMinute drives the runnable the
// way the production ticker does, one Tick per AutoSnapshotPeriod with
// up to half a second of delivery jitter either way, and expects one
// auto-snapshot per RunEvery minute, as LINSTOR takes them. A poll as
// long as the interval, or an interval counted from the poll that
// noticed the deadline rather than from the deadline, both lose
// snapshots: RunEvery 1 then took one every two minutes on a cluster.
func TestAutoSnapshotRunEveryOneTakesOnePerMinute(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-cadence"

	cli := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(makeRDWithAutoSnapshot(t, rdName, 1, "100")).
		Build()

	start := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	clk := &stubClock{t: start}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	// Thirty minutes and a half: the slots at 0, 1, ..., 30 minutes.
	horizon := 30*time.Minute + 30*time.Second

	var first time.Time

	for k := 0; time.Duration(k)*controllerpkg.AutoSnapshotPeriod <= horizon; k++ {
		jitter := time.Duration((k*389+500)%1000-500) * time.Millisecond
		clk.t = start.Add(time.Duration(k)*controllerpkg.AutoSnapshotPeriod + jitter)

		if k == 0 {
			first = clk.t
		}

		err := r.Tick(context.Background())
		if err != nil {
			t.Fatalf("Tick #%d: %v", k, err)
		}

		// Every slot reached so far is taken by the first poll at or
		// after it, not one poll later.
		since := clk.t.Sub(first)

		want := int(since/time.Minute) + 1
		if got := len(listAutoSnapshotsByRD(t, cli, rdName)); got != want {
			t.Fatalf("auto-snapshots %v after the first = %d, want %d", since, got, want)
		}
	}

	if got := len(listAutoSnapshotsByRD(t, cli, rdName)); got != 31 {
		t.Errorf("auto-snapshots in 30 minutes of RunEvery 1 = %d, want 31", got)
	}
}

// TestAutoSnapshotAfterDowntimeTakesOneNotABurst: a definition whose
// last auto-snapshot is hours old, as after the controller was down,
// gets one snapshot and the next one a full RunEvery later, not one
// per poll for every slot it missed: a catch-up burst would only
// rotate the older snapshots out of the keep budget.
func TestAutoSnapshotAfterDowntimeTakesOneNotABurst(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-cadence-downtime"

	start := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rd := makeRDWithAutoSnapshot(t, rdName, 1, "100")
	rd.Annotations = map[string]string{
		controllerpkg.AnnotationAutoSnapshotLastAt: start.Add(-3 * time.Hour).Format(time.RFC3339Nano),
	}

	cli := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(rd).Build()

	clk := &stubClock{t: start}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	// Polls up to just short of a minute after the first one.
	for k := 0; time.Duration(k)*controllerpkg.AutoSnapshotPeriod < time.Minute; k++ {
		clk.t = start.Add(time.Duration(k) * controllerpkg.AutoSnapshotPeriod)

		err := r.Tick(context.Background())
		if err != nil {
			t.Fatalf("Tick #%d: %v", k, err)
		}
	}

	if got := len(listAutoSnapshotsByRD(t, cli, rdName)); got != 1 {
		t.Errorf("auto-snapshots in the first minute after downtime = %d, want 1", got)
	}
}
