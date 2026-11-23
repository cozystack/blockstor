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
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// snapshotFlagSuccessful is the status flag of a finished take, the
// value LINSTOR reports for a snapshot that completed.
const snapshotFlagSuccessful = "SUCCESSFUL"

// withCadenceReplicas returns rd followed by one diskful replica of it
// on each of n1 and n2. An auto-snapshot is taken on the definition's
// diskful replicas, and a tick for a definition without one is skipped.
func withCadenceReplicas(rd *blockstoriov1alpha1.ResourceDefinition) []client.Object {
	objs := []client.Object{rd}

	for _, node := range []string{"n1", "n2"} {
		objs = append(objs, &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rd.Name + "." + node},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rd.Name,
				NodeName:               node,
			},
		})
	}

	return objs
}

// finishCadenceTakes records every auto-snapshot of the definition as
// a finished take, the way the satellites leave it, so the next tick is
// not held back by a take that still holds I/O suspended.
func finishCadenceTakes(t *testing.T, cli client.Client, rdName string) {
	t.Helper()

	for _, snap := range listAutoSnapshotsByRD(t, cli, rdName) {
		if slices.Contains(snap.Status.Flags, snapshotFlagSuccessful) {
			continue
		}

		snap.Spec.SuspendIO = false
		snap.Spec.TakeSnapshot = false
		snap.Status.Flags = append(snap.Status.Flags, snapshotFlagSuccessful)

		if err := cli.Update(context.Background(), &snap); err != nil {
			t.Fatalf("finish the take of %s: %v", snap.Name, err)
		}
	}
}

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
		WithObjects(withCadenceReplicas(makeRDWithAutoSnapshot(t, rdName, 1, "100"))...).
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

		finishCadenceTakes(t, cli, rdName)

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

	cli := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(withCadenceReplicas(rd)...).Build()

	clk := &stubClock{t: start}
	r := &controllerpkg.AutoSnapshotRunnable{Client: cli, Clock: clk}

	// Polls up to just short of a minute after the first one.
	for k := 0; time.Duration(k)*controllerpkg.AutoSnapshotPeriod < time.Minute; k++ {
		clk.t = start.Add(time.Duration(k) * controllerpkg.AutoSnapshotPeriod)

		err := r.Tick(context.Background())
		if err != nil {
			t.Fatalf("Tick #%d: %v", k, err)
		}

		finishCadenceTakes(t, cli, rdName)
	}

	if got := len(listAutoSnapshotsByRD(t, cli, rdName)); got != 1 {
		t.Errorf("auto-snapshots in the first minute after downtime = %d, want 1", got)
	}
}
