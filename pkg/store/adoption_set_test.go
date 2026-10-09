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

package store_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// readBackHook runs hook on the n-th read past the cache, before answering it.
type readBackHook struct {
	store.ResourceDefinitionStore

	reads *atomic.Int32
	hook  func(n int32)
}

func (r readBackHook) GetUncached(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	r.hook(r.reads.Add(1))

	return r.ResourceDefinitionStore.GetUncached(ctx, name) //nolint:wrapcheck // test double
}

type readBackHookStore struct {
	store.Store

	reads *atomic.Int32
	hook  func(n int32)
}

func (s readBackHookStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return readBackHook{ResourceDefinitionStore: s.Store.ResourceDefinitions(), reads: s.reads, hook: s.hook}
}

// Two claims on one leftover the creator is rolling back are both refused,
// and whichever is released first, neither token is left behind: a token that
// outlives its claim stands for nobody and leaves the definition to the
// rollback's yield for good.
func TestTwoRefusedClaimsLeaveNoMarkEitherOrder(t *testing.T) {
	t.Parallel()

	for _, firstReleasesFirst := range []bool{true, false} {
		name := "the later claim is released first"
		if firstReleasesFirst {
			name = "the earlier claim is released first"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name: "pvc-two", Props: map[string]string{store.RollbackAbandonedProp: store.RollbackInProgress},
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			var reads atomic.Int32

			// The first claim's read-back is where the second claim lands:
			// both tokens are on the definition before either is released.
			patched := make(chan struct{})
			firstReleased := make(chan struct{})
			secondDone := make(chan error, 1)

			var st store.Store

			st = readBackHookStore{Store: backend, reads: &reads, hook: func(n int32) {
				if n != 1 {
					if firstReleasesFirst {
						<-firstReleased
					}

					return
				}

				go func() { secondDone <- store.ClaimAdoptedLeftover(ctx, st, "pvc-two", nil) }()

				if firstReleasesFirst {
					// The second claim holds at its read-back until the
					// first is released.
					waitForTokens(ctx, t, backend, "pvc-two", 2)
				} else if err := <-secondDone; err == nil {
					// The second claim went on and released while the
					// first's token was on the definition.
					t.Error("the second claim was not refused")
				}

				close(patched)
			}}

			if err := store.ClaimAdoptedLeftover(ctx, st, "pvc-two", nil); err == nil {
				t.Error("the first claim was not refused")
			}

			<-patched
			close(firstReleased)

			if firstReleasesFirst {
				if err := <-secondDone; err == nil {
					t.Error("the second claim was not refused")
				}
			}

			rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-two")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			if got := rd.Props[store.RestoreAdoptedProp]; got != "" {
				t.Errorf("adoption mark after two refused claims = %q, want none", got)
			}
		})
	}
}

// waitForTokens waits until the adoption mark on rdName holds n claims.
func waitForTokens(ctx context.Context, t *testing.T, st store.Store, rdName string, n int) {
	t.Helper()

	for range 1000 {
		rd, err := st.ResourceDefinitions().Get(ctx, rdName)
		if err == nil && countTokens(rd.Props[store.RestoreAdoptedProp]) == n {
			return
		}

		<-time.After(time.Millisecond)
	}

	t.Fatalf("the adoption mark never held %d claims", n)
}

func countTokens(value string) int {
	if value == "" {
		return 0
	}

	return len(strings.Split(value, ","))
}

// A refused claim leaves the volume record as it found it: the rollback judges
// "finished" by that record, and one left by a claim that answered for nothing
// turns a leftover the rollback spares into one it deletes.
func TestARefusedClaimLeavesTheVolumeRecordAlone(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-record",
		Props: map[string]string{
			store.RestoreVolumesProp:    "0=1024",
			store.RollbackAbandonedProp: store.RollbackInProgress,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-record", NodeName: "node-a"}); err != nil {
		t.Fatalf("seed the replica: %v", err)
	}

	if err := backend.VolumeDefinitions().Create(ctx, "pvc-record",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed the volume: %v", err)
	}

	retaken := &apiv1.Snapshot{VolumeDefinitions: []apiv1.SnapshotVolumeDef{
		{VolumeNumber: 0, SizeKib: 1024}, {VolumeNumber: 1, SizeKib: 2048},
	}}

	if err := store.ClaimAdoptedLeftover(ctx, backend, "pvc-record", retaken); err == nil {
		t.Fatal("the claim was not refused")
	}

	finished, err := store.ReadsAsFinished(ctx, backend, "pvc-record", true)
	if err != nil {
		t.Fatalf("judge the leftover: %v", err)
	}

	if !finished {
		t.Error("a refused claim's volume record turned the finished leftover unfinished")
	}
}

// An accepted claim settles the mark to its own token, so claims that keep
// coming for a finished definition do not grow it, and records the volumes of
// the snapshot it was answered from.
func TestAnAcceptedClaimSettlesTheMarkAndRecordsItsSnapshot(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-settled"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	snap := &apiv1.Snapshot{VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024}}}

	for range 3 {
		if err := store.ClaimAdoptedLeftover(ctx, backend, "pvc-settled", snap); err != nil {
			t.Fatalf("claim: %v", err)
		}
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-settled")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if got := countTokens(rd.Props[store.RestoreAdoptedProp]); got != 1 {
		t.Errorf("adoption mark after three accepted claims holds %d tokens, want 1", got)
	}

	if got := rd.Props[store.RestoreVolumesProp]; got != "0=1024" {
		t.Errorf("volume record = %q, want the claim's snapshot", got)
	}
}

// The CLI door's refusal over a rollback still in progress does not warn of a
// clear handing the definition to a retry the rollback then deletes: the
// rollback checks its own mark before its first delete, so a clear stops it.
func TestTheInProgressRefusalSaysAClearStopsTheRollback(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-inprog",
		Props: map[string]string{
			store.RestoreFromSnapshotProp: "pvc-src:snap-1",
			store.RollbackAbandonedProp:   store.RollbackInProgress,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	snap := &apiv1.Snapshot{
		ResourceName: "pvc-src", Name: "snap-1",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024}},
	}

	_, _, err := store.JudgeRestoreLeftover(ctx, backend, "pvc-inprog", snap)
	if err == nil {
		t.Fatal("a leftover under a rollback in progress was not refused")
	}

	if !strings.Contains(err.Error(), "stops that rollback before it deletes anything") ||
		strings.Contains(err.Error(), "rollback then deletes") {
		t.Errorf("refusal = %q, want it to say a clear stops the rollback", err)
	}
}
