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
	"testing"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestSplitByDiskless: replicas with the DISKLESS flag land in the
// diskless bucket, everything else in diskful. Pins the per-flag
// classification — a regression that swapped which slice a Resource
// lands in would silently flip the quorum decision for every RD.
func TestSplitByDiskless(t *testing.T) {
	t.Parallel()

	replicas := []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"}, // diskful
		{Name: "pvc-1", NodeName: "n2"}, // diskful
		{
			Name:     "pvc-1",
			NodeName: "n3",
			Flags:    []string{apiv1.ResourceFlagDiskless},
		},
		{
			Name:     "pvc-1",
			NodeName: "n4",
			Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		},
	}

	diskful, diskless := controllerpkg.SplitByDiskless(replicas)

	if len(diskful) != 2 {
		t.Errorf("diskful: got %d, want 2; %+v", len(diskful), diskful)
	}

	if len(diskless) != 2 {
		t.Errorf("diskless: got %d, want 2; %+v", len(diskless), diskless)
	}

	for _, r := range diskful {
		for _, f := range r.Flags {
			if f == apiv1.ResourceFlagDiskless {
				t.Errorf("diskful slice contains DISKLESS-flagged replica %s", r.NodeName)
			}
		}
	}
}

// TestSplitByDisklessEmpty: empty input → empty slices, no panic.
// Pins the auto-promote path that gets called before any replica
// is created (during the very first reconcile).
func TestSplitByDisklessEmpty(t *testing.T) {
	t.Parallel()

	diskful, diskless := controllerpkg.SplitByDiskless(nil)
	if len(diskful) != 0 || len(diskless) != 0 {
		t.Errorf("nil input: got diskful=%d diskless=%d, want 0/0",
			len(diskful), len(diskless))
	}
}

// TestFilterTieBreaker: only TIE_BREAKER-flagged DISKLESS replicas
// surface — regular DISKLESS replicas (operator-added or auto-
// diskful candidates) stay in the input slice. Pins the witness-
// remove decision: drop only auto-witnesses, never user-supplied
// diskless replicas.
func TestFilterTieBreaker(t *testing.T) {
	t.Parallel()

	diskless := []apiv1.Resource{
		{
			Name:     "pvc-1",
			NodeName: "n1",
			Flags:    []string{apiv1.ResourceFlagDiskless}, // user-added, NOT a witness
		},
		{
			Name:     "pvc-1",
			NodeName: "n2",
			Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		},
		{
			Name:     "pvc-1",
			NodeName: "n3",
			Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		},
	}

	got := controllerpkg.FilterTieBreaker(diskless)

	if len(got) != 2 {
		t.Fatalf("got %d witnesses, want 2", len(got))
	}

	for _, r := range got {
		hasWitness := false
		for _, f := range r.Flags {
			if f == apiv1.ResourceFlagTieBreaker {
				hasWitness = true
				break
			}
		}
		if !hasWitness {
			t.Errorf("filter returned non-witness replica %s", r.NodeName)
		}
	}
}

// TestFilterTieBreakerNoWitnesses: a list of regular DISKLESS
// replicas (no TIE_BREAKER) must yield an empty result so the
// witness-create decision sees zero witnesses.
func TestFilterTieBreakerNoWitnesses(t *testing.T) {
	t.Parallel()

	diskless := []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1", Flags: []string{apiv1.ResourceFlagDiskless}},
		{Name: "pvc-1", NodeName: "n2", Flags: []string{apiv1.ResourceFlagDiskless}},
	}

	got := controllerpkg.FilterTieBreaker(diskless)
	if len(got) != 0 {
		t.Errorf("got %d, want 0 (no TIE_BREAKER flagged); %+v", len(got), got)
	}
}
