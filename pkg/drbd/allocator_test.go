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

package drbd_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

func TestLowestFreeNodeID_EmptyTakenReturnsZero(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreeNodeID(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

func TestLowestFreeNodeID_PicksGapNotMaxPlusOne(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreeNodeID([]int32{0, 2, 3})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 1 {
		t.Errorf("got %d, want 1 (the gap)", got)
	}
}

func TestLowestFreeNodeID_FillsContiguous(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreeNodeID([]int32{0, 1, 2})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 3 {
		t.Errorf("got %d, want 3", got)
	}
}

func TestLowestFreeNodeID_DeterministicAcrossOrderings(t *testing.T) {
	t.Parallel()

	a, _ := drbd.LowestFreeNodeID([]int32{5, 1, 3})
	b, _ := drbd.LowestFreeNodeID([]int32{3, 1, 5})

	if a != b {
		t.Errorf("non-deterministic: %d vs %d", a, b)
	}

	if a != 0 {
		t.Errorf("got %d, want 0 (lowest gap)", a)
	}
}

func TestLowestFreeNodeID_Exhausted(t *testing.T) {
	t.Parallel()

	taken := make([]int32, drbd.MaxPeers)
	for i := range int32(drbd.MaxPeers) {
		taken[i] = i
	}

	_, err := drbd.LowestFreeNodeID(taken)
	if !errors.Is(err, drbd.ErrNodeIDExhausted) {
		t.Errorf("err: got %v, want ErrNodeIDExhausted", err)
	}
}

func TestLowestFreeNodeID_IgnoresOutOfRange(t *testing.T) {
	t.Parallel()

	// 99 is past MaxPeers; the allocator must not let it block id 0.
	got, err := drbd.LowestFreeNodeID([]int32{99})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}
