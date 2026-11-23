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

package v1_test

import (
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Each placement adds its own hold and removes only its own; the latest
// due one says how long the witness waits, and the ones left behind by
// placements that never removed theirs are dropped on the next write.
func TestPlacementHolds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 22, 6, 49, 34, 0, time.UTC)
	at := func(d time.Duration) string { return now.Add(d).Format(time.RFC3339Nano) }

	first, second, expired := at(time.Minute), at(2*time.Minute), at(-time.Second)

	holds := apiv1.AddPlacementHold(expired, first, now)
	holds = apiv1.AddPlacementHold(holds, second, now)

	if want := first + " " + second; holds != want {
		t.Fatalf("holds = %q, want %q", holds, want)
	}

	until, ok := apiv1.PlacementInProgressUntil(map[string]string{apiv1.PlacementInProgressUntilAnnotation: holds}, now)
	if !ok || !until.Equal(now.Add(2*time.Minute)) {
		t.Errorf("PlacementInProgressUntil = %v, %v; want %v, true", until, ok, now.Add(2*time.Minute))
	}

	holds = apiv1.RemovePlacementHold(holds, second, now)
	if holds != first {
		t.Errorf("after the second is done, holds = %q, want %q", holds, first)
	}

	if holds = apiv1.RemovePlacementHold(holds, first, now); holds != "" {
		t.Errorf("after both are done, holds = %q, want none", holds)
	}

	for _, value := range []string{"", "garbage", expired} {
		if _, ok := apiv1.PlacementInProgressUntil(map[string]string{apiv1.PlacementInProgressUntilAnnotation: value}, now); ok {
			t.Errorf("PlacementInProgressUntil(%q) reports a placement in progress", value)
		}
	}
}
