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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestPoolMissingBackoff pins the Bug 358 exponential back-off: a
// freshly-stamped PoolMissing condition requeues at the short initial
// interval (so the common CDP-create race resolves fast), while a
// long-missing pool ramps to the cap instead of spinning at a fixed
// interval — the fixed spin was the apiserver write flood that starved
// the events2 diskState observer.
func TestPoolMissingBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		age     time.Duration
		wantMin time.Duration
		wantMax time.Duration
	}{
		{"fresh", 0, poolMissingInitialRequeue, poolMissingInitialRequeue},
		{"after-3s", 3 * time.Second, 4 * time.Second, poolMissingRequeue},
		{"long-missing", 5 * time.Minute, poolMissingRequeue, poolMissingRequeue},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cond := &metav1.Condition{
				LastTransitionTime: metav1.NewTime(time.Now().Add(-tc.age)),
			}

			got := poolMissingBackoff(cond)

			if got < tc.wantMin || got > tc.wantMax {
				t.Errorf("poolMissingBackoff(age=%v) = %v, want in [%v, %v]",
					tc.age, got, tc.wantMin, tc.wantMax)
			}

			if got > poolMissingRequeue {
				t.Errorf("poolMissingBackoff exceeded cap: got %v, cap %v", got, poolMissingRequeue)
			}
		})
	}
}

// TestPoolMissingBackoffNilCondition guards the defensive nil path —
// a nil condition returns the initial interval rather than panicking.
func TestPoolMissingBackoffNilCondition(t *testing.T) {
	t.Parallel()

	if got := poolMissingBackoff(nil); got != poolMissingInitialRequeue {
		t.Errorf("poolMissingBackoff(nil) = %v, want %v", got, poolMissingInitialRequeue)
	}
}
