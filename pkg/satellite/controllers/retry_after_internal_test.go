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
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
)

// A resize request waits for every replica to grow, so a grow still
// pending retries on the short timer even when the pass also carries a
// refused create-md, which on its own backs off for minutes.
func TestRetryAfterKeepsAPendingGrowOnTheShortTimer(t *testing.T) {
	t.Parallel()

	refused := &intent.ResourceApplyResult{Name: "pvc-1", Message: refusedMessage + drbd.ErrCreateMDRefused.Error()}
	growing := &intent.ResourceApplyResult{Name: "pvc-1", Ok: true, GrowPending: true}
	failed := &intent.ResourceApplyResult{Name: "pvc-1", Message: "drbdadm adjust: exit status 10"}

	for name, tc := range map[string]struct {
		results   []*intent.ResourceApplyResult
		anyFailed bool
		want      time.Duration
	}{
		"converged":                   {results: []*intent.ResourceApplyResult{{Name: "pvc-1", Ok: true}}, want: 0},
		"grow pending":                {results: []*intent.ResourceApplyResult{growing}, want: applyFailureRequeue},
		"refused create-md only":      {results: []*intent.ResourceApplyResult{refused}, anyFailed: true, want: refusedApplyRequeue},
		"refused create-md and grow":  {results: []*intent.ResourceApplyResult{refused, growing}, anyFailed: true, want: applyFailureRequeue},
		"other failure":               {results: []*intent.ResourceApplyResult{failed}, anyFailed: true, want: applyFailureRequeue},
		"other failure and a refusal": {results: []*intent.ResourceApplyResult{refused, failed}, anyFailed: true, want: applyFailureRequeue},
	} {
		if got := retryAfter(tc.results, tc.anyFailed); got != tc.want {
			t.Errorf("%s: retryAfter = %v, want %v", name, got, tc.want)
		}
	}
}
