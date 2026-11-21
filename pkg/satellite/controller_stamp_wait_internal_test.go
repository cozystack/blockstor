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

package satellite

import (
	"testing"

	"github.com/cockroachdb/errors"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// The apply result carries the gate's error wrapped in the steps that
// ran before it; WaitsForController finds both controller-stamp gates
// in it and nothing else. It goes by the error's identity: a drbdadm
// failure that happens to quote a gate's text is not a wait.
func TestWaitsForControllerRecognisesTheStampGates(t *testing.T) {
	t.Parallel()

	fresh := &intent.DesiredResource{Name: "pvc-1"}

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"node-id", refuseUnresolvedLocalNodeID(fresh), true},
		{"skip-init-sync", refuseUnresolvedSkipInitialSync(fresh, true), true},
		{"port in use", errors.New("drbdadm adjust pvc-1: Failure: (103) Remote address(port) already in use"), false},
		{"gate text only", errors.New(errUnresolvedSkipInitialSync.Error()), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.err == nil {
				t.Fatal("the gate let a fresh replica through")
			}

			err := errors.Wrap(tc.err, "apply pvc-1")
			if got := WaitsForController(err); got != tc.want {
				t.Errorf("WaitsForController(%q) = %v, want %v", err, got, tc.want)
			}

			res := &intent.ResourceApplyResult{Ok: true}
			failApply(res, err)

			if res.Ok || res.Message != err.Error() || res.WaitingForController != tc.want {
				t.Errorf("failApply: Ok=%v Message=%q WaitingForController=%v, want false, %q, %v",
					res.Ok, res.Message, res.WaitingForController, err.Error(), tc.want)
			}
		})
	}
}
