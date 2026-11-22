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

package drbd_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// The plan of a full adjust tells a connection the operator
// disconnected, which it only connects again, from one the kernel
// dropped over a stale peer configuration, which it reconfigures
// first. The second plan is the one drbd-utils 9.36 printed for a
// replica whose peer node-id 2 had been set up as diskless and turned
// diskful.
func TestPlanReconfiguresPeer(t *testing.T) {
	t.Parallel()

	const res = "pvc-a"

	for _, tc := range []struct {
		name string
		plan []string
		peer int32
		want bool
	}{
		{
			name: "operator disconnect",
			plan: []string{"drbdsetup connect pvc-a 2", "drbdsetup wait-connect-connection pvc-a 2 --wait-after-sb=yes"},
			peer: 2,
			want: false,
		},
		{
			name: "peer set up as diskless",
			plan: []string{"drbdsetup peer-device-options pvc-a 2 0 --set-defaults", "drbdsetup connect pvc-a 2"},
			peer: 2,
			want: true,
		},
		{
			name: "net options of the peer",
			plan: []string{"/usr/sbin/drbdsetup net-options pvc-a 2 --set-defaults --protocol=C", "drbdsetup connect pvc-a 2"},
			peer: 2,
			want: true,
		},
		{
			name: "another peer reconfigured",
			plan: []string{"drbdsetup peer-device-options pvc-a 1 0 --set-defaults", "drbdsetup connect pvc-a 2"},
			peer: 2,
			want: false,
		},
		{
			name: "a minor that reads like the peer",
			plan: []string{"drbdsetup disk-options 2 --set-defaults", "drbdsetup connect pvc-a 2"},
			peer: 2,
			want: false,
		},
		{
			name: "another resource",
			plan: []string{"drbdsetup peer-device-options pvc-b 2 0 --set-defaults"},
			peer: 2,
			want: false,
		},
		{
			name: "empty plan",
			plan: nil,
			peer: 2,
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := drbd.PlanReconfiguresPeer(tc.plan, res, tc.peer); got != tc.want {
				t.Errorf("PlanReconfiguresPeer(%q, %d) = %v, want %v", tc.plan, tc.peer, got, tc.want)
			}
		})
	}
}

// AdjustPlan runs drbdadm's dry run and returns its commands one per
// line, blank lines dropped.
func TestAdjustPlanRunsTheDryRun(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdadm -d adjust pvc-a", storage.FakeResponse{
		Stdout: []byte("drbdsetup peer-device-options pvc-a 2 0 --set-defaults\n\ndrbdsetup connect pvc-a 2\n"),
	})

	plan, err := drbd.NewAdm(fx).AdjustPlan(t.Context(), "pvc-a")
	if err != nil {
		t.Fatalf("AdjustPlan: %v", err)
	}

	want := []string{"drbdsetup peer-device-options pvc-a 2 0 --set-defaults", "drbdsetup connect pvc-a 2"}
	if len(plan) != len(want) || plan[0] != want[0] || plan[1] != want[1] {
		t.Errorf("plan = %q, want %q", plan, want)
	}
}
