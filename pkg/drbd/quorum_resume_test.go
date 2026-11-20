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
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

var errNoSuchResource = errors.New("pvc-a: No such resource")

// quorumStatusJSON is `drbdsetup status <res> --json` in drbd-utils' own
// layout, for a resource with the given suspension reasons and one device
// per entry of quorum ("" leaves the key out).
func quorumStatusJSON(user, quorum bool, devices ...string) string {
	devs := ""

	for i, q := range devices {
		if i > 0 {
			devs += ",\n"
		}

		key := ""
		if q != "" {
			key = fmt.Sprintf(",\n      \"quorum\": %s", q)
		}

		devs += fmt.Sprintf("    {\n      \"volume\": %d,\n      \"minor\": %d,\n      \"disk-state\": \"UpToDate\",\n"+
			"      \"client\": false,\n      \"open\": false%s\n    }", i, 20055+i, key)
	}

	return fmt.Sprintf("[\n{\n  \"name\": \"pvc-a\",\n  \"node-id\": 2,\n  \"role\": \"Secondary\",\n"+
		"  \"suspended\": %t,\n  \"suspended-user\": %t,\n  \"suspended-no-data\": false,\n"+
		"  \"suspended-fencing\": false,\n  \"suspended-quorum\": %t,\n  \"force-io-failures\": false,\n"+
		"  \"write-ordering\": \"flush\",\n  \"devices\": [\n%s\n  ],\n  \"connections\": []\n}\n]\n",
		user || quorum, user, quorum, devs)
}

// After a volume is removed the replica that removed it last is left with
// the resource suspended on quorum while every volume it still has reports
// quorum. Only that suspension is resumed: one that a snapshot's suspend-io
// also holds, or a volume that has really lost quorum, is left to DRBD.
func TestResumeQuorumSuspensionOnlyLiftsAStaleQuorumSuspension(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		status     string
		statusErr  error
		wantResume bool
		wantErr    bool
	}{
		{
			name:       "suspended on quorum, the remaining volume has quorum",
			status:     quorumStatusJSON(false, true, "true"),
			wantResume: true,
		},
		{
			name:   "not suspended",
			status: quorumStatusJSON(false, false, "true"),
		},
		{
			name:   "a snapshot holds the resource suspended too",
			status: quorumStatusJSON(true, true, "true"),
		},
		{
			name:   "a remaining volume has no quorum",
			status: quorumStatusJSON(false, true, "true", "false"),
		},
		{
			name:   "quorum not reported",
			status: quorumStatusJSON(false, true, ""),
		},
		{
			name:   "no volume left",
			status: quorumStatusJSON(false, true),
		},
		{
			name: "nothing reported",
		},
		{
			name:      "resource not loaded",
			statusErr: errNoSuchResource,
		},
		{
			name:      "status fails",
			statusErr: errFakeFailure,
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status pvc-a --json", storage.FakeResponse{Stdout: []byte(tc.status), Err: tc.statusErr})

			resumed, err := drbd.NewAdm(fx).ResumeQuorumSuspension(t.Context(), "pvc-a")
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResumeQuorumSuspension: %v, want error %t", err, tc.wantErr)
			}

			if resumed != tc.wantResume {
				t.Errorf("resumed %t, want %t", resumed, tc.wantResume)
			}

			if got := slices.Contains(fx.CommandLines(), "drbdsetup resume-io 20055"); got != tc.wantResume {
				t.Errorf("resume-io run %t, want %t; commands %q", got, tc.wantResume, fx.CommandLines())
			}
		})
	}
}
