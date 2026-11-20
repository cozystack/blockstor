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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

var errNoSuchResource = errors.New("pvc-a: No such resource")

// quorumSusp is the suspension reasons a status reports.
type quorumSusp struct {
	user, noData, fencing, quorum bool
}

// quorumStatusJSON is `drbdsetup status <res> --json` in drbd-utils' own
// layout, for a resource with the given suspension reasons and one device
// per entry of quorum ("" leaves the key out).
func quorumStatusJSON(s quorumSusp, devices ...string) string {
	devs := make([]string, 0, len(devices))

	for i, q := range devices {
		key := ""
		if q != "" {
			key = fmt.Sprintf(",\n      \"quorum\": %s", q)
		}

		devs = append(devs, fmt.Sprintf("    {\n      \"volume\": %d,\n      \"minor\": %d,\n      \"disk-state\": \"UpToDate\",\n"+
			"      \"client\": false,\n      \"open\": false%s\n    }", i, 20055+i, key))
	}

	return fmt.Sprintf("[\n{\n  \"name\": \"pvc-a\",\n  \"node-id\": 2,\n  \"role\": \"Primary\",\n"+
		"  \"suspended\": %t,\n  \"suspended-user\": %t,\n  \"suspended-no-data\": %t,\n"+
		"  \"suspended-fencing\": %t,\n  \"suspended-quorum\": %t,\n  \"force-io-failures\": false,\n"+
		"  \"write-ordering\": \"flush\",\n  \"devices\": [\n%s\n  ],\n  \"connections\": []\n}\n]\n",
		s.user || s.noData || s.fencing || s.quorum, s.user, s.noData, s.fencing, s.quorum, strings.Join(devs, ",\n"))
}

// quorumShowJSON is `drbdsetup show <res> --json` in drbd-utils' layout,
// which leaves options at their default out: the resource options given as
// JSON members, and one connection per fencing policy ("" for none set).
func quorumShowJSON(options string, fencing ...string) string {
	conns := make([]string, 0, len(fencing))

	for i, f := range fencing {
		net := ""
		if f != "" {
			net = fmt.Sprintf(",\n      \"net\": {\n        \"fencing\": %q\n      }", f)
		}

		conns = append(conns, fmt.Sprintf("    {\n      \"_peer_node_id\": %d,\n      \"_cstate\": \"Connected\"%s\n    }", i, net))
	}

	opts := ""
	if options != "" {
		opts = "  \"options\": {\n    " + options + "\n  },\n"
	}

	return "[\n{\n  \"resource\": \"pvc-a\",\n" + opts +
		"  \"_this_host\": {\n    \"node-id\": 2,\n    \"volumes\": []\n  },\n" +
		"  \"connections\": [\n" + strings.Join(conns, ",\n") + "\n  ]\n}\n]\n"
}

const (
	suspendIOOptions = `"quorum": "majority", "on-no-quorum": "suspend-io"`
	ioErrorOptions   = `"quorum": "majority", "on-no-quorum": "io-error"`
)

// After a volume is removed the replica that removed it last is left with a
// stale quorum verdict while every volume it still has reports quorum: with
// on-no-quorum suspend-io the resource stays suspended on quorum, with
// io-error a Primary's requests fail with EIO although nothing shows as
// suspended. resume-io has DRBD recompute it. A suspension another reason
// also holds, a volume that has really lost quorum, and a resource that
// fences peers or suspends on no data, where resume-io could lift a
// suspension landing after the status read, are left to DRBD.
func TestRecheckQuorumAfterVolumeRemoval(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		status    string
		statusErr error
		show      string
		showErr   error
		resumeErr error
		want      drbd.QuorumRecheck
		wantShow  bool
		wantErr   bool
	}{
		{
			name:     "suspend-io: suspended on quorum, the remaining volume has quorum",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions, "", ""),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
		},
		{
			name:     "suspend-io: not suspended",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON(suspendIOOptions, "", ""),
			wantShow: true,
		},
		{
			name:     "io-error: nothing suspended, the cached verdict may be stale",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON(ioErrorOptions, "", ""),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
		},
		{
			name:     "io-error as the default on-no-quorum",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON(`"quorum": "majority"`, "", ""),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
		},
		{
			name:     "quorum off",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON("", "", ""),
			wantShow: true,
		},
		{
			name:   "a snapshot holds the resource suspended too",
			status: quorumStatusJSON(quorumSusp{user: true, quorum: true}, "true"),
		},
		{
			name:   "suspended on no data too",
			status: quorumStatusJSON(quorumSusp{noData: true, quorum: true}, "true"),
		},
		{
			name:   "suspended on fencing too",
			status: quorumStatusJSON(quorumSusp{fencing: true, quorum: true}, "true"),
		},
		{
			name:     "a connection fences its peer",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions, "", "resource-and-stonith"),
			want:     drbd.QuorumRecheckUnsafe,
			wantShow: true,
		},
		{
			name:     "fencing set to its default",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions, "dont-care"),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
		},
		{
			name:     "io-error with fencing",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON(ioErrorOptions, "resource-only"),
			want:     drbd.QuorumRecheckUnsafe,
			wantShow: true,
		},
		{
			name:     "suspends on no data",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions+`, "on-no-data-accessible": "suspend-io"`, ""),
			want:     drbd.QuorumRecheckUnsafe,
			wantShow: true,
		},
		{
			name:   "a remaining volume has no quorum",
			status: quorumStatusJSON(quorumSusp{quorum: true}, "true", "false"),
		},
		{
			name:   "quorum not reported",
			status: quorumStatusJSON(quorumSusp{quorum: true}, ""),
		},
		{
			name:   "no volume left",
			status: quorumStatusJSON(quorumSusp{quorum: true}),
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
		{
			name:     "show fails",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			showErr:  errFakeFailure,
			wantShow: true,
			wantErr:  true,
		},
		{
			name:      "resume-io fails",
			status:    quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:      quorumShowJSON(suspendIOOptions, ""),
			resumeErr: errFakeFailure,
			wantShow:  true,
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status pvc-a --json", storage.FakeResponse{Stdout: []byte(tc.status), Err: tc.statusErr})
			fx.Expect("drbdsetup show pvc-a --json", storage.FakeResponse{Stdout: []byte(tc.show), Err: tc.showErr})
			fx.Expect("drbdsetup resume-io 20055", storage.FakeResponse{Err: tc.resumeErr})

			got, err := drbd.NewAdm(fx).RecheckQuorumAfterVolumeRemoval(t.Context(), "pvc-a")
			if (err != nil) != tc.wantErr {
				t.Fatalf("RecheckQuorumAfterVolumeRemoval: %v, want error %t", err, tc.wantErr)
			}

			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}

			lines := fx.CommandLines()

			if ran := slices.Contains(lines, "drbdsetup show pvc-a --json"); ran != tc.wantShow {
				t.Errorf("show run %t, want %t; commands %q", ran, tc.wantShow, lines)
			}

			wantResume := tc.want == drbd.QuorumRecheckRan || tc.resumeErr != nil
			if ran := slices.Contains(lines, "drbdsetup resume-io 20055"); ran != wantResume {
				t.Errorf("resume-io run %t, want %t; commands %q", ran, wantResume, lines)
			}
		})
	}
}
