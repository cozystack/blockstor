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

	toIOError   = "drbdsetup resource-options pvc-a --on-no-quorum=io-error"
	toSuspendIO = "drbdsetup resource-options pvc-a --on-no-quorum=suspend-io"
)

// resourceOptionsLines is the on-no-quorum changes among the commands run.
func resourceOptionsLines(lines []string) []string {
	var out []string

	for _, line := range lines {
		if strings.HasPrefix(line, "drbdsetup resource-options ") {
			out = append(out, line)
		}
	}

	return out
}

// After a volume is removed the way `drbdadm adjust` removes it, the replica
// that removed it last is left with a stale quorum verdict while every
// volume it still has reports quorum: with on-no-quorum suspend-io the
// resource stays suspended on quorum, with io-error a Primary's requests
// fail with EIO although nothing shows as suspended. Switching on-no-quorum
// to the other policy and back has DRBD recompute it and lifts no other
// suspension, so a resource that suspends on no data, fences peers or is
// held by a snapshot as well is rechecked too: a stale quorum suspension
// left on such a resource is the case seen on a real cluster, where every
// replicated StorageClass sets on-no-data-accessible suspend-io. A volume
// that has really lost quorum is left to DRBD.
func TestRecheckQuorumAfterVolumeRemoval(t *testing.T) {
	t.Parallel()

	toggleSuspendIO := []string{toIOError, toSuspendIO}
	toggleIOError := []string{toSuspendIO, toIOError}

	for _, tc := range []struct {
		name      string
		status    string
		statusErr error
		show      string
		showErr   error
		firstErr  error
		secondErr error
		want      drbd.QuorumRecheck
		wantShow  bool
		wantCmds  []string
		wantErr   bool
	}{
		{
			name:     "suspend-io: suspended on quorum, the remaining volume has quorum",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions, "", ""),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
			wantCmds: toggleSuspendIO,
		},
		{
			name:     "suspend-io as the default on-no-quorum",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(`"quorum": "majority"`, ""),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
			wantCmds: toggleSuspendIO,
		},
		{
			name:     "suspend-io: not suspended",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON(suspendIOOptions, "", ""),
			wantShow: true,
		},
		{
			name:     "default on-no-quorum: not suspended",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON(`"quorum": "majority"`, ""),
			wantShow: true,
		},
		{
			name:     "io-error: nothing suspended, the cached verdict may be stale",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON(ioErrorOptions, "", ""),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
			wantCmds: toggleIOError,
		},
		{
			name:     "quorum off",
			status:   quorumStatusJSON(quorumSusp{}, "true"),
			show:     quorumShowJSON("", "", ""),
			wantShow: true,
		},
		{
			name:     "quorum off, spelled out",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(`"quorum": "off"`, ""),
			wantShow: true,
		},
		{
			name:     "suspends on no data, as every replicated StorageClass sets",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions+`, "on-no-data-accessible": "suspend-io"`, ""),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
			wantCmds: toggleSuspendIO,
		},
		{
			name:     "a connection fences its peer",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions, "", "resource-and-stonith"),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
			wantCmds: toggleSuspendIO,
		},
		{
			name:     "a snapshot holds the resource suspended too",
			status:   quorumStatusJSON(quorumSusp{user: true, quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions, ""),
			want:     drbd.QuorumRecheckRan,
			wantShow: true,
			wantCmds: toggleSuspendIO,
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
			name:     "switching to the other policy fails",
			status:   quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:     quorumShowJSON(suspendIOOptions, ""),
			firstErr: errFakeFailure,
			wantShow: true,
			wantCmds: []string{toIOError},
			wantErr:  true,
		},
		{
			name:      "switching back fails",
			status:    quorumStatusJSON(quorumSusp{quorum: true}, "true"),
			show:      quorumShowJSON(suspendIOOptions, ""),
			secondErr: errFakeFailure,
			wantShow:  true,
			wantCmds:  toggleSuspendIO,
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status pvc-a --json", storage.FakeResponse{Stdout: []byte(tc.status), Err: tc.statusErr})
			fx.Expect("drbdsetup show pvc-a --json", storage.FakeResponse{Stdout: []byte(tc.show), Err: tc.showErr})
			fx.Expect(toIOError, storage.FakeResponse{Err: tc.firstErr})
			fx.Expect(toSuspendIO, storage.FakeResponse{Err: tc.secondErr})

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

			if got := resourceOptionsLines(lines); !slices.Equal(got, tc.wantCmds) {
				t.Errorf("on-no-quorum changes %q, want %q", got, tc.wantCmds)
			}

			for _, line := range lines {
				if strings.Contains(line, "resume-io") {
					t.Errorf("%q run; it lifts every other suspension too", line)
				}
			}
		})
	}
}
