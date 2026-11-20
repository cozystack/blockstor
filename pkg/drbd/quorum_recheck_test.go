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

// secondaryStatus turns quorumStatusJSON's Primary into a Secondary, with
// the first device open when open is set.
func secondaryStatus(status string, open bool) string {
	status = strings.Replace(status, `"role": "Primary"`, `"role": "Secondary"`, 1)
	if open {
		status = strings.Replace(status, `"open": false`, `"open": true`, 1)
	}

	return status
}

// removalCommands is the commands that change the kernel, in order.
func removalCommands(lines []string) []string {
	var out []string

	for _, line := range lines {
		if !strings.HasPrefix(line, "drbdsetup status ") && !strings.HasPrefix(line, "drbdsetup show ") {
			out = append(out, line)
		}
	}

	return out
}

// del-minor turns replication to each peer off before it unregisters the
// device, so the volume going away loses quorum, and DRBD suspends the
// whole resource under on-no-quorum suspend-io; nothing lifts that once the
// device is gone. On a Secondary that is not suspended and whose remaining
// volumes have quorum and no opener, the removal runs under io-error, which
// never suspends, and suspend-io is set back afterwards, which recomputes
// the verdict from the volumes left. Everywhere else the policy is left as
// it is, and it is restored even when the removal fails.
func TestRemoveMinorsKeepingQuorum(t *testing.T) {
	t.Parallel()

	const (
		detach   = "drbdsetup detach 20056"
		delMinor = "drbdsetup del-minor 20056"
	)

	relaxed := []string{toIOError, detach, delMinor, toSuspendIO}
	plain := []string{detach, delMinor}
	secondary := secondaryStatus(quorumStatusJSON(quorumSusp{}, "true", "true"), false)

	for _, tc := range []struct {
		name      string
		status    string
		statusErr error
		show      string
		relaxErr  error
		delErr    error
		restErr   error
		want      []string
		wantErr   bool
	}{
		{name: "secondary with quorum", status: secondary, show: quorumShowJSON(suspendIOOptions, ""), want: relaxed},
		{name: "default on-no-quorum", status: secondary, show: quorumShowJSON(`"quorum": "majority"`, ""), want: relaxed},
		{
			name:   "the departing volume has lost quorum already",
			status: secondaryStatus(quorumStatusJSON(quorumSusp{}, "true", "false"), false),
			show:   quorumShowJSON(suspendIOOptions, ""),
			want:   relaxed,
		},
		{name: "io-error never suspends", status: secondary, show: quorumShowJSON(ioErrorOptions, ""), want: plain},
		{name: "quorum off", status: secondary, show: quorumShowJSON("", ""), want: plain},
		{name: "primary", status: quorumStatusJSON(quorumSusp{}, "true", "true"), want: plain},
		{
			name:   "suspended on quorum",
			status: secondaryStatus(quorumStatusJSON(quorumSusp{quorum: true}, "true", "true"), false),
			want:   plain,
		},
		{
			name:   "a snapshot holds it suspended",
			status: secondaryStatus(quorumStatusJSON(quorumSusp{user: true}, "true", "true"), false),
			want:   plain,
		},
		{
			name:   "suspended on no data",
			status: secondaryStatus(quorumStatusJSON(quorumSusp{noData: true}, "true", "true"), false),
			want:   plain,
		},
		{
			name:   "suspended on fencing",
			status: secondaryStatus(quorumStatusJSON(quorumSusp{fencing: true}, "true", "true"), false),
			want:   plain,
		},
		{
			name:   "a remaining volume is open",
			status: secondaryStatus(quorumStatusJSON(quorumSusp{}, "true", "true"), true),
			want:   plain,
		},
		{
			name:   "a remaining volume has no quorum",
			status: secondaryStatus(quorumStatusJSON(quorumSusp{}, "false", "true"), false),
			want:   plain,
		},
		{
			name:   "no remaining volume",
			status: strings.Replace(secondaryStatus(quorumStatusJSON(quorumSusp{}, "true"), false), `"minor": 20055`, `"minor": 20056`, 1),
			want:   plain,
		},
		{name: "resource not loaded", statusErr: errNoSuchResource, want: plain},
		{name: "status fails", statusErr: errFakeFailure, wantErr: true},
		{
			name:    "del-minor fails",
			status:  secondary,
			show:    quorumShowJSON(suspendIOOptions, ""),
			delErr:  errFakeFailure,
			want:    relaxed,
			wantErr: true,
		},
		{
			name:    "restoring the policy fails",
			status:  secondary,
			show:    quorumShowJSON(suspendIOOptions, ""),
			restErr: errFakeFailure,
			want:    relaxed,
			wantErr: true,
		},
		{
			name:     "relaxing the policy fails",
			status:   secondary,
			show:     quorumShowJSON(suspendIOOptions, ""),
			relaxErr: errFakeFailure,
			want:     []string{toIOError},
			wantErr:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			show := tc.show
			if show == "" {
				show = quorumShowJSON(suspendIOOptions, "")
			}

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status pvc-a --json", storage.FakeResponse{Stdout: []byte(tc.status), Err: tc.statusErr})
			fx.Expect("drbdsetup show pvc-a --json", storage.FakeResponse{Stdout: []byte(show)})
			fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte(
				"pvc-a node-id:2 role:Secondary suspended:no\n" +
					"  volume:0 minor:20055 disk:UpToDate open:no\n" +
					"  volume:1 minor:20056 disk:UpToDate open:no\n")})
			fx.Expect(toIOError, storage.FakeResponse{Err: tc.relaxErr})
			fx.Expect(delMinor, storage.FakeResponse{Err: tc.delErr})
			fx.Expect(toSuspendIO, storage.FakeResponse{Err: tc.restErr})

			err := drbd.NewAdm(fx).RemoveMinorsKeepingQuorum(t.Context(), "pvc-a", []int32{20056})
			if (err != nil) != tc.wantErr {
				t.Fatalf("RemoveMinorsKeepingQuorum: %v, want error %t", err, tc.wantErr)
			}

			if got := removalCommands(fx.CommandLines()); !slices.Equal(got, tc.want) {
				t.Errorf("commands %q, want %q", got, tc.want)
			}
		})
	}
}

// Nothing locks out an opener while a removal runs under io-error, and an
// open on a remaining volume can auto-promote the resource, whose requests
// would then fail with EIO instead of being held. So the role and the
// openers are read again right before each del-minor and after each one
// another follows, and suspend-io is set back before anything else runs
// once the resource is Primary or a remaining volume is open.
func TestRemoveMinorsKeepingQuorumWatchesForAnOpener(t *testing.T) {
	t.Parallel()

	const (
		detach56 = "drbdsetup detach 20056"
		del56    = "drbdsetup del-minor 20056"
		detach57 = "drbdsetup detach 20057"
		del57    = "drbdsetup del-minor 20057"
	)

	three := quorumStatusJSON(quorumSusp{}, "true", "true", "true")
	secondary := secondaryStatus(three, false)

	for _, tc := range []struct {
		name    string
		minors  []int32
		later   []storage.FakeResponse
		want    []string
		wantErr bool
	}{
		{
			name:   "still a secondary with no opener",
			minors: []int32{20056},
			later:  []storage.FakeResponse{{Stdout: []byte(secondary)}},
			want:   []string{toIOError, detach56, del56, toSuspendIO},
		},
		{
			name:   "promoted during the detach",
			minors: []int32{20056},
			later:  []storage.FakeResponse{{Stdout: []byte(three)}},
			want:   []string{toIOError, detach56, toSuspendIO, del56},
		},
		{
			name:   "a remaining volume opened during the detach",
			minors: []int32{20056},
			later:  []storage.FakeResponse{{Stdout: []byte(secondaryStatus(three, true))}},
			want:   []string{toIOError, detach56, toSuspendIO, del56},
		},
		{
			name:   "promoted between two removals",
			minors: []int32{20056, 20057},
			later:  []storage.FakeResponse{{Stdout: []byte(secondary)}, {Stdout: []byte(three)}},
			want:   []string{toIOError, detach56, del56, toSuspendIO, detach57, del57},
		},
		{
			name:    "the status cannot be read again",
			minors:  []int32{20056},
			later:   []storage.FakeResponse{{Err: errFakeFailure}},
			want:    []string{toIOError, detach56, toSuspendIO},
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.ExpectSequence("drbdsetup status pvc-a --json",
				append([]storage.FakeResponse{{Stdout: []byte(secondary)}}, tc.later...)...)
			fx.Expect("drbdsetup status pvc-a --json", storage.FakeResponse{Stdout: []byte(secondary)})
			fx.Expect("drbdsetup show pvc-a --json", storage.FakeResponse{Stdout: []byte(quorumShowJSON(suspendIOOptions, ""))})
			fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte(
				"pvc-a node-id:2 role:Secondary suspended:no\n" +
					"  volume:0 minor:20055 disk:UpToDate open:no\n" +
					"  volume:1 minor:20056 disk:UpToDate open:no\n" +
					"  volume:2 minor:20057 disk:UpToDate open:no\n")})

			err := drbd.NewAdm(fx).RemoveMinorsKeepingQuorum(t.Context(), "pvc-a", tc.minors)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RemoveMinorsKeepingQuorum: %v, want error %t", err, tc.wantErr)
			}

			if got := removalCommands(fx.CommandLines()); !slices.Equal(got, tc.want) {
				t.Errorf("commands %q, want %q", got, tc.want)
			}
		})
	}
}
