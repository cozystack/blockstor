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

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Deleting a replica drops its peer from the kernel before the observer
// hears of it. The GI read that followed asked the kernel through that
// peer only, failed, and fell back to drbdmeta --force on the attached
// backing device of the in-use Primary. The kernel is asked through
// every peer the observer holds, and a replica with peers never has its
// attached backing device read.
func TestObserverReadsTheGIThroughAPeerTheKernelStillHas(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		peers map[string]int32
		want  string
	}{
		{"another peer answers", map[string]int32{"n-gone": 1, "n-kept": 2}, kernelTestGI},
		{"no peer answers", map[string]int32{"n-gone": 1}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := statusTestExec()
			fx.Expect("drbdsetup get-gi "+statusTestRD+" 1 0", storage.FakeResponse{Err: storage.FakeExitError{Code: 10}})
			fx.Expect("drbdsetup get-gi "+statusTestRD+" 2 0", storage.FakeResponse{Stdout: []byte(kernelTestGI + ":0:0:0\n")})

			o := &ObserverRunnable{}
			for peer, id := range tc.peers {
				o.mergeConnections(peerConnectedWithID(peer, id))
			}

			vol := deviceUpToDate().Volumes[0]

			if got := o.readCurrentGI(t.Context(), drbd.NewAdm(fx), statusTestRD, &vol); got != tc.want {
				t.Errorf("GI %q, want %q", got, tc.want)
			}

			if got := metaGIReads(fx); got != 0 {
				t.Errorf("read the attached backing device of a replica with peers %d times: %v", got, fx.CommandLines())
			}
		})
	}
}
