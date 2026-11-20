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
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// RemoveMinor checks the device before it detaches anything: DRBD 9 lets an
// open Primary detach while its peers are UpToDate, so a check left to
// del-minor comes after the consumer has lost its disk.
func TestRemoveMinorChecksTheDeviceBeforeDetaching(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		status    string
		detachErr error
		wantErr   error
		wantCalls []string
	}{
		{
			name:      "secondary",
			status:    "pvc-a node-id:0 role:Secondary suspended:no\n  volume:0 minor:1000 disk:UpToDate open:no\n",
			wantCalls: []string{"drbdsetup detach 1000", "drbdsetup del-minor 1000"},
		},
		{
			name:    "primary",
			status:  "pvc-a node-id:0 role:Primary suspended:no\n  volume:0 minor:1000 disk:UpToDate\n",
			wantErr: drbd.ErrMinorInUse,
		},
		{
			name:    "open secondary",
			status:  "pvc-a node-id:0 role:Secondary suspended:no\n  volume:0 minor:1000 disk:UpToDate open:yes\n",
			wantErr: drbd.ErrMinorInUse,
		},
		{
			name:      "detach fails",
			status:    "pvc-a node-id:0 role:Secondary suspended:no\n  volume:0 minor:1000 disk:UpToDate\n",
			detachErr: errFakeFailure,
			wantErr:   errFakeFailure,
			wantCalls: []string{"drbdsetup detach 1000"},
		},
		{
			name:      "diskless",
			status:    "pvc-a node-id:0 role:Secondary suspended:no\n  volume:0 minor:1000 disk:Diskless\n",
			wantCalls: []string{"drbdsetup del-minor 1000"},
		},
		{
			name:   "not loaded",
			status: "pvc-b node-id:0 role:Primary suspended:no\n  volume:0 minor:1001 disk:UpToDate\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte(tc.status)})
			fx.Expect("drbdsetup detach 1000", storage.FakeResponse{Err: tc.detachErr})

			err := drbd.NewAdm(fx).RemoveMinor(t.Context(), 1000)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil) != (err == nil) {
				t.Fatalf("RemoveMinor: %v, want %v", err, tc.wantErr)
			}

			var got []string

			for _, line := range fx.CommandLines() {
				if line != "drbdsetup status --verbose" {
					got = append(got, line)
				}
			}

			if !slices.Equal(got, tc.wantCalls) {
				t.Errorf("commands %q, want %q", got, tc.wantCalls)
			}
		})
	}
}
