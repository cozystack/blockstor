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
)

func mustParseEvent(t *testing.T, line string) drbd.Event {
	t.Helper()

	ev, err := drbd.ParseEvent(line)
	if err != nil {
		t.Fatalf("parse %q: %v", line, err)
	}

	return ev
}

// After a dump is taken as ended, a dump line that comes later is
// older than any frame applied since for its object. A destroyed
// resource or connection takes its parts with it: a late line for one
// of them would bring back what the kernel had already removed.
func TestLateDumpCoversWhatFramesSetSince(t *testing.T) {
	cases := []struct {
		name   string
		frames []string
		line   string
		covers bool
	}{
		{
			name:   "a change to the same peer device",
			frames: []string{"change peer-device name:r0 peer-node-id:1 conn-name:n2 volume:0 peer-disk:Outdated"},
			line:   "exists peer-device name:r0 peer-node-id:1 conn-name:n2 volume:0 peer-disk:UpToDate",
			covers: true,
		},
		{
			name:   "a change to another volume",
			frames: []string{"change device name:r0 volume:1 disk:UpToDate"},
			line:   "exists device name:r0 volume:0 disk:Inconsistent",
		},
		{
			name:   "a change to the resource leaves its devices",
			frames: []string{"change resource name:r0 role:Primary"},
			line:   "exists device name:r0 volume:0 disk:UpToDate",
		},
		{
			name:   "a destroyed resource takes its devices",
			frames: []string{"destroy resource name:r0"},
			line:   "exists device name:r0 volume:0 disk:UpToDate",
			covers: true,
		},
		{
			name:   "a destroyed connection takes its peer devices",
			frames: []string{"destroy connection name:r0 peer-node-id:1 conn-name:n2"},
			line:   "exists peer-device name:r0 peer-node-id:1 conn-name:n2 volume:0 peer-disk:UpToDate",
			covers: true,
		},
		{
			name:   "a destroyed connection leaves the other peers",
			frames: []string{"destroy connection name:r0 peer-node-id:1 conn-name:n2"},
			line:   "exists peer-device name:r0 peer-node-id:2 conn-name:n3 volume:0 peer-disk:UpToDate",
		},
		{
			name:   "a destroyed resource leaves other resources",
			frames: []string{"destroy resource name:r0"},
			line:   "exists device name:r1 volume:0 disk:UpToDate",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			late := &lateDump{}

			for _, frame := range tc.frames {
				late.record(mustParseEvent(t, frame))
			}

			if got := late.covers(mustParseEvent(t, tc.line)); got != tc.covers {
				t.Fatalf("covers = %v, want %v", got, tc.covers)
			}
		})
	}
}
