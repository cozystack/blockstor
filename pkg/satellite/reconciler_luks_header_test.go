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

package satellite_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
)

// luksDumpAt is `cryptsetup luksDump` of a LUKS2 header whose data starts
// at offset bytes.
func luksDumpAt(offset string) storage.FakeResponse {
	return storage.FakeResponse{Stdout: []byte("Version:       \t2\nData segments:\n  0: crypt\n\toffset: " +
		offset + " [bytes]\n\tlength: (whole device)\n")}
}

// A new volume whose header cryptsetup aligned to 32 MiB has its backing
// grown by the 16 MiB over the header it was created for after the
// format and before the open, so the mapper the consumer gets is not
// short of the volume. Under a 16 MiB header nothing grows.
func TestLUKSNewVolumeBackingHoldsTheHeaderCryptsetupWrote(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		offset   string
		wantGrow bool
	}{
		{"32 MiB header", "33554432", true},
		{"16 MiB header", "16777216", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			run := &luksRun{
				blkid:  emptyDevice(),
				keys:   &fakeLUKSKeys{},
				expect: map[string]storage.FakeResponse{"cryptsetup luksDump " + keyDev: luksDumpAt(tc.offset)},
			}
			run.apply(t)

			if !run.ok {
				t.Fatalf("apply failed: %s", run.message)
			}

			calls := run.fx.CommandLines()
			format := slices.Index(calls, keyFormat)
			open := slices.Index(calls, keyOpen)
			grow := slices.IndexFunc(calls, func(line string) bool { return strings.HasPrefix(line, "lvextend ") })

			if format < 0 || open < 0 {
				t.Fatalf("format at %d, open at %d: %v", format, open, calls)
			}

			if !tc.wantGrow {
				if grow >= 0 {
					t.Fatalf("grew the backing under a 16 MiB header: %q", calls[grow])
				}

				return
			}

			if grow < format || grow > open {
				t.Fatalf("grow at %d, want between the format at %d and the open at %d: %v", grow, format, open, calls)
			}

			// The volume is 1 GiB: the backing holds it and the 32 MiB header.
			if !strings.Contains(calls[grow], "1081344k") {
				t.Fatalf("grow %q, want the backing at 1081344 KiB", calls[grow])
			}
		})
	}
}
