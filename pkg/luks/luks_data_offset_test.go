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

package luks_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/storage"
)

// luks2Dump is `cryptsetup luksDump` of a LUKS2 header whose keyslot
// area sits at its own offset, ahead of the data segment.
const luks2Dump = `LUKS header information
Version:       	2
Epoch:         	3
Metadata area: 	16384 [bytes]
Keyslots area: 	16744448 [bytes]
UUID:          	0b1c

Data segments:
  0: crypt
	offset: 16777216 [bytes]
	length: (whole device)
	cipher: aes-xts-plain64
	sector: 512 [bytes]

Keyslots:
  0: luks2
	Key:        512 bits
	Area offset:32768 [bytes]
	Area length:258048 [bytes]
`

// luks1Dump is `cryptsetup luksDump` of a LUKS1 header.
const luks1Dump = `LUKS header information for /dev/vg/lv

Version:       	1
Cipher name:   	aes
Cipher mode:   	xts-plain64
Hash spec:     	sha256
Payload offset:	4096
MK bits:       	512
`

// The data offset is read from the header, so a LUKS1 header's 2 MiB is
// not taken for the 16 MiB a LUKS2 header Format writes.
func TestDataOffsetKiBReadsTheHeader(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		resp   storage.FakeResponse
		want   int64
		wantOK bool
	}{
		{"LUKS2", storage.FakeResponse{Stdout: []byte(luks2Dump)}, 16384, true},
		{"LUKS1", storage.FakeResponse{Stdout: []byte(luks1Dump)}, 2048, true},
		{"LUKS2 with a 4 MiB offset", storage.FakeResponse{Stdout: []byte(
			"Version: 2\nData segments:\n  0: crypt\n\toffset: 4194304 [bytes]\n")}, 4096, true},
		{"no header", storage.FakeResponse{Err: storage.FakeExitError{Code: 1}}, 0, false},
		{"keyslot offsets only", storage.FakeResponse{Stdout: []byte(
			"Version: 2\nKeyslots:\n  0: luks2\n\tArea offset:32768 [bytes]\n\toffset: 32768 [bytes]\n")}, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("cryptsetup luksDump /dev/vg/lv", tc.resp)

			got, ok := luks.NewCryptsetup(fx).DataOffsetKiB(t.Context(), "/dev/vg/lv")
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("DataOffsetKiB = %d, %v; want %d, %v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
