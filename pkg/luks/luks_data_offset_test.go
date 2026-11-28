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

// luks2AlignedDump is `cryptsetup luksDump`, from cryptsetup 2.7.5, of a
// LUKS2 header Format wrote on a device asking for 32 MiB alignment: the
// keyslots area is the default one, and the data starts 32 MiB in, not
// right after it.
const luks2AlignedDump = `LUKS header information
Version:       	2
Epoch:         	3
Metadata area: 	16384 [bytes]
Keyslots area: 	16744448 [bytes]
UUID:          	b3b0218c-674a-4c99-a84d-c20c10c27a49
Label:         	(no label)
Subsystem:     	(no subsystem)
Flags:       	(no flags)

Data segments:
  0: crypt
	offset: 33554432 [bytes]
	length: (whole device)
	cipher: aes-xts-plain64
	sector: 4096 [bytes]

Keyslots:
  0: luks2
	Key:        512 bits
	Priority:   normal
	Cipher:     aes-xts-plain64
	Cipher key: 512 bits
	PBKDF:      pbkdf2
	Hash:       sha256
	Iterations: 1000
	AF stripes: 4000
	AF hash:    sha256
	Area offset:32768 [bytes]
	Area length:258048 [bytes]
	Digest ID:  0
Tokens:
Digests:
  0: pbkdf2
	Hash:       sha256
	Iterations: 1000
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

// The data offset is read from the header, so neither a LUKS1 header's 2
// MiB nor an aligned LUKS2 header's 32 MiB is taken for the 16 MiB a
// LUKS2 header takes by default.
func TestDataOffsetKiBReadsTheHeader(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		resp   storage.FakeResponse
		want   int64
		wantOK bool
	}{
		{"LUKS2", storage.FakeResponse{Stdout: []byte(luks2Dump)}, 16384, true},
		{"LUKS2 aligned to 32 MiB", storage.FakeResponse{Stdout: []byte(luks2AlignedDump)}, 32768, true},
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
