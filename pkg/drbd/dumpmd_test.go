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

package drbd

import (
	"slices"
	"strings"
	"testing"
)

// The shape drbdmeta prints for a v09 volume (print_dump_gi in drbd-utils'
// drbdmeta.c): one statement per line for the current and the bitmap UUIDs,
// the history as a block of four to a line, then the sizes and the bitmap.
const dumpV09 = `# DRBD meta data dump
# 2026-10-24 12:00:00 +0000 [1792800000]
# node-1> drbdmeta 1000 v09 /dev/vg/pvc-r_00000 internal dump-md
#

version "v09";

max-peers 2;
# md_size_sect 2048
# md_offset 10737414144
# al_offset 10737385472
# bm_offset 10737057792

node-id 0;
current-uuid 0x9F00AA11BB22CC33;
flags 0x00000011;
members 0x0000000000000003;
features 0x0000000000000000;
peer[0] {
    bitmap-index -1;
    bitmap-uuid 0x0000000000000000;
    bitmap-dagtag 0x0000000000000000;
    flags 0x00000000;
}
peer[1] {
    bitmap-index 0;
    bitmap-uuid 0x6e3a5d0b2c1f4a96;
    bitmap-dagtag 0x0000000000000000;
    flags 0x00000005; # PEER_CONNECTED | PEER_HAVE_DATA
}
history-uuids {
        0x1234567890ABCDEF; 0x0000000000000000; 0x0000000000000000; 0x0000000000000000;
        0x0000000000000000; 0x0000000000000000; 0x0000000000000000; 0x00000000000000A0;
}
# al-extents 1237;
la-size-sect 20971520;
bm-byte-per-bit 4096;
device-uuid 0x5A5A5A5A5A5A5A5A;
la-peer-max-bio-size 1048576;
al-stripes 1;
al-stripe-size-4k 8;
# bm-bytes 327680;
bitmap[0] {
   # at 0kB
    5120 times 0x0000000000000000;
   0xDEADBEEFDEADBEEF; 0x0000000000000000; 0x0000000000000000; 0x0000000000000000;
}
# bits-set[0] 64;
`

const dumpV08 = `version "v08";

# md_size_sect 262144
uuid {
    0x9F00AA11BB22CC33; 0x6E3A5D0B2C1F4A96; 0x1234567890ABCDEF; 0x0000000000000000;
    flags 0x00000011;
}
la-size-sect 20971520;
bm {
   0xDEADBEEFDEADBEEF;
}
`

func TestParseMetadataUUIDsReadsEverySlot(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		dump string
		want []string
	}{
		{
			name: "v09",
			dump: dumpV09,
			want: []string{"9F00AA11BB22CC33", "6E3A5D0B2C1F4A96", "1234567890ABCDEF", "00000000000000A0"},
		},
		{
			name: "v08",
			dump: dumpV08,
			want: []string{"9F00AA11BB22CC33", "6E3A5D0B2C1F4A96", "1234567890ABCDEF"},
		},
		{
			name: "history on one line",
			dump: "current-uuid 0x9F00AA11BB22CC33;\nhistory-uuids { 0x1234567890ABCDEF; }\n" +
				"bm {\n   0xDEADBEEFDEADBEEF;\n}\n",
			want: []string{"9F00AA11BB22CC33", "1234567890ABCDEF"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseMetadataUUIDs([]byte(tc.dump))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}

			if !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// Output that names no current UUID is not read as "no generations": empty
// output, a renamed keyword and a line the scanner gives up on are errors.
func TestParseMetadataUUIDsRefusesWhatItCannotRead(t *testing.T) {
	t.Parallel()

	for name, dump := range map[string]string{
		"empty":           "",
		"renamed keyword": "version \"v09\";\ncurrent-gi 0x9F00AA11BB22CC33;\npeer[1] {\n    bitmap-uuid 0x6E3A5D0B2C1F4A96;\n}\n",
		"line too long":   "current-uuid 0x9F00AA11BB22CC33;\n" + strings.Repeat("x", dumpLineMax+1) + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got, err := ParseMetadataUUIDs([]byte(dump)); err == nil {
				t.Fatalf("parse = %v, nil; want an error", got)
			}
		})
	}
}

// The lowest bit marks a UUID issued while Primary and is cleared when the
// UUID moves into history.
func TestSameGenerationIgnoresThePrimaryBit(t *testing.T) {
	t.Parallel()

	if !SameGeneration("6E3A5D0B2C1F4A97", "6e3a5d0b2c1f4a96") {
		t.Error("a UUID and its history form are not the same generation")
	}

	if SameGeneration("6E3A5D0B2C1F4A97", "6E3A5D0B2C1F4A95") {
		t.Error("two generations that differ above the Primary bit compare equal")
	}
}
