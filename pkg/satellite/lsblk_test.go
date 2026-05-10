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

package satellite

import (
	"testing"
)

// TestParseLsblkPairsHappyPath pins the per-line `KEY="value"`
// parser against a representative `lsblk -P` snapshot. Real
// production hardware emits these exact shapes — a regression
// that mis-handles spaces in MODEL strings, missing fields, or
// quoted empty values would silently drop devices from the
// PhysicalDevice discovery loop.
func TestParseLsblkPairsHappyPath(t *testing.T) {
	t.Parallel()

	raw := `NAME="sda" KNAME="sda" SIZE="1000204886016" FSTYPE="" TYPE="disk" MOUNTPOINT="" WWN="0x5000c500a3b1c2d3" MODEL="Samsung SSD 980 PRO 1TB" SERIAL="S6BUNS0R123456" ROTA="0" TRAN="nvme"
NAME="sdb" KNAME="sdb" SIZE="2000398934016" FSTYPE="ext4" TYPE="disk" MOUNTPOINT="/data" WWN="0x5000c500a3b1c2d4" MODEL="Seagate ST2000NM" SERIAL="ZA1234567" ROTA="1" TRAN="sata"
NAME="sdc1" KNAME="sdc1" SIZE="1000204886015" FSTYPE="" TYPE="part" MOUNTPOINT="" WWN="" MODEL="" SERIAL="" ROTA="0" TRAN=""`

	devs := parseLsblkPairs(raw)

	if len(devs) != 3 {
		t.Fatalf("got %d devices, want 3", len(devs))
	}

	if devs[0].Name != "sda" || devs[0].SizeBytes != 1000204886016 || devs[0].Model != "Samsung SSD 980 PRO 1TB" {
		t.Errorf("sda parse mismatch: %+v", devs[0])
	}

	if devs[0].Rotational {
		t.Errorf("sda: ROTA=0 should map to Rotational=false, got true")
	}

	if devs[0].WWN != "0x5000c500a3b1c2d3" {
		t.Errorf("sda WWN: got %q, want 0x5000c500a3b1c2d3", devs[0].WWN)
	}

	if !devs[1].Rotational {
		t.Errorf("sdb: ROTA=1 should map to Rotational=true, got false")
	}
}

// TestIsFreeBlockDeviceFiltersCorrectly pins the four-way filter
// the discovery loop runs over each lsblk row before publishing a
// PhysicalDevice CRD. A regression in any of these would silently
// publish a device that's already in use by LVM/ZFS/ext4 and
// stamp it for wipe, risking data loss.
func TestIsFreeBlockDeviceFiltersCorrectly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		dev  LsblkDevice
		want bool
	}{
		{
			name: "free disk",
			dev:  LsblkDevice{Name: "sda", Type: "disk"},
			want: true,
		},
		{
			name: "partition not disk",
			dev:  LsblkDevice{Name: "sda1", Type: "part"},
			want: false,
		},
		{
			name: "lvm child not disk",
			dev:  LsblkDevice{Name: "vg-thin", Type: "lvm"},
			want: false,
		},
		{
			name: "loop not disk",
			dev:  LsblkDevice{Name: "loop0", Type: "loop"},
			want: false,
		},
		{
			name: "disk with filesystem",
			dev:  LsblkDevice{Name: "sdb", Type: "disk", FSType: "ext4"},
			want: false,
		},
		{
			name: "disk with mountpoint",
			dev:  LsblkDevice{Name: "sdc", Type: "disk", Mountpoint: "/data"},
			want: false,
		},
		{
			name: "disk with both fs and mount",
			dev:  LsblkDevice{Name: "sdd", Type: "disk", FSType: "xfs", Mountpoint: "/srv"},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.dev.IsFreeBlockDevice()
			if got != tc.want {
				t.Errorf("got %v, want %v for %+v", got, tc.want, tc.dev)
			}
		})
	}
}

// TestParseLsblkLineModelWithSpaces pins the most-common
// real-world quirk: MODEL strings like "Samsung SSD 980 PRO" carry
// embedded spaces. A naive split-on-space parser would corrupt
// every subsequent field. The hand-rolled key="value" reader
// handles this — pinning it here means any future "let's just
// use strings.Fields" refactor breaks loudly.
func TestParseLsblkLineModelWithSpaces(t *testing.T) {
	t.Parallel()

	line := `NAME="sda" MODEL="Samsung SSD 980 PRO 1TB" SERIAL="S6BUNS0R123456"`

	fields := parseLsblkLine(line)

	if fields["NAME"] != "sda" {
		t.Errorf("NAME: got %q, want sda", fields["NAME"])
	}

	if fields["MODEL"] != "Samsung SSD 980 PRO 1TB" {
		t.Errorf("MODEL: got %q, want %q", fields["MODEL"], "Samsung SSD 980 PRO 1TB")
	}

	if fields["SERIAL"] != "S6BUNS0R123456" {
		t.Errorf("SERIAL: got %q, want S6BUNS0R123456", fields["SERIAL"])
	}
}

// TestParseLsblkPairsEmptyAndBlankLines pins that empty input and
// blank lines (which lsblk emits trailingly) don't produce phantom
// LsblkDevice entries with zero fields. A regression would have
// the discovery loop publishing a PhysicalDevice with empty Name
// and zero size.
func TestParseLsblkPairsEmptyAndBlankLines(t *testing.T) {
	t.Parallel()

	if got := parseLsblkPairs(""); got != nil {
		t.Errorf("empty input: got %d devices, want nil", len(got))
	}

	raw := `
NAME="sda" KNAME="sda" SIZE="1000" FSTYPE="" TYPE="disk" MOUNTPOINT="" WWN="" MODEL="" SERIAL="" ROTA="0" TRAN=""

`
	devs := parseLsblkPairs(raw)
	if len(devs) != 1 {
		t.Errorf("blank-line-padded input: got %d devices, want 1", len(devs))
	}
}
