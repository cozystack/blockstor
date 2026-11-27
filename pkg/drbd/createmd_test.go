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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// truncationRefusal is what drbdmeta prints on stdout, before it exits 40,
// when metadata with more peer slots than the lower disk was laid out for
// would cut into the filesystem on it (check_for_existing_data).
const truncationRefusal = "md_offset 1073487872\nal_offset 1073455104\nbm_offset 1072963584\n\n" +
	"Found ext4 filesystem\n     1048576 kB data area apparently used\n" +
	"     1048304 kB left usable by current configuration\n\n" +
	"Device size would be truncated, which\nwould corrupt data and result in\n" +
	"'access beyond end of device' errors.\nYou need to either\n" +
	"   * use external meta data (recommended)\n   * shrink that filesystem first\n" +
	"   * zero out the device (destroy the filesystem)\nOperation refused.\n\n"

const createMD15 = "drbdadm create-md --force --max-peers=15 pvc-1/0"

// drbdmeta gives its reason for refusing on stdout and exits with a bare
// 40, and the exec error carries stderr only. Before, the satellite logged
// "exit code 40" and nothing an operator could act on. The refusal is now
// named, so the satellite can tell it from a failure worth retrying, and
// carries the reason.
func TestCreateMDNamesARefusalAndItsReason(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(createMD15, storage.FakeResponse{
		Stdout: []byte(truncationRefusal),
		Err:    storage.FakeExitError{Code: 40},
	})

	err := drbd.NewAdm(fx).CreateMDWithMaxPeers(t.Context(), "pvc-1/0", drbd.LegacyPeerSlots)
	if !errors.Is(err, drbd.ErrCreateMDRefused) {
		t.Fatalf("CreateMD = %v, want ErrCreateMDRefused", err)
	}

	for _, want := range []string{
		drbd.ErrCreateMDRefused.Error(),
		"Found ext4 filesystem",
		"Device size would be truncated",
		"Operation refused.",
		"exit status 40",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("CreateMD error %q does not carry %q", err, want)
		}
	}

	if strings.Contains(err.Error(), "md_offset") {
		t.Errorf("CreateMD error %q carries the offsets drbdmeta prints before its reason", err)
	}
}

// Any other failure stays what it was: not a refusal.
func TestCreateMDOtherFailuresAreNotRefusals(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(createMD15, storage.FakeResponse{
		Stdout: []byte("open(/dev/zvol/data/pvc-1_00000) failed: Device or resource busy\n"),
		Err:    storage.FakeExitError{Code: 20},
	})

	err := drbd.NewAdm(fx).CreateMDWithMaxPeers(t.Context(), "pvc-1/0", drbd.LegacyPeerSlots)
	if err == nil || errors.Is(err, drbd.ErrCreateMDRefused) {
		t.Fatalf("CreateMD = %v, want a failure that is not a refusal", err)
	}
}

func TestCreateMDWithMaxPeersPassesTheCount(t *testing.T) {
	fx := storage.NewFakeExec()

	if err := drbd.NewAdm(fx).CreateMDWithMaxPeers(t.Context(), "pvc-1/0", 7); err != nil {
		t.Fatalf("CreateMDWithMaxPeers: %v", err)
	}

	want := "drbdadm create-md --force --max-peers=7 pvc-1/0"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// geometryHeader is the header drbdmeta prints for a v09 superblock, here
// one LINSTOR created for 7 peer slots whose activity log was never
// applied, with the fields between the ones the layout needs cut out.
const geometryHeader = "# DRBD meta data dump\n" +
	"This_is_an_unclean_meta_data_dump._Don't_trust_the_bitmap.\n" +
	"# You should \"apply-al\" first, if you plan to restore this.\n\n" +
	"version \"v09\";\n\nmax-peers 7;\n# md_size_sect 520\nnode-id 2;\n" +
	"current-uuid 0x9F00AA11BB22CC33;\nla-size-sect 2097152;\nbm-byte-per-bit 4096;\n" +
	"la-peer-max-bio-size 1048576;\nal-stripes 1;\nal-stripe-size-4k 8;\n"

// The layout is read with --force, so a superblock whose activity log was
// never applied, the state every snapshot of an attached volume carries,
// still answers.
func TestMetadataGeometryReadsAnUncleanSuperblock(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md --force pvc-1/0", storage.FakeResponse{Stdout: []byte(geometryHeader)})

	got, err := drbd.NewAdm(fx).MetadataGeometry(t.Context(), "pvc-1/0")
	want := drbd.MetadataGeometry{MaxPeers: 7, ALStripes: 1, ALStripeSize4k: 8, NodeID: 2}

	if err != nil || got != want {
		t.Fatalf("MetadataGeometry = %+v, %v; want %+v", got, err, want)
	}
}

// LINSTOR lets a resource have up to 31 peer slots. Such a superblock is
// read, not reported as unreadable, and clamped to the replica's own
// count: smaller metadata fits where larger metadata was.
func TestMetadataGeometryReadsMoreSlotsThanBlockstorCreates(t *testing.T) {
	got, err := drbd.ParseMetadataGeometry([]byte(strings.Replace(geometryHeader, "max-peers 7;", "max-peers 31;", 1)))
	if err != nil || got.MaxPeers != 31 {
		t.Fatalf("ParseMetadataGeometry = %+v, %v; want 31 peer slots", got, err)
	}

	if clamped := got.ClampedTo(drbd.LegacyPeerSlots); clamped.MaxPeers != drbd.LegacyPeerSlots || clamped.ALStripes != 1 {
		t.Errorf("ClampedTo(15) = %+v, want 15 peer slots and the layout kept", clamped)
	}

	if clamped := got.ClampedTo(drbd.DefaultPeerSlots); clamped.MaxPeers != drbd.DefaultPeerSlots {
		t.Errorf("ClampedTo(7) = %+v, want 7 peer slots", clamped)
	}
}

// Metadata is sized over data with this layout, so every statement it is
// made of has to be there and in range; a guess is an error.
func TestParseMetadataGeometryRefusesWhatItCannotRead(t *testing.T) {
	for name, dump := range map[string]string{
		"empty":           "",
		"v08":             "version \"v08\";\nuuid {\n    0x1; 0x0; 0x0; 0x0;\n    flags 0x1;\n}\n",
		"not a number":    strings.Replace(geometryHeader, "max-peers 7;", "max-peers seven;", 1),
		"zero":            strings.Replace(geometryHeader, "max-peers 7;", "max-peers 0;", 1),
		"out of range":    strings.Replace(geometryHeader, "max-peers 7;", "max-peers 32;", 1),
		"no peer slots":   strings.Replace(geometryHeader, "max-peers 7;", "", 1),
		"no al-stripes":   strings.Replace(geometryHeader, "al-stripes 1;", "", 1),
		"no stripe size":  strings.Replace(geometryHeader, "al-stripe-size-4k 8;", "", 1),
		"no node id":      strings.Replace(geometryHeader, "node-id 2;", "", 1),
		"zero al-stripes": strings.Replace(geometryHeader, "al-stripes 1;", "al-stripes 0;", 1),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := drbd.ParseMetadataGeometry([]byte(dump))
			if !errors.Is(err, drbd.ErrNoGeometry) {
				t.Errorf("ParseMetadataGeometry = %+v, %v; want ErrNoGeometry", got, err)
			}
		})
	}
}

// A superblock with another activity-log layout is re-created in it:
// with the default one its metadata changes size and the data area under
// it moves. The default layout is left to drbdmeta, so the command of
// every other volume stays what it was.
func TestCreateMDWithGeometryPassesTheActivityLogLayout(t *testing.T) {
	fx := storage.NewFakeExec()
	geometry := drbd.MetadataGeometry{MaxPeers: 7, ALStripes: 2, ALStripeSize4k: 16, NodeID: -1}

	if err := drbd.NewAdm(fx).CreateMDWithGeometry(t.Context(), "pvc-1/0", geometry); err != nil {
		t.Fatalf("CreateMDWithGeometry: %v", err)
	}

	if err := drbd.NewAdm(fx).CreateMDWithGeometry(t.Context(), "pvc-1/0", drbd.GeometryForPeerSlots(drbd.LegacyPeerSlots)); err != nil {
		t.Fatalf("CreateMDWithGeometry: %v", err)
	}

	want := []string{
		"drbdadm create-md --force --max-peers=7 --al-stripes=2 --al-stripe-size-kB=64 pvc-1/0",
		createMD15,
	}
	if got := fx.CommandLines(); !slices.Equal(got, want) {
		t.Errorf("calls = %q, want %q", got, want)
	}
}
