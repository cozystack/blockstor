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
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// drbdmetaOnFile runs the drbdadm calls of the metadata layout code as the
// drbdmeta calls drbdadm makes of them, against an image file: drbdmeta
// works on a regular file, drbdadm needs a resource file and a minor.
type drbdmetaOnFile struct {
	storage.RealExec

	image string
}

func (d drbdmetaOnFile) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	name, args = d.translate(name, args)

	return d.RealExec.Run(ctx, name, args...) //nolint:wrapcheck // stands in for the exec the code under test wraps
}

func (d drbdmetaOnFile) translate(name string, args []string) (string, []string) {
	if name != "drbdadm" || len(args) < 2 {
		return name, args
	}

	meta := []string{"--force", "0", "v09", d.image, "internal"}

	switch args[0] {
	case "dump-md":
		return "drbdmeta", append(meta, "dump-md")
	case "create-md":
		options := []string{"create-md"}
		peers := ""

		for _, arg := range args[1 : len(args)-1] {
			if value, ok := strings.CutPrefix(arg, "--max-peers="); ok {
				peers = value
			} else if arg != "--force" {
				options = append(options, arg)
			}
		}

		return "drbdmeta", append(append(meta, options...), peers)
	}

	return name, args
}

func requireTools(t *testing.T, tools ...string) {
	t.Helper()

	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed: %v", tool, err)
		}
	}
}

// newImage is a sparse 1 GiB image file.
func newImage(t *testing.T) string {
	t.Helper()

	image := filepath.Join(t.TempDir(), "lower.img")

	f, err := os.Create(image)
	if err != nil {
		t.Fatal(err)
	}

	if err := f.Truncate(1 << 30); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	return image
}

// The restore this code exists for, against the real drbdmeta: a lower disk
// LINSTOR laid out for 7 peer slots, with ext4 filling its data area.
// Metadata for 15 slots does not fit behind that filesystem, and drbdmeta
// refuses it; the layout read back off the superblock fits.
func TestRealDrbdmetaRecreatesMetadataInTheLayoutItReads(t *testing.T) {
	requireTools(t, "drbdmeta", "mkfs.ext4")

	ctx := t.Context()
	image := newImage(t)
	adm := drbd.NewAdm(drbdmetaOnFile{image: image})

	if err := adm.CreateMDWithMaxPeers(ctx, "pvc-1/0", 7); err != nil {
		t.Fatalf("create the 7-slot metadata LINSTOR would: %v", err)
	}

	dump, err := drbdmetaOnFile{image: image}.Run(ctx, "drbdadm", "dump-md", "--force", "pvc-1/0")
	if err != nil {
		t.Fatalf("dump-md: %v", err)
	}

	dataBytes := 0

	for _, line := range strings.Split(string(dump), "\n") {
		if value, ok := strings.CutPrefix(line, "# bm_offset "); ok {
			dataBytes, _ = strconv.Atoi(strings.TrimSpace(value))
		}
	}

	if dataBytes == 0 {
		t.Fatalf("no bm_offset in the dump:\n%s", dump)
	}

	out, err := exec.CommandContext(ctx, "mkfs.ext4", "-q", "-F", "-b", "4096", image, strconv.Itoa(dataBytes/4096)).CombinedOutput()
	if err != nil {
		t.Fatalf("mkfs.ext4: %v: %s", err, out)
	}

	geometry, err := adm.MetadataGeometry(ctx, "pvc-1/0")
	want := drbd.MetadataGeometry{MaxPeers: 7, ALStripes: 1, ALStripeSize4k: 8, NodeID: -1}

	if err != nil || geometry != want {
		t.Fatalf("MetadataGeometry = %+v, %v; want %+v", geometry, err, want)
	}

	err = adm.CreateMDWithGeometry(ctx, "pvc-1/0", drbd.GeometryForPeerSlots(drbd.LegacyPeerSlots))
	// Older drbdmeta names an ext4 without the newer features ext3.
	if !errors.Is(err, drbd.ErrCreateMDRefused) || !strings.Contains(err.Error(), "Device size would be truncated") {
		t.Fatalf("create-md with 15 slots over the filesystem = %v, want ErrCreateMDRefused with drbdmeta's reason", err)
	}

	if err := adm.CreateMDWithGeometry(ctx, "pvc-1/0", geometry.ClampedTo(drbd.LegacyPeerSlots)); err != nil {
		t.Fatalf("create-md in the layout read off the superblock: %v", err)
	}
}

// A non-default activity-log layout survives the round trip through
// create-md and dump-md, so the parser reads what drbdmeta writes.
func TestRealDrbdmetaKeepsTheActivityLogLayout(t *testing.T) {
	requireTools(t, "drbdmeta")

	ctx := t.Context()
	adm := drbd.NewAdm(drbdmetaOnFile{image: newImage(t)})
	want := drbd.MetadataGeometry{MaxPeers: 7, ALStripes: 2, ALStripeSize4k: 16, NodeID: -1}

	if err := adm.CreateMDWithGeometry(ctx, "pvc-1/0", want); err != nil {
		t.Fatalf("CreateMDWithGeometry: %v", err)
	}

	got, err := adm.MetadataGeometry(ctx, "pvc-1/0")
	if err != nil || got != want {
		t.Fatalf("MetadataGeometry = %+v, %v; want %+v", got, err, want)
	}
}
