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

package satellite

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

const (
	headerDev  = "/dev/zvol/data/pvc-h_00000"
	headerZvol = "data/pvc-h_00000"
	header16   = 16 * 1024
	header32   = 32 * 1024
	gib1       = 1024 * 1024
	gib2       = 2 * gib1
)

// luks2Dump is `cryptsetup luksDump` of a LUKS2 header whose data starts
// at offsetKib.
func luks2Dump(offsetKib int64) storage.FakeResponse {
	return storage.FakeResponse{Stdout: []byte("LUKS header information\nVersion:       \t2\n" +
		"Data segments:\n  0: crypt\n\toffset: " + strconv.FormatInt(offsetKib*1024, 10) + " [bytes]\n" +
		"\tlength: (whole device)\n\tcipher: aes-xts-plain64\n\tsector: 4096 [bytes]\n\n" +
		"Keyslots:\n  0: luks2\n\tArea offset:32768 [bytes]\n")}
}

// zfsVolsize is the size a `zfs set volsize=` in calls set, or 0.
func zfsVolsize(t *testing.T, calls []string) int64 {
	t.Helper()

	var got int64

	for _, line := range calls {
		rest, ok := strings.CutPrefix(line, "zfs set volsize=")
		if !ok {
			continue
		}

		n, err := strconv.ParseInt(strings.TrimSuffix(strings.Fields(rest)[0], "K"), 10, 64)
		if err != nil {
			t.Fatalf("volsize in %q: %v", line, err)
		}

		got = n
	}

	return got
}

func countPrefixed(calls []string, prefix string) int {
	n := 0

	for _, line := range calls {
		if strings.HasPrefix(line, prefix) {
			n++
		}
	}

	return n
}

// TestLUKSHeaderKibIsReadOffTheDevice: the header size is the one
// luksDump reports, read once per device, read again after a format and
// for another device; one that cannot be read counts as the default and
// is read again; a resource without LUKS reads nothing.
func TestLUKSHeaderKibIsReadOffTheDevice(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup luksDump "+headerDev, luks2Dump(header32))
	fx.Expect("cryptsetup luksDump /dev/other", storage.FakeResponse{Err: storage.FakeExitError{Code: 1}})

	r := NewReconciler(ReconcilerConfig{Cryptsetup: luks.NewCryptsetup(fx)})
	dr := &intent.DesiredResource{Name: "pvc-h", LayerStack: []string{"DRBD", "LUKS", "STORAGE"}}

	for range 3 {
		if got := r.luksHeaderKib(t.Context(), dr, 0, headerDev); got != header32 {
			t.Fatalf("header: got %d KiB, want %d", got, header32)
		}
	}

	if n := countPrefixed(fx.CommandLines(), "cryptsetup luksDump "+headerDev); n != 1 {
		t.Fatalf("luksDump ran %d times, want once", n)
	}

	r.noteLUKSFormatted("pvc-h", 0)
	r.luksHeaderKib(t.Context(), dr, 0, headerDev)

	if n := countPrefixed(fx.CommandLines(), "cryptsetup luksDump "+headerDev); n != 2 {
		t.Fatalf("luksDump after a format ran %d times in all, want 2", n)
	}

	for range 2 {
		if got := r.luksHeaderKib(t.Context(), dr, 0, "/dev/other"); got != luks.HeaderKiB {
			t.Fatalf("unreadable header: got %d KiB, want the default %d", got, luks.HeaderKiB)
		}
	}

	if n := countPrefixed(fx.CommandLines(), "cryptsetup luksDump /dev/other"); n != 2 {
		t.Fatalf("an unreadable header was read %d times, want every time", n)
	}

	plain := &intent.DesiredResource{Name: "pvc-p", LayerStack: []string{"DRBD", "STORAGE"}}
	if got := r.luksHeaderKib(t.Context(), plain, 0, "/dev/zvol/data/pvc-p_00000"); got != 0 {
		t.Fatalf("no LUKS layer: got %d KiB, want 0", got)
	}

	if n := countPrefixed(fx.CommandLines(), "cryptsetup luksDump /dev/zvol/data/pvc-p"); n != 0 {
		t.Fatalf("luksDump ran for a resource without LUKS")
	}
}

// deviceOver is the device the consumer sees over a backing of
// backingKib under a LUKS header of headerKib: the mapper, or the DRBD
// device on it with metadata for the 15 peer slots a grow sizes for.
func deviceOver(backingKib, headerKib int64, withDRBD bool) int64 {
	mapper := backingKib - headerKib
	if !withDRBD {
		return mapper
	}

	return drbd.InternalMetadataNetKiB(mapper, drbd.GeometryForPeerSlots(drbd.LegacyPeerSlots))
}

// TestLUKSResizeEndsAtTheDefinition grows a 1 GiB LUKS volume to 2 GiB,
// under DRBD and without, over a header of 16 MiB and of 32 MiB, from a
// backing at the old size and from one an earlier grow sized for a 16
// MiB header. That last one is where a grow under a 32 MiB header used
// to stop: the backing could not grow further, the device stayed 16 MiB
// short and the resize never finished. Every case ends with the device
// at least the definition and never more than the allocation unit past
// it.
func TestLUKSResizeEndsAtTheDefinition(t *testing.T) {
	t.Parallel()

	const allocationKib = 1024 // the zfs provider's fallback volblocksize

	for _, withDRBD := range []bool{true, false} {
		stack := []string{"LUKS", "STORAGE"}
		if withDRBD {
			stack = []string{"DRBD", "LUKS", "STORAGE"}
		}

		dr := &intent.DesiredResource{Name: "pvc-h", LayerStack: stack}
		grown := &intent.DesiredVolume{SizeKib: gib2, AppliedSizeKib: gib1}

		sizedFor := func(headerKib int64) int64 {
			return alignUp(growBackingKib(dr, grown, withDRBD, drbd.LegacyPeerSlots, headerKib), allocationKib)
		}

		old := alignUp(lowerDiskKibUnder(dr, &intent.DesiredVolume{SizeKib: gib1}, header16), allocationKib)

		for _, tc := range []struct {
			name    string
			header  int64
			backing int64
		}{
			{"16 MiB header, backing at the old size", header16, old},
			{"32 MiB header, backing at the old size", header32, old},
			{"16 MiB header, backing already grown", header16, sizedFor(header16)},
			{"32 MiB header, backing grown for a 16 MiB one", header32, sizedFor(header16)},
		} {
			name := tc.name
			if withDRBD {
				name += " under DRBD"
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				storageExec := storage.NewFakeExec()
				provider := zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, storageExec)

				cryptExec := storage.NewFakeExec()
				cryptExec.Expect("cryptsetup luksDump "+headerDev, luks2Dump(tc.header))

				cfg := ReconcilerConfig{Cryptsetup: luks.NewCryptsetup(cryptExec)}
				if withDRBD {
					cfg.Adm = drbd.NewAdm(storage.NewFakeExec())
				}

				r := NewReconciler(cfg)
				vol := &intent.DesiredVolume{SizeKib: gib2, AppliedSizeKib: gib1}

				_, err := r.growBacking(t.Context(), provider, dr, vol,
					storage.VolumeStatus{DevicePath: headerDev, UsableKib: tc.backing},
					newDisklessProbe(r.cfg.Adm, dr.GetName()))
				if err != nil {
					t.Fatalf("growBacking: %v", err)
				}

				backing := tc.backing
				if set := zfsVolsize(t, storageExec.CommandLines()); set > 0 {
					if set < tc.backing {
						t.Fatalf("backing shrunk from %d to %d KiB", tc.backing, set)
					}

					backing = set
				}

				device := deviceOver(backing, tc.header, withDRBD)
				if device < gib2 || device > gib2+allocationKib {
					t.Fatalf("device over a %d KiB backing: %d KiB, want %d to %d",
						backing, device, gib2, gib2+allocationKib)
				}
			})
		}
	}
}

// TestFitBackingToLUKSHeader: a backing this process just formatted grows
// by what its header takes beyond the 16 MiB it was created for, before
// it is opened, so a new volume is not short of its definition. A 16 MiB
// header, a backing already large enough and a backing this process did
// not format are left alone, nothing shrinks, and a failed grow is tried
// again on the next open.
func TestFitBackingToLUKSHeader(t *testing.T) {
	t.Parallel()

	for _, withDRBD := range []bool{true, false} {
		stack := []string{"LUKS", "STORAGE"}
		if withDRBD {
			stack = []string{"DRBD", "LUKS", "STORAGE"}
		}

		dr := &intent.DesiredResource{Name: "pvc-h", LayerStack: stack}
		vol := &intent.DesiredVolume{SizeKib: gib1, StoragePool: "data"}
		created := alignUp(lowerDiskKib(dr, vol), 1024)

		for _, tc := range []struct {
			name      string
			header    int64
			backing   int64
			formatted bool
			wantGrow  bool
		}{
			{"32 MiB header on a new backing", header32, created, true, true},
			{"16 MiB header on a new backing", header16, created, true, false},
			{"32 MiB header on a backing large enough", header32, created + 64*1024, true, false},
			{"32 MiB header this process did not write", header32, created, false, false},
		} {
			name := tc.name
			if withDRBD {
				name += " under DRBD"
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				fx := storage.NewFakeExec()
				fx.Expect("cryptsetup luksDump "+headerDev, luks2Dump(tc.header))
				fx.Expect("zfs list -H -p -o name,volsize,used "+headerZvol, storage.FakeResponse{
					Stdout: []byte(headerZvol + "\t" + strconv.FormatInt(tc.backing*1024, 10) + "\t4096\n"),
				})

				r := NewReconciler(ReconcilerConfig{
					Cryptsetup: luks.NewCryptsetup(fx),
					Providers:  map[string]storage.Provider{"data": zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, fx)},
				})

				if tc.formatted {
					r.noteLUKSFormatted(dr.GetName(), 0)
				}

				err := r.fitBackingToLUKSHeader(t.Context(), dr, vol, headerDev)
				if err != nil {
					t.Fatalf("fitBackingToLUKSHeader: %v", err)
				}

				set := zfsVolsize(t, fx.CommandLines())
				if (set > 0) != tc.wantGrow {
					t.Fatalf("grew to %d KiB, want a grow %v (%v)", set, tc.wantGrow, fx.CommandLines())
				}

				backing := tc.backing
				if set > 0 {
					backing = set
				}

				if backing < tc.backing {
					t.Fatalf("backing shrunk from %d to %d KiB", tc.backing, backing)
				}

				if tc.formatted {
					device := deviceOver(backing, tc.header, withDRBD)
					if !withDRBD && device < gib1 {
						t.Fatalf("mapper over a %d KiB backing: %d KiB, want at least %d", backing, device, gib1)
					}

					// A new replica's metadata is created for its own
					// seven peer slots, fewer than deviceOver counts.
					if withDRBD && drbd.InternalMetadataNetKiB(backing-tc.header, replicaGeometry(dr)) < gib1 {
						t.Fatalf("DRBD device over a %d KiB backing is short of %d KiB", backing, gib1)
					}
				}

				before := len(fx.CommandLines())

				err = r.fitBackingToLUKSHeader(t.Context(), dr, vol, headerDev)
				if err != nil || len(fx.CommandLines()) != before {
					t.Fatalf("second call: %v, ran %v", err, fx.CommandLines()[before:])
				}
			})
		}
	}
}

// TestFitBackingToLUKSHeaderRetriesAFailedGrow: when the grow fails, the
// open fails too, and the next open grows the backing.
var errPoolBusy = errors.New("pool is busy")

func TestFitBackingToLUKSHeaderRetriesAFailedGrow(t *testing.T) {
	t.Parallel()

	dr := &intent.DesiredResource{Name: "pvc-h", LayerStack: []string{"LUKS", "STORAGE"}}
	vol := &intent.DesiredVolume{SizeKib: gib1, StoragePool: "data"}
	created := alignUp(lowerDiskKib(dr, vol), 1024)
	want := lowerDiskKibUnder(dr, vol, header32)

	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup luksDump "+headerDev, luks2Dump(header32))
	fx.Expect("zfs list -H -p -o name,volsize,used "+headerZvol, storage.FakeResponse{
		Stdout: []byte(headerZvol + "\t" + strconv.FormatInt(created*1024, 10) + "\t4096\n"),
	})

	setVolsize := "zfs set volsize=" + strconv.FormatInt(want, 10) + "K " + headerZvol
	fx.ExpectSequence(setVolsize, storage.FakeResponse{Err: errPoolBusy}, storage.FakeResponse{})

	r := NewReconciler(ReconcilerConfig{
		Cryptsetup: luks.NewCryptsetup(fx),
		Providers:  map[string]storage.Provider{"data": zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, fx)},
	})
	r.noteLUKSFormatted(dr.GetName(), 0)

	err := r.fitBackingToLUKSHeader(t.Context(), dr, vol, headerDev)
	if err == nil {
		t.Fatal("a failed grow did not fail the open")
	}

	err = r.fitBackingToLUKSHeader(t.Context(), dr, vol, headerDev)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}

	if n := countPrefixed(fx.CommandLines(), setVolsize); n != 2 {
		t.Fatalf("%q ran %d times, want 2 (%v)", setVolsize, n, fx.CommandLines())
	}
}

// TestLUKSMapperLagsHeader: a mapper open over a 32 MiB header lags its
// device by exactly that header and is not behind, so it is not resized,
// with DRBD over it, on every pass; one whose device grew past it is.
// Under the default header the 32 MiB tolerance is unchanged.
func TestLUKSMapperLagsHeader(t *testing.T) {
	t.Parallel()

	const mib = int64(1) << 20

	mapper := int64(gib2) * 1024

	for _, tc := range []struct {
		name      string
		dev       int64
		headerKib int64
		want      bool
	}{
		{"settled over a 16 MiB header", mapper + 16*mib, header16, false},
		{"settled over a 32 MiB header", mapper + 32*mib, header32, false},
		{"settled over a 64 MiB header", mapper + 64*mib, 64 * 1024, false},
		{"device grew 1 GiB over a 16 MiB header", mapper + 16*mib + 1024*mib, header16, true},
		{"device grew 1 GiB over a 32 MiB header", mapper + 32*mib + 1024*mib, header32, true},
		{"device grew 16 MiB over a 16 MiB header", mapper + 32*mib, header16, false},
		{"device grew 17 MiB over a 16 MiB header", mapper + 33*mib, header16, true},
		{"device grew 17 MiB over a 64 MiB header", mapper + 81*mib, 64 * 1024, true},
	} {
		got := luksMapperBehind(tc.dev, mapper) && luksMapperLagsHeader(tc.dev, mapper, tc.headerKib)
		if got != tc.want {
			t.Errorf("%s: behind = %v, want %v", tc.name, got, tc.want)
		}
	}
}
