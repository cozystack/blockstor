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

package validate_test

import (
	"errors"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/validate"
)

// This package exists so the two write doors answer the same question the
// same way, which means its rules have to be exercised directly rather than
// through whichever caller happens to reach them.

func TestLayerStackOrder(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		layers []string
		wantOK bool
	}{
		"empty defaults later":   {nil, true},
		"drbd over storage":      {[]string{"DRBD", "STORAGE"}, true},
		"luks between them":      {[]string{"DRBD", "LUKS", "STORAGE"}, true},
		"mixed case as upstream": {[]string{"drbd", "Luks", "storage"}, true},
		"luks above drbd":        {[]string{"LUKS", "DRBD", "STORAGE"}, false},
		"storage not last":       {[]string{"DRBD", "STORAGE", "LUKS"}, false},
		"a one-character typo":   {[]string{"DRBD", "LUSK", "STORAGE"}, false},
		"an unknown layer":       {[]string{"DRBD", "ZFS", "STORAGE"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := validate.LayerStack(tc.layers)
			if tc.wantOK && err != nil {
				t.Errorf("%v was refused: %v", tc.layers, err)
			}

			if !tc.wantOK && err == nil {
				t.Errorf("%v was accepted", tc.layers)
			}
		})
	}
}

// LUKS above DRBD replicates plaintext, and the operator believes the
// opposite. That is the reason the order is checked at all, so it gets its
// own assertion rather than living only in the table.
func TestLayerStackRefusesPlaintextReplication(t *testing.T) {
	t.Parallel()

	if err := validate.LayerStack([]string{"LUKS", "DRBD", "STORAGE"}); err == nil {
		t.Fatal("LUKS above DRBD was accepted, so DRBD would replicate plaintext")
	}
}

func TestVolumeSizeBounds(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		size   int64
		wantOK bool
	}{
		"at the floor":    {validate.MinVolumeSizeKib, true},
		"at the ceiling":  {validate.MaxVolumeSizeKib, true},
		"in between":      {8 * 1024, true},
		"one below floor": {validate.MinVolumeSizeKib - 1, false},
		"one above top":   {validate.MaxVolumeSizeKib + 1, false},
		"zero":            {0, false},
		"negative":        {-1, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := validate.VolumeSizeKib(tc.size)
			if tc.wantOK && err != nil {
				t.Errorf("%d was refused: %v", tc.size, err)
			}

			if !tc.wantOK && err == nil {
				t.Errorf("%d was accepted", tc.size)
			}
		})
	}
}

// Zero is the one value that must never reach the satellite: it does not
// fail there, it loops on `drbdadm create-md` forever.
func TestVolumeSizeZeroIsRefused(t *testing.T) {
	t.Parallel()

	err := validate.VolumeSizeKib(0)
	if !errors.Is(err, validate.ErrVolumeSizeBelowMinimum) {
		t.Fatalf("zero was not refused as below the minimum: %v", err)
	}
}

func TestStoragePoolPropEdit(t *testing.T) {
	t.Parallel()

	const backing = "StorDriver/LvmVg"

	for name, tc := range map[string]struct {
		before, after map[string]string
		wantOK        bool
	}{
		"an unrelated key changes": {
			map[string]string{backing: "vg0", "PrefNic": "a"},
			map[string]string{backing: "vg0", "PrefNic": "b"},
			true,
		},
		"nothing changes": {
			map[string]string{backing: "vg0"},
			map[string]string{backing: "vg0"},
			true,
		},
		"the backing key is rewritten": {
			map[string]string{backing: "vg0"},
			map[string]string{backing: "vg1"},
			false,
		},
		"the backing key is deleted": {
			map[string]string{backing: "vg0"},
			map[string]string{},
			false,
		},
		"the backing key appears": {
			map[string]string{},
			map[string]string{backing: "vg0"},
			false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := validate.StoragePoolPropEdit(tc.before, tc.after)
			if tc.wantOK && err != nil {
				t.Errorf("refused: %v", err)
			}

			if !tc.wantOK && err == nil {
				t.Error("accepted")
			}
		})
	}
}

// The state comparison cannot see an operator setting a backing key to the
// value it already has, and the REST door refuses exactly that because it
// inspects the operation. Both questions have to be asked.
func TestStoragePoolPropNamedCatchesTheNoOpEdit(t *testing.T) {
	t.Parallel()

	const backing = "StorDriver/LvmVg"

	same := map[string]string{backing: "vg0"}
	if err := validate.StoragePoolPropEdit(same, map[string]string{backing: "vg0"}); err != nil {
		t.Fatalf("the state comparison is supposed to see no change here: %v", err)
	}

	if err := validate.StoragePoolPropNamed(backing); !errors.Is(err, validate.ErrImmutableProp) {
		t.Errorf("naming an immutable key was accepted: %v", err)
	}

	if err := validate.StoragePoolPropNamed("PrefNic"); err != nil {
		t.Errorf("an ordinary key was refused: %v", err)
	}
}

// Every key that pins where a pool's data physically lives has to be in the
// set: one left out is a pool that can be pointed at another backing store
// while its replicas keep reporting UpToDate.
//
// The expectation is spelled out rather than derived from the list under
// test. Iterating the list and asserting each entry is refused passes no
// matter what the list contains, so dropping a key from it — losing the rule
// for that backing store — went unnoticed.
func TestImmutableStoragePoolPropsCoverEveryBackingKey(t *testing.T) {
	t.Parallel()

	want := []string{
		"StorDriver/StorPoolName",
		"StorDriver/LvmVg",
		"StorDriver/ThinPool",
		"StorDriver/ZPool",
		"StorDriver/ZPoolThin",
		"StorDriver/FileDir",
	}

	got := validate.ImmutableStoragePoolProps()
	if len(got) != len(want) {
		t.Fatalf("immutable set = %v, want %v", got, want)
	}

	for _, key := range want {
		if !slices.Contains(got, key) {
			t.Errorf("%s is not in the immutable set, so a pool can be pointed "+
				"at another backing store while its replicas report UpToDate", key)
		}

		if err := validate.StoragePoolPropNamed(key); err == nil {
			t.Errorf("%s is in the set but is not refused when named", key)
		}
	}
}

func TestRestoreNodesHoldSnapshot(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		named, holds []string
		wantOK       bool
	}{
		"no nodes named falls back to the snapshot's own": {nil, []string{"node-1"}, true},
		"a snapshot recording no nodes is not judged":     {[]string{"node-9"}, nil, true},
		"every named node holds it":                       {[]string{"node-1"}, []string{"node-1", "node-2"}, true},
		"a named node does not hold it":                   {[]string{"node-9"}, []string{"node-1"}, false},
		"one of several does not hold it":                 {[]string{"node-1", "node-9"}, []string{"node-1"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := validate.RestoreNodesHoldSnapshot(tc.named, tc.holds)
			if tc.wantOK && err != nil {
				t.Errorf("refused: %v", err)
			}

			if !tc.wantOK && !errors.Is(err, validate.ErrRestoreNodeLacksSnapshot) {
				t.Errorf("accepted or refused for the wrong reason: %v", err)
			}
		})
	}
}

func diskful(node string, states ...string) apiv1.Resource {
	res := apiv1.Resource{NodeName: node}
	for _, s := range states {
		res.Volumes = append(res.Volumes, apiv1.Volume{State: apiv1.VolumeState{DiskState: s}})
	}

	return res
}

// Deleting the last complete copy while a peer is still syncing from it
// strands that peer with no source. The judgement is asymmetric on purpose:
// a diskful replica whose disk state has not been projected yet counts as a
// possible source, because concluding "not a source" from an unknown
// projection is the false-allow that does the damage.
func TestMidSyncDeleteRefusal(t *testing.T) {
	t.Parallel()

	upToDate := func(node string) apiv1.Resource { return diskful(node, "UpToDate") }
	syncing := func(node string) apiv1.Resource { return diskful(node, "SyncTarget") }
	unprojected := func(node string) apiv1.Resource { return diskful(node) }

	diskless := func(node string) apiv1.Resource {
		return apiv1.Resource{NodeName: node, Flags: []string{apiv1.ResourceFlagDiskless}}
	}

	for name, tc := range map[string]struct {
		target   apiv1.Resource
		siblings []apiv1.Resource
		refuse   bool
	}{
		"last complete copy, a peer syncing from it": {
			upToDate("node-1"), []apiv1.Resource{syncing("node-2")}, true,
		},
		"another complete copy survives": {
			upToDate("node-1"), []apiv1.Resource{upToDate("node-2"), syncing("node-3")}, false,
		},
		"no peer is syncing": {
			upToDate("node-1"), []apiv1.Resource{upToDate("node-2")}, false,
		},
		"a diskless target is never a source": {
			diskless("node-1"), []apiv1.Resource{syncing("node-2")}, false,
		},
		"an unprojected target counts as a possible source": {
			unprojected("node-1"), []apiv1.Resource{syncing("node-2")}, true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			target := tc.target
			if got := validate.MidSyncDeleteRefusal(&target, tc.siblings); got != tc.refuse {
				t.Errorf("refusal = %v, want %v", got, tc.refuse)
			}
		})
	}
}

func TestResourceIsDiskful(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		flags   []string
		diskful bool
	}{
		"no flags":    {nil, true},
		"diskless":    {[]string{apiv1.ResourceFlagDiskless}, false},
		"tie breaker": {[]string{apiv1.ResourceFlagTieBreaker}, false},
		"unrelated":   {[]string{"DRBD_DISKLESS_ALLOWED"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := apiv1.Resource{Flags: tc.flags}
			if got := validate.ResourceIsDiskful(&res); got != tc.diskful {
				t.Errorf("diskful = %v, want %v", got, tc.diskful)
			}
		})
	}
}

// The refusal echoes the operator's own order. The REST door is an
// upstream-compatible surface and has always reported the nodes as they were
// passed, so sorting them here would change a message clients may read.
func TestRestoreRefusalKeepsTheOperatorsOrder(t *testing.T) {
	t.Parallel()

	missing := validate.RestoreNodesMissingSnapshot(
		[]string{"node-9", "node-8", "node-9", ""}, []string{"node-1"})

	want := []string{"node-9", "node-8"}
	if !slices.Equal(missing, want) {
		t.Errorf("missing = %v, want %v (input order, deduped, empties dropped)", missing, want)
	}
}
