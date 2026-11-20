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

package blocksize_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/blocksize"
)

func TestPoolIoSize(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		value string
		want  int64
	}{
		{value: "", want: 512},
		{value: "4096", want: 4096},
		{value: "16384", want: 4096},
		{value: "256", want: 512},
		{value: "junk", want: 512},
	} {
		got := blocksize.PoolIoSize(map[string]string{blocksize.PropPoolMinIoSize: tc.value})
		if got != tc.want {
			t.Errorf("PoolIoSize(%q) = %d, want %d", tc.value, got, tc.want)
		}
	}
}

func TestHasSpecialLayers(t *testing.T) {
	t.Parallel()

	if blocksize.HasSpecialLayers([]string{"DRBD", "STORAGE"}) {
		t.Error("DRBD,STORAGE counted as special")
	}

	if blocksize.HasSpecialLayers(nil) {
		t.Error("an empty stack counted as special")
	}

	if !blocksize.HasSpecialLayers([]string{"DRBD", "LUKS", "STORAGE"}) {
		t.Error("LUKS not counted as special")
	}
}

func TestAutoEnabled(t *testing.T) {
	t.Parallel()

	off := map[string]string{blocksize.PropAuto: "false"}
	on := map[string]string{blocksize.PropAuto: "true"}

	if !blocksize.AutoEnabled() {
		t.Error("unset is not on")
	}

	if blocksize.AutoEnabled(nil, off) {
		t.Error("false on a lower level was ignored")
	}

	if !blocksize.AutoEnabled(on, off) {
		t.Error("the closest level did not win")
	}
}

// TestSwitchesParseLikeLINSTOR: LINSTOR reads both switches with Java's
// Boolean.parseBoolean, so only "true", in any case, is true. "yes" or
// "1" on auto-block-size switch the rule off there, and "yes" on the
// freeze stamp does not freeze.
func TestSwitchesParseLikeLINSTOR(t *testing.T) {
	t.Parallel()

	for value, want := range map[string]bool{
		"true": true, "True": true, "TRUE": true,
		"false": false, "yes": false, "no": false, "1": false, "0": false, "off": false, "on": false, "": false,
	} {
		if got := blocksize.AutoEnabled(map[string]string{blocksize.PropAuto: value}); got != want {
			t.Errorf("auto-block-size %q: on = %v, want %v", value, got, want)
		}

		if got := blocksize.Frozen(map[string]string{blocksize.PropFreeze: value}); got != want {
			t.Errorf("freeze-block-size %q: frozen = %v, want %v", value, got, want)
		}
	}
}

func TestDecide(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		in   blocksize.Input
		want blocksize.Decision
	}{
		{
			name: "auto-off",
			in:   blocksize.Input{ReplicaSize: 4096, CanChange: true},
			want: blocksize.Decision{Verdict: blocksize.Keep},
		},
		{
			name: "unset-takes-pool",
			in:   blocksize.Input{Auto: true, ReplicaSize: 4096, CanChange: true},
			want: blocksize.Decision{Verdict: blocksize.Set, Size: 4096},
		},
		{
			name: "unset-takes-default-pool",
			in:   blocksize.Input{Auto: true, ReplicaSize: 512, CanChange: true},
			want: blocksize.Decision{Verdict: blocksize.Set, Size: 512},
		},
		{
			name: "unset-cannot-change",
			in:   blocksize.Input{Auto: true, ReplicaSize: 4096},
			want: blocksize.Decision{Verdict: blocksize.Incompatible},
		},
		{
			name: "smaller-pool-keeps",
			in: blocksize.Input{
				Auto: true, ReplicaSize: 512,
				VDProps: map[string]string{blocksize.PropBlockSize: "4096"},
			},
			want: blocksize.Decision{Verdict: blocksize.Keep},
		},
		{
			name: "larger-pool-raises",
			in: blocksize.Input{
				Auto: true, ReplicaSize: 4096, CanChange: true,
				VDProps: map[string]string{blocksize.PropBlockSize: "512"},
			},
			want: blocksize.Decision{Verdict: blocksize.Set, Size: 4096},
		},
		{
			name: "larger-pool-frozen",
			in: blocksize.Input{
				Auto: true, ReplicaSize: 4096,
				VDProps: map[string]string{blocksize.PropBlockSize: "512"},
			},
			want: blocksize.Decision{Verdict: blocksize.Incompatible, Size: 512},
		},
	} {
		if got := blocksize.Decide(tc.in); got != tc.want {
			t.Errorf("%s: Decide = %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

func TestFreezeAndRelease(t *testing.T) {
	t.Parallel()

	props := map[string]string{blocksize.PropBlockSize: "4096", "Other": "x"}

	if !blocksize.Freeze(props) || !blocksize.Frozen(props) {
		t.Fatalf("Freeze did not freeze: %v", props)
	}

	if blocksize.Freeze(props) {
		t.Fatal("a second Freeze reported a change")
	}

	if !blocksize.Release(props) {
		t.Fatal("Release reported no change")
	}

	if len(props) != 1 || props["Other"] != "x" {
		t.Fatalf("Release left %v", props)
	}
}

// TestCanChange pins LINSTOR's canChangeMinIoSize.
func TestCanChange(t *testing.T) {
	t.Parallel()

	frozen := []map[string]string{{blocksize.PropFreeze: "True"}}

	for _, tc := range []struct {
		name                  string
		placed, auto, primary bool
		vds                   []map[string]string
		want                  bool
	}{
		{name: "unplaced", auto: false, primary: true, vds: frozen, want: true},
		{name: "placed-auto", placed: true, auto: true, want: true},
		{name: "placed-auto-off", placed: true},
		{name: "placed-primary", placed: true, auto: true, primary: true},
		{name: "placed-frozen", placed: true, auto: true, vds: frozen},
	} {
		if got := blocksize.CanChange(tc.placed, tc.auto, tc.primary, tc.vds); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestFloor pins LINSTOR's getFloorVolumesMinIoSize.
func TestFloor(t *testing.T) {
	t.Parallel()

	sized := func(v string) map[string]string { return map[string]string{blocksize.PropBlockSize: v} }

	for _, tc := range []struct {
		name                  string
		placed, special, auto bool
		vds                   []map[string]string
		want                  int64
		has                   bool
	}{
		{name: "unplaced", auto: true, vds: []map[string]string{sized("4096")}},
		{name: "special", placed: true, special: true, want: 4096, has: true},
		{name: "auto-lowest", placed: true, auto: true, vds: []map[string]string{sized("4096"), sized("512")}, want: 512, has: true},
		{name: "auto-unset-is-default", placed: true, auto: true, vds: []map[string]string{sized("4096"), nil}, want: 512, has: true},
		{name: "auto-all-4096", placed: true, auto: true, vds: []map[string]string{sized("4096")}, want: 4096, has: true},
		{name: "auto-off-only-set", placed: true, vds: []map[string]string{sized("4096"), nil}, want: 4096, has: true},
		{name: "auto-off-none-set", placed: true, vds: []map[string]string{nil}},
	} {
		got, has := blocksize.Floor(tc.placed, tc.special, tc.auto, tc.vds)
		if got != tc.want || has != tc.has {
			t.Errorf("%s: (%d, %v), want (%d, %v)", tc.name, got, has, tc.want, tc.has)
		}
	}
}
