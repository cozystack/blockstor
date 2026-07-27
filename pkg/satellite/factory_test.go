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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestFactoryZFSPrefersZPoolForThickKind pins the kind-specific
// prop-key precedence introduced by commit 13d1215dc: when BOTH
// `StorDriver/ZPool` and `StorDriver/ZPoolThin` are present, the
// canonical key for the requested kind wins. For ZFS (thick) that
// is `ZPool`; for ZFS_THIN it is `ZPoolThin`. A regression that
// always picked one key regardless of kind would cause a thick
// pool to talk to the operator's thin-pool dataset (or vice
// versa) — silent data routing to the wrong pool.
func TestFactoryZFSPrefersZPoolForThickKind(t *testing.T) {
	t.Parallel()

	props := map[string]string{
		"StorDriver/ZPool":     "tank",
		"StorDriver/ZPoolThin": "other",
	}

	// Thick kind → pick `tank` (the ZPool key).
	thickExec := storage.NewFakeExec()

	provThick, err := satellite.NewProviderFromKind(satellite.ProviderKindZFS, props, thickExec)
	if err != nil {
		t.Fatalf("NewProviderFromKind(ZFS): %v", err)
	}

	if provThick == nil {
		t.Fatalf("NewProviderFromKind(ZFS) returned nil provider")
	}

	if got := provThick.Kind(); got != "ZFS" {
		t.Errorf("thick provider Kind: got %q, want ZFS", got)
	}

	assertZFSProviderUsesPool(t, provThick, thickExec, "tank")

	// Flip kind → pick `other` (the ZPoolThin key).
	thinExec := storage.NewFakeExec()

	provThin, err := satellite.NewProviderFromKind(satellite.ProviderKindZFSThin, props, thinExec)
	if err != nil {
		t.Fatalf("NewProviderFromKind(ZFS_THIN): %v", err)
	}

	if provThin == nil {
		t.Fatalf("NewProviderFromKind(ZFS_THIN) returned nil provider")
	}

	if got := provThin.Kind(); got != "ZFS_THIN" {
		t.Errorf("thin provider Kind: got %q, want ZFS_THIN", got)
	}

	assertZFSProviderUsesPool(t, provThin, thinExec, "other")
}

// TestFactoryZFSFallsBackBetweenKeys exercises the legacy
// configuration where only the "wrong" key is set — operators
// who configured a ZFS_THIN pool using `StorDriver/ZPool` (or
// the reverse) still need to bring the pool up so existing CRDs
// don't fail provider construction after the rename. The factory
// must fall back to the secondary key when the primary is
// missing.
func TestFactoryZFSFallsBackBetweenKeys(t *testing.T) {
	t.Parallel()

	// Thick kind but only the thin key is set → fall back, pool=zthin.
	thickExec := storage.NewFakeExec()

	provThick, err := satellite.NewProviderFromKind(
		satellite.ProviderKindZFS,
		map[string]string{"StorDriver/ZPoolThin": "zthin"},
		thickExec,
	)
	if err != nil {
		t.Fatalf("NewProviderFromKind(ZFS, only ZPoolThin): %v", err)
	}

	if provThick == nil {
		t.Fatalf("NewProviderFromKind(ZFS, only ZPoolThin) returned nil provider")
	}

	assertZFSProviderUsesPool(t, provThick, thickExec, "zthin")

	// Thin kind but only the thick key is set → fall back, pool=zthick.
	thinExec := storage.NewFakeExec()

	provThin, err := satellite.NewProviderFromKind(
		satellite.ProviderKindZFSThin,
		map[string]string{"StorDriver/ZPool": "zthick"},
		thinExec,
	)
	if err != nil {
		t.Fatalf("NewProviderFromKind(ZFS_THIN, only ZPool): %v", err)
	}

	if provThin == nil {
		t.Fatalf("NewProviderFromKind(ZFS_THIN, only ZPool) returned nil provider")
	}

	assertZFSProviderUsesPool(t, provThin, thinExec, "zthick")
}

// TestFactoryZFSAdoptsStorPoolNameKey pins B1: a real LINSTOR ≥1.x
// database stores the zpool name under the generic
// `StorDriver/StorPoolName` with BOTH ZPool keys blank (verified
// against a production ZFS-backed cluster: `sp l` reports
// StorPoolName=data with ZPool/ZPoolThin empty, and the node's zpool is
// named `data`). Before the fallback, NewProviderFromKind
// errored on these exact props → the pool never registered → NO
// diskful resource could adopt. This test fails on the pre-fix factory
// and passes with the StorPoolName fallback.
func TestFactoryZFSAdoptsStorPoolNameKey(t *testing.T) {
	t.Parallel()

	prodProps := map[string]string{
		"StorDriver/StorPoolName":                   "data",
		"StorDriver/internal/AllocationGranularity": "16",
		"StorDriver/internal/optIoSize":             "33554432",
		"StorDriver/internal/minIoSize":             "4096",
	}

	thickExec := storage.NewFakeExec()

	provThick, err := satellite.NewProviderFromKind(satellite.ProviderKindZFS, prodProps, thickExec)
	if err != nil {
		t.Fatalf("NewProviderFromKind(ZFS, StorPoolName only): %v", err)
	}

	if provThick == nil {
		t.Fatalf("NewProviderFromKind(ZFS, StorPoolName only) returned nil provider")
	}

	assertZFSProviderUsesPool(t, provThick, thickExec, "data")

	thinExec := storage.NewFakeExec()

	provThin, err := satellite.NewProviderFromKind(satellite.ProviderKindZFSThin, prodProps, thinExec)
	if err != nil {
		t.Fatalf("NewProviderFromKind(ZFS_THIN, StorPoolName only): %v", err)
	}

	if provThin == nil {
		t.Fatalf("NewProviderFromKind(ZFS_THIN, StorPoolName only) returned nil provider")
	}

	assertZFSProviderUsesPool(t, provThin, thinExec, "data")
}

// TestFactoryLVMAdoptsStorPoolNameKey pins the same generic-key gap for
// LVM and LVM_THIN that the ZFS test pins: LINSTOR records the backing
// volume group (LVM) or `<vg>/<thinpool>` (LVM_THIN) under the generic
// `StorDriver/StorPoolName`, leaving `StorDriver/LvmVg` and
// `StorDriver/ThinPool` blank. Without the fallback an adopted
// LVM-backed pool never registers a provider — the same total-adoption
// failure the ZFS fix addressed, and it would only surface at cutover,
// after the source controller is already stopped.
func TestFactoryLVMAdoptsStorPoolNameKey(t *testing.T) {
	t.Parallel()

	thick, err := satellite.NewProviderFromKind(
		satellite.ProviderKindLVM,
		map[string]string{"StorDriver/StorPoolName": "vg0"},
		storage.NewFakeExec(),
	)
	if err != nil {
		t.Fatalf("NewProviderFromKind(LVM, StorPoolName only): %v", err)
	}

	if thick == nil || thick.Kind() != "LVM" {
		t.Fatalf("LVM provider not registered from the generic key: %v", thick)
	}

	// LVM_THIN records both halves in one generic value.
	thin, err := satellite.NewProviderFromKind(
		satellite.ProviderKindLVMThin,
		map[string]string{"StorDriver/StorPoolName": "vg0/thinpool"},
		storage.NewFakeExec(),
	)
	if err != nil {
		t.Fatalf("NewProviderFromKind(LVM_THIN, StorPoolName only): %v", err)
	}

	if thin == nil || thin.Kind() != "LVM_THIN" {
		t.Fatalf("LVM_THIN provider not registered from the generic key: %v", thin)
	}

	// A generic value carrying only the VG still needs the thin pool:
	// fail loudly rather than guess a thin-pool name.
	_, err = satellite.NewProviderFromKind(
		satellite.ProviderKindLVMThin,
		map[string]string{"StorDriver/StorPoolName": "vg0"},
		storage.NewFakeExec(),
	)
	if err == nil {
		t.Error("LVM_THIN with no thin pool in either key must error, not guess")
	}
}

// TestFactoryZFSMissingBothKeysErrors documents the negative
// path: when neither key is present the factory must surface a
// readable error mentioning the canonical (primary) key so
// operators know which prop to add.
func TestFactoryZFSMissingBothKeysErrors(t *testing.T) {
	t.Parallel()

	_, err := satellite.NewProviderFromKind(
		satellite.ProviderKindZFS,
		map[string]string{},
		storage.NewFakeExec(),
	)
	if err == nil {
		t.Fatalf("NewProviderFromKind(ZFS, empty props): want error, got nil")
	}

	_, err = satellite.NewProviderFromKind(
		satellite.ProviderKindZFSThin,
		map[string]string{},
		storage.NewFakeExec(),
	)
	if err == nil {
		t.Fatalf("NewProviderFromKind(ZFS_THIN, empty props): want error, got nil")
	}
}

// assertZFSProviderUsesPool drives a CreateVolume through the
// provider and asserts that the resulting `zfs create` command
// targets a dataset under <wantPool>/. This is the only
// black-box way to verify which prop key was picked, because the
// Config struct lives in the zfs package and isn't exposed back
// through the storage.Provider interface.
//
// Pre-loads the post-create ensureRefreservation observability lookups
// (Bug 255 retrofit) so the thick variant's helper finishes cleanly;
// thin providers skip those lookups inside the helper itself.
func assertZFSProviderUsesPool(t *testing.T, prov storage.Provider, fx *storage.FakeExec, wantPool string) {
	t.Helper()

	probeDataset := wantPool + "/probe_00000"
	fx.Expect("zfs get -Hp -o value volsize "+probeDataset,
		storage.FakeResponse{Stdout: []byte("1048576\n")})
	fx.Expect("zfs get -Hp -o value refreservation "+probeDataset,
		storage.FakeResponse{Stdout: []byte("1048576\n")})

	err := prov.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "probe",
		VolumeNumber: 0,
		SizeKib:      1024,
	})
	if err != nil {
		t.Fatalf("probe CreateVolume on %s: %v", wantPool, err)
	}

	wantDataset := wantPool + "/probe_00000"

	var saw bool

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs create ") && strings.HasSuffix(line, " "+wantDataset) {
			saw = true

			break
		}
	}

	if !saw {
		t.Errorf("expected zfs create targeting %q; got commands %v", wantDataset, fx.CommandLines())
	}
}
