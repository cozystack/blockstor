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
func assertZFSProviderUsesPool(t *testing.T, prov storage.Provider, fx *storage.FakeExec, wantPool string) {
	t.Helper()

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
