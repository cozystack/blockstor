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

package linstormigrate

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// The fixture under testdata/dump is a synthetic LINSTOR k8s-backend
// dump. Its shape (tables, column sets, JSON-array-in-string columns,
// layer-id joins, props instance paths, flag bitmask populations) was
// modelled on two production cluster dumps; every identifier, IP,
// DRBD shared-secret and LUKS ciphertext in it is fabricated.
//
// Cases covered:
//   - vol1: DRBD,STORAGE ×2 diskful + a TIE_BREAKER witness (flags 388)
//     whose diskless replica has NO LAYER_STORAGE_VOLUMES row (LINSTOR
//     writes none for diskless) — the pool falls back to the replica's
//     StorPoolName prop; a STALE TIE_BREAKER bit (flags 128) on a
//     diskful replica that must be dropped, mirroring auto-diskful
//     leftovers observed in production; a preset TCP port + per-volume
//     minor + per-replica node-ids; a SUCCESSFUL and a
//     FAILED_DEPLOYMENT snapshot (only the former migrates).
//   - vol2: STORAGE-only single-replica volume.
//   - vol3: DRBD,LUKS,STORAGE with LAYER_LUKS_VOLUMES ciphertexts.
//   - vol4: RD marked DELETE — skipped entirely.
//   - vol5: allow-two-primaries (RWX) prop + an unknown flag bit 2048
//     replica that must convert with the bit reported, not guessed.

//nolint:gochecknoglobals // standard Go golden-test update flag
var update = flag.Bool("update", false, "rewrite the golden files under testdata/golden")

func convertFixture(t *testing.T) *Result {
	t.Helper()

	dump, err := LoadDump(filepath.Join("testdata", "dump"))
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	return res
}

// TestConvertGolden pins the full manifest stream and report so ANY
// behaviour change in the converter surfaces as a reviewable diff.
func TestConvertGolden(t *testing.T) {
	res := convertFixture(t)

	var manifests, report bytes.Buffer

	err := WriteManifests(&manifests, res)
	if err != nil {
		t.Fatalf("WriteManifests: %v", err)
	}

	err = WriteReport(&report, res)
	if err != nil {
		t.Fatalf("WriteReport: %v", err)
	}

	compareGolden(t, filepath.Join("testdata", "golden", "manifests.yaml"), manifests.Bytes())
	compareGolden(t, filepath.Join("testdata", "golden", "report.txt"), report.Bytes())
}

func compareGolden(t *testing.T, path string, got []byte) {
	t.Helper()

	if *update {
		err := os.WriteFile(path, got, 0o600)
		if err != nil {
			t.Fatalf("update golden %s: %v", path, err)
		}

		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run `go test ./pkg/linstormigrate -run TestConvertGolden -update` to create): %v", path, err)
	}

	if !bytes.Equal(want, got) {
		t.Errorf("%s differs from golden output; re-run with -update and review the diff", path)
	}
}

func findResource(t *testing.T, res *Result, name string) *crdv1alpha1.Resource {
	t.Helper()

	for i := range res.Resources {
		if res.Resources[i].Name == name {
			return &res.Resources[i]
		}
	}

	t.Fatalf("converted output has no Resource %q", name)

	return nil
}

// TestTieBreakerFlagsRequireDiskless pins the flag-decode calibration:
// the diskless witness keeps TIE_BREAKER while the STALE TIE_BREAKER
// bit on a diskful replica (an auto-diskful leftover observed in
// production) is dropped and reported. Written against the initial
// implementation that resolved diskless-ness from LAYER_STORAGE_VOLUMES
// — which has no rows for diskless replicas, so the witness LOST its
// TIE_BREAKER flag (this test fails on that code).
func TestTieBreakerFlagsRequireDiskless(t *testing.T) {
	res := convertFixture(t)

	witness := findResource(t, res, "pvc-vol1.node-c")
	if !slices.Equal(witness.Spec.Flags, []string{"DISKLESS", "DRBD_DISKLESS", "TIE_BREAKER"}) {
		t.Errorf("witness flags = %v, want [DISKLESS DRBD_DISKLESS TIE_BREAKER]", witness.Spec.Flags)
	}

	if witness.Spec.StoragePool != "DfltDisklessStorPool" {
		t.Errorf("witness storagePool = %q, want DfltDisklessStorPool (StorPoolName prop fallback)", witness.Spec.StoragePool)
	}

	stale := findResource(t, res, "pvc-vol1.node-b")
	if len(stale.Spec.Flags) != 0 {
		t.Errorf("diskful replica with stale TIE_BREAKER bit converted flags = %v, want none", stale.Spec.Flags)
	}

	if !hasWarning(res, "pvc-vol1.node-b: unhandled flags bits 128") {
		t.Errorf("stale TIE_BREAKER bit drop was not reported; warnings: %v", res.Warnings)
	}
}

// TestAdoptionSafetyLatches pins the two fields that make adopting
// LINSTOR volumes data-safe: every RD arrives Initialized and every
// replica arrives with the day0 skip disabled.
func TestAdoptionSafetyLatches(t *testing.T) {
	res := convertFixture(t)

	for i := range res.ResourceDefinitions {
		rd := &res.ResourceDefinitions[i]
		if rd.Spec.Initialized == nil || !*rd.Spec.Initialized {
			t.Errorf("RD %s: Initialized not latched true", rd.Name)
		}
	}

	for i := range res.Resources {
		replica := &res.Resources[i]
		if replica.Spec.SkipInitialSync == nil || *replica.Spec.SkipInitialSync {
			t.Errorf("Resource %s: skipInitialSync must be pinned false", replica.Name)
		}
	}
}

// TestDRBDIdentityCarriedVerbatim pins minors / node-ids / the RD TCP
// port — the identities that must survive migration byte-for-byte or
// the adopted kernel state no longer matches the CRDs.
func TestDRBDIdentityCarriedVerbatim(t *testing.T) {
	res := convertFixture(t)

	var vol1 *crdv1alpha1.ResourceDefinition

	for i := range res.ResourceDefinitions {
		if res.ResourceDefinitions[i].Name == "pvc-vol1" {
			vol1 = &res.ResourceDefinitions[i]

			break
		}
	}

	if vol1 == nil {
		t.Fatal("pvc-vol1 not converted")
	}

	if vol1.Spec.DRBDPort == nil || *vol1.Spec.DRBDPort != 7001 {
		t.Errorf("pvc-vol1 drbdPort = %v, want 7001", vol1.Spec.DRBDPort)
	}

	if len(vol1.Spec.VolumeDefinitions) != 1 || vol1.Spec.VolumeDefinitions[0].DRBDMinor == nil ||
		*vol1.Spec.VolumeDefinitions[0].DRBDMinor != 1001 {
		t.Errorf("pvc-vol1 volume minor not carried: %+v", vol1.Spec.VolumeDefinitions)
	}

	if vol1.Spec.ExtraProps[drbdSharedSecretProp] != "fixture-secret-vol1" {
		t.Errorf("pvc-vol1 shared secret not carried into %s", drbdSharedSecretProp)
	}

	for name, wantID := range map[string]int32{
		"pvc-vol1.node-a": 0,
		"pvc-vol1.node-b": 1,
		"pvc-vol1.node-c": 2,
	} {
		replica := findResource(t, res, name)
		if replica.Spec.DRBDNodeID == nil || *replica.Spec.DRBDNodeID != wantID {
			t.Errorf("%s drbdNodeID = %v, want %d", name, replica.Spec.DRBDNodeID, wantID)
		}

		if replica.Spec.DRBDPort == nil || *replica.Spec.DRBDPort != 7001 {
			t.Errorf("%s drbdPort = %v, want 7001", name, replica.Spec.DRBDPort)
		}
	}
}

// TestSkippedRows pins the never-guess policy: DELETE'd RDs and
// non-SUCCESSFUL snapshots are dropped with a report line instead of
// being converted into live objects.
func TestSkippedRows(t *testing.T) {
	res := convertFixture(t)

	for i := range res.ResourceDefinitions {
		if res.ResourceDefinitions[i].Name == "pvc-vol4" {
			t.Error("DELETE'd RD pvc-vol4 must not convert")
		}
	}

	if !hasWarning(res, "pvc-vol4: marked DELETE") {
		t.Errorf("DELETE'd RD skip not reported; warnings: %v", res.Warnings)
	}

	names := make([]string, 0, len(res.Snapshots))
	for i := range res.Snapshots {
		names = append(names, res.Snapshots[i].Name)
	}

	if !slices.Equal(names, []string{"pvc-vol1.snap-good"}) {
		t.Errorf("snapshots = %v, want only pvc-vol1.snap-good (snap-bad is FAILED_DEPLOYMENT)", names)
	}

	if !hasWarning(res, "snap-bad: FAILED_DEPLOYMENT") {
		t.Errorf("failed snapshot skip not reported; warnings: %v", res.Warnings)
	}
}

func hasWarning(res *Result, substr string) bool {
	for _, w := range res.Warnings {
		if strings.Contains(w, substr) {
			return true
		}
	}

	return false
}
