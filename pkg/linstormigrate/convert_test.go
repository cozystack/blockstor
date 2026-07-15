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

	if len(vol1.Spec.VolumeDefinitions) == 0 || vol1.Spec.VolumeDefinitions[0].DRBDMinor == nil ||
		*vol1.Spec.VolumeDefinitions[0].DRBDMinor != 1001 {
		t.Errorf("pvc-vol1 volume-0 minor not carried: %+v", vol1.Spec.VolumeDefinitions)
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

// TestLiveDRBDPortWins pins the adoption-critical port behaviour: a
// live port map (captured from the running kernel) is preset on the RD
// and every replica, overriding the dump. Without it, an RD whose dump
// carries no tcp_port (the LINSTOR-1.33 norm) gets nil DRBDPort and a
// reported gap — because a mismatched port makes adoption's `drbdadm
// adjust` reconnect the live mesh.
func TestLiveDRBDPortWins(t *testing.T) {
	dump, err := LoadDump(filepath.Join("testdata", "dump"))
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	// vol5 carries NO tcp_port in the fixture; vol1 carries 7001.
	res, err := ConvertWithOptions(dump, Options{
		DRBDPorts: map[string]int32{"pvc-vol5": 7042, "pvc-vol1": 7099},
	})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	ports := map[string]*int32{}
	for i := range res.ResourceDefinitions {
		ports[res.ResourceDefinitions[i].Name] = res.ResourceDefinitions[i].Spec.DRBDPort
	}

	if ports["pvc-vol5"] == nil || *ports["pvc-vol5"] != 7042 {
		t.Errorf("vol5 (no dump port) DRBDPort = %v, want live 7042", ports["pvc-vol5"])
	}

	if ports["pvc-vol1"] == nil || *ports["pvc-vol1"] != 7099 {
		t.Errorf("vol1 DRBDPort = %v, want live 7099 overriding dump 7001", ports["pvc-vol1"])
	}

	// replicas of vol5 must carry the same live port.
	replica := findResource(t, res, "pvc-vol5.node-a")
	if replica.Spec.DRBDPort == nil || *replica.Spec.DRBDPort != 7042 {
		t.Errorf("vol5.node-a DRBDPort = %v, want 7042", replica.Spec.DRBDPort)
	}
}

// TestMissingDRBDPortReported pins that a DRBD RD with no port in the
// dump AND no live map yields nil (blockstor allocates) and reports
// the gap exactly once — the operator MUST see it to decide on the
// reconnect blip.
func TestMissingDRBDPortReported(t *testing.T) {
	res := convertFixture(t) // no port map

	var vol5Port *int32

	for i := range res.ResourceDefinitions {
		if res.ResourceDefinitions[i].Name == "pvc-vol5" {
			vol5Port = res.ResourceDefinitions[i].Spec.DRBDPort
		}
	}

	if vol5Port != nil {
		t.Errorf("vol5 DRBDPort = %v, want nil (no dump port, no live map)", vol5Port)
	}

	count := 0

	for _, w := range res.Warnings {
		if strings.Contains(w, "pvc-vol5") && strings.Contains(w, "DRBD port") {
			count++
		}
	}

	if count != 1 {
		t.Errorf("vol5 missing-port warning count = %d, want exactly 1 (deduped across RD + replicas)", count)
	}
}

// TestParseDRBDPorts pins the port-file parser incl. rejection.
func TestParseDRBDPorts(t *testing.T) {
	ports, err := ParseDRBDPorts("# comment\nPVC-ABC 7001\n\npvc-def   7002\n")
	if err != nil {
		t.Fatalf("ParseDRBDPorts: %v", err)
	}

	if ports["pvc-abc"] != 7001 || ports["pvc-def"] != 7002 {
		t.Errorf("parsed ports = %v", ports)
	}

	for _, bad := range []string{"only-one-field", "rd 70000", "rd notaport", "rd 0"} {
		if _, err := ParseDRBDPorts(bad); err == nil {
			t.Errorf("ParseDRBDPorts(%q) accepted a malformed line", bad)
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

	// The adopted-snapshot annotations keep the controller from
	// re-running the suspend→take orchestration against production
	// I/O; the created-at annotation carries the newest per-node
	// create_timestamp (node-b's 1770000000011 in the fixture).
	if len(res.Snapshots) == 1 {
		ann := res.Snapshots[0].Annotations
		if ann[crdv1alpha1.AnnotationSnapshotAdopted] != "true" {
			t.Errorf("migrated snapshot missing %s annotation: %v", crdv1alpha1.AnnotationSnapshotAdopted, ann)
		}

		if ann[crdv1alpha1.AnnotationSnapshotAdoptedCreatedAt] != "1770000000011" {
			t.Errorf("adopted-created-at = %q, want 1770000000011", ann[crdv1alpha1.AnnotationSnapshotAdoptedCreatedAt])
		}
	}

	if !hasWarning(res, "snap-bad: FAILED_DEPLOYMENT") {
		t.Errorf("failed snapshot skip not reported; warnings: %v", res.Warnings)
	}
}

// TestMultiVolumeMinorsVerbatim pins the multi-volume case observed in
// production (two volume slots whose DRBD minors are NOT contiguous):
// each volume must carry its own minor verbatim — a base+k derivation
// would corrupt the second volume's device identity.
func TestMultiVolumeMinorsVerbatim(t *testing.T) {
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

	if len(vol1.Spec.VolumeDefinitions) != 2 {
		t.Fatalf("pvc-vol1 volumeDefinitions = %d, want 2", len(vol1.Spec.VolumeDefinitions))
	}

	for i, want := range []struct {
		number int32
		minor  int32
		size   int64
	}{{0, 1001, 1048576}, {1, 1042, 307200}} {
		got := vol1.Spec.VolumeDefinitions[i]
		if got.VolumeNumber != want.number || got.SizeKib != want.size ||
			got.DRBDMinor == nil || *got.DRBDMinor != want.minor {
			t.Errorf("volume[%d] = {nr %d, size %d, minor %v}, want {nr %d, size %d, minor %d}",
				i, got.VolumeNumber, got.SizeKib, got.DRBDMinor, want.number, want.size, want.minor)
		}
	}
}

// TestControllerConfigDistilled pins the /CTRL handling: cluster-wide
// DrbdOptions/* carry into the ControllerConfig singleton while
// LINSTOR runtime plumbing (NetCom ports, Cluster/LocalID) and the
// master-key crypto material MUST NOT leak into blockstor.
func TestControllerConfigDistilled(t *testing.T) {
	res := convertFixture(t)

	cc := res.ControllerConfig
	if cc == nil {
		t.Fatal("ControllerConfig not emitted despite /CTRL DrbdOptions props")
	}

	if cc.Name != crdv1alpha1.ControllerConfigName {
		t.Errorf("ControllerConfig name = %q, want %q", cc.Name, crdv1alpha1.ControllerConfigName)
	}

	if cc.Spec.ExtraProps["DrbdOptions/auto-add-quorum-tiebreaker"] != "True" {
		t.Errorf("auto-add-quorum-tiebreaker not carried; extraProps: %v", cc.Spec.ExtraProps)
	}

	if cc.Spec.ExtraProps["DrbdOptions/auto-verify-algo-allowed-list"] == "" {
		t.Errorf("auto-verify-algo-allowed-list not carried; extraProps: %v", cc.Spec.ExtraProps)
	}

	for _, forbidden := range []string{"NetCom/SslConnector/Port", "Cluster/LocalID", "encryptedMasterKey"} {
		if _, ok := cc.Spec.ExtraProps[forbidden]; ok {
			t.Errorf("LINSTOR plumbing key %q leaked into ControllerConfig", forbidden)
		}
	}
}

// TestTypedDRBDOptionsRouting pins the props→typed split on the two
// behaviour-bearing cases from production: replication protocol A on a
// resource group and dual-primary (RWX) on an RD.
func TestTypedDRBDOptionsRouting(t *testing.T) {
	res := convertFixture(t)

	var rg *crdv1alpha1.ResourceGroup

	for i := range res.ResourceGroups {
		if res.ResourceGroups[i].Name == "sc-replicated" {
			rg = &res.ResourceGroups[i]

			break
		}
	}

	if rg == nil {
		t.Fatal("sc-replicated not converted")
	}

	if rg.Spec.DRBDOptions == nil || rg.Spec.DRBDOptions.Net == nil || rg.Spec.DRBDOptions.Net.Protocol != "A" {
		t.Errorf("sc-replicated protocol not typed: %+v", rg.Spec.DRBDOptions)
	}

	var vol5 *crdv1alpha1.ResourceDefinition

	for i := range res.ResourceDefinitions {
		if res.ResourceDefinitions[i].Name == "pvc-vol5" {
			vol5 = &res.ResourceDefinitions[i]

			break
		}
	}

	if vol5 == nil {
		t.Fatal("pvc-vol5 not converted")
	}

	if vol5.Spec.DRBDOptions == nil || vol5.Spec.DRBDOptions.Net == nil ||
		vol5.Spec.DRBDOptions.Net.AllowTwoPrimaries == nil || !*vol5.Spec.DRBDOptions.Net.AllowTwoPrimaries {
		t.Errorf("pvc-vol5 allow-two-primaries not typed: %+v", vol5.Spec.DRBDOptions)
	}
}

// TestRemoteWarning pins the backup-shipping posture: the LINSTOR
// auto-generated self-remote stays silent, an operator-created remote
// is reported.
func TestRemoteWarning(t *testing.T) {
	res := convertFixture(t)

	if !hasWarning(res, "offsite-dr") {
		t.Errorf("operator-created linstor remote not reported; warnings: %v", res.Warnings)
	}

	if hasWarning(res, "local-remote-generated-by-linstor") {
		t.Errorf("auto-generated self-remote must stay silent; warnings: %v", res.Warnings)
	}
}

// TestOrphanReplicaSkipped pins referential integrity: vol4's RD is
// DELETE'd (skipped), so vol4.node-a — a replica whose parent RD did
// not convert — must NOT be emitted as an orphan Resource (whose
// <rd>.<node> CRD would reference a non-existent RD), and the drop
// must be reported. Without the guard the replica converts and the
// server-side CRD apply would leave a dangling Resource.
func TestOrphanReplicaSkipped(t *testing.T) {
	res := convertFixture(t)

	for i := range res.Resources {
		if res.Resources[i].Name == "pvc-vol4.node-a" {
			t.Error("replica of a DELETE'd RD (pvc-vol4) must not convert into an orphan Resource")
		}

		if res.Resources[i].Spec.ResourceDefinitionName == "pvc-vol4" {
			t.Errorf("Resource %s references un-migrated RD pvc-vol4", res.Resources[i].Name)
		}
	}

	if !hasWarning(res, "pvc-vol4.node-a: parent resource definition was not migrated") {
		t.Errorf("orphan-replica skip not reported; warnings: %v", res.Warnings)
	}
}

// TestLuksPassphraseWarning pins the phase-1 LUKS posture: the
// encrypted volume converts with its layer stack intact, and the
// non-migratable master-key-encrypted passphrase is loudly reported
// instead of silently dropped.
func TestLuksPassphraseWarning(t *testing.T) {
	res := convertFixture(t)

	var vol3 *crdv1alpha1.ResourceDefinition

	for i := range res.ResourceDefinitions {
		if res.ResourceDefinitions[i].Name == "pvc-vol3" {
			vol3 = &res.ResourceDefinitions[i]

			break
		}
	}

	if vol3 == nil {
		t.Fatal("LUKS RD pvc-vol3 not converted")
	}

	if !slices.Equal(vol3.Spec.LayerStack, []string{"DRBD", "LUKS", "STORAGE"}) {
		t.Errorf("pvc-vol3 layerStack = %v, want [DRBD LUKS STORAGE]", vol3.Spec.LayerStack)
	}

	if !hasWarning(res, "pvc-vol3: LUKS passphrase NOT migrated") {
		t.Errorf("LUKS passphrase warning missing; warnings: %v", res.Warnings)
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
