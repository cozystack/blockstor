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
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
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

// TestZvolNameAdoptionInvariant pins B2: adoption is name-based —
// blockstor addresses each zvol as `<zpool>/<RD metadata.name>_<vol%05d>`
// (pkg/storage/zfs/zfs.go volumeDataset) and CreateVolume is idempotent
// only when that dataset already exists. LINSTOR wrote the on-disk zvols
// as `<zpool>/<rd-lowercased>_00000`. So the RD's converted
// metadata.name MUST equal the lowercased LINSTOR name verbatim — any
// slug/hash from k8sstore.Name (triggered by a non-RFC-1123 name) would
// make blockstor look for a dataset that does not exist and create a
// fresh EMPTY zvol. Every production RD name is a clean `pvc-<uuid>`
// (verified across both dumps: 113/113 + 679/679), which lowercases to
// an RFC-1123-clean name that Name() passes through unchanged. This
// test pins that verbatim-lowercase invariant for the fixture and for a
// real uppercase UUID; if a future change made the converter hash a
// clean name, single-replica STORAGE-only volumes would silently adopt
// an empty disk.
func TestZvolNameAdoptionInvariant(t *testing.T) {
	res := convertFixture(t)

	for i := range res.ResourceDefinitions {
		rd := &res.ResourceDefinitions[i]
		// The fixture original is the uppercase LINSTOR key (e.g.
		// PVC-VOL1); its lowercase form is RFC-1123-clean, so the
		// converted metadata.name must be exactly that — no hash prefix.
		original := k8sstore.OriginalName(&rd.ObjectMeta)
		if original == "" {
			continue // name needed no annotation → already lowercase-clean
		}

		want := strings.ToLower(original)

		if rd.Name != want {
			t.Errorf("RD metadata.name = %q, want verbatim-lowercase %q (a slug/hash breaks zvol adoption)", rd.Name, want)
		}
	}

	// A real production-shaped uppercase UUID must pass through as the
	// plain lowercased name (no hash) so its zvol path is predictable.
	const prodName = "PVC-6A5F9F68-5DF2-4398-A92F-CDF41C4F2653"

	got := objectMeta(prodName).Name
	if got != strings.ToLower(prodName) {
		t.Errorf("k8sstore.Name(%q) = %q, want plain lowercase; a hashed name would miss the on-disk zvol", prodName, got)
	}
}

// TestConvertedZFSPoolsRegisterProvider pins B1 end-to-end at the
// migration layer: a real LINSTOR ZFS pool stores its zpool name under
// StorDriver/StorPoolName (the fixture mirrors a production ZFS-backed
// shape — ZFS thick, StorPoolName=data, no ZPool key). The migrator
// copies props verbatim, so unless the satellite factory accepts that
// key, every converted ZFS StoragePool fails to register a provider
// and NO diskful resource can adopt. Running the converted props
// through the real satellite.NewProviderFromKind proves the whole
// chain works; it fails on the pre-fix factory (the B1 blocker).
func TestConvertedZFSPoolsRegisterProvider(t *testing.T) {
	res := convertFixture(t)

	zfsPools := 0

	for i := range res.StoragePools {
		pool := &res.StoragePools[i]
		if !strings.HasPrefix(pool.Spec.ProviderKind, "ZFS") {
			continue // DISKLESS pools register no provider by design
		}

		zfsPools++

		prov, err := satellite.NewProviderFromKind(pool.Spec.ProviderKind, pool.Spec.Props, storage.NewFakeExec())
		if err != nil {
			t.Errorf("StoragePool %s (kind %s): provider did not register from migrated props %v: %v",
				pool.Name, pool.Spec.ProviderKind, pool.Spec.Props, err)

			continue
		}

		if prov == nil {
			t.Errorf("StoragePool %s: nil provider from migrated props", pool.Name)
		}
	}

	if zfsPools == 0 {
		t.Fatal("fixture has no ZFS StoragePools — the B1 regression is not being exercised")
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

// TestDivergentPerVolumePoolsSkipped pins the never-guess rule for the
// one data-bearing field blockstor cannot represent faithfully:
// LINSTOR allows a per-VOLUME storage pool, blockstor's Resource holds
// ONE pool per replica. Collapsing a divergent set to volume 0's pool
// would send blockstor looking for the other volumes' backing devices
// in the wrong pool — a fresh empty zvol beside the real data (silent
// for a single-replica STORAGE-only volume). The replica must be
// skipped and reported instead.
func TestDivergentPerVolumePoolsSkipped(t *testing.T) {
	res := convertFixture(t)

	for i := range res.Resources {
		if res.Resources[i].Name == "pvc-vol7.node-a" {
			t.Error("replica whose volumes span two storage pools must be skipped, not collapsed to volume 0's pool")
		}
	}

	if !hasWarning(res, "pvc-vol7.node-a: volumes span multiple storage pools") {
		t.Errorf("divergent per-volume pools not reported; warnings: %v", res.Warnings)
	}

	// A replica whose volumes share one pool still converts.
	findResource(t, res, "pvc-vol1.node-a")
}

// TestMultiVolumeSnapshotCoverageReported pins the honest-coverage rule
// for adopted snapshots: blockstor's ZFS provider addresses a snapshot
// as `<pool>/<rd>_00000@<snap>` (zfs.go snapshotDataset — volume 0
// only), so a snapshot that captured several volumes adopts with just
// its first volume restorable. The extra volume slots must not imply
// coverage that does not exist on disk.
func TestMultiVolumeSnapshotCoverageReported(t *testing.T) {
	res := convertFixture(t)

	var multi bool

	for i := range res.Snapshots {
		if len(res.Snapshots[i].Spec.VolumeDefinitions) > 1 {
			multi = true
		}
	}

	if !multi {
		t.Fatal("fixture has no multi-volume snapshot — the coverage warning is not exercised")
	}

	if !hasWarning(res, "blockstor addresses only volume 0") {
		t.Errorf("multi-volume snapshot coverage limit not reported; warnings: %v", res.Warnings)
	}
}

// TestVolumeAndNodeFlagsReported pins that the two tables previously
// loaded-but-never-examined now surface their markers: a non-zero
// VOLUMES.vlm_flags (DELETE/RESIZE and friends) is reported rather than
// adopted silently. Bit values are deliberately NOT decoded — that
// would be a guess — but the operator sees them.
func TestVolumeAndNodeFlagsReported(t *testing.T) {
	res := convertFixture(t)

	if !hasWarning(res, "volume 0: non-zero vlm_flags 64") {
		t.Errorf("non-zero VOLUMES.vlm_flags not reported; warnings: %v", res.Warnings)
	}
}

// TestReferencesResolveWithinConvertedSet pins cross-object referential
// integrity (the class Gemini flagged): every reference a converted
// object carries must resolve to another converted object's
// metadata.name — a `Resource` to its `ResourceDefinition` and `Node`, a
// `Snapshot` to its `ResourceDefinition`, an `RD` to its `ResourceGroup`
// (or empty). References use the LINSTOR display name and metadata.name
// is `k8sstore.Name(display)`; for the clean lowercase names blockstor
// carries on the wire the two are identical, so a converted reference
// never dangles or points at a differently-cased object.
func TestReferencesResolveWithinConvertedSet(t *testing.T) {
	res := convertFixture(t)

	rdNames := map[string]bool{}
	for i := range res.ResourceDefinitions {
		rdNames[res.ResourceDefinitions[i].Name] = true
	}

	nodeNames := map[string]bool{}
	for i := range res.Nodes {
		nodeNames[res.Nodes[i].Name] = true
	}

	rgNames := map[string]bool{}
	for i := range res.ResourceGroups {
		rgNames[res.ResourceGroups[i].Name] = true
	}

	for i := range res.Resources {
		r := &res.Resources[i]
		if !rdNames[k8sstore.Name(r.Spec.ResourceDefinitionName)] {
			t.Errorf("Resource %s references RD %q with no converted object", r.Name, r.Spec.ResourceDefinitionName)
		}

		if !nodeNames[k8sstore.Name(r.Spec.NodeName)] {
			t.Errorf("Resource %s references Node %q with no converted object", r.Name, r.Spec.NodeName)
		}
	}

	for i := range res.Snapshots {
		s := &res.Snapshots[i]
		if !rdNames[k8sstore.Name(s.Spec.ResourceDefinitionName)] {
			t.Errorf("Snapshot %s references RD %q with no converted object", s.Name, s.Spec.ResourceDefinitionName)
		}
	}

	for i := range res.ResourceDefinitions {
		rd := &res.ResourceDefinitions[i]
		if rd.Spec.ResourceGroupName != "" && !rgNames[k8sstore.Name(rd.Spec.ResourceGroupName)] {
			t.Errorf("RD %s references RG %q with no converted object", rd.Name, rd.Spec.ResourceGroupName)
		}
	}
}

// TestDanglingResourceGroupRefCleared pins the referential-integrity
// guard for RG references (CodeRabbit finding): an RD whose
// resource_group_name names a group that did not convert must keep the
// RD (dropping it would lose a real volume) but clear the dangling
// reference and report it, so the RD never points at a non-existent RG.
func TestDanglingResourceGroupRefCleared(t *testing.T) {
	res := convertFixture(t)

	var vol6 *crdv1alpha1.ResourceDefinition

	for i := range res.ResourceDefinitions {
		if res.ResourceDefinitions[i].Name == "pvc-vol6" {
			vol6 = &res.ResourceDefinitions[i]

			break
		}
	}

	if vol6 == nil {
		t.Fatal("pvc-vol6 (dangling-RG RD) must still convert, not be dropped")
	}

	if vol6.Spec.ResourceGroupName != "" {
		t.Errorf("dangling RG ref = %q, want cleared", vol6.Spec.ResourceGroupName)
	}

	if !hasWarning(res, "pvc-vol6: resource group") {
		t.Errorf("dangling RG ref not reported; warnings: %v", res.Warnings)
	}
}

// TestSnapshotDropsUnmigratedNodes pins the referential-integrity guard
// for snapshot placements (CodeRabbit finding): a snapshot node that did
// not convert is dropped from Spec.Nodes (else the adopted Snapshot
// waits for Ready from a Node object that never exists). The fixture's
// snap-good is placed on node-a/node-b (both migrated), so both survive
// and no diskless/controller node leaks in.
func TestSnapshotDropsUnmigratedNodes(t *testing.T) {
	res := convertFixture(t)

	for i := range res.Snapshots {
		snap := &res.Snapshots[i]
		for _, node := range snap.Spec.Nodes {
			if !slices.Contains([]string{"node-a", "node-b", "node-c"}, node) {
				t.Errorf("snapshot %s targets un-migrated node %q", snap.Name, node)
			}
		}
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

// TestInitializedLatchNeedsAnAdoptedReplica: the Initialized latch tells
// blockstor the volume already holds committed data, which also
// suppresses the auto-primary election that seeds a first sync. Latch it
// on a definition whose every replica was skipped and the volume is
// stranded — nothing holds the data, and nothing may become the source.
//
// Seen on a live migration: a definition whose only LINSTOR replica was
// a tie-breaker flagged DELETE arrived Initialized, and once the
// controller placed replicas for it they sat Inconsistent on every node
// with no way out.
// TestLatchHoldsOnADiskfulReplicaWithAStaleTieBreakerBit: production
// dumps carry TIE_BREAKER (bit 128) stuck on replicas that are diskful,
// left behind by an auto-diskful toggle. decodeResourceFlags only
// honours that bit together with the diskless one, and convertResources
// adopts such a row as an ordinary diskful replica — so the latch has
// to count it as evidence too.
//
// Dropping it instead un-latches a definition whose data was just
// adopted, which re-enables auto-primary election and lets an empty
// first sync overwrite that data. The skip must be narrower than the
// adoption, never wider.
func TestLatchHoldsOnADiskfulReplicaWithAStaleTieBreakerBit(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-STUCK", ResourceDspName: "pvc-stuck", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-stuck"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-STUCK", VlmNr: 0, VlmSize: 1048576, UUID: "vd-stuck"},
		},
		Resources: []ResourceRow{
			// Bit 128 with no diskless bit: diskful, carrying data.
			{NodeName: "NODE-A", ResourceName: "PVC-STUCK", ResourceFlags: 128, UUID: "r-stuck"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.ResourceDefinitions) != 1 {
		t.Fatalf("resource definitions = %d, want 1", len(res.ResourceDefinitions))
	}

	rd := &res.ResourceDefinitions[0]
	if rd.Spec.Initialized == nil || !*rd.Spec.Initialized {
		t.Errorf("Initialized = %v, want latched: the replica is diskful and was adopted, "+
			"so un-latching lets auto-primary overwrite it", rd.Spec.Initialized)
	}
}

// A genuine witness carries the diskless bit alongside TIE_BREAKER, and
// that one must not hold the latch on its own.
func TestLatchDoesNotHoldOnAWitnessAlone(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-WITNESS", ResourceDspName: "pvc-witness", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-w"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-WITNESS", VlmNr: 0, VlmSize: 1048576, UUID: "vd-w"},
		},
		Resources: []ResourceRow{
			// DISKLESS (4) + TIE_BREAKER (128): a real quorum witness.
			{NodeName: "NODE-A", ResourceName: "PVC-WITNESS", ResourceFlags: 132, UUID: "r-w"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.ResourceDefinitions) != 1 {
		t.Fatalf("resource definitions = %d, want 1", len(res.ResourceDefinitions))
	}

	rd := &res.ResourceDefinitions[0]
	if rd.Spec.Initialized != nil && *rd.Spec.Initialized {
		t.Error("a storage-free witness held the latch on its own: it carries no copy of anything")
	}
}

func TestInitializedLatchNeedsAnAdoptedReplica(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-LIVE", ResourceDspName: "pvc-live", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-live"},
			{ResourceName: "PVC-EMPTY", ResourceDspName: "pvc-empty", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-empty"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-LIVE", VlmNr: 0, VlmSize: 1048576, UUID: "vd-live"},
			{ResourceName: "PVC-EMPTY", VlmNr: 0, VlmSize: 1048576, UUID: "vd-empty"},
		},
		Resources: []ResourceRow{
			{NodeName: "NODE-A", ResourceName: "PVC-LIVE", UUID: "r-live"},
			// The only replica of PVC-EMPTY is on its way out.
			{NodeName: "NODE-A", ResourceName: "PVC-EMPTY", ResourceFlags: 2, UUID: "r-empty"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	latched := map[string]bool{}
	for i := range res.ResourceDefinitions {
		rd := &res.ResourceDefinitions[i]
		latched[rd.Name] = rd.Spec.Initialized != nil && *rd.Spec.Initialized
	}

	if !latched["pvc-live"] {
		t.Error("pvc-live has an adopted replica: Initialized must stay latched")
	}

	if latched["pvc-empty"] {
		t.Error("pvc-empty has no adopted replica: Initialized must not be latched, " +
			"or no replica can ever seed the first sync")
	}
}

// TestSparseZfsPoolMigratesAsThin: LINSTOR lets a pool be declared thick
// ZFS while StorDriver/ZfscreateOptions carries `-s`, which makes every
// zvol sparse. Blockstor's thick provider reserves each volume's full
// size and applies that to volumes it adopts, so migrating such a pool
// as ZFS converts a live, deliberately oversubscribed pool to thick
// during adoption and strands whatever no longer fits. Carry the
// provisioning the volumes actually have.
//
// The `-s` must be matched as a flag: an option that merely contains
// the letter must not flip the kind.
func TestSparseZfsPoolMigratesAsThin(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "SPARSE", DriverName: "ZFS", UUID: "sp-1"},
			{NodeName: "NODE-A", PoolName: "THICK", DriverName: "ZFS", UUID: "sp-2"},
			{NodeName: "NODE-A", PoolName: "TRAP", DriverName: "ZFS", UUID: "sp-3"},
			{NodeName: "NODE-A", PoolName: "LVMPOOL", DriverName: "LVM", UUID: "sp-4"},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/STOR_POOLS/NODE-A/SPARSE", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s -o compression=lz4"},
			{PropsInstance: "/STOR_POOLS/NODE-A/THICK", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-o compression=lz4"},
			// Contains "-s" as a substring but no sparse flag.
			{PropsInstance: "/STOR_POOLS/NODE-A/TRAP", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-o volmode=-static"},
			{PropsInstance: "/STOR_POOLS/NODE-A/LVMPOOL", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	kind := map[string]string{}
	for i := range res.StoragePools {
		sp := &res.StoragePools[i]
		kind[sp.Spec.PoolName] = sp.Spec.ProviderKind
	}

	for name, want := range map[string]string{
		"SPARSE":  "ZFS_THIN",
		"THICK":   "ZFS",
		"TRAP":    "ZFS",
		"LVMPOOL": "LVM",
	} {
		if got := kind[name]; got != want {
			t.Errorf("pool %s: providerKind = %q, want %q", name, got, want)
		}
	}

	if !hasWarning(res, "declared ZFS") {
		t.Errorf("sparse-pool remap was not reported; warnings: %v", res.Warnings)
	}
}

// TestSparseFlagAtControllerScopeIsSeen: LINSTOR resolves StorDriver/*
// through a priority chain — storage pool, then node, then controller —
// so `linstor controller set-property StorDriver/ZfscreateOptions -s`
// makes every zvol in the cluster sparse while leaving every per-pool
// bag empty. Reading only the pool bag reports such a cluster as
// genuinely thick and walks into the adoption-time pool overflow the
// remap exists to prevent.
func TestSparseFlagAtControllerScopeIsSeen(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "CLUSTERWIDE", DriverName: "ZFS", UUID: "sp-1"},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: PropsInstanceController, PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for i := range res.StoragePools {
		sp := &res.StoragePools[i]
		if sp.Spec.PoolName != "CLUSTERWIDE" {
			continue
		}

		if sp.Spec.ProviderKind != "ZFS_THIN" {
			t.Errorf("a controller-scope sparse flag was not seen: providerKind = %q, want ZFS_THIN",
				sp.Spec.ProviderKind)
		}

		return
	}

	t.Fatalf("pool CLUSTERWIDE did not convert")
}

// TestSparseFlagAtNodeScopeIsSeen: the middle rung of the same chain.
func TestSparseFlagAtNodeScopeIsSeen(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "PERNODE", DriverName: "ZFS", UUID: "sp-1"},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/NODES/NODE-A", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for i := range res.StoragePools {
		if res.StoragePools[i].Spec.PoolName == "PERNODE" &&
			res.StoragePools[i].Spec.ProviderKind != "ZFS_THIN" {
			t.Errorf("a node-scope sparse flag was not seen: providerKind = %q, want ZFS_THIN",
				res.StoragePools[i].Spec.ProviderKind)
		}
	}
}

// TestPoolScopeWinsOverControllerScope: the chain stops at the first bag
// that carries the key, so a pool that overrides the cluster-wide flag
// with its own options stays thick.
func TestPoolScopeWinsOverControllerScope(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "OVERRIDDEN", DriverName: "ZFS", UUID: "sp-1"},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: PropsInstanceController, PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
			{PropsInstance: "/STOR_POOLS/NODE-A/OVERRIDDEN", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-o compression=lz4"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for i := range res.StoragePools {
		if res.StoragePools[i].Spec.PoolName == "OVERRIDDEN" &&
			res.StoragePools[i].Spec.ProviderKind != "ZFS" {
			t.Errorf("the pool's own options did not win: providerKind = %q, want ZFS",
				res.StoragePools[i].Spec.ProviderKind)
		}
	}
}

// TestRemapDoesNotAdmitADeliberatelyExcludedThinPool: a cluster can hold
// a remapped sparse pool and a genuinely thin one at the same time, with
// a resource group that named [ZFS] precisely to keep its replicas off
// the thin one. Widening that group's allow-list to ZFS_THIN so the
// remapped pool stays reachable must not also hand it the pool the
// operator excluded.
func TestRemapDoesNotAdmitADeliberatelyExcludedThinPool(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			// Declared thick, sparse in practice — this one remaps.
			{NodeName: "NODE-A", PoolName: "SPARSE", DriverName: "ZFS", UUID: "sp-1"},
			// Genuinely thin, and the group was kept away from it.
			{NodeName: "NODE-A", PoolName: "REALTHIN", DriverName: "ZFS_THIN", UUID: "sp-2"},
		},
		ResourceGroups: []ResourceGroupRow{
			{
				ResourceGroupName:    "RG",
				ResourceGroupDspName: "rg",
				AllowedProviderList:  `["ZFS"]`,
				UUID:                 "rg-1",
			},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/STOR_POOLS/NODE-A/SPARSE", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.ResourceGroups) != 1 {
		t.Fatalf("resource groups = %d, want 1", len(res.ResourceGroups))
	}

	filter := res.ResourceGroups[0].Spec.SelectFilter

	// The remapped pool has to stay reachable, so the kind is widened.
	if !containsString(filter.ProviderList, "ZFS_THIN") {
		t.Errorf("allow-list = %v, want ZFS_THIN added so the remapped pool stays reachable",
			filter.ProviderList)
	}

	// ... but the pool the group could never place on must not become
	// placeable as a side effect.
	if len(filter.StoragePoolList) == 0 {
		t.Fatalf("group was left unpinned, so widening the kind admits the excluded pool too")
	}

	if containsString(filter.StoragePoolList, "REALTHIN") {
		t.Errorf("storage pool list = %v, must not include the deliberately excluded thin pool",
			filter.StoragePoolList)
	}

	if !containsString(filter.StoragePoolList, "SPARSE") {
		t.Errorf("storage pool list = %v, want the remapped pool kept reachable",
			filter.StoragePoolList)
	}
}

// TestSameNameDifferentDriverPerNode: LINSTOR pools are node-scoped, and
// naming them uniformly across nodes is the common practice — so the
// same name can be thick-with-`-s` on one node and genuinely thin on
// another. Keyed on the bare name, the remap bookkeeping collapses the
// two into one entry and the exclusion breaks.
//
// A group's pool list carries no node, so admitting the name admits it
// on every node. Here the exclusion cannot be expressed at all, and the
// allow-list must be left alone rather than widened: an empty pin list
// reads as "no restriction", which would put replicas on exactly the
// pool the operator kept them off.
func TestSameNameDifferentDriverPerNode(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
			{NodeName: "NODE-B", NodeDspName: "node-b", NodeType: 2, UUID: "n-b"},
		},
		NodeStorPools: []NodeStorPoolRow{
			// Thick by declaration, sparse in practice — remaps.
			{NodeName: "NODE-A", PoolName: "TANK", DriverName: "ZFS", UUID: "sp-1"},
			// Same name, genuinely thin, and the group was kept off it.
			{NodeName: "NODE-B", PoolName: "TANK", DriverName: "ZFS_THIN", UUID: "sp-2"},
		},
		ResourceGroups: []ResourceGroupRow{
			{
				ResourceGroupName:    "RG",
				ResourceGroupDspName: "rg",
				AllowedProviderList:  `["ZFS"]`,
				UUID:                 "rg-1",
			},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/STOR_POOLS/NODE-A/TANK", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.ResourceGroups) != 1 {
		t.Fatalf("resource groups = %d, want 1", len(res.ResourceGroups))
	}

	filter := res.ResourceGroups[0].Spec.SelectFilter

	// Widening the kind while the pin list stays empty is the trap: the
	// placer reads an empty list as "any pool", so the group lands on
	// the genuinely thin TANK on node-b.
	if containsString(filter.ProviderList, "ZFS_THIN") && len(filter.StoragePoolList) == 0 {
		t.Errorf("allow-list widened to %v with no pins: the group can now place on the thin pool it excluded",
			filter.ProviderList)
	}

	if !hasWarning(res, "by hand") {
		t.Errorf("the unresolvable case was not reported to the operator; warnings: %v", res.Warnings)
	}
}

// The per-node keying must not make the ordinary case worse: a pool
// remapped on every node, with no genuinely thin pool anywhere, is still
// widened plainly.
func TestRemapWidensWhenNoThinPoolExists(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
			{NodeName: "NODE-B", NodeDspName: "node-b", NodeType: 2, UUID: "n-b"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "TANK", DriverName: "ZFS", UUID: "sp-1"},
			{NodeName: "NODE-B", PoolName: "TANK", DriverName: "ZFS", UUID: "sp-2"},
		},
		ResourceGroups: []ResourceGroupRow{
			{
				ResourceGroupName:    "RG",
				ResourceGroupDspName: "rg",
				AllowedProviderList:  `["ZFS"]`,
				UUID:                 "rg-1",
			},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: PropsInstanceController, PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	filter := res.ResourceGroups[0].Spec.SelectFilter
	if !containsString(filter.ProviderList, "ZFS_THIN") {
		t.Errorf("allow-list = %v, want ZFS_THIN: every pool remapped and none was declared thin",
			filter.ProviderList)
	}
}

// TestPinnedGroupNamingAThinPoolIsNotWidened: a pin scopes the widening only
// when none of the pools it names was already thin. Here the group names both
// the pool that remaps and one the source declared ZFS_THIN — before the
// migration the provider filter kept it off the second, and widening the kind
// removes the only thing that did, because pool names and provider kinds are
// independent filters at the placer.
//
// This shape has a scoped answer, and taking it is what keeps the group
// placeable: REALTHIN is dropped from the pins because the provider filter
// already kept the group off it, and SPARSE — the pool it actually used —
// stays. Leaving the allow-list alone instead would strand the group, since
// its only remaining pool migrated out from under its [ZFS] filter.
func TestPinnedGroupNamingAThinPoolIsScopedNotWidened(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "SPARSE", DriverName: "ZFS", UUID: "sp-1"},
			{NodeName: "NODE-A", PoolName: "REALTHIN", DriverName: "ZFS_THIN", UUID: "sp-2"},
		},
		ResourceGroups: []ResourceGroupRow{
			{
				ResourceGroupName:    "RG",
				ResourceGroupDspName: "rg",
				AllowedProviderList:  `["ZFS"]`,
				PoolName:             `["sparse","realThin"]`,
				UUID:                 "rg-1",
			},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/STOR_POOLS/NODE-A/SPARSE", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	filter := res.ResourceGroups[0].Spec.SelectFilter

	// Widened, because the pool this group actually placed on is ZFS_THIN
	// now and an unwidened filter matches nothing.
	if !containsString(filter.ProviderList, "ZFS_THIN") {
		t.Errorf("allow-list = %v: the pool this group placed on migrated to ZFS_THIN, "+
			"so an unwidened filter leaves it with nowhere to place", filter.ProviderList)
	}

	// Scoped, so the widened kind does not hand it the pool the provider
	// filter used to keep it off. The pins keep the operator's own casing.
	if containsString(filter.StoragePoolList, "realThin") {
		t.Errorf("pins = %v: the group was kept off this pool by the provider filter, "+
			"and widening the kind must not hand it over", filter.StoragePoolList)
	}

	if !containsString(filter.StoragePoolList, "sparse") {
		t.Errorf("pins = %v: the pool this group placed on was dropped", filter.StoragePoolList)
	}

	if !hasWarning(res, "pins narrowed") {
		t.Errorf("the scoping was not reported; warnings: %v", res.Warnings)
	}
}

// The same shape with one pool name on two nodes: SPARSE remaps on node-a
// while the same name is declared thin on node-b. A pin cannot separate them,
// since a pin carries no node.
func TestPinnedGroupNamingASplitPoolIsNotWidened(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
			{NodeName: "NODE-B", NodeDspName: "node-b", NodeType: 2, UUID: "n-b"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "TANK", DriverName: "ZFS", UUID: "sp-1"},
			{NodeName: "NODE-B", PoolName: "TANK", DriverName: "ZFS_THIN", UUID: "sp-2"},
		},
		ResourceGroups: []ResourceGroupRow{
			{
				ResourceGroupName:    "RG",
				ResourceGroupDspName: "rg",
				AllowedProviderList:  `["ZFS"]`,
				PoolName:             `["tank"]`,
				UUID:                 "rg-1",
			},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/STOR_POOLS/NODE-A/TANK", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	filter := res.ResourceGroups[0].Spec.SelectFilter
	if containsString(filter.ProviderList, "ZFS_THIN") {
		t.Errorf("allow-list = %v: the pinned name is thin on another node", filter.ProviderList)
	}
}

// A pinned group whose pools are all thick is the ordinary case and must
// still be widened, or the remapped pool it names goes unreachable.
func TestPinnedGroupOnThickPoolsIsWidened(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "SPARSE", DriverName: "ZFS", UUID: "sp-1"},
		},
		ResourceGroups: []ResourceGroupRow{
			{
				ResourceGroupName:    "RG",
				ResourceGroupDspName: "rg",
				AllowedProviderList:  `["ZFS"]`,
				PoolName:             `["sparse"]`,
				UUID:                 "rg-1",
			},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/STOR_POOLS/NODE-A/SPARSE", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	filter := res.ResourceGroups[0].Spec.SelectFilter
	if !containsString(filter.ProviderList, "ZFS_THIN") {
		t.Errorf("allow-list = %v, want ZFS_THIN: the group's own pool remapped", filter.ProviderList)
	}
}

// TestRemapLeavesUnrelatedGroupsAlone: a group that cannot place on the
// remapped pool has no reason to learn about ZFS_THIN, and giving it the
// kind anyway is how an exclusion quietly disappears.
func TestRemapLeavesUnrelatedGroupsAlone(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "SPARSE", DriverName: "ZFS", UUID: "sp-1"},
			{NodeName: "NODE-A", PoolName: "PLAIN", DriverName: "ZFS", UUID: "sp-2"},
		},
		ResourceGroups: []ResourceGroupRow{
			{
				ResourceGroupName:    "PINNED",
				ResourceGroupDspName: "pinned",
				AllowedProviderList:  `["ZFS"]`,
				PoolName:             `["plain"]`,
				UUID:                 "rg-1",
			},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/STOR_POOLS/NODE-A/SPARSE", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.ResourceGroups) != 1 {
		t.Fatalf("resource groups = %d, want 1", len(res.ResourceGroups))
	}

	got := res.ResourceGroups[0].Spec.SelectFilter.ProviderList
	if containsString(got, "ZFS_THIN") {
		t.Errorf("allow-list = %v, want ZFS_THIN NOT added: the group is pinned to a pool that did not remap", got)
	}
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}

	return false
}

// TestVolumelessDefinitionIsSkipped: a resource definition that owns no
// volume cannot become a usable volume — no size, no DRBD minor. Migrate
// one anyway and the controller still places replicas for it from the
// resource group's placeCount, allocates a port, and the satellite then
// spins forever on "waiting for controller-side DRBD-ID allocation":
// no .res file, no backing device, a hot reconcile loop, and replicas the
// CLI can only report as Unknown.
//
// A production dump carried exactly one such definition, with zero
// volumes and its only replica already flagged DELETE.
func TestVolumelessDefinitionIsSkipped(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-REAL", ResourceDspName: "pvc-real", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-real"},
			{ResourceName: "PVC-NOVOL", ResourceDspName: "pvc-novol", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-novol"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-REAL", VlmNr: 0, VlmSize: 1048576, UUID: "vd-real"},
		},
		Resources: []ResourceRow{
			{NodeName: "NODE-A", ResourceName: "PVC-REAL", UUID: "r-real"},
			{NodeName: "NODE-A", ResourceName: "PVC-NOVOL", UUID: "r-novol"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for i := range res.ResourceDefinitions {
		if res.ResourceDefinitions[i].Name == "pvc-novol" {
			t.Error("volume-less definition pvc-novol must not convert")
		}
	}

	// Its replicas must go with it, or they dangle against an object
	// that was never applied.
	for i := range res.Resources {
		if res.Resources[i].Spec.ResourceDefinitionName == "pvc-novol" {
			t.Errorf("replica %s outlived its skipped definition", res.Resources[i].Name)
		}
	}

	var kept bool

	for i := range res.ResourceDefinitions {
		if res.ResourceDefinitions[i].Name == "pvc-real" {
			kept = true
		}
	}

	if !kept {
		t.Error("pvc-real owns a volume and must still convert")
	}

	if !hasWarning(res, "pvc-novol: no volume definitions") {
		t.Errorf("volume-less skip was not reported; warnings: %v", res.Warnings)
	}
}

// TestInitializedLatchSurvivesAnUnmigratedNode: a replica skipped
// because its host node is not in the dump is not a replica that is
// gone — the data is still on that node's disk. Unlatching there is the
// direction the latch exists to prevent: the controller would place
// fresh replicas, elect an auto-primary and seed a blank first sync,
// and the real replica becomes SyncTarget of the blank set the moment
// the operator brings that node in.
//
// Only a DELETE-flagged replica means the data was being discarded.
func TestInitializedLatchSurvivesAnUnmigratedNode(t *testing.T) {
	dump := &Dump{
		// NODE-B is deliberately absent: a staged or incomplete dump.
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-ELSEWHERE", ResourceDspName: "pvc-elsewhere", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-e"},
			{ResourceName: "PVC-DELETED", ResourceDspName: "pvc-deleted", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-d"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-ELSEWHERE", VlmNr: 0, VlmSize: 1048576, UUID: "vd-e"},
			{ResourceName: "PVC-DELETED", VlmNr: 0, VlmSize: 1048576, UUID: "vd-d"},
		},
		Resources: []ResourceRow{
			// Real data, on a node this dump does not carry.
			{NodeName: "NODE-B", ResourceName: "PVC-ELSEWHERE", UUID: "r-e"},
			// Genuinely on its way out.
			{NodeName: "NODE-A", ResourceName: "PVC-DELETED", ResourceFlags: 2, UUID: "r-d"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	latched := map[string]bool{}
	for i := range res.ResourceDefinitions {
		rd := &res.ResourceDefinitions[i]
		latched[rd.Name] = rd.Spec.Initialized != nil && *rd.Spec.Initialized
	}

	if !latched["pvc-elsewhere"] {
		t.Error("a replica held back by an unmigrated node still holds data: " +
			"Initialized must stay latched")
	}

	if latched["pvc-deleted"] {
		t.Error("every replica was DELETE-flagged: Initialized must not be latched, " +
			"or nothing can seed the first sync")
	}

	if !hasWarning(res, "pvc-elsewhere: every replica lives on a node that was not migrated") {
		t.Errorf("the operator was not told the definition is incomplete; warnings: %v", res.Warnings)
	}
}

// TestSparseRemapWidensTheProviderAllowList: the placer filters
// candidate pools on the resource group's provider allow-list with an
// exact match, so remapping a pool from ZFS to ZFS_THIN without telling
// the group leaves it pinned to a kind no pool has any more. Adoption
// still succeeds and the volumes keep working, but nothing new can be
// placed and a lost replica cannot be healed — silently, until someone
// edits the group.
//
// A cluster that deliberately ran thick-declared sparse ZFS is exactly
// the kind that pins the list this way.
func TestSparseRemapWidensTheProviderAllowList(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		NodeStorPools: []NodeStorPoolRow{
			{NodeName: "NODE-A", PoolName: "SPARSE", DriverName: "ZFS", UUID: "sp-1"},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/STOR_POOLS/NODE-A/SPARSE", PropKey: "StorDriver/ZfscreateOptions", PropValue: "-s"},
		},
		ResourceGroups: []ResourceGroupRow{
			{ResourceGroupName: "RG-PINNED", ResourceGroupDspName: "rg-pinned", AllowedProviderList: `["ZFS"]`, UUID: "rg-1"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.ResourceGroups) != 1 {
		t.Fatalf("resource groups = %d, want 1", len(res.ResourceGroups))
	}

	list := res.ResourceGroups[0].Spec.SelectFilter.ProviderList

	if !slices.Contains(list, "ZFS_THIN") {
		t.Errorf("the pool migrated as ZFS_THIN but the group still allows only %v; "+
			"the placer would find no eligible pool", list)
	}

	// Widened, not substituted: a genuinely thick pool elsewhere in the
	// cluster must stay eligible for a group that named ZFS.
	if !slices.Contains(list, "ZFS") {
		t.Errorf("ZFS was dropped from the allow-list: %v", list)
	}
}
