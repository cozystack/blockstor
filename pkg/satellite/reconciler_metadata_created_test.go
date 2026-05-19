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
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// fakeMetadataStamper records every StampMetadataCreated call so the
// tests can assert the satellite reconciler invokes the stamper
// exactly when create-md is run. Concurrency-safe so the parallel
// test driver doesn't tear the slice under multi-Apply workloads.
type fakeMetadataStamper struct {
	mu     sync.Mutex
	calls  []string
	stamps func(string) bool // optional: returns true to fail the call
}

func (f *fakeMetadataStamper) StampMetadataCreated(_ context.Context, resourceName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, resourceName)

	if f.stamps != nil && f.stamps(resourceName) {
		return os.ErrPermission
	}

	return nil
}

func (f *fakeMetadataStamper) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, len(f.calls))
	copy(out, f.calls)

	return out
}

// TestEnsureMetadataStampsCondition pins Phase 11.3 Stage 1: a
// successful create-md on a fresh diskful replica MUST drive the
// MetadataCreatedStamper exactly once with the Resource name. The
// on-disk `.md-created` marker remains as belt-and-braces (same
// pass), so this test is additive — it does not assert the file is
// absent.
//
// Pre-seeds the .res file so Phase 11.2.c's FSM dispatch (which
// fires only on PhaseUnprovisioned → renderRes) does NOT divert
// applyDRBD before ensureMetadata runs. Two-pass simulations of
// the FSM dispatch path live in fsm_dispatch_test.go.
func TestEnsureMetadataStampsCondition(t *testing.T) {
	dir := t.TempDir()

	resPath := filepath.Join(dir, "pvc-stamp.res")
	if err := os.WriteFile(resPath, []byte("resource pvc-stamp {}\n"), 0o600); err != nil {
		t.Fatalf("seed .res: %v", err)
	}

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-stamp_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	stamper := &fakeMetadataStamper{}
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:              map[string]storage.Provider{"thin1": thin},
		Adm:                    drbd.NewAdm(fx),
		StateDir:               dir,
		NodeName:               "n1",
		MetadataCreatedStamper: stamper,
	})

	dr := []*intent.DesiredResource{
		{
			Name:     "pvc-stamp",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{"n2"},
			DrbdOptions: map[string]string{
				"port":            "7000",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1000",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7000",
			},
		},
	}

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	calls := stamper.Calls()
	if len(calls) != 1 {
		t.Fatalf("stamper called %d times, want exactly 1: %v", len(calls), calls)
	}

	// Bug 344: stamper receives the per-node Resource CRD name
	// (`<rd>.<node>`), not the RD-only name — the SSA patch
	// targets Resource objects which are sharded per node.
	if calls[0] != "pvc-stamp.n1" {
		t.Errorf("stamper called with %q, want %q", calls[0], "pvc-stamp.n1")
	}

	// Belt-and-braces: file marker still written on the same path.
	mdMarker := filepath.Join(dir, "pvc-stamp.md-created")
	if _, err := os.Stat(mdMarker); err != nil {
		t.Errorf("md-created file marker missing after Apply: %v — belt-and-braces lost", err)
	}
}

// TestApplyDRBDDerivesFirstActivationFromCondition pins the reader
// half: when the Resource carries MetadataCreated=true (set by the
// dispatcher from Status.Conditions), firstActivation flips to
// false even without an on-disk `.md-created` marker. Concretely:
// no marker file beforehand → the legacy code would derive
// firstActivation=true → create-md would run. With the Condition
// flag set, create-md MUST NOT run (HasMD probe still happens for
// the diskfulFlip branch, but the create-md command line must not
// appear).
func TestApplyDRBDDerivesFirstActivationFromCondition(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// Storage provider sees an existing LV — the apply skips the
	// CreateVolume + create-md path.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-cond_00000",
		storage.FakeResponse{Stdout: []byte("pvc-cond_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-cond_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-cond_00000|1048576\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	stamper := &fakeMetadataStamper{}
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:              map[string]storage.Provider{"thin1": thin},
		Adm:                    drbd.NewAdm(fx),
		StateDir:               dir,
		NodeName:               "n1",
		MetadataCreatedStamper: stamper,
	})

	dr := []*intent.DesiredResource{
		{
			Name:            "pvc-cond",
			NodeName:        "n1",
			MetadataCreated: true,
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{"n2"},
			DrbdOptions: map[string]string{
				"port":            "7000",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1000",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7000",
			},
		},
	}

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// With Condition set + no marker file, firstActivation must be
	// false — ensureMetadata's create-md path is skipped, and the
	// stamper is never invoked (no `os.WriteFile` of the marker
	// either).
	calls := stamper.Calls()
	if len(calls) != 0 {
		t.Errorf("stamper called %d times, want 0 (Condition already set): %v", len(calls), calls)
	}

	for _, cmd := range fx.CommandLines() {
		if cmd == "drbdadm create-md --force pvc-cond" {
			t.Errorf("create-md ran despite Condition already true; cmds: %v", fx.CommandLines())
		}
	}
}

// TestApplyDRBDFallsBackToFileWhenConditionAbsent pins the
// migration-window contract: a pre-existing `.md-created` marker
// on disk MUST still suppress firstActivation, even when the
// Resource carries no Condition (cluster upgraded from a pre-11.3
// satellite, backfill not yet stamped). Without this fallback the
// migration would re-run create-md against an already-initialised
// metadata block on every reconcile until backfill catches up.
func TestApplyDRBDFallsBackToFileWhenConditionAbsent(t *testing.T) {
	dir := t.TempDir()

	// Pre-stamp the .md-created marker as if a pre-11.3 satellite
	// had successfully run create-md against this resource.
	mdMarker := filepath.Join(dir, "pvc-fallback.md-created")
	if err := os.WriteFile(mdMarker, nil, 0o600); err != nil {
		t.Fatalf("seed .md-created: %v", err)
	}

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-fallback_00000",
		storage.FakeResponse{Stdout: []byte("pvc-fallback_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-fallback_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-fallback_00000|1048576\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	stamper := &fakeMetadataStamper{}
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:              map[string]storage.Provider{"thin1": thin},
		Adm:                    drbd.NewAdm(fx),
		StateDir:               dir,
		NodeName:               "n1",
		MetadataCreatedStamper: stamper,
	})

	dr := []*intent.DesiredResource{
		{
			Name:            "pvc-fallback",
			NodeName:        "n1",
			MetadataCreated: false, // Condition NOT yet set
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []string{"n2"},
			DrbdOptions: map[string]string{
				"port":            "7000",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1000",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7000",
			},
		},
	}

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// File marker present + Condition absent → firstActivation
	// MUST resolve false (file fallback active). create-md is
	// skipped, stamper is not invoked.
	calls := stamper.Calls()
	if len(calls) != 0 {
		t.Errorf("stamper called %d times, want 0 (file-marker fallback should suppress firstActivation): %v",
			len(calls), calls)
	}

	for _, cmd := range fx.CommandLines() {
		if cmd == "drbdadm create-md --force pvc-fallback" {
			t.Errorf("create-md ran despite on-disk .md-created marker; cmds: %v", fx.CommandLines())
		}
	}
}
