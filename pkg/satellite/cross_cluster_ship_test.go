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

// Scenario 4.17 — cluster-to-cluster snapshot ship via LINSTOR
// remote. The REST surface accepts the remote registration but
// returns 501 on the ship endpoint (pkg/rest/remotes.go), and the
// satellite must reject SourceSnapshot values that reference a
// cross-cluster remote so an upstream caller can't sneak the
// not-implemented path past the REST stub via the
// snapshot-restore-resource Props side channel.

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// crossClusterFakeProvider implements storage.Provider AND
// storage.SnapshotShipper so the reconciler's cross-cluster guard
// has the chance to fire BEFORE any provider call — pinning that
// the rejection is per-resource (results[0].Ok=false), not a
// fall-through to blank CreateVolume that would look like success
// to the controller's Apply caller.
type crossClusterFakeProvider struct {
	mu           sync.Mutex
	createCalls  int
	restoreCalls int
	recvCalls    int
}

func (*crossClusterFakeProvider) Kind() string { return "ZFS_THIN" }

func (*crossClusterFakeProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{SupportsSnapshots: true}, nil
}

func (f *crossClusterFakeProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++

	return nil
}

func (*crossClusterFakeProvider) DeleteVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (*crossClusterFakeProvider) ResizeVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (*crossClusterFakeProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{
		DevicePath:   "/dev/fake/" + vol.ResourceName,
		AllocatedKib: vol.SizeKib,
		UsableKib:    vol.SizeKib,
		State:        "PROVISIONED",
	}, nil
}

func (*crossClusterFakeProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (*crossClusterFakeProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (f *crossClusterFakeProvider) RestoreVolumeFromSnapshot(_ context.Context,
	_ storage.Volume, _ storage.Snapshot,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoreCalls++

	return storage.ErrNotFound
}

func (*crossClusterFakeProvider) SendSnapshot(_ context.Context, _ storage.Snapshot) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (f *crossClusterFakeProvider) RecvSnapshot(_ context.Context, _ storage.Volume, src io.Reader) error {
	_, _ = io.ReadAll(src)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.recvCalls++

	return nil
}

// TestCrossClusterShipNotSupported pins the satellite-side
// invariant for scenario 4.17: a SourceSnapshot encoded in the
// upstream LINSTOR cross-cluster form (`<remote_name>:<rsc>:<snap>`)
// MUST be rejected with a clear error mentioning the remote and
// pointing at the in-cluster alternative. Crucially:
//
//  1. The error must surface as results[0].Ok=false so the
//     controller sets Status.Conditions and the operator notices.
//  2. No provider call may fire — neither RestoreVolumeFromSnapshot
//     nor CreateVolume — because either would commit to a partial
//     cross-cluster operation that the satellite can't finish.
//  3. The message must name the remote so the operator can
//     correlate it with the LinstorRemote CRD / REST entry that
//     caused the attempt.
//
// Pinning the no-provider-call invariant matters: a naive guard
// that returns nil after the colon-split would let the blank
// CreateVolume path land a stub LV, which DRBD would then try to
// network-resync from a peer that doesn't host the data either.
// The end state is a permanently-Inconsistent resource with no
// operator-facing error.
func TestCrossClusterShipNotSupported(t *testing.T) {
	prov := &crossClusterFakeProvider{}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"zfs1": prov},
	})

	// Drive the 3-part `<remote>:<srcRD>:<snap>` shape that upstream
	// LINSTOR's BackupShip handler produces.
	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name: "pvc-cross-cluster", NodeName: "n1",
			Volumes: []*intent.DesiredVolume{{
				VolumeNumber:   0,
				SizeKib:        1024 * 1024,
				StoragePool:    "zfs1",
				SourceSnapshot: "peer-east:pvc-source:snap-1",
			}},
		},
	})
	if err != nil {
		t.Fatalf("Apply (transport): %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results len: got %d, want 1", len(results))
	}

	if results[0].GetOk() {
		t.Fatalf("expected !ok on cross-cluster source; got %+v", results[0])
	}

	msg := results[0].GetMessage()

	// Required substrings — change here means the operator-facing
	// hint changed. Require an explicit test edit to confirm intent.
	for _, want := range []string{
		"cross-cluster",
		"peer-east",
		"not implemented",
		"snapshot-restore-resource",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("cross-cluster reject message %q is missing required substring %q",
				msg, want)
		}
	}

	if prov.restoreCalls != 0 {
		t.Errorf("RestoreVolumeFromSnapshot must NOT fire on cross-cluster reject; got %d calls",
			prov.restoreCalls)
	}

	if prov.createCalls != 0 {
		t.Errorf("CreateVolume must NOT fire on cross-cluster reject; got %d calls",
			prov.createCalls)
	}

	if prov.recvCalls != 0 {
		t.Errorf("RecvSnapshot must NOT fire on cross-cluster reject; got %d calls",
			prov.recvCalls)
	}
}

// TestSourceSnapshotTwoPartFormStillAccepted: regression guard for
// the cross-cluster guard. The 2-part `<srcRD>:<snap>` form is
// the legitimate in-cluster ship path — the new 3-part rejection
// must not accidentally swallow it. Drives a 2-part source through
// the satellite and confirms the provider's restore (then blank
// CreateVolume fallback because the fake returns ErrNotFound)
// fires as expected.
func TestSourceSnapshotTwoPartFormStillAccepted(t *testing.T) {
	prov := &crossClusterFakeProvider{}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"zfs1": prov},
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name: "pvc-in-cluster-clone", NodeName: "n1",
			Volumes: []*intent.DesiredVolume{{
				VolumeNumber:   0,
				SizeKib:        1024 * 1024,
				StoragePool:    "zfs1",
				SourceSnapshot: "pvc-source:snap-1",
			}},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("2-part source must be accepted; got %+v", results[0])
	}

	// Restore was attempted (returns ErrNotFound from the fake),
	// then the no-CrossNodeFetcher fallback ran CreateVolume.
	if prov.restoreCalls != 1 {
		t.Errorf("RestoreVolumeFromSnapshot calls: got %d, want 1", prov.restoreCalls)
	}

	if prov.createCalls != 1 {
		t.Errorf("CreateVolume fallback calls: got %d, want 1", prov.createCalls)
	}
}
