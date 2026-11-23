// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The volume-less door started from a copy of the source, flags included, and
// had no delete gate: a clone taken inside the source's delete window came back
// carrying DELETE for good.
func TestRDCloneOfAVolumelessSourceBeingDeletedIsRefused(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "src-dying", Flags: []string{rdFlagDelete},
	}); err != nil {
		t.Fatalf("seed the dying source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-dying", map[string]any{"name": "dst-dying"})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("clone of a volume-less source being deleted = %d, want 409", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-dying"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the refused clone left a target behind: %v", err)
	}
}

func TestRDCloneOfAVolumelessSourceDoesNotInheritItsFlags(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "src-flag", Flags: []string{"FAILED"},
	}); err != nil {
		t.Fatalf("seed the flagged source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-flag", map[string]any{"name": "dst-flag"})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", resp.StatusCode)
	}

	dst, err := st.ResourceDefinitions().Get(ctx, "dst-flag")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	if len(dst.Flags) != 0 {
		t.Errorf("clone flags = %v, want none: a shell took the source's lifecycle with it", dst.Flags)
	}
}

type failingVolumeLists struct{ store.VolumeDefinitionStore }

func (failingVolumeLists) List(context.Context, string) ([]apiv1.VolumeDefinition, error) {
	return nil, errSnapshotReadFailed
}

type failingVolumeListStore struct{ store.Store }

func (f failingVolumeListStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return failingVolumeLists{f.Store.VolumeDefinitions()}
}

// python-linstor hands every non-2xx clone body to CloneStarted and reads its
// messages, so a bare []ApiCallRc loses the operator's error to an
// AttributeError. Every refusal on the POST has to be the object.
func TestRDCloneRefusalsKeepTheCloneStartedEnvelope(t *testing.T) {
	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-env")

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "src-shell"}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	blind, stopBlind := startServerWithStore(t, failingVolumeListStore{st})
	defer stopBlind()

	if code := cloneOnce(t, base, "src-shell", "dst-shell", nil); code != http.StatusCreated {
		t.Fatalf("first volume-less clone = %d, want 201", code)
	}

	for _, tc := range []struct {
		name, base, src string
		body            map[string]any
	}{
		{name: "missing-source", base: base, src: "no-such-src", body: map[string]any{"name": "dst-x"}},
		{name: "no-name", base: base, src: "src-env", body: map[string]any{"use_zfs_clone": true}},
		{name: "volume-less-replay", base: base, src: "src-shell", body: map[string]any{"name": "dst-shell"}},
		{name: "source-volumes-unreadable", base: blind, src: "src-env", body: map[string]any{"name": "dst-y"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := postClone(t, tc.base, tc.src, tc.body)
			defer func() { _ = resp.Body.Close() }()

			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read the body: %v", err)
			}

			if resp.StatusCode < http.StatusBadRequest {
				t.Fatalf("status = %d, want a refusal", resp.StatusCode)
			}

			var envelope cloneStartedResponse
			if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Messages == nil || len(*envelope.Messages) == 0 {
				t.Errorf("refusal body is not a CloneStarted object with messages: %s", raw)
			}
		})
	}
}

// Only a leftover that is not being deleted may be answered as a finished
// replay. Every existing DELETE fixture seeds an unfinished leftover, so the
// term never decided anything there.
func TestRDCloneReplayOfAFinishedCloneBeingDeletedIsNotAReplay(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-fd")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-fd", "dst-fd", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	dst, err := st.ResourceDefinitions().Get(ctx, "dst-fd")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	dst.Flags = append(dst.Flags, rdFlagDelete)
	if err := st.ResourceDefinitions().Update(ctx, &dst); err != nil {
		t.Fatalf("mark the clone for deletion: %v", err)
	}

	if code := cloneOnce(t, base, "src-fd", "dst-fd", nil); code != http.StatusConflict {
		t.Errorf("replay over a finished clone being deleted = %d, want 409", code)
	}
}

// A volume the snapshot never recorded, on a target with no replica, is not
// this clone's: resuming would hydrate around it and report complete a
// definition carrying a volume nothing restored.
func TestRDCloneRefusesATargetHoldingAVolumeTheSnapshotNeverRecorded(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-ex")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-ex", "dst-ex", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	if err := st.Resources().Delete(ctx, "dst-ex", "node-a"); err != nil {
		t.Fatalf("drop the replica: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-ex",
		&apiv1.VolumeDefinition{VolumeNumber: 7, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("add a volume the snapshot never recorded: %v", err)
	}

	if code := cloneOnce(t, base, "src-ex", "dst-ex", nil); code != http.StatusConflict {
		t.Errorf("retry over a target with an unrecorded volume and no replica = %d, want 409", code)
	}

	if code, _ := cloneStatusOnce(t, base+"/v1/resource-definitions/src-ex/clone/dst-ex"); code != http.StatusNotFound {
		t.Errorf("poll = %d, want 404", code)
	}
}

// `vd create` on a finished clone is an ordinary operation, like expanding one.
func TestRDCloneReplayOfAFinishedCloneThatGainedAVolume(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-gv")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-gv", "dst-gv", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-gv",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("add a volume to the finished clone: %v", err)
	}

	if code := cloneOnce(t, base, "src-gv", "dst-gv", nil); code != http.StatusCreated {
		t.Errorf("replay of a finished clone that gained a volume = %d, want 201", code)
	}

	if _, status := cloneStatusOnce(t, base+"/v1/resource-definitions/src-gv/clone/dst-gv"); status != "COMPLETE" {
		t.Errorf("poll = %q, want COMPLETE", status)
	}
}

// Renumbering keeps the volume count and the size, so only this arm names what
// changed. The status alone came out the same without it.
func TestRDCloneStaleSnapshotNamesTheVolumeItDoesNotCover(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-cov")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-cov"),
		ResourceName:      "src-cov",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	if err := st.VolumeDefinitions().Delete(ctx, "src-cov", 0); err != nil {
		t.Fatalf("drop the source volume: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "src-cov",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("add the replacement volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-cov", map[string]any{"name": "dst-cov", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	envelope := decodeCloneStarted(t, resp)
	if envelope.Messages == nil || len(*envelope.Messages) == 0 {
		t.Fatalf("status %d with an empty envelope", resp.StatusCode)
	}

	if msg := (*envelope.Messages)[0].Message; !strings.Contains(msg, "does not cover volume 1") {
		t.Errorf("refusal %q does not name the volume the snapshot misses", msg)
	}
}

// Beside a snapshot volume that is still missing, a volume the snapshot never
// recorded is somebody else's even when a replica exists: the resume would
// hydrate the missing one next to it and report the clone complete.
func TestRDCloneRefusesAPartialTargetHoldingAnUnrecordedVolume(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-mx")

	if err := st.VolumeDefinitions().Create(ctx, "src-mx",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("give the source a second volume: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         cloneSnapshotName("dst-mx"),
		ResourceName: "src-mx",
		Nodes:        []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64 * 1024},
			{VolumeNumber: 1, SizeKib: 32 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	seedCloneLeftover(t, st, "src-mx", "dst-mx")

	for _, vd := range []apiv1.VolumeDefinition{
		{VolumeNumber: 0, SizeKib: 64 * 1024},
		{VolumeNumber: 7, SizeKib: 8 * 1024},
	} {
		if err := st.VolumeDefinitions().Create(ctx, "dst-mx", &vd); err != nil {
			t.Fatalf("seed leftover volume %d: %v", vd.VolumeNumber, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-mx", NodeName: "node-a", Props: map[string]string{"StorPoolName": "zfs-thin"},
	}); err != nil {
		t.Fatalf("seed the leftover replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-mx", "dst-mx", nil); code != http.StatusConflict {
		t.Errorf("retry over a partial target holding an unrecorded volume = %d, want 409", code)
	}
}
