// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedTwoVolumeCloneLeftover leaves the state a clone of a two-volume source
// can be in once its internal snapshot is gone: the marker and the record of
// what the snapshot held, the given volumes, and a live replica.
func seedTwoVolumeCloneLeftover(t *testing.T, st store.Store, src, dst string, hydrated ...int32) {
	t.Helper()

	ctx := t.Context()
	seedDeployedCloneSource(t, st, src)

	if err := st.VolumeDefinitions().Create(ctx, src,
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("seed the source's second volume: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: dst,
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker(src, cloneSnapshotName(dst)),
			store.RestoreVolumesProp: store.EncodeRestoreVolumes([]apiv1.SnapshotVolumeDef{
				{VolumeNumber: 0, SizeKib: 64 * 1024},
				{VolumeNumber: 1, SizeKib: 32 * 1024},
			}),
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	sizes := map[int32]int64{0: 64 * 1024, 1: 32 * 1024}

	for _, number := range hydrated {
		if err := st.VolumeDefinitions().Create(ctx, dst,
			&apiv1.VolumeDefinition{VolumeNumber: number, SizeKib: sizes[number]}); err != nil {
			t.Fatalf("seed volume %d of the leftover: %v", number, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: dst, NodeName: "node-a", Props: map[string]string{"StorPoolName": "zfs-thin"},
	}); err != nil {
		t.Fatalf("seed the leftover's replica: %v", err)
	}
}

func cloneStatusOf(t *testing.T, base, src, dst string) (int, string) {
	t.Helper()

	resp := httpGet(t, base+"/v1/resource-definitions/"+src+"/clone/"+dst)
	defer func() { _ = resp.Body.Close() }()

	var got struct {
		Status string `json:"status"`
	}

	_ = json.NewDecoder(resp.Body).Decode(&got)

	return resp.StatusCode, got.Status
}

// With the clone's internal snapshot gone, a leftover holding one volume and a
// live replica read as finished whatever the snapshot held, so a two-volume
// clone that had restored only its first volume was reported complete.
func TestRDCloneJudgesALeftoverAgainstItsRecordWhenTheSnapshotIsGone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoVolumeCloneLeftover(t, st, "src-partial", "dst-partial", 0)

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code, status := cloneStatusOf(t, base, "src-partial", "dst-partial"); status == "COMPLETE" {
		t.Errorf("clone status over a partial clone = %d %q, want not COMPLETE", code, status)
	}

	if code := cloneOnce(t, base, "src-partial", "dst-partial", nil); code == http.StatusCreated {
		t.Errorf("retry over a partial clone whose snapshot is gone = 201")
	}
}

// The record is what makes a whole clone stay whole once its snapshot is
// reaped: every recorded volume present and a live replica is finished.
func TestRDCloneStaysCompleteWhenItsSnapshotIsGone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoVolumeCloneLeftover(t, st, "src-whole", "dst-whole", 0, 1)

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code, status := cloneStatusOf(t, base, "src-whole", "dst-whole"); status != "COMPLETE" {
		t.Errorf("clone status over a whole clone = %d %q, want COMPLETE", code, status)
	}

	if code := cloneOnce(t, base, "src-whole", "dst-whole", nil); code != http.StatusCreated {
		t.Errorf("retry over a whole clone whose snapshot is gone = %d, want 201", code)
	}
}

// A resume finishes the leftover from the snapshot it takes now, which can
// hold other volumes than the one the leftover was created from. The record
// kept from the first attempt judged the finished clone against volumes it was
// never restored with once the snapshot was gone, and the poll and a retry
// both called a working clone unfinished.
func TestRDCloneResumeRecordsTheVolumesOfTheSnapshotItTook(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-retake")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-retake",
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("src-retake", cloneSnapshotName("dst-retake")),
			store.RestoreVolumesProp: store.EncodeRestoreVolumes([]apiv1.SnapshotVolumeDef{
				{VolumeNumber: 0, SizeKib: 64 * 1024},
				{VolumeNumber: 1, SizeKib: 32 * 1024},
			}),
		},
	}); err != nil {
		t.Fatalf("seed the empty leftover of a two-volume snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-retake", "dst-retake", nil); code != http.StatusCreated {
		t.Fatalf("resume from a retaken snapshot = %d, want 201", code)
	}

	if err := st.Snapshots().Delete(ctx, "src-retake", cloneSnapshotName("dst-retake")); err != nil {
		t.Fatalf("delete the clone's snapshot: %v", err)
	}

	if code, status := cloneStatusOf(t, base, "src-retake", "dst-retake"); status != "COMPLETE" {
		t.Errorf("clone status once its retaken snapshot is gone = %d %q, want COMPLETE", code, status)
	}

	if code := cloneOnce(t, base, "src-retake", "dst-retake", nil); code != http.StatusCreated {
		t.Errorf("retry over the finished clone once its snapshot is gone = %d, want 201", code)
	}
}

// The replay of a finished clone takes the adoption mark before it answers
// 201, as the restore's replay does, so a creator rolling the clone back
// yields instead of deleting what the replay was answered for.
func TestRDCloneReplayMarksTheCloneAdopted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoVolumeCloneLeftover(t, st, "src-mark", "dst-mark", 0, 1)

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-mark", "dst-mark", nil); code != http.StatusCreated {
		t.Fatalf("replay of the finished clone = %d, want 201", code)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "dst-mark")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	if rd.Props[store.RestoreAdoptedProp] == "" {
		t.Error("the replay answered 201 without marking the clone adopted")
	}
}
