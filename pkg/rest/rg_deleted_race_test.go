// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedGroupedCloneSource is seedDeployedCloneSource with the source parented
// to a resource group, which is the shape every definition linstor-csi creates
// has.
func seedGroupedCloneSource(t *testing.T, st store.Store, rdName, rgName string, createGroup bool) {
	t.Helper()

	seedDeployedCloneSource(t, st, rdName)

	src, err := st.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("read the seeded source: %v", err)
	}

	src.ResourceGroupName = rgName

	if err := st.ResourceDefinitions().Update(t.Context(), &src); err != nil {
		t.Fatalf("parent the source: %v", err)
	}

	if createGroup {
		if err := st.ResourceGroups().Create(t.Context(),
			&apiv1.ResourceGroup{Name: rgName}); err != nil {
			t.Fatalf("seed RG: %v", err)
		}
	}
}

// `POST /v1/resource-definitions` checks the resource group twice — before the
// write and again after it, rolling the definition back when a concurrent
// `rg d` won the race. A definition left pointing at a group that is gone
// lists fine and places badly: the placer's Controller→RG→RD walk drops the RG
// tier without a word, taking auto-place, auto-diskful, place_count and
// rebalance with it.
//
// Clone creates a definition the same way and inherits the group the same way,
// and had neither half.
func TestRDCloneRollsBackWhenTheParentGroupIsGone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, st, "src-rg-race", "grp-gone", false)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-rg-race", map[string]any{
		"name":          "dst-rg-race",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — the parent group is gone", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-rg-race"); err == nil {
		t.Error("the definition survived, parented to a group that does not exist")
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "dst-rg-race")
	if err != nil {
		t.Fatalf("list the target's replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("%d replica(s) left behind pointing at a definition that was rolled back",
			len(replicas))
	}

	// The source is untouched, and so is the snapshot the clone took: it may
	// be the only copy of something, and deleting one is the operator's call.
	if _, err := st.ResourceDefinitions().Get(ctx, "src-rg-race"); err != nil {
		t.Errorf("the rollback took the source with it: %v", err)
	}

	if _, err := st.Snapshots().Get(ctx, "src-rg-race",
		cloneSnapshotName("dst-rg-race")); err != nil {
		t.Errorf("the rollback deleted the internal snapshot: %v", err)
	}
}

// The positive control: the same clone with the group where it should be.
func TestRDCloneKeepsGoingWhenTheParentGroupIsThere(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, st, "src-rg-ok", "grp-there", true)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-rg-ok", map[string]any{
		"name":          "dst-rg-ok",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-rg-ok"); err != nil {
		t.Errorf("target RD not persisted: %v", err)
	}
}

// The restore path inherits the same group the same way, and had the same gap.
func TestSnapshotRestoreRollsBackWhenTheParentGroupIsGone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "restore-src",
		ResourceGroupName: "grp-gone-restore",
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-rg",
		ResourceName: "restore-src",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "restore-dst",
		"from_snapshot": "snap-rg",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/restore-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — the parent group is gone", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "restore-dst"); err == nil {
		t.Error("the definition survived, parented to a group that does not exist")
	}
}

// The positive control for the restore path.
func TestSnapshotRestoreKeepsGoingWhenTheParentGroupIsThere(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx,
		&apiv1.ResourceGroup{Name: "grp-there-restore"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "restore-src-ok",
		ResourceGroupName: "grp-there-restore",
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-rg-ok",
		ResourceName: "restore-src-ok",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "restore-dst-ok",
		"from_snapshot": "snap-rg-ok",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/restore-src-ok/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "restore-dst-ok"); err != nil {
		t.Errorf("target RD not persisted: %v", err)
	}
}

// errReplicaDeleteFailed stands in for what a satellite writing status on the
// very replicas being reaped produces: a conflict, a timeout, an RBAC gap.
var errReplicaDeleteFailed = errors.New("probe: replica delete failed")

// failingReplicaDeletes is a store whose replica deletes fail and whose
// everything else works.
type failingReplicaDeletes struct {
	store.ResourceStore
}

func (f failingReplicaDeletes) Delete(context.Context, string, string) error {
	return errReplicaDeleteFailed
}

type failingCascadeStore struct {
	store.Store
}

func (f failingCascadeStore) Resources() store.ResourceStore {
	return failingReplicaDeletes{f.Store.Resources()}
}

// The rollback drops the definition ONLY if the replicas went with it.
// CascadeDeleteResources stops at the first replica it cannot delete and
// leaves the rest untried, so deleting the parent anyway produces the orphan
// this rollback exists to avoid: a Resource whose RD vanished never gets a
// DeletionTimestamp, its satellite's finalizer never runs, and the DRBD minor,
// port and peer entries stay live until the next create with that name
// collides with them — which on the CSI path is the retry, because the target
// name is deterministic.
//
// Both other doors that perform this teardown refuse to proceed on a failed
// cascade. This one does too, and says what it left behind.
func TestRDCloneRollbackKeepsTheDefinitionWhenTheCascadeFails(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, backend, "src-orphan", "grp-orphan-gone", false)

	st := failingCascadeStore{backend}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-orphan", map[string]any{
		"name":          "dst-orphan",
		"use_zfs_clone": true,
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		t.Fatalf("status = 404, the code the successful rollback uses — a failed cascade " +
			"must not be reported as a rollback")
	}

	// The definition stays, because its replicas are still there.
	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-orphan"); err != nil {
		t.Fatalf("the definition was deleted over replicas that could not be: %v", err)
	}

	replicas, err := backend.Resources().ListByDefinition(ctx, "dst-orphan")
	if err != nil {
		t.Fatalf("list the replicas: %v", err)
	}

	if len(replicas) == 0 {
		t.Fatal("the fixture left no replicas, so this test proves nothing")
	}

	// And the operator is told which definition is now parented to nothing.
	var envelope cloneStartedResponse
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode the envelope: %v", err)
	}

	if envelope.Messages == nil || len(*envelope.Messages) == 0 {
		t.Fatal("empty envelope")
	}

	msg := (*envelope.Messages)[0].Message
	if !strings.Contains(msg, "dst-orphan") || !strings.Contains(msg, "still there") {
		t.Errorf("message = %q, want it to name the definition left behind", msg)
	}
}

// errRGReadFailed stands in for what a re-check can hit that says nothing
// about the group: apiserver unavailability, a timeout, a decode failure, a
// cancelled request context. getRGWithCacheRetry returns immediately on
// anything that is not NotFound, so all of them arrive here.
var errRGReadFailed = errors.New("probe: transient failure reading the resource group")

type failingRGReads struct {
	store.ResourceGroupStore
}

func (f failingRGReads) Get(context.Context, string) (apiv1.ResourceGroup, error) {
	return apiv1.ResourceGroup{}, errRGReadFailed
}

type failingRGReadStore struct {
	store.Store
}

func (f failingRGReadStore) ResourceGroups() store.ResourceGroupStore {
	return failingRGReads{f.Store.ResourceGroups()}
}

// The post-write check is a safety net over a restore that already succeeded.
// When the net itself cannot be inspected, undoing the restore trades a rare
// dangling parent group for a certain lost restore — and a worse one, because
// this endpoint has no idempotent-replay gate: the definition stays behind and
// every later attempt under that name meets AlreadyExists and answers 409 from
// then on. A blip in a check that has nothing to do with whether the restore
// worked must not be able to do that.
func TestSnapshotRestoreSurvivesAFailedParentGroupRecheck(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.ResourceGroups().Create(ctx,
		&apiv1.ResourceGroup{Name: "grp-flaky"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "flaky-src",
		ResourceGroupName: "grp-flaky",
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-flaky",
		ResourceName: "flaky-src",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, failingRGReadStore{backend})
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "flaky-dst",
		"from_snapshot": "snap-flaky",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/flaky-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 — the restore succeeded; only the re-check failed",
			resp.StatusCode)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "flaky-dst"); err != nil {
		t.Errorf("the restored definition was rolled back over a failed check: %v", err)
	}

	vds, err := backend.VolumeDefinitions().List(ctx, "flaky-dst")
	if err != nil {
		t.Fatalf("list the restored volumes: %v", err)
	}

	if len(vds) != 1 {
		t.Errorf("restored definition has %d volume(s), want 1", len(vds))
	}
}
