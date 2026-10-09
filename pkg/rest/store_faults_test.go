// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errStoreFault = errors.New("injected store failure")

// storeFaults fails or bends one store call each, for the definition named
// target. The volume and replica stores are wrapped without their uncached
// reads, so the code under test falls back to the methods failed here.
type storeFaults struct {
	target string

	snapshotList   error // Snapshots().ListByDefinitionUncached of any definition
	volumeList     error // VolumeDefinitions().List of target
	volumeCollides bool  // VolumeDefinitions().Create answers AlreadyExists
	volumeGet      error // VolumeDefinitions().Get of target
	rdCollides     bool  // ResourceDefinitions().Create of target answers AlreadyExists
	rdGetUncached  error // ResourceDefinitions().GetUncached of target
	rdGet          error // ResourceDefinitions().Get of target
	// rdGetUncachedFails, when set, limits rdGetUncached to that many reads;
	// the ones after it go through.
	rdGetUncachedFails *atomic.Int32
	// rdGetPasses and rdGetUncachedPasses, when set, let that many reads of
	// the target through and fail the one after them alone, so a test can
	// aim at a single read site.
	rdGetPasses, rdGetUncachedPasses *atomic.Int32
	replicaCollide                   bool  // Resources().Create of target answers AlreadyExists
	replicaGet                       error // Resources().Get of target
	replicaPatch                     error // Resources().PatchResourceSpec of target
	groupGet                         error // ResourceGroups().Get of any group
}

type faultyStore struct {
	store.Store

	f storeFaults
}

type faultySnapshots struct {
	store.SnapshotStore

	f storeFaults
}

func (s faultySnapshots) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	if s.f.snapshotList != nil {
		return nil, s.f.snapshotList
	}

	return s.SnapshotStore.ListByDefinitionUncached(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

type faultyVolumes struct {
	store.VolumeDefinitionStore

	f storeFaults
}

func (v faultyVolumes) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	if rdName == v.f.target && v.f.volumeList != nil {
		return nil, v.f.volumeList
	}

	return v.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (v faultyVolumes) Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if rdName == v.f.target && v.f.volumeCollides {
		return store.ErrAlreadyExists
	}

	return v.VolumeDefinitionStore.Create(ctx, rdName, vd) //nolint:wrapcheck // pass-through test double
}

func (v faultyVolumes) Get(ctx context.Context, rdName string, number int32) (apiv1.VolumeDefinition, error) {
	if rdName == v.f.target && v.f.volumeGet != nil {
		return apiv1.VolumeDefinition{}, v.f.volumeGet
	}

	return v.VolumeDefinitionStore.Get(ctx, rdName, number) //nolint:wrapcheck // pass-through test double
}

type faultyDefinitions struct {
	store.ResourceDefinitionStore

	f storeFaults
}

func (d faultyDefinitions) Create(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if rd.Name == d.f.target && d.f.rdCollides {
		return store.ErrAlreadyExists
	}

	return d.ResourceDefinitionStore.Create(ctx, rd) //nolint:wrapcheck // pass-through test double
}

func (d faultyDefinitions) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if name == d.f.target && d.f.rdGet != nil {
		return apiv1.ResourceDefinition{}, d.f.rdGet
	}

	if name == d.f.target && d.f.rdGetPasses != nil && d.f.rdGetPasses.Add(-1) == -1 {
		return apiv1.ResourceDefinition{}, errStoreFault
	}

	return d.ResourceDefinitionStore.Get(ctx, name) //nolint:wrapcheck // pass-through test double
}

func (d faultyDefinitions) GetUncached(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if name == d.f.target && d.f.rdGetUncached != nil &&
		(d.f.rdGetUncachedFails == nil || d.f.rdGetUncachedFails.Add(-1) >= 0) {
		return apiv1.ResourceDefinition{}, d.f.rdGetUncached
	}

	if name == d.f.target && d.f.rdGetUncachedPasses != nil && d.f.rdGetUncachedPasses.Add(-1) == -1 {
		return apiv1.ResourceDefinition{}, errStoreFault
	}

	return d.ResourceDefinitionStore.GetUncached(ctx, name) //nolint:wrapcheck // pass-through test double
}

type faultyReplicas struct {
	store.ResourceStore

	f storeFaults
}

func (r faultyReplicas) Create(ctx context.Context, res *apiv1.Resource) error {
	if res.Name == r.f.target && r.f.replicaCollide {
		return store.ErrAlreadyExists
	}

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // pass-through test double
}

func (r faultyReplicas) PatchResourceSpec(
	ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error,
) error {
	if rdName == r.f.target && r.f.replicaPatch != nil {
		return r.f.replicaPatch
	}

	return r.ResourceStore.PatchResourceSpec(ctx, rdName, node, mutate) //nolint:wrapcheck // pass-through test double
}

func (r faultyReplicas) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	if rdName == r.f.target && r.f.replicaGet != nil {
		return apiv1.Resource{}, r.f.replicaGet
	}

	return r.ResourceStore.Get(ctx, rdName, node) //nolint:wrapcheck // pass-through test double
}

type faultyGroups struct {
	store.ResourceGroupStore

	f storeFaults
}

func (g faultyGroups) Get(ctx context.Context, name string) (apiv1.ResourceGroup, error) {
	if g.f.groupGet != nil {
		return apiv1.ResourceGroup{}, g.f.groupGet
	}

	return g.ResourceGroupStore.Get(ctx, name) //nolint:wrapcheck // pass-through test double
}

func (s faultyStore) Snapshots() store.SnapshotStore {
	return faultySnapshots{s.Store.Snapshots(), s.f}
}

func (s faultyStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return faultyVolumes{s.Store.VolumeDefinitions(), s.f}
}

func (s faultyStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return faultyDefinitions{s.Store.ResourceDefinitions(), s.f}
}

func (s faultyStore) Resources() store.ResourceStore { return faultyReplicas{s.Store.Resources(), s.f} }

func (s faultyStore) ResourceGroups() store.ResourceGroupStore {
	return faultyGroups{s.Store.ResourceGroups(), s.f}
}

func seedMarkedCloneLeftover(t *testing.T, st store.Store, src, dst string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  dst,
		Props: map[string]string{restoreFromSnapshotKey: restoreMarker(src, cloneSnapshotName(dst))},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}
}

// A clone resume that cannot read whether its internal snapshot is there does
// not go on as if it were, nor as if it were gone: it answers that the read
// failed and writes nothing.
func TestRDCloneResumeStopsWhenItCannotReadItsSnapshot(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-snapread")
	seedMarkedCloneLeftover(t, backend, "src-snapread", "dst-snapread")

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{target: "dst-snapread", snapshotList: errStoreFault}})
	defer stop()

	resp := postClone(t, base, "src-snapread", map[string]any{"name": "dst-snapread"})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("clone resume over an unreadable snapshot list = %d, want 500", resp.StatusCode)
	}

	if vds, _ := backend.VolumeDefinitions().List(t.Context(), "dst-snapread"); len(vds) != 0 {
		t.Errorf("the resume hydrated %d volume(s) without knowing its snapshot", len(vds))
	}
}

// The judgement of a clone leftover that cannot read its volumes is no
// judgement: it answers the failure instead of calling the leftover anything.
func TestRDCloneStopsWhenItCannotReadTheLeftoversVolumes(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-vdread")
	seedMarkedCloneLeftover(t, backend, "src-vdread", "dst-vdread")

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{target: "dst-vdread", volumeList: errStoreFault}})
	defer stop()

	resp := postClone(t, base, "src-vdread", map[string]any{"name": "dst-vdread"})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("clone over a leftover whose volumes cannot be read = %d, want 500", resp.StatusCode)
	}

	if snaps, _ := backend.Snapshots().ListByDefinition(t.Context(), "src-vdread"); len(snaps) != 0 {
		t.Errorf("the clone took %d snapshot(s) without judging its leftover", len(snaps))
	}
}

// A restore whose create collided cannot tell whose definition it met without
// reading it, and answers the read failure, not the collision.
func TestSnapshotRestoreAnswersAFailedReadOfTheDefinitionItCollidedWith(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-rdread")
	seedRestoreSnapshot(t, backend, "src-rdread", "snap-rdread", []string{"node-a"})

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{
		target: "dst-rdread", rdCollides: true, rdGetUncached: errStoreFault,
	}})
	defer stop()

	code, rc := restoreAnswer(t, base, "src-rdread", "snap-rdread", map[string]any{"to_resource": "dst-rdread"})
	if code != http.StatusInternalServerError {
		t.Errorf("restore whose collided definition cannot be read = %d %q, want 500", code, rc.Message)
	}
}

// A replica create that collided with one that cannot be read back is not a
// replica placed.
func TestSnapshotRestoreDoesNotCountAReplicaItCannotReadBack(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-repread")
	seedRestoreSnapshot(t, backend, "src-repread", "snap-repread", []string{"node-a"})

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{
		target: "dst-repread", replicaCollide: true, replicaGet: errStoreFault,
	}})
	defer stop()

	code, rc := restoreAnswer(t, base, "src-repread", "snap-repread",
		map[string]any{"to_resource": "dst-repread", "node_names": []string{"node-a"}})
	if code == http.StatusCreated {
		t.Errorf("restore over a replica it could not read back = 201 %q", rc.Message)
	}
}

// A volume create that collided with one that cannot be read back is not a
// volume restored.
func TestSnapshotRestoreDoesNotCountAVolumeItCannotReadBack(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-volread")
	seedRestoreSnapshot(t, backend, "src-volread", "snap-volread", []string{"node-a"})

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{
		target: "dst-volread", volumeCollides: true, volumeGet: errStoreFault,
	}})
	defer stop()

	code, rc := restoreAnswer(t, base, "src-volread", "snap-volread", map[string]any{"to_resource": "dst-volread"})
	if code == http.StatusCreated {
		t.Errorf("restore over a volume it could not read back = 201 %q", rc.Message)
	}
}

// A clone naming a group that cannot be read is told so, not told the group
// does not exist.
func TestRDCloneAnswersAnUnreadableGroupAsAReadFailure(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-rgread")
	seedCloneRG(t, backend, "grp-rgread")

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{groupGet: errStoreFault}})
	defer stop()

	resp := postClone(t, base, "src-rgread", map[string]any{"name": "dst-rgread", "resource_group": "grp-rgread"})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("clone into a group that cannot be read = %d, want 500", resp.StatusCode)
	}
}

// The creator's rollback that finds the definition adopted says it left it to
// the adopter, rather than reporting a rollback that never ran.
func TestTheCreatorsRollbackYieldsToAnAdopter(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-yield", Props: map[string]string{store.RestoreAdoptedProp: "2026-10-07T00:00:00Z"},
	}); err != nil {
		t.Fatalf("seed the adopted definition: %v", err)
	}

	s := &Server{Store: backend}

	if err := s.rollBackCompensating(ctx, "dst-yield", nil, rollbackEvenIfFinished); !errors.Is(err, errRollbackYielded) {
		t.Errorf("rollback over an adopted definition = %v, want it to yield", err)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-yield"); err != nil {
		t.Errorf("the adopted definition was rolled back: %v", err)
	}
}

// The clone status a poll reads cannot call a leftover anything without its
// volumes: an unreadable listing is a 500 linstor-csi retries, not the 404 that
// sends it back to the POST as if the clone were unfinished.
func TestRDCloneStatusAnswersAnUnreadableLeftoverAsAReadFailure(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-stread")
	seedMarkedCloneLeftover(t, backend, "src-stread", "dst-stread")

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{target: "dst-stread", volumeList: errStoreFault}})
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/src-stread/clone/dst-stread")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("clone status over a leftover whose volumes cannot be read = %d, want 500", resp.StatusCode)
	}
}

// A finished restore's replay answers for a definition nobody verified, so a
// parent group it cannot read refuses it, as the clone's replay and the CLI
// restore do. It used to answer 201 with a warning, which binds a PV to a
// definition whose group may be gone.
func TestRestoreReplayRefusesAGroupItCannotRead(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-grpread")
	seedRestoreSnapshot(t, backend, "src-grpread", "snap-grpread", []string{"node-a"})

	if err := backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp-read"}); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	plain, stopPlain := startServerWithStore(t, backend)

	body := map[string]any{"to_resource": "dst-grpread"}
	if code, rc := restoreAnswer(t, plain, "src-grpread", "snap-grpread", body); code != http.StatusCreated {
		stopPlain()
		t.Fatalf("first restore = %d %q, want 201", code, rc.Message)
	}

	stopPlain()

	if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "dst-grpread",
		func(rd *apiv1.ResourceDefinition) error {
			rd.ResourceGroupName = "grp-read"

			return nil
		}); err != nil {
		t.Fatalf("parent the restored definition: %v", err)
	}

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{target: "dst-grpread", groupGet: errStoreFault}})
	defer stop()

	if code, rc := restoreAnswer(t, base, "src-grpread", "snap-grpread", body); code == http.StatusCreated {
		t.Errorf("replay over a group that could not be read = %d %q, want a refusal", code, rc.Message)
	}
}

// A finished clone whose parent group cannot be read is not answered COMPLETE,
// and not sent back to the POST with a 404 either: a read failure is a 500,
// which linstor-csi returns from CreateVolume and retries.
func TestRDCloneStatusAnswersAnUnreadableGroupAsAReadFailure(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-stgrp")

	plain, stopPlain := startServerWithStore(t, backend)

	if code := cloneOnce(t, plain, "src-stgrp", "dst-stgrp", nil); code != http.StatusCreated {
		stopPlain()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stopPlain()

	if err := backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp-stgrp"}); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "dst-stgrp",
		func(rd *apiv1.ResourceDefinition) error {
			rd.ResourceGroupName = "grp-stgrp"

			return nil
		}); err != nil {
		t.Fatalf("parent the clone: %v", err)
	}

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{target: "dst-stgrp", groupGet: errStoreFault}})
	defer stop()

	if code, status := cloneStatusOnce(t, base+"/v1/resource-definitions/src-stgrp/clone/dst-stgrp"); code != http.StatusInternalServerError {
		t.Errorf("poll of a finished clone whose group cannot be read = %d %q, want 500", code, status)
	}
}

// A target that cannot be read is not a target that is absent. Read as absent,
// the create that followed collided and was adopted past every judgement of a
// leftover, and a replay of a finished clone or restore re-placed a replica on
// the node the operator had emptied. It is a 500 the caller retries.
func TestAnUnreadableTargetIsRefusedNotReplaced(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		faults storeFaults
		replay func(t *testing.T, base string) int
	}{
		{
			name:   "clone",
			faults: storeFaults{target: "dst-unread", rdGet: errStoreFault},
			replay: func(t *testing.T, base string) int {
				t.Helper()

				return cloneOnce(t, base, "src-unread", "dst-unread", nil)
			},
		},
		{
			name: "restore",
			faults: storeFaults{
				target: "dst-unread", rdGetUncached: errStoreFault, rdGetUncachedFails: budgetOf(2),
			},
			replay: func(t *testing.T, base string) int {
				t.Helper()

				code, _ := restoreAnswer(t, base, "src-unread", "snap-unread", map[string]any{
					"to_resource": "dst-unread", "nodes": []string{"node-a", "node-b"},
				})

				return code
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()
			seedTwoNodeSource(t, backend, "src-unread")
			seedRestoreSnapshot(t, backend, "src-unread", "snap-unread", []string{"node-a", "node-b"})

			plain, stopPlain := startServerWithStore(t, backend)

			if code := tc.replay(t, plain); code != http.StatusCreated {
				stopPlain()
				t.Fatalf("first %s = %d, want 201", tc.name, code)
			}

			stopPlain()

			if err := backend.Resources().Delete(ctx, "dst-unread", "node-b"); err != nil {
				t.Fatalf("empty node-b: %v", err)
			}

			base, stop := startServerWithStore(t, faultyStore{backend, tc.faults})
			defer stop()

			if code := tc.replay(t, base); code != http.StatusInternalServerError {
				t.Errorf("replay over a target that cannot be read = %d, want 500", code)
			}

			replicas, err := backend.Resources().ListByDefinition(ctx, "dst-unread")
			if err != nil || len(replicas) != 1 {
				t.Errorf("the replay left replicas %v (%v), want node-a only", nodeNamesOf(replicas), err)
			}
		})
	}
}

func budgetOf(n int32) *atomic.Int32 {
	var b atomic.Int32

	b.Store(n)

	return &b
}

// Each read site that judges the target refuses on its own when it alone
// cannot read it: the four of them back each other up when every read fails,
// so a single one going back to "unreadable is absent" went unnoticed. A
// replay of a restore whose later read fails re-placed the emptied node, and a
// clone resume over an unfinished leftover was adopted past every judgement.
func TestAnUnreadableTargetIsRefusedAtEachReadSite(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		faults  storeFaults
		emptyA  bool
		replay  func(t *testing.T, base string) int
		wantAll int
	}{
		{
			name:   "clone-target-state",
			faults: storeFaults{target: "dst-site", rdGetPasses: budgetOf(2)},
			emptyA: true,
			replay: func(t *testing.T, base string) int {
				t.Helper()

				return cloneOnce(t, base, "src-site", "dst-site", nil)
			},
		},
		{
			name:    "clone-replay",
			faults:  storeFaults{target: "dst-site", rdGetPasses: budgetOf(1)},
			wantAll: 1,
			replay: func(t *testing.T, base string) int {
				t.Helper()

				return cloneOnce(t, base, "src-site", "dst-site", nil)
			},
		},
		{
			name:    "restore-prepared-target",
			faults:  storeFaults{target: "dst-site", rdGetUncachedPasses: budgetOf(0)},
			wantAll: 1,
			replay: func(t *testing.T, base string) int {
				t.Helper()

				code, _ := restoreAnswer(t, base, "src-site", "snap-site", map[string]any{
					"to_resource": "dst-site", "nodes": []string{"node-a", "node-b"},
				})

				return code
			},
		},
		{
			name:    "restore-target-state",
			faults:  storeFaults{target: "dst-site", rdGetUncachedPasses: budgetOf(2)},
			wantAll: 1,
			replay: func(t *testing.T, base string) int {
				t.Helper()

				code, _ := restoreAnswer(t, base, "src-site", "snap-site", map[string]any{
					"to_resource": "dst-site", "nodes": []string{"node-a", "node-b"},
				})

				return code
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()
			seedTwoNodeSource(t, backend, "src-site")
			seedRestoreSnapshot(t, backend, "src-site", "snap-site", []string{"node-a", "node-b"})

			plain, stopPlain := startServerWithStore(t, backend)

			if code := tc.replay(t, plain); code != http.StatusCreated {
				stopPlain()
				t.Fatalf("first %s = %d, want 201", tc.name, code)
			}

			stopPlain()

			if err := backend.Resources().Delete(ctx, "dst-site", "node-b"); err != nil {
				t.Fatalf("empty node-b: %v", err)
			}

			if tc.emptyA {
				if err := backend.Resources().Delete(ctx, "dst-site", "node-a"); err != nil {
					t.Fatalf("leave the clone unfinished: %v", err)
				}
			}

			base, stop := startServerWithStore(t, faultyStore{backend, tc.faults})
			defer stop()

			if code := tc.replay(t, base); code != http.StatusInternalServerError {
				t.Errorf("replay whose one read of the target failed = %d, want 500", code)
			}

			replicas, err := backend.Resources().ListByDefinition(ctx, "dst-site")
			if err != nil || len(replicas) != tc.wantAll {
				t.Errorf("the replay left replicas %v (%v), want %d", nodeNamesOf(replicas), err, tc.wantAll)
			}
		})
	}
}

// The read that finds the stored name is the one that decides the name every
// later gate judges under. One that failed and was taken as "keep the
// request's spelling" let a blip that cleared by the next read judge a
// finished clone or restore under another case: no replica counted, and a
// replica re-placed on the node the operator had emptied. It is refused.
func TestAStoredNameThatCannotBeReadIsRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		replay func(t *testing.T, base string) int
	}{
		{name: "clone", replay: func(t *testing.T, base string) int {
			t.Helper()

			return cloneOnce(t, base, "src-blip", "DST-BLIP", nil)
		}},
		{name: "restore", replay: func(t *testing.T, base string) int {
			t.Helper()

			code, _ := restoreAnswer(t, base, "src-blip", "snap-blip", map[string]any{
				"to_resource": "DST-BLIP", "nodes": []string{"node-a", "node-b"},
			})

			return code
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()
			seedTwoNodeSource(t, backend, "src-blip")
			seedRestoreSnapshot(t, backend, "src-blip", "snap-blip", []string{"node-a", "node-b"})

			plain, stopPlain := startServerWithStore(t, backend)

			first := func() int {
				if tc.name == "clone" {
					return cloneOnce(t, plain, "src-blip", "dst-blip", nil)
				}

				code, _ := restoreAnswer(t, plain, "src-blip", "snap-blip", map[string]any{
					"to_resource": "dst-blip", "nodes": []string{"node-a", "node-b"},
				})

				return code
			}

			if code := first(); code != http.StatusCreated {
				stopPlain()
				t.Fatalf("first %s = %d, want 201", tc.name, code)
			}

			stopPlain()

			if err := backend.Resources().Delete(ctx, "dst-blip", "node-b"); err != nil {
				t.Fatalf("empty node-b: %v", err)
			}

			blip := faultyStore{caseFoldingDefinitions{backend}, storeFaults{target: "DST-BLIP", rdGetPasses: budgetOf(0)}}

			base, stop := startServerWithStore(t, blip)
			defer stop()

			if code := tc.replay(t, base); code != http.StatusInternalServerError {
				t.Errorf("replay whose stored-name read failed = %d, want 500", code)
			}

			all, err := backend.Resources().List(ctx)
			if err != nil {
				t.Fatalf("list replicas: %v", err)
			}

			var placed []string

			for i := range all {
				if strings.EqualFold(all[i].Name, "dst-blip") {
					placed = append(placed, all[i].Name+"@"+all[i].NodeName)
				}
			}

			if len(placed) != 1 {
				t.Errorf("the replay left replicas %v, want only dst-blip@node-a", placed)
			}
		})
	}
}
