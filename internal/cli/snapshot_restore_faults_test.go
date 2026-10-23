// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cozystack/blockstor/internal/cli"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errInjected = errors.New("injected store failure")

// restoreFaults names the calls a restoreFaultStore fails or bends for the
// restore target. A zero field passes the call through.
type restoreFaults struct {
	// targetGet fails every read of the target definition.
	targetGet error
	// targetMissesOnce answers the first read of the target NotFound, the
	// read a definition created a moment later did not see.
	targetMissesOnce bool
	// targetGetAfter fails reads of the target past the first one with
	// this error: the read that found it succeeds, the next does not.
	targetGetAfter error
	// volumeCollides answers AlreadyExists to every volume create, and
	// volumeList fails the read-back that follows.
	volumeCollides bool
	volumeList     error
	// rdCreate fails the definition create.
	rdCreate error
	// replicaCollides answers AlreadyExists to every replica create, and
	// replicaGet or replicaDeleting decide what the read-back finds.
	replicaCollides bool
	replicaGet      error
	replicaDeleting bool
	// replicaCreate fails every replica create, which sends a restore that
	// created its definition into its rollback.
	replicaCreate error
	// replicaCreateOn narrows replicaCreate to the replica on this node, so
	// the restore has placed the others when it fails.
	replicaCreateOn string
	// rdDelete fails the rollback's delete of the definition.
	rdDelete error
	// rdPatch fails every definition patch past the first patchesAllowed.
	rdPatch        error
	patchesAllowed int32
}

type restoreFaultStore struct {
	store.Store

	target  string
	f       restoreFaults
	missed  *atomic.Bool
	patches *atomic.Int32
	reads   *atomic.Int32
}

func (s restoreFaultStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return restoreFaultRDs{ResourceDefinitionStore: s.Store.ResourceDefinitions(), s: s}
}

func (s restoreFaultStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return restoreFaultVolumes{VolumeDefinitionStore: s.Store.VolumeDefinitions(), s: s}
}

type restoreFaultVolumes struct {
	store.VolumeDefinitionStore

	s restoreFaultStore
}

func (v restoreFaultVolumes) Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if v.s.f.volumeCollides {
		return store.ErrAlreadyExists
	}

	return v.VolumeDefinitionStore.Create(ctx, rdName, vd) //nolint:wrapcheck // pass-through test double
}

func (v restoreFaultVolumes) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	if v.s.f.volumeList != nil {
		return nil, v.s.f.volumeList
	}

	return v.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (s restoreFaultStore) Resources() store.ResourceStore {
	return restoreFaultReplicas{ResourceStore: s.Store.Resources(), s: s}
}

type restoreFaultRDs struct {
	store.ResourceDefinitionStore

	s restoreFaultStore
}

func (r restoreFaultRDs) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if name == r.s.target {
		if r.s.f.targetGet != nil {
			return apiv1.ResourceDefinition{}, r.s.f.targetGet
		}

		if r.s.f.targetMissesOnce && r.s.missed.CompareAndSwap(false, true) {
			return apiv1.ResourceDefinition{}, store.ErrNotFound
		}

		if r.s.f.targetGetAfter != nil && r.s.reads.Add(1) > 1 {
			return apiv1.ResourceDefinition{}, r.s.f.targetGetAfter
		}
	}

	return r.ResourceDefinitionStore.Get(ctx, name) //nolint:wrapcheck // pass-through test double
}

func (r restoreFaultRDs) Delete(ctx context.Context, name string) error {
	if r.s.f.rdDelete != nil {
		return r.s.f.rdDelete
	}

	return r.ResourceDefinitionStore.Delete(ctx, name) //nolint:wrapcheck // pass-through test double
}

func (r restoreFaultRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if r.s.f.rdPatch != nil && r.s.patches.Add(1) > r.s.f.patchesAllowed {
		return r.s.f.rdPatch
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through test double
}

func (r restoreFaultRDs) Create(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if r.s.f.rdCreate != nil {
		return r.s.f.rdCreate
	}

	return r.ResourceDefinitionStore.Create(ctx, rd) //nolint:wrapcheck // pass-through test double
}

type restoreFaultReplicas struct {
	store.ResourceStore

	s restoreFaultStore
}

func (r restoreFaultReplicas) Create(ctx context.Context, res *apiv1.Resource) error {
	if r.s.f.replicaCreate != nil && (r.s.f.replicaCreateOn == "" || r.s.f.replicaCreateOn == res.NodeName) {
		return r.s.f.replicaCreate
	}

	if r.s.f.replicaCollides {
		return store.ErrAlreadyExists
	}

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // pass-through test double
}

func (r restoreFaultReplicas) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	if r.s.f.replicaGet != nil {
		return apiv1.Resource{}, r.s.f.replicaGet
	}

	if r.s.f.replicaDeleting {
		return apiv1.Resource{Name: rdName, NodeName: node, Flags: []string{apiv1.ResourceFlagDelete}}, nil
	}

	return r.ResourceStore.Get(ctx, rdName, node) //nolint:wrapcheck // pass-through test double
}

func runRestoreWithFaults(t *testing.T, backend store.Store, target string, f restoreFaults, extra ...string) (int, string) {
	t.Helper()

	var (
		errBuf  bytes.Buffer
		missed  atomic.Bool
		patches atomic.Int32
		reads   atomic.Int32
	)

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return restoreFaultStore{Store: backend, target: target, f: f, missed: &missed, patches: &patches, reads: &reads}, nil
		},
	}

	argv := restoreResourceArgv(target)
	if len(extra) > 0 {
		argv = append([]string{
			"s", "resource", "restore", "--from-resource", "pvc-x", "--from-snapshot", extra[0], "--to-resource", target,
		}, extra[1:]...)
	}

	return app.Run(t.Context(), argv), errBuf.String()
}

// A restore that cannot read, create or place its target stops with the error
// and leaves nothing behind: no definition it made, no replica. The one
// exception is a failure that also blinds the rollback's own handshake: one
// that cannot read the volumes cannot tell whether another run already reported
// the definition restored, so it leaves the definition, says so, and the same
// restore run again finishes it.
func TestSnapshotRestoreStopsOnAFailedStoreCall(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		f      restoreFaults
		stderr string
		kept   bool
	}{
		{name: "target-read", f: restoreFaults{targetGet: errInjected}, stderr: "injected store failure"},
		{name: "definition-create", f: restoreFaults{rdCreate: errInjected}, stderr: "injected store failure"},
		{
			name: "replica-read-back", f: restoreFaults{replicaCollides: true, replicaGet: errInjected},
			stderr: "injected store failure",
		},
		{
			name: "replica-still-deleting", f: restoreFaults{replicaCollides: true, replicaDeleting: true},
			stderr: "still being deleted",
		},
		{
			name: "volume-read-back", f: restoreFaults{volumeCollides: true, volumeList: errInjected},
			stderr: "was not rolled back", kept: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			seedSnapshotSource(t.Context(), backend)

			got, stderr := runRestoreWithFaults(t, backend, "pvc-fault", tc.f)
			if got == 0 {
				t.Fatal("the restore succeeded over a failed store call")
			}

			if !strings.Contains(stderr, tc.stderr) {
				t.Errorf("stderr %q does not carry %q", stderr, tc.stderr)
			}

			if tc.kept {
				assertRestoreLeftUnmarked(t, backend, "pvc-fault")

				return
			}

			if _, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-fault"); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("the failed restore left its definition behind: %v", err)
			}

			replicas, err := backend.Resources().ListByDefinition(t.Context(), "pvc-fault")
			if err != nil || len(replicas) != 0 {
				t.Errorf("the failed restore left replicas %v (%v)", replicas, err)
			}
		})
	}
}

// A definition that appears between the read that found none and the create
// is the target, judged like any other existing one: here one prepared for
// this restore, so it is taken, not refused and not left half-done.
func TestSnapshotRestoreJudgesATargetThatAppearedBeforeTheCreate(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	var errBuf bytes.Buffer

	prepareTarget(t, &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return backend, nil
		},
	}, &errBuf, "pvc-late")

	got, stderr := runRestoreWithFaults(t, backend, "pvc-late", restoreFaults{targetMissesOnce: true})
	if got != 0 {
		t.Fatalf("exit = %d (stderr: %s), want the late target taken", got, stderr)
	}

	assertRestoredInto(t, backend, "pvc-late", "node-1", "node-2")
}

// A snapshot that captured no volumes restores nothing, and saying it restored
// would hand back a definition with no data. The definition the restore made
// for it is taken back.
func TestSnapshotRestoreRefusesASnapshotWithNoVolumes(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	if err := backend.Snapshots().Create(t.Context(), &apiv1.Snapshot{
		Name: "snap-empty", ResourceName: "pvc-x", Nodes: []string{"node-1"},
	}); err != nil {
		t.Fatalf("seed the empty snapshot: %v", err)
	}

	got, stderr := runRestoreWithFaults(t, backend, "pvc-empty", restoreFaults{}, "snap-empty")
	if got == 0 {
		t.Fatal("restoring a snapshot that captured no volumes succeeded")
	}

	if !strings.Contains(stderr, "captured no volumes") {
		t.Errorf("stderr %q does not say the snapshot captured nothing", stderr)
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-empty"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the definition made for nothing was not taken back: %v", err)
	}
}

// A rollback that cannot hold the handshake deletes nothing: it cannot tell
// whether another request took the definition over and answered for it.
func TestSnapshotRestoreRollbackDeletesNothingWhenTheHandshakeFails(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	got, stderr := runRestoreWithFaults(t, backend, "pvc-nohold",
		restoreFaults{replicaCreate: errInjected, rdPatch: errInjected})
	if got == 0 {
		t.Fatal("the restore whose placement failed succeeded")
	}

	if !strings.Contains(stderr, "could not be checked") {
		t.Errorf("stderr %q does not say the handshake could not be checked", stderr)
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-nohold"); err != nil {
		t.Errorf("the rollback deleted a definition it could not check: %v", err)
	}
}

// A rollback that held the handshake, could not delete, and then could not
// take its mark back off says the definition still carries it, so the
// operator knows a retry would meet it.
func TestSnapshotRestoreRollbackSaysWhenItsMarkStays(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	got, stderr := runRestoreWithFaults(t, backend, "pvc-markstays", restoreFaults{
		// The hold and the step mark land; the release does not.
		replicaCreate: errInjected, rdDelete: errInjected, rdPatch: errInjected, patchesAllowed: 2,
	})
	if got == 0 {
		t.Fatal("the restore whose placement failed succeeded")
	}

	if !strings.Contains(stderr, "still carries the rollback mark") {
		t.Errorf("stderr %q does not say the mark stayed", stderr)
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-markstays")
	if err != nil {
		t.Fatalf("read the definition: %v", err)
	}

	if rd.Props[store.RollbackAbandonedProp] != store.RollbackStepDeleteDefinition {
		t.Errorf("fixture: rollback mark = %q, want the one that could not be cleared", rd.Props[store.RollbackAbandonedProp])
	}
}

// seedLeftover puts an earlier attempt's leftover of this restore under the
// name: the marker and the snapshot's volume, plus the given replicas.
func seedLeftover(t *testing.T, backend store.Store, target string, nodes ...string) {
	t.Helper()

	ctx := t.Context()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: target, ResourceGroupName: "grp",
		Props: map[string]string{store.RestoreFromSnapshotProp: "pvc-x:snap-1"},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	if err := backend.VolumeDefinitions().Create(ctx, target,
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20}); err != nil {
		t.Fatalf("seed the leftover's volume: %v", err)
	}

	for _, node := range nodes {
		if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: target, NodeName: node}); err != nil {
			t.Fatalf("seed the leftover's replica: %v", err)
		}
	}
}

// A resume that cannot claim the leftover writes nothing onto it: without the
// claim, the attempt that created it could roll it back under this run.
func TestSnapshotRestoreResumeWritesNothingWithoutItsClaim(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)
	seedLeftover(t, backend, "pvc-noclaim")

	got, stderr := runRestoreWithFaults(t, backend, "pvc-noclaim", restoreFaults{rdPatch: errInjected})
	if got == 0 {
		t.Fatal("the resume went on without its claim")
	}

	if !strings.Contains(stderr, "injected store failure") {
		t.Errorf("stderr %q does not carry the claim's failure", stderr)
	}

	replicas, err := backend.Resources().ListByDefinition(t.Context(), "pvc-noclaim")
	if err != nil || len(replicas) != 0 {
		t.Errorf("the unclaimed resume placed replicas %v (%v)", replicas, err)
	}
}

var errOutputClosed = errors.New("output closed")

// failingWriter is a stdout that cannot be written.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errOutputClosed }

// "Already restored" is the whole answer of a re-run over a finished restore.
// One that could not be printed is a failed command, not a silent success.
func TestSnapshotRestoreRerunFailsWhenItCannotSayItIsDone(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)
	seedLeftover(t, backend, "pvc-done-quiet", "node-1")

	var errBuf bytes.Buffer

	app := &cli.App{
		Out: failingWriter{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return backend, nil
		},
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-done-quiet")); got == 0 {
		t.Fatal("the re-run exited 0 without saying the restore is done")
	}

	if !strings.Contains(errBuf.String(), "write output") {
		t.Errorf("stderr %q does not say the output failed", errBuf.String())
	}
}

// The read that judges an existing target is not the read that found it, and
// a failure there stops the restore before anything is written onto it.
func TestSnapshotRestoreStopsWhenTheExistingTargetCannotBeRead(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)
	seedLeftover(t, backend, "pvc-unread")

	got, stderr := runRestoreWithFaults(t, backend, "pvc-unread", restoreFaults{targetGetAfter: errInjected})
	if got == 0 {
		t.Fatal("the restore went on over a target it could not read")
	}

	if !strings.Contains(stderr, "injected store failure") {
		t.Errorf("stderr %q does not carry the read's failure", stderr)
	}

	replicas, err := backend.Resources().ListByDefinition(t.Context(), "pvc-unread")
	if err != nil || len(replicas) != 0 {
		t.Errorf("the restore placed replicas %v (%v) over a target it could not read", replicas, err)
	}
}

// assertRestoreLeftUnmarked checks a definition the rollback could not judge
// is still there and carries no rollback mark, so the same restore run again
// resumes it rather than being refused.
func assertRestoreLeftUnmarked(t *testing.T, backend store.Store, rdName string) {
	t.Helper()

	rd, err := backend.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("the definition the rollback could not judge is gone: %v", err)
	}

	if mark := rd.Props[store.RollbackAbandonedProp]; mark != "" {
		t.Errorf("the definition left in place carries rollback mark %q", mark)
	}
}

// A restore whose second replica fails has placed the first, so it already
// reads as finished, and another run may have reported it restored: the
// rollback leaves it. A re-run judges it finished too and places nothing, so
// the error names the node that never got its replica and how to place it.
func TestSnapshotRestoreLeftShortNamesTheReplicaItNeverPlaced(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	got, stderr := runRestoreWithFaults(t, backend, "pvc-short",
		restoreFaults{replicaCreate: errInjected, replicaCreateOn: "node-2"})
	if got == 0 {
		t.Fatal("the restore whose node-2 replica failed succeeded")
	}

	if !strings.Contains(stderr, "no replica on node-2") || !strings.Contains(stderr, "blockstor resource create") {
		t.Errorf("stderr %q does not name the replica it never placed", stderr)
	}

	assertRestoreLeftUnmarked(t, backend, "pvc-short")
}

// `rd clone` runs a restore of its internal snapshot, so a rollback that left
// the target told the operator to "run the same restore again", and re-running
// the clone is refused over a target that already exists. The advice names the
// restore command that resumes it, and following it finishes the clone.
func TestCloneLeftByItsRollbackNamesTheRestoreThatFinishesIt(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	var (
		errBuf  bytes.Buffer
		missed  atomic.Bool
		patches atomic.Int32
		reads   atomic.Int32
	)

	faulty := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return restoreFaultStore{
				Store: backend, target: "pvc-clone", f: restoreFaults{replicaCreate: errInjected, rdPatch: errInjected},
				missed: &missed, patches: &patches, reads: &reads,
			}, nil
		},
	}

	if got := faulty.Run(t.Context(), []string{"rd", "clone", "pvc-x", "pvc-clone"}); got == 0 {
		t.Fatal("the clone whose placement failed succeeded")
	}

	const resume = "blockstor snapshot resource restore --from-resource pvc-x --from-snapshot clone-pvc-clone " +
		"--to-resource pvc-clone"
	if !strings.Contains(errBuf.String(), resume) {
		t.Fatalf("stderr %q does not name the restore that finishes the clone", errBuf.String())
	}

	clean := &cli.App{
		Out:      &bytes.Buffer{},
		Err:      &bytes.Buffer{},
		StoreFor: func(context.Context) (store.Store, error) { return backend, nil },
	}

	argv := strings.Fields(strings.TrimPrefix(resume, "blockstor "))
	argv[0], argv[1], argv[2] = "s", "resource", "restore"

	if got := clean.Run(t.Context(), argv); got != 0 {
		t.Fatalf("following the advice = exit %d, want 0", got)
	}

	replicas, err := backend.Resources().ListByDefinition(t.Context(), "pvc-clone")
	if err != nil || len(replicas) == 0 {
		t.Errorf("following the advice left the clone with replicas %v (%v)", replicas, err)
	}
}

// The advice a rollback leaves carries the nodes and the pool the operator
// named: without them the restore it names places a replica on every node
// holding the snapshot, a node the operator deliberately left out included.
func TestTheAdviceToRestoreAgainKeepsTheNodesAndPoolTheOperatorNamed(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	got, stderr := runRestoreWithFaults(t, backend, "pvc-onenode",
		restoreFaults{replicaCreate: errInjected, rdPatch: errInjected},
		"snap-1", "--nodes", "node-1", "--storage-pool", "pool-1")
	if got == 0 {
		t.Fatal("the restore whose placement failed succeeded")
	}

	const resume = "blockstor snapshot resource restore --from-resource pvc-x --from-snapshot snap-1 " +
		"--to-resource pvc-onenode --nodes node-1 --storage-pool pool-1"
	if !strings.Contains(stderr, resume) {
		t.Fatalf("stderr %q does not name the restore with the operator's nodes and pool", stderr)
	}

	clean := &cli.App{
		Out:      &bytes.Buffer{},
		Err:      &bytes.Buffer{},
		StoreFor: func(context.Context) (store.Store, error) { return backend, nil },
	}

	argv := strings.Fields(strings.TrimPrefix(resume, "blockstor "))
	argv[0], argv[1], argv[2] = "s", "resource", "restore"

	if code := clean.Run(t.Context(), argv); code != 0 {
		t.Fatalf("following the advice = exit %d, want 0", code)
	}

	replicas, err := backend.Resources().ListByDefinition(t.Context(), "pvc-onenode")
	if err != nil || len(replicas) != 1 || replicas[0].NodeName != "node-1" {
		t.Errorf("following the advice placed %v (%v), want node-1 only", replicas, err)
	}
}

// A snapshot that recorded no volumes has nothing to restore. A resume over a
// leftover of one hydrated nothing, placed a replica and exited 0 over a
// definition with no volume, and a fresh restore wrote the definition before
// refusing. Both are refused before anything is written.
func TestSnapshotRestoreOfAnEmptySnapshotWritesNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		leftover bool
	}{
		{name: "fresh"},
		{name: "over-a-leftover", leftover: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()
			seedSnapshotSource(ctx, backend)

			empty := apiv1.Snapshot{Name: "snap-e", ResourceName: "pvc-x", Nodes: []string{"node-1"}}
			if err := backend.Snapshots().Create(ctx, &empty); err != nil {
				t.Fatalf("seed the empty snapshot: %v", err)
			}

			if tc.leftover {
				if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
					Name: "dst-e", Props: store.WithRestoreMarker(nil, &empty),
				}); err != nil {
					t.Fatalf("seed the leftover: %v", err)
				}
			}

			got, stderr := runRestoreWithFaults(t, backend, "dst-e", restoreFaults{}, "snap-e")
			if got == 0 {
				t.Fatal("the restore of an empty snapshot succeeded")
			}

			if !strings.Contains(stderr, "captured no volumes") {
				t.Errorf("stderr %q does not say the snapshot has nothing to restore", stderr)
			}

			if replicas, _ := backend.Resources().ListByDefinition(ctx, "dst-e"); len(replicas) != 0 {
				t.Errorf("the refused restore placed %d replica(s)", len(replicas))
			}

			_, err := backend.ResourceDefinitions().Get(ctx, "dst-e")
			if !tc.leftover && !errors.Is(err, store.ErrNotFound) {
				t.Errorf("the refused restore wrote the definition: %v", err)
			}
		})
	}
}

// A restore into a prepared target, or the resume of its own leftover, has no
// rollback, and once one replica landed a re-run judges it finished and places
// nothing. So a placement that fails after some replicas landed names the
// ones that did not and how to place them, as the creating path does.
func TestSnapshotRestoreIntoAnExistingTargetNamesTheReplicaItNeverPlaced(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)
	var prepErr bytes.Buffer

	prepareTarget(t, &cli.App{
		Out: &bytes.Buffer{}, Err: &prepErr,
		StoreFor: func(context.Context) (store.Store, error) { return backend, nil },
	}, &prepErr, "pvc-prep")

	got, stderr := runRestoreWithFaults(t, backend, "pvc-prep",
		restoreFaults{replicaCreate: errInjected, replicaCreateOn: "node-2"})
	if got == 0 {
		t.Fatal("the restore whose node-2 replica failed succeeded")
	}

	if !strings.Contains(stderr, "no replica on node-2") || !strings.Contains(stderr, "blockstor resource create") {
		t.Errorf("stderr %q does not name the replica it never placed", stderr)
	}
}
