// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cozystack/blockstor/internal/cli"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// LINSTOR documents a restore as `rd c`, `s vd restore`, then `s rsc
// restore`, and the last step refused the definition the first one made.
func TestSnapshotRestoreFinishesTheTargetLinstorsSequencePrepares(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
	})

	for _, argv := range [][]string{
		{"rd", "c", "pvc-prep"},
		{"s", "vd", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-prep"},
		{"s", "resource", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-prep"},
	} {
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v exit = %d (stderr: %s)", argv, got, errBuf.String())
		}
	}

	rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-prep")
	if err != nil {
		t.Fatalf("read the restored definition: %v", err)
	}

	if got := rd.Props[store.RestoreFromSnapshotProp]; got != "pvc-x:snap-1" {
		t.Errorf("marker = %q, want %q", got, "pvc-x:snap-1")
	}

	replicas, err := appStore(t, app).Resources().ListByDefinition(t.Context(), "pvc-prep")
	if err != nil || len(replicas) == 0 {
		t.Errorf("replicas after the restore = %d (%v), want the snapshot's nodes", len(replicas), err)
	}
}

// A definition that already has a live replica may hold data, and the
// restore into it stays refused.
func TestSnapshotRestoreRefusesATargetWithALiveReplica(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-live"})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-live", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-live", NodeName: "node-1"})
	})

	if got := app.Run(t.Context(), []string{
		"s", "resource", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-live",
	}); got == 0 {
		t.Fatal("restore into a definition with a live replica succeeded")
	}

	rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-live")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if marker := rd.Props[store.RestoreFromSnapshotProp]; marker != "" {
		t.Errorf("the refused target was marked %q", marker)
	}
}

// A prepared target of another layer stack would hold the source's bytes
// under layers that did not write them, so the CLI refuses it the way the REST
// door does, in both directions, and leaves the operator's definition unmarked.
func TestSnapshotRestoreRefusesAPreparedTargetOfAnotherLayerStack(t *testing.T) {
	t.Parallel()

	luks := []string{"DRBD", "LUKS", "STORAGE"}

	for _, tc := range []struct {
		name           string
		source, target []string
	}{
		{name: "plaintext-source-into-luks", source: []string{"DRBD", "STORAGE"}, target: luks},
		{name: "luks-source-into-plaintext", source: luks},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
				_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
					Name: "pvc-src", LayerStack: tc.source,
				})
				_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
					Name: "snap-1", ResourceName: "pvc-src", Nodes: []string{"node-1"},
					VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
				})
				_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
					Name: "pvc-dst", LayerStack: tc.target,
				})
				_ = backend.VolumeDefinitions().Create(ctx, "pvc-dst",
					&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20})
			})

			if got := app.Run(t.Context(), []string{
				"s", "resource", "restore", "--from-resource", "pvc-src", "--from-snapshot", "snap-1",
				"--to-resource", "pvc-dst", "--storage-pool", "pool-1",
			}); got == 0 {
				t.Fatal("restore into a target of another layer stack succeeded")
			}

			if !strings.Contains(errBuf.String(), "layer stack") {
				t.Errorf("refusal %q does not name the layer stack", errBuf.String())
			}

			rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-dst")
			if err != nil {
				t.Fatalf("read the target: %v", err)
			}

			if marker := rd.Props[store.RestoreFromSnapshotProp]; marker != "" {
				t.Errorf("the refused target was marked %q", marker)
			}
		})
	}
}

// prepareTarget runs the first two steps of LINSTOR's restore sequence.
func prepareTarget(t *testing.T, app *cli.App, errBuf *bytes.Buffer, target string) {
	t.Helper()

	for _, argv := range [][]string{
		{"rd", "c", target},
		{"s", "vd", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", target},
	} {
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v exit = %d (stderr: %s)", argv, got, errBuf.String())
		}
	}
}

func restoreResourceArgv(target string, extra ...string) []string {
	return append([]string{
		"s", "resource", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", target,
	}, extra...)
}

func assertRestoredInto(t *testing.T, st store.Store, target string, nodes ...string) {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), target)
	if err != nil {
		t.Fatalf("read the restored definition: %v", err)
	}

	if got := rd.Props[store.RestoreFromSnapshotProp]; got != "pvc-x:snap-1" {
		t.Errorf("marker = %q, want %q", got, "pvc-x:snap-1")
	}

	replicas, err := st.Resources().ListByDefinition(t.Context(), target)
	if err != nil {
		t.Fatalf("list the restored replicas: %v", err)
	}

	got := make([]string, 0, len(replicas))
	for i := range replicas {
		got = append(got, replicas[i].NodeName)
	}

	slices.Sort(got)

	if !slices.Equal(got, nodes) {
		t.Errorf("replicas on %v, want %v", got, nodes)
	}
}

// A request the restore refuses for its own shape, a node that does not hold
// the snapshot, left the marker on the operator's definition, and the
// corrected command then met "already exists" with no way out but deleting it.
func TestSnapshotRestoreIntoAPreparedTargetRefusesABadNodeBeforeMarkingIt(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)
	prepareTarget(t, app, errBuf, "pvc-prep")

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-prep", "--nodes", "node-9")); got == 0 {
		t.Fatal("restore onto a node without the snapshot succeeded")
	}

	rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-prep")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if marker := rd.Props[store.RestoreFromSnapshotProp]; marker != "" {
		t.Errorf("the refused request marked the target %q", marker)
	}

	errBuf.Reset()

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-prep", "--nodes", "node-1")); got != 0 {
		t.Fatalf("the corrected restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	assertRestoredInto(t, appStore(t, app), "pvc-prep", "node-1")
}

// errPlacementRefused is what the API server answers the second replica with.
var errPlacementRefused = errors.New("admission webhook refused the replica")

// failOneReplica fails the first create of one replica, the way an API server
// that refuses one write does.
type failOneReplica struct {
	store.ResourceStore

	node   string
	failed *atomic.Bool
}

func (f failOneReplica) Create(ctx context.Context, res *apiv1.Resource) error {
	if res.NodeName == f.node && f.failed.CompareAndSwap(false, true) {
		return errPlacementRefused
	}

	return f.ResourceStore.Create(ctx, res) //nolint:wrapcheck // pass-through test double
}

type failOneReplicaStore struct {
	store.Store

	node   string
	failed *atomic.Bool
}

func (f failOneReplicaStore) Resources() store.ResourceStore {
	return failOneReplica{ResourceStore: f.Store.Resources(), node: f.node, failed: f.failed}
}

// A placement that failed after the marker went on left a definition no retry
// could finish: no longer prepared since it is marked, and refused by the
// create since it exists. Running the same command again finishes it.
func TestSnapshotRestoreIntoAPreparedTargetFinishesOnRetryAfterAPlacementFailure(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	var (
		errBuf bytes.Buffer
		failed atomic.Bool
	)

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return failOneReplicaStore{Store: backend, node: "node-2", failed: &failed}, nil
		},
	}

	prepareTarget(t, app, &errBuf, "pvc-prep")

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-prep")); got == 0 {
		t.Fatal("the restore whose second replica was refused succeeded")
	}

	errBuf.Reset()

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-prep")); got != 0 {
		t.Fatalf("the retry exit = %d (stderr: %s)", got, errBuf.String())
	}

	// One live replica holds the restore, so the retry leaves placement
	// where it is: topping it up is the placement reconciliation's job, the
	// rule the REST replay follows too.
	assertRestoredInto(t, backend, "pvc-prep", "node-1")
}

// A retry over this restore's leftover whose every replica is going away
// refuses rather than placing over the tear-down.
func TestSnapshotRestoreRetryRefusesALeftoverReplicaBeingDeleted(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-left", Props: map[string]string{store.RestoreFromSnapshotProp: "pvc-x:snap-1"},
		})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-left", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-left", NodeName: "node-2", Flags: []string{apiv1.ResourceFlagDelete},
		})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-left")); got == 0 {
		t.Fatal("a retry placed over a leftover whose every replica is being deleted")
	}

	if !strings.Contains(errBuf.String(), "being deleted") {
		t.Errorf("refusal %q does not name the replicas being deleted", errBuf.String())
	}
}

// Re-running a finished restore left a node the operator had emptied since
// alone on the REST door and re-stamped it here, restored from the
// point-in-time beside a replica that had moved on with live writes.
func TestSnapshotRestoreRerunLeavesAnEmptiedNodeAlone(t *testing.T) {
	t.Parallel()

	app, outBuf, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-done")); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Contains(outBuf.String(), "already restored") {
		t.Errorf("the first restore said it was already done: %q", outBuf.String())
	}

	st := appStore(t, app)
	assertRestoredInto(t, st, "pvc-done", "node-1", "node-2")

	if err := st.Resources().Delete(t.Context(), "pvc-done", "node-2"); err != nil {
		t.Fatalf("empty node-2: %v", err)
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-done")); got != 0 {
		t.Fatalf("re-run exit = %d (stderr: %s)", got, errBuf.String())
	}

	assertRestoredInto(t, st, "pvc-done", "node-1")

	// A re-run that exits 0 in silence reads as a restore that just ran.
	if !strings.Contains(outBuf.String(), "pvc-done is already restored") {
		t.Errorf("the re-run of a finished restore said nothing: %q", outBuf.String())
	}
}

// A leftover an earlier attempt left when its rollback gave up is refused,
// the way the REST door refuses it, not finished.
func TestSnapshotRestoreRetryRefusesALeftoverWhoseRollbackGaveUp(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-gaveup", Props: map[string]string{
				store.RestoreFromSnapshotProp: "pvc-x:snap-1",
				store.RollbackAbandonedProp:   "replicas",
			},
		})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-gaveup", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-gaveup")); got == 0 {
		t.Fatal("a retry finished a leftover whose rollback gave up")
	}

	if !strings.Contains(errBuf.String(), "rollback gave up") {
		t.Errorf("refusal %q does not say the rollback gave up", errBuf.String())
	}

	replicas, err := appStore(t, app).Resources().ListByDefinition(t.Context(), "pvc-gaveup")
	if err != nil || len(replicas) != 0 {
		t.Errorf("the refused retry placed %d replica(s) (%v)", len(replicas), err)
	}
}

// A finished restore is judged before anything about the source is planned:
// the source losing the replicas its pools were read from failed a re-run
// over a target that was already done.
func TestSnapshotRestoreRerunOfAFinishedRestoreDoesNotNeedTheSource(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-whole")); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	st := appStore(t, app)
	for _, node := range []string{"node-1", "node-2"} {
		if err := st.Resources().Delete(t.Context(), "pvc-x", node); err != nil {
			t.Fatalf("drop the source replica on %s: %v", node, err)
		}
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-whole")); got != 0 {
		t.Fatalf("re-run over a finished restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	assertRestoredInto(t, st, "pvc-whole", "node-1", "node-2")
}

// A prepared definition whose group is gone is refused before the marker goes
// on, the way the REST door refuses it: marked and placed, it was a definition
// parented to nothing that the next run then refused over.
func TestSnapshotRestoreRefusesAPreparedTargetWhoseGroupIsGone(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-orphan", ResourceGroupName: "grp-gone",
		})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-orphan", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-orphan")); got == 0 {
		t.Fatal("restore into a prepared definition whose group is gone succeeded")
	}

	if !strings.Contains(errBuf.String(), "no longer exists") {
		t.Errorf("refusal %q does not say the group is gone", errBuf.String())
	}

	st := appStore(t, app)

	rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-orphan")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if marker := rd.Props[store.RestoreFromSnapshotProp]; marker != "" {
		t.Errorf("the refused target was marked %q", marker)
	}

	if replicas, _ := st.Resources().ListByDefinition(t.Context(), "pvc-orphan"); len(replicas) != 0 {
		t.Errorf("the refused restore placed %d replica(s)", len(replicas))
	}
}

// The CLI finishing a leftover takes the adoption mark the REST door takes,
// so a rollback of the attempt that created it yields instead of deleting
// what this run finished.
func TestSnapshotRestoreRetryClaimsTheLeftoverItFinishes(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-left", ResourceGroupName: "grp",
			Props: map[string]string{store.RestoreFromSnapshotProp: "pvc-x:snap-1"},
		})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-left")); got != 0 {
		t.Fatalf("retry exit = %d (stderr: %s)", got, errBuf.String())
	}

	rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-left")
	if err != nil {
		t.Fatalf("read the leftover: %v", err)
	}

	if rd.Props[store.RestoreAdoptedProp] == "" {
		t.Error("the retry finished the leftover without claiming it")
	}
}

// adoptOnFirstReplica plays a concurrent request that adopts the definition
// the moment this command places its first replica, and refuses that
// placement, so this command's rollback runs over a definition someone else
// has already answered for.
type adoptOnFirstReplica struct {
	store.ResourceStore

	backend store.Store
	fired   *atomic.Bool
}

func (a adoptOnFirstReplica) Create(ctx context.Context, res *apiv1.Resource) error {
	if a.fired.CompareAndSwap(false, true) {
		if err := store.ClaimAdoptedLeftover(ctx, a.backend, res.Name, &apiv1.Snapshot{}); err != nil {
			return err //nolint:wrapcheck // test double
		}

		return errPlacementRefused
	}

	return a.ResourceStore.Create(ctx, res) //nolint:wrapcheck // pass-through test double
}

type adoptOnFirstReplicaStore struct {
	store.Store

	fired *atomic.Bool
}

func (a adoptOnFirstReplicaStore) Resources() store.ResourceStore {
	return adoptOnFirstReplica{ResourceStore: a.Store.Resources(), backend: a.Store, fired: a.fired}
}

// The rollback of a definition this command created was a plain delete, so a
// request that adopted the definition meanwhile, and answered for it, lost it.
func TestSnapshotRestoreRollbackLeavesADefinitionAnotherRequestAdopted(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	var (
		errBuf bytes.Buffer
		fired  atomic.Bool
	)

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return adoptOnFirstReplicaStore{Store: backend, fired: &fired}, nil
		},
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-adopted")); got == 0 {
		t.Fatal("the restore whose placement was refused succeeded")
	}

	if !fired.Load() {
		t.Fatal("fixture: the concurrent adoption never ran")
	}

	// Both sides of the handshake can back off, so the adopter is not
	// promised to finish it: the way out is the restore, run again, named as a
	// command so a clone that ran it under the hood can follow it too.
	if !strings.Contains(errBuf.String(), "another request marked it adopted") ||
		!strings.Contains(errBuf.String(), "blockstor snapshot resource restore --from-resource pvc-x --from-snapshot snap-1") {
		t.Errorf("stderr %q does not say another request took the definition over and how to finish it", errBuf.String())
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-adopted")
	if err != nil {
		t.Fatalf("the definition another request adopted was rolled back: %v", err)
	}

	if mark, ok := rd.Props[store.RollbackAbandonedProp]; ok {
		t.Errorf("the definition left to its adopter still carries rollback mark %q", mark)
	}
}

// failPlacementAndDelete refuses the first replica and the definition delete,
// so the rollback holds the handshake and then cannot delete.
type failPlacementAndDelete struct {
	store.Store
}

type failingFirstReplica struct{ store.ResourceStore }

func (failingFirstReplica) Create(context.Context, *apiv1.Resource) error {
	return errPlacementRefused
}

type failingRDDelete struct{ store.ResourceDefinitionStore }

func (failingRDDelete) Delete(context.Context, string) error {
	return errPlacementRefused
}

func (f failPlacementAndDelete) Resources() store.ResourceStore {
	return failingFirstReplica{f.Store.Resources()}
}

func (f failPlacementAndDelete) ResourceDefinitions() store.ResourceDefinitionStore {
	return failingRDDelete{f.Store.ResourceDefinitions()}
}

// A rollback that held the handshake and then could not delete left the
// in-progress mark behind, and nothing was rolling the definition back any
// more: every retry was refused as "being rolled back" instead of finishing it.
func TestSnapshotRestoreRollbackThatCouldNotDeleteReleasesItsMark(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	var errBuf bytes.Buffer

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return failPlacementAndDelete{Store: backend}, nil
		},
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-undeleted")); got == 0 {
		t.Fatal("the restore whose placement was refused succeeded")
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-undeleted")
	if err != nil {
		t.Fatalf("fixture: the definition the rollback could not delete is gone: %v", err)
	}

	if mark, ok := rd.Props[store.RollbackAbandonedProp]; ok {
		t.Errorf("a rollback that could not delete left mark %q", mark)
	}

	if !strings.Contains(errBuf.String(), "blockstor snapshot resource restore --from-resource pvc-x --from-snapshot snap-1") {
		t.Errorf("stderr %q does not say the retry finishes it", errBuf.String())
	}
}

// Blockstor records what it did to a definition in props it reads back. An
// operator's set-property or delete-property on one of them changes what
// blockstor believes about the definition without it having done anything.
func TestResourceDefinitionPropertyVerbsRefuseServerOwnedKeys(t *testing.T) {
	t.Parallel()

	for _, argv := range [][]string{
		{"rd", "set-property", "pvc-owned", store.RestoreFromSnapshotProp, "other:snap"},
		{"rd", "delete-property", "pvc-owned", store.RestoreAdoptedProp},
		{"rd", "set-property", "pvc-owned", store.RestoreVolumesProp, "0:1"},
		{"rd", "set-property", "pvc-owned", store.RollbackAbandonedProp, store.RollbackInProgress},
	} {
		t.Run(argv[1], func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
				_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
					Name: "pvc-owned",
					Props: map[string]string{
						store.RestoreFromSnapshotProp: "pvc-x:snap-1",
						store.RestoreAdoptedProp:      "2026-10-07T00:00:00Z",
					},
				})
			})

			if got := app.Run(t.Context(), argv); got == 0 {
				t.Fatalf("%v succeeded on a key blockstor sets itself", argv)
			}

			if !strings.Contains(errBuf.String(), "blockstor sets this property itself") {
				t.Errorf("stderr %q does not say why", errBuf.String())
			}

			rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-owned")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			if rd.Props[store.RestoreFromSnapshotProp] != "pvc-x:snap-1" || rd.Props[store.RestoreAdoptedProp] == "" {
				t.Errorf("the refused edit still changed the props: %v", rd.Props)
			}
		})
	}
}

// The CLI restore writes the record of what the snapshot held beside the
// marker, as the REST door does.
func TestSnapshotRestoreRecordsTheSnapshotVolumes(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-rec")); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-rec")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if _, ok := store.RecordedRestoreVolumes(rd.Props); !ok {
		t.Errorf("the restored definition carries no record: %v", rd.Props)
	}
}

// volumeWrittenConcurrently plays a concurrent run that has already adopted
// the definition this command just created and hydrated a volume into it
// before this command gets to that volume.
type volumeWrittenConcurrently struct {
	store.ResourceDefinitionStore

	backend store.Store
	sizeKib int64
}

func (v volumeWrittenConcurrently) Create(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if err := v.ResourceDefinitionStore.Create(ctx, rd); err != nil {
		return err //nolint:wrapcheck // pass-through test double
	}

	return v.backend.VolumeDefinitions().Create(ctx, rd.Name, //nolint:wrapcheck // test double
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: v.sizeKib})
}

type volumeWrittenConcurrentlyStore struct {
	store.Store

	sizeKib int64
}

func (v volumeWrittenConcurrentlyStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return volumeWrittenConcurrently{
		ResourceDefinitionStore: v.Store.ResourceDefinitions(), backend: v.Store, sizeKib: v.sizeKib,
	}
}

// A run that created the definition met the volume a concurrent run had
// already hydrated into it, took that for a collision and unwound the volumes,
// under a run that had answered for them.
func TestSnapshotRestoreKeepsAVolumeAConcurrentRunHydrated(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		sizeKib int64
		wantOK  bool
	}{
		{name: "same-size", sizeKib: 1 << 20, wantOK: true},
		{name: "grown", sizeKib: 1 << 21, wantOK: true},
		{name: "smaller", sizeKib: 1 << 19},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			seedSnapshotSource(t.Context(), backend)

			var errBuf bytes.Buffer

			app := &cli.App{
				Out: &bytes.Buffer{},
				Err: &errBuf,
				StoreFor: func(context.Context) (store.Store, error) {
					return volumeWrittenConcurrentlyStore{Store: backend, sizeKib: tc.sizeKib}, nil
				},
			}

			got := app.Run(t.Context(), restoreResourceArgv("pvc-race"))
			if (got == 0) != tc.wantOK {
				t.Fatalf("exit = %d (stderr: %s), want success=%v", got, errBuf.String(), tc.wantOK)
			}

			if !tc.wantOK {
				return
			}

			vds, err := backend.VolumeDefinitions().List(t.Context(), "pvc-race")
			if err != nil || len(vds) != 1 || vds[0].SizeKib != tc.sizeKib {
				t.Errorf("volumes after the restore = %v (%v), want the one already there", vds, err)
			}
		})
	}
}

// volumeCreateCollides answers AlreadyExists for volume 0. With writeKib set
// it first puts a volume of that size there, the way a concurrent restore
// would; with it zero nothing is there, a volume deleted between the create
// and the read-back.
type volumeCreateCollides struct {
	store.VolumeDefinitionStore

	writeKib int64
}

func (v volumeCreateCollides) Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if vd.VolumeNumber != 0 {
		return v.VolumeDefinitionStore.Create(ctx, rdName, vd) //nolint:wrapcheck // pass-through test double
	}

	if v.writeKib != 0 {
		written := apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: v.writeKib}
		if err := v.VolumeDefinitionStore.Create(ctx, rdName, &written); err != nil {
			return err //nolint:wrapcheck // test double
		}
	}

	return store.ErrAlreadyExists
}

type volumeCreateCollidesStore struct {
	store.Store

	writeKib int64
}

func (v volumeCreateCollidesStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return volumeCreateCollides{VolumeDefinitionStore: v.Store.VolumeDefinitions(), writeKib: v.writeKib}
}

func runOnCollidingVolumes(t *testing.T, backend store.Store, writeKib int64, argv []string) (int, string) {
	t.Helper()

	var errBuf bytes.Buffer

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return volumeCreateCollidesStore{Store: backend, writeKib: writeKib}, nil
		},
	}

	return app.Run(t.Context(), argv), errBuf.String()
}

// A volume the create says exists and the read-back cannot find was deleted in
// between. Counting it restored would report success over a definition
// without it.
func TestSnapshotRestoreRefusesAVolumeGoneOnReadBack(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	got, stderr := runOnCollidingVolumes(t, backend, 0, restoreResourceArgv("pvc-gone"))
	if got == 0 {
		t.Fatal("the restore reported success over a volume that is not there")
	}

	if !strings.Contains(stderr, "gone when read back") || !strings.Contains(stderr, "run the same command again") {
		t.Errorf("stderr %q does not say the volume went away and how to start over", stderr)
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-gone"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the definition this restore created was not rolled back: %v", err)
	}
}

// A resume has no rollback to name the way out, so a volume gone on its
// read-back says which restore finishes the target.
func TestSnapshotRestoreResumeNamesTheRestoreWhenAVolumeIsGoneOnReadBack(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedSnapshotSource(ctx, backend)

	snap, err := backend.Snapshots().Get(ctx, "pvc-x", "snap-1")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-resume", Props: store.WithRestoreMarker(nil, &snap),
	}); err != nil {
		t.Fatalf("seed the unfinished leftover: %v", err)
	}

	got, stderr := runOnCollidingVolumes(t, backend, 0, restoreResourceArgv("pvc-resume"))
	if got == 0 {
		t.Fatal("the resume reported success over a volume that is not there")
	}

	const resume = "blockstor snapshot resource restore --from-resource pvc-x --from-snapshot snap-1 --to-resource pvc-resume"
	if !strings.Contains(stderr, "gone when read back") || !strings.Contains(stderr, resume) {
		t.Errorf("stderr %q does not name the restore that finishes the target", stderr)
	}
}

// `s vd restore` fails on a volume a concurrent restore of the same snapshot
// wrote between its check and its create, whatever its size, and leaves it as
// it is. Accepting a same-size one let the losing run go on, fail on a later
// volume, and unwind volumes the winning run had answered for: both runs create
// in the snapshot's order, so the loser has to stop on the first volume, with
// nothing of its own to unwind.
func TestSnapshotVolumeDefinitionRestoreFailsOnAVolumeAConcurrentRestoreWrote(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		writeKib int64
	}{
		{name: "same-size", writeKib: 1 << 20},
		{name: "other-size", writeKib: 1 << 19},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			seedSnapshotSource(t.Context(), backend)

			if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-vd"}); err != nil {
				t.Fatalf("seed the target: %v", err)
			}

			got, stderr := runOnCollidingVolumes(t, backend, tc.writeKib, []string{
				"s", "vd", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-vd",
			})
			if got == 0 {
				t.Fatalf("exit = 0 (stderr: %s), want a failure over the volume another restore wrote", stderr)
			}

			vds, err := backend.VolumeDefinitions().List(t.Context(), "pvc-vd")
			if err != nil || len(vds) != 1 || vds[0].SizeKib != tc.writeKib {
				t.Errorf("volumes = %v (%v), want the one the concurrent restore wrote, untouched", vds, err)
			}
		})
	}
}

// replicaGoneOnReadBack answers AlreadyExists to the first replica create and
// has nothing to read back: a replica deleted between the two calls.
type replicaGoneOnReadBack struct {
	store.ResourceStore

	fired *atomic.Bool
}

func (r replicaGoneOnReadBack) Create(ctx context.Context, res *apiv1.Resource) error {
	if r.fired.CompareAndSwap(false, true) {
		return store.ErrAlreadyExists
	}

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // pass-through test double
}

type replicaGoneOnReadBackStore struct {
	store.Store

	fired *atomic.Bool
}

func (r replicaGoneOnReadBackStore) Resources() store.ResourceStore {
	return replicaGoneOnReadBack{ResourceStore: r.Store.Resources(), fired: r.fired}
}

// A replica the create collided with and that was gone when read back was
// deleted in between, and is created again, as the REST door does.
func TestSnapshotRestoreRecreatesAReplicaGoneOnReadBack(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	var (
		errBuf bytes.Buffer
		fired  atomic.Bool
	)

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return replicaGoneOnReadBackStore{Store: backend, fired: &fired}, nil
		},
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-regone", "--nodes", "node-1")); got != 0 {
		t.Fatalf("exit = %d (stderr: %s), want the replica created again", got, errBuf.String())
	}

	if _, err := backend.Resources().Get(t.Context(), "pvc-regone", "node-1"); err != nil {
		t.Errorf("the replica gone on read-back was not created again: %v", err)
	}
}

// alwaysCollides answers AlreadyExists to every replica create and has
// nothing to read back.
type alwaysCollides struct{ store.ResourceStore }

func (alwaysCollides) Create(context.Context, *apiv1.Resource) error { return store.ErrAlreadyExists }

type alwaysCollidesStore struct{ store.Store }

func (a alwaysCollidesStore) Resources() store.ResourceStore {
	return alwaysCollides{a.Store.Resources()}
}

// A collision that keeps recurring with nothing to read back is not a replica
// placed: counting it one would report the restore done with no replica there.
func TestSnapshotRestoreRefusesAReplicaCollisionThatKeepsRecurring(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	var errBuf bytes.Buffer

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return alwaysCollidesStore{backend}, nil
		},
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-collide", "--nodes", "node-1")); got == 0 {
		t.Fatal("the restore reported success with no replica placed")
	}

	if !strings.Contains(errBuf.String(), "kept colliding") {
		t.Errorf("stderr %q does not say the collision kept recurring", errBuf.String())
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-collide"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the definition this restore created was not rolled back: %v", err)
	}
}

// staleReplicaCache collides on the first replica create and serves, from its
// cache, a live replica the API server already holds as being deleted.
type staleReplicaCache struct {
	store.ResourceStore

	fired *atomic.Bool
}

func (s staleReplicaCache) Create(ctx context.Context, res *apiv1.Resource) error {
	if s.fired.CompareAndSwap(false, true) {
		return store.ErrAlreadyExists
	}

	return s.ResourceStore.Create(ctx, res) //nolint:wrapcheck // pass-through test double
}

func (staleReplicaCache) Get(_ context.Context, rdName, node string) (apiv1.Resource, error) {
	return apiv1.Resource{Name: rdName, NodeName: node}, nil
}

func (staleReplicaCache) GetUncached(_ context.Context, rdName, node string) (apiv1.Resource, error) {
	return apiv1.Resource{Name: rdName, NodeName: node, Flags: []string{apiv1.ResourceFlagDelete}}, nil
}

type staleReplicaCacheStore struct {
	store.Store

	fired *atomic.Bool
}

func (s staleReplicaCacheStore) Resources() store.ResourceStore {
	return staleReplicaCache{ResourceStore: s.Store.Resources(), fired: s.fired}
}

// The replica a create collided with is judged from the API server: a cache
// that trails shows one already accepted for deletion as live.
func TestSnapshotRestoreReadsACollidingReplicaPastTheCache(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	var (
		errBuf bytes.Buffer
		fired  atomic.Bool
	)

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return staleReplicaCacheStore{Store: backend, fired: &fired}, nil
		},
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-stale", "--nodes", "node-1")); got == 0 {
		t.Fatal("the restore counted a replica being deleted as placed")
	}

	if !strings.Contains(errBuf.String(), "still being deleted") {
		t.Errorf("stderr %q does not say the replica is being deleted", errBuf.String())
	}
}

// A diskless replica already on the node holds no copy of the data, and is not
// counted as the restore's own placement.
func TestSnapshotRestoreDoesNotCountADisklessReplicaAsPlaced(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	if err := backend.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-dl", NodeName: "node-1", Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed the diskless replica: %v", err)
	}

	var errBuf bytes.Buffer

	app := &cli.App{
		Out:      &bytes.Buffer{},
		Err:      &errBuf,
		StoreFor: func(context.Context) (store.Store, error) { return backend, nil },
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-dl", "--nodes", "node-1")); got == 0 {
		t.Fatal("the restore counted a diskless replica as placed")
	}

	if !strings.Contains(errBuf.String(), "diskless") {
		t.Errorf("stderr %q does not say the replica is diskless", errBuf.String())
	}
}

// The controller's tie-breaker witness on a node the restore places onto is
// promoted to the replica the restore asked for, with the pool it planned.
func TestSnapshotRestorePromotesATieBreakerOnARequestedNode(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	if err := backend.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-tb", NodeName: "node-1",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed the witness: %v", err)
	}

	var errBuf bytes.Buffer

	app := &cli.App{
		Out:      &bytes.Buffer{},
		Err:      &errBuf,
		StoreFor: func(context.Context) (store.Store, error) { return backend, nil },
	}

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-tb", "--nodes", "node-1")); got != 0 {
		t.Fatalf("restore onto a node holding the witness = %d (stderr: %s)", got, errBuf.String())
	}

	res, err := backend.Resources().Get(t.Context(), "pvc-tb", "node-1")
	if err != nil {
		t.Fatalf("read the replica: %v", err)
	}

	if !store.HoldsData(&res) || res.Props["StorPoolName"] != "data" {
		t.Errorf("witness after the restore: flags %v, pool %q; want a replica with a disk in pool data",
			res.Flags, res.Props["StorPoolName"])
	}
}

// A source stored without a stack is brought up with the default whatever its
// group says now; the restored definition carries that stack, stamped.
func TestSnapshotRestoreStampsTheSourcesDataPlaneStack(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name: "grp-luks", SelectFilter: apiv1.AutoSelectFilter{LayerStack: []string{"DRBD", "LUKS", "STORAGE"}},
		})

		src, _ := backend.ResourceDefinitions().Get(ctx, "pvc-x")
		src.LayerStack = nil
		src.ResourceGroupName = "grp-luks"
		_ = backend.ResourceDefinitions().Update(ctx, &src)
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-stack")); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-stack")
	if err != nil {
		t.Fatalf("read the restored definition: %v", err)
	}

	if !slices.Equal(rd.LayerStack, apiv1.DefaultLayerStack()) {
		t.Errorf("restored stack = %v, want the source's data-plane %v", rd.LayerStack, apiv1.DefaultLayerStack())
	}
}

// A rollback that left a definition whole and could not take its mark back
// off leaves one every retry refuses. Deleting the mark is the operator's way
// out short of deleting the definition, so that one edit goes through.
func TestResourceDefinitionDeletePropertyClearsAStuckRollbackMark(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-stuck",
			Props: map[string]string{
				store.RestoreFromSnapshotProp: "pvc-x:snap-1",
				store.RollbackAbandonedProp:   store.RollbackInProgress,
			},
		})
	})

	if got := app.Run(t.Context(), []string{"rd", "delete-property", "pvc-stuck", store.RollbackAbandonedProp}); got != 0 {
		t.Fatalf("delete-property of the rollback mark = exit %d (stderr: %s)", got, errBuf.String())
	}

	rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-stuck")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if mark, ok := rd.Props[store.RollbackAbandonedProp]; ok {
		t.Errorf("the mark is still there: %q", mark)
	}

	if rd.Props[store.RestoreFromSnapshotProp] != "pvc-x:snap-1" {
		t.Errorf("clearing the mark changed the marker: %v", rd.Props)
	}
}

// Only a mark over a definition the rollback left whole is the operator's to
// clear: one naming a later step a rollback gave up at stays, on both verbs
// that delete. Setting the key to
// nothing is the CLI's delete, and clears an in-progress mark the same way.
func TestResourceDefinitionPropertyVerbsClearOnlyAMarkOverAWholeDefinition(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		mark  string
		argv  []string
		clear bool
		other bool
	}{
		{name: "delete-step", mark: "reap-replicas", argv: []string{"rd", "delete-property", "pvc-m", store.RollbackAbandonedProp}},
		{name: "set-empty-step", mark: "reap-replicas", argv: []string{"rd", "set-property", "pvc-m", store.RollbackAbandonedProp}},
		{
			name: "set-empty-in-progress", mark: store.RollbackInProgress, clear: true,
			argv: []string{"rd", "set-property", "pvc-m", store.RollbackAbandonedProp},
		},
		{
			name: "delete-snapshots-step", mark: store.RollbackStepSnapshots, clear: true,
			argv: []string{"rd", "delete-property", "pvc-m", store.RollbackAbandonedProp},
		},
		{name: "other-key-on-a-step-mark", mark: "reap-replicas", other: true, argv: []string{"rd", "set-property", "pvc-m", "Aux/x", "1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
				_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
					Name: "pvc-m", Props: map[string]string{store.RollbackAbandonedProp: tc.mark},
				})
			})

			got := app.Run(t.Context(), tc.argv)

			rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-m")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			_, kept := rd.Props[store.RollbackAbandonedProp]

			if tc.other {
				if got != 0 || !kept || rd.Props["Aux/x"] != "1" {
					t.Errorf("%v = exit %d, props %v (stderr: %s), want the edit through and the mark kept",
						tc.argv, got, rd.Props, errBuf.String())
				}

				return
			}

			if tc.clear {
				if got != 0 || kept {
					t.Errorf("%v = exit %d, mark kept %v (stderr: %s), want it cleared", tc.argv, got, kept, errBuf.String())
				}

				return
			}

			if got == 0 || !kept {
				t.Errorf("%v = exit %d, mark kept %v, want it refused and kept", tc.argv, got, kept)
			}
		})
	}
}

// markerLandsLate hides the restore marker from every read until this run
// writes its own stamp: a concurrent run of the same restore stamped it in
// between, so this run judged the target prepared and then found the marker
// already there.
type markerLandsLate struct {
	store.ResourceDefinitionStore

	stamped *atomic.Bool
}

func (m markerLandsLate) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	return m.hide(m.ResourceDefinitionStore.Get(ctx, name))
}

func (m markerLandsLate) GetUncached(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	return m.hide(m.ResourceDefinitionStore.GetUncached(ctx, name))
}

func (m markerLandsLate) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	m.stamped.Store(true)

	return m.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
}

func (m markerLandsLate) hide(rd apiv1.ResourceDefinition, err error) (apiv1.ResourceDefinition, error) {
	if err == nil && !m.stamped.Load() {
		rd.Props = maps.Clone(rd.Props)
		delete(rd.Props, store.RestoreFromSnapshotProp)
		delete(rd.Props, store.RestoreVolumesProp)
	}

	return rd, err
}

type markerLandsLateStore struct {
	store.Store

	stamped *atomic.Bool
}

func (s markerLandsLateStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return markerLandsLate{ResourceDefinitionStore: s.Store.ResourceDefinitions(), stamped: s.stamped}
}

func (s markerLandsLateStore) Resources() store.ResourceStore {
	return replicaLandsLate{ResourceStore: s.Store.Resources(), stamped: s.stamped}
}

// replicaLandsLate hides the concurrent run's replica along with its marker:
// that run placed it after this one judged the target prepared.
type replicaLandsLate struct {
	store.ResourceStore

	stamped *atomic.Bool
}

func (r replicaLandsLate) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	replicas, err := r.ResourceStore.ListByDefinition(ctx, rdName)
	if err != nil || r.stamped.Load() {
		return replicas, err //nolint:wrapcheck // test double
	}

	return slices.DeleteFunc(replicas, func(res apiv1.Resource) bool { return res.NodeName == "node-1" }), nil
}

// A run that finds the marker already on a target it judged prepared lost the
// stamp to a concurrent run of the same restore. It judges that run's
// definition as a leftover, and a finished one is left as it is rather than
// placed over with this run's nodes.
func TestSnapshotRestoreJudgesATargetAConcurrentRunMarkedFirst(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedSnapshotSource(ctx, backend)

	snap, err := backend.Snapshots().Get(ctx, "pvc-x", "snap-1")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-late", ResourceGroupName: "grp", LayerStack: []string{"DRBD", "STORAGE"},
		Props: store.WithRestoreMarker(nil, &snap),
	}); err != nil {
		t.Fatalf("seed the target the other run marked: %v", err)
	}

	if err := backend.VolumeDefinitions().Create(ctx, "pvc-late",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20}); err != nil {
		t.Fatalf("seed the target's volume: %v", err)
	}

	if err := backend.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-late", NodeName: "node-1", Props: map[string]string{"StorPoolName": "data"},
	}); err != nil {
		t.Fatalf("seed the other run's replica: %v", err)
	}

	var (
		errBuf  bytes.Buffer
		stamped atomic.Bool
	)

	app := &cli.App{
		Out: &bytes.Buffer{},
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return markerLandsLateStore{Store: backend, stamped: &stamped}, nil
		},
	}

	if got := app.Run(ctx, restoreResourceArgv("pvc-late", "--nodes", "node-2")); got != 0 {
		t.Fatalf("restore over a finished target another run marked = %d (stderr: %s)", got, errBuf.String())
	}

	if _, err := backend.Resources().Get(ctx, "pvc-late", "node-2"); err == nil {
		t.Errorf("the run placed over the other run's finished restore with its own nodes")
	}
}

var errWitnessPatchRefused = errors.New("apiserver refused the witness patch")

// witnessPatchFaults fails the patch that would promote a witness, or shows
// the witness already being deleted when the patch reads it.
type witnessPatchFaults struct {
	store.ResourceStore

	refuse, deleting, replaced bool
}

func (w witnessPatchFaults) PatchResourceSpec(
	ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error,
) error {
	if w.refuse {
		return errWitnessPatchRefused
	}

	return w.ResourceStore.PatchResourceSpec(ctx, rdName, node, func(live *apiv1.Resource) error { //nolint:wrapcheck // test double
		if w.deleting {
			live.Flags = append(live.Flags, apiv1.ResourceFlagDelete)
		}

		if w.replaced {
			live.Flags = []string{apiv1.ResourceFlagDiskless}
		}

		return mutate(live)
	})
}

type witnessPatchFaultStore struct {
	store.Store

	refuse, deleting, replaced bool
}

func (s witnessPatchFaultStore) Resources() store.ResourceStore {
	return witnessPatchFaults{
		ResourceStore: s.Store.Resources(), refuse: s.refuse, deleting: s.deleting, replaced: s.replaced,
	}
}

// A witness the restore could not promote, because the patch failed, the
// witness started going away under it, or an operator's diskless replica took
// its place, fails the restore: reported done, it
// would leave a diskless witness where the restore asked for its data.
func TestSnapshotRestoreFailsOnAWitnessItCouldNotPromote(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                       string
		refuse, deleting, replaced bool
	}{
		{name: "patch-refused", refuse: true},
		{name: "being-deleted", deleting: true},
		{name: "replaced-by-a-diskless", replaced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			seedSnapshotSource(t.Context(), backend)

			if err := backend.Resources().Create(t.Context(), &apiv1.Resource{
				Name: "pvc-tbf", NodeName: "node-1",
				Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
			}); err != nil {
				t.Fatalf("seed the witness: %v", err)
			}

			app := &cli.App{
				Out: &bytes.Buffer{},
				Err: &bytes.Buffer{},
				StoreFor: func(context.Context) (store.Store, error) {
					return witnessPatchFaultStore{
						Store: backend, refuse: tc.refuse, deleting: tc.deleting, replaced: tc.replaced,
					}, nil
				},
			}

			if got := app.Run(t.Context(), restoreResourceArgv("pvc-tbf", "--nodes", "node-1")); got == 0 {
				t.Error("the restore reported success over a witness it could not promote")
			}
		})
	}
}

// A re-run that finishes a leftover records the volumes of the snapshot it
// restores from, replacing a record an earlier attempt left from another one.
func TestSnapshotRestoreResumeRecordsTheVolumesOfItsSnapshot(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedSnapshotSource(ctx, backend)

	snap, err := backend.Snapshots().Get(ctx, "pvc-x", "snap-1")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	props := store.WithRestoreMarker(nil, &snap)
	props[store.RestoreVolumesProp] = store.EncodeRestoreVolumes([]apiv1.SnapshotVolumeDef{
		{VolumeNumber: 0, SizeKib: 1 << 20}, {VolumeNumber: 1, SizeKib: 1 << 10},
	})

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-rec", ResourceGroupName: "grp", LayerStack: []string{"DRBD", "STORAGE"}, Props: props,
	}); err != nil {
		t.Fatalf("seed the leftover with a stale record: %v", err)
	}

	var errBuf bytes.Buffer

	app := &cli.App{
		Out:      &bytes.Buffer{},
		Err:      &errBuf,
		StoreFor: func(context.Context) (store.Store, error) { return backend, nil },
	}

	if got := app.Run(ctx, restoreResourceArgv("pvc-rec", "--nodes", "node-1")); got != 0 {
		t.Fatalf("resume of the leftover = %d (stderr: %s)", got, errBuf.String())
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-rec")
	if err != nil {
		t.Fatalf("read the restored definition: %v", err)
	}

	if want := store.EncodeRestoreVolumes(snap.VolumeDefinitions); rd.Props[store.RestoreVolumesProp] != want {
		t.Errorf("volume record = %q, want %q", rd.Props[store.RestoreVolumesProp], want)
	}
}

// `s vd restore` into a definition another restore has marked as its own is
// refused before anything is written: that restore hydrates the volumes
// itself, and one this command wrote and then unwound would go from under it.
func TestSnapshotVolumeDefinitionRestoreRefusesATargetUnderARestoreMarker(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedSnapshotSource(ctx, backend)

	snap, err := backend.Snapshots().Get(ctx, "pvc-x", "snap-1")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-marked", Props: store.WithRestoreMarker(nil, &snap),
	}); err != nil {
		t.Fatalf("seed the marked target: %v", err)
	}

	var errBuf bytes.Buffer

	app := &cli.App{
		Out:      &bytes.Buffer{},
		Err:      &errBuf,
		StoreFor: func(context.Context) (store.Store, error) { return backend, nil },
	}

	if got := app.Run(ctx, []string{
		"s", "vd", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-marked",
	}); got == 0 {
		t.Fatal("volume-definition restore into a marked target succeeded")
	}

	if vds, _ := backend.VolumeDefinitions().List(ctx, "pvc-marked"); len(vds) != 0 {
		t.Errorf("the refused restore wrote %d volume(s)", len(vds))
	}
}
