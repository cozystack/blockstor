// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// rollbackGate holds the RG-deleted rollback at its first step, the snapshot
// listing of the target, until the test has abandoned the request. It then
// gives an ended context time to show before letting the rollback go on, and
// refuses on one the way a real API client does.
type rollbackGate struct {
	store.SnapshotStore

	target  string
	reached chan struct{}
	release chan struct{}
	once    *sync.Once
}

func (g rollbackGate) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	if rdName == g.target {
		g.once.Do(func() { close(g.reached) })

		<-g.release

		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "list snapshots on an ended context")
		case <-time.After(500 * time.Millisecond):
		}
	}

	snaps, err := g.SnapshotStore.ListByDefinition(ctx, rdName)

	return snaps, errors.Wrap(err, "list snapshots through the gate")
}

type rollbackGateStore struct {
	store.Store

	gate rollbackGate
}

func (s rollbackGateStore) Snapshots() store.SnapshotStore {
	gate := s.gate
	gate.SnapshotStore = s.Store.Snapshots()

	return gate
}

func newRollbackGateStore(backend store.Store, target string) rollbackGateStore {
	return rollbackGateStore{Store: backend, gate: rollbackGate{
		target:  target,
		reached: make(chan struct{}),
		release: make(chan struct{}),
		once:    &sync.Once{},
	}}
}

// abandonAtTheRollback posts body to url, abandons the request once the
// rollback has started, and lets the rollback go on.
func abandonAtTheRollback(t *testing.T, url string, body any, gated rollbackGateStore) {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	reqCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	done := make(chan error, 1)

	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}

		done <- err
	}()

	select {
	case <-gated.gate.reached:
	case err := <-done:
		t.Fatalf("the request finished (err=%v) before the rollback started; the fixture needs it abandoned", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the rollback never started")
	}

	cancel()

	if err := <-done; err == nil {
		t.Fatal("the request finished; the fixture needs it abandoned")
	}

	close(gated.gate.release)
}

func waitForDefinitionGone(t *testing.T, backend store.Store, name string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := backend.ResourceDefinitions().Get(t.Context(), name); errors.Is(err, store.ErrNotFound) {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Errorf("definition %q outlived an abandoned request, parented to a group that is gone", name)
}

// The RG-deleted rollback is the long one, waiting out two convergence
// budgets, so a CSI caller that gives up inside it is the ordinary case. On the
// request's context the compensation died with the caller, left the definition
// parented to a group that is gone, and the replay gate refused every retry.
func TestRDCloneRGDeletedRollbackOutlivesTheRequest(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-rg-abandon", "grp-rg-abandon-gone", false)

	gated := newRollbackGateStore(backend, "dst-rg-abandon")

	base, stop := startServerWithStore(t, gated)
	defer stop()

	abandonAtTheRollback(t, base+"/v1/resource-definitions/src-rg-abandon/clone",
		map[string]any{"name": "dst-rg-abandon", "use_zfs_clone": true}, gated)

	waitForDefinitionGone(t, backend, "dst-rg-abandon")
}

// The restore door has the same rollback and had the same context.
func TestSnapshotRestoreRGDeletedRollbackOutlivesTheRequest(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedRestoreSource(t, backend, "restore-abandon-src", "grp-restore-abandon-gone", "snap-abandon")

	gated := newRollbackGateStore(backend, "restore-abandon-dst")

	base, stop := startServerWithStore(t, gated)
	defer stop()

	abandonAtTheRollback(t, base+"/v1/resource-definitions/restore-abandon-src/snapshot-restore-resource",
		map[string]string{"to_resource": "restore-abandon-dst", "from_snapshot": "snap-abandon"}, gated)

	waitForDefinitionGone(t, backend, "restore-abandon-dst")
}

// seedGroupedRestoreSource seeds a source parented to rgName, which is not
// created, and a one-volume snapshot of it.
func seedGroupedRestoreSource(t *testing.T, st store.Store, src, rgName, snap string) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              src,
		ResourceGroupName: rgName,
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         snap,
		ResourceName: src,
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}
}

// seedAdoptedTarget is a definition an earlier attempt left, with a replica,
// parented to a group that is gone.
func seedAdoptedTarget(t *testing.T, st store.Store, name, rgName string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:              name,
		ResourceGroupName: rgName,
	}); err != nil {
		t.Fatalf("seed the adopted target: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{Name: name, NodeName: "node-a"}); err != nil {
		t.Fatalf("seed its replica: %v", err)
	}
}

// Once a door tolerates a leftover, "materialise succeeded" can mean "adopted a
// definition another attempt created", and the RG-deleted rollback cascades
// every replica under the name, not only this request's. Over a definition
// this request did not create the door refuses and leaves it in place; over
// one it did, it rolls back. Both doors, both ways.
func TestPostWriteGroupCheckLeavesAnAdoptedDefinitionInPlace(t *testing.T) {
	t.Parallel()

	doors := map[string]func(s *Server, rec *httptest.ResponseRecorder, made materialisedRD) bool{
		"clone": func(s *Server, rec *httptest.ResponseRecorder, made materialisedRD) bool {
			_, ok := s.cloneParentRGSurvived(t.Context(), rec,
				&apiv1.ResourceDefinition{Name: "adopt-src", ResourceGroupName: made.StampedRG}, made.Name, made)

			return ok
		},
		"restore": func(s *Server, rec *httptest.ResponseRecorder, made materialisedRD) bool {
			_, ok := s.restoreParentRGSurvived(t.Context(), rec, made)

			return ok
		},
	}

	for door, check := range doors {
		for _, created := range []bool{false, true} {
			t.Run(door+"/created="+map[bool]string{false: "false", true: "true"}[created], func(t *testing.T) {
				t.Parallel()

				st := store.NewInMemory()
				seedAdoptedTarget(t, st, "adopt-dst", "grp-adopt-gone")

				rec := httptest.NewRecorder()
				made := materialisedRD{
					Name: "adopt-dst", StampedRG: "grp-adopt-gone", Placed: []string{"node-a"}, Created: created,
				}

				if check(&Server{Store: st}, rec, made) {
					t.Fatal("the check passed over a parent group that does not exist")
				}

				_, getErr := st.ResourceDefinitions().Get(t.Context(), "adopt-dst")
				replicas, listErr := st.Resources().ListByDefinition(t.Context(), "adopt-dst")

				if listErr != nil && !errors.Is(listErr, store.ErrNotFound) {
					t.Fatalf("list the target's replicas: %v", listErr)
				}

				if created {
					if getErr == nil {
						t.Error("this request's own definition was not rolled back")
					}

					return
				}

				if getErr != nil || len(replicas) != 1 {
					t.Errorf("an adopted definition was reaped: get err=%v, %d replica(s) left", getErr, len(replicas))
				}

				if rec.Code != http.StatusConflict {
					t.Errorf("status = %d, want 409", rec.Code)
				}

				if !strings.Contains(rec.Body.String(), "not created by this request") {
					t.Errorf("body %s does not say the definition was left because this request did not create it",
						rec.Body.String())
				}
			})
		}
	}
}

var errRestoreHydrateBlip = errors.New("probe: one volume create failed")

// failOnceTargetVolumeCreates fails the first hydration of one definition and
// lets every later one through: a transient failure.
type failOnceTargetVolumeCreates struct {
	store.VolumeDefinitionStore

	target string
	failed *atomic.Bool
}

func (f failOnceTargetVolumeCreates) Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if rdName == f.target && f.failed.CompareAndSwap(false, true) {
		return errRestoreHydrateBlip
	}

	return errors.Wrap(f.VolumeDefinitionStore.Create(ctx, rdName, vd), "create through the fail-once double")
}

type failOnceTargetVolumeCreateStore struct {
	store.Store

	target string
	failed *atomic.Bool
}

func (f failOnceTargetVolumeCreateStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return failOnceTargetVolumeCreates{VolumeDefinitionStore: f.Store.VolumeDefinitions(), target: f.target, failed: f.failed}
}

// The restore endpoint has no idempotent-replay gate, and its marker-bearing
// definition was left behind after a hydrate failure, so one transient failure
// turned every retry under the deterministic CSI target name into a 409.
func TestSnapshotRestoreRetrySucceedsAfterAHydrateFailure(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp-hydrate-restore"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	seedGroupedRestoreSource(t, backend, "hydrate-restore-src", "grp-hydrate-restore", "snap-hydrate")

	base, stop := startServerWithStore(t, failOnceTargetVolumeCreateStore{
		Store: backend, target: "hydrate-restore-dst", failed: &atomic.Bool{},
	})
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "hydrate-restore-dst",
		"from_snapshot": "snap-hydrate",
	})

	url := base + "/v1/resource-definitions/hydrate-restore-src/snapshot-restore-resource"

	first := httpPost(t, url, body)
	defer func() { _ = first.Body.Close() }()

	if first.StatusCode != http.StatusInternalServerError {
		t.Fatalf("first restore = %d, want 500", first.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(first.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the restore's envelope: %v (%d entries)", err, len(rcs))
	}

	if !strings.Contains(rcs[0].Message, "rolled back") {
		t.Errorf("message = %q, want it to say the partial restore was rolled back", rcs[0].Message)
	}

	retry := httpPost(t, url, body)
	_ = retry.Body.Close()

	if retry.StatusCode != http.StatusCreated {
		t.Errorf("retry after one transient hydrate failure = %d, want 201", retry.StatusCode)
	}
}
