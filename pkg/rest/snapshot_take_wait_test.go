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

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// takeDelay is how long the stand-in controller takes a snapshot for.
const takeDelay = 300 * time.Millisecond

// postDeadline bounds every create a test sends; each wait under test is
// either settled well before it or capped below it.
const postDeadline = 15 * time.Second

// startTakeWaitServer serves st with the given take wait and one RD with a
// diskful replica, the shape both create paths accept.
func startTakeWaitServer(t *testing.T, st store.Store, wait time.Duration) string {
	t.Helper()

	ctx := t.Context()

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"})
	if err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	err = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: "n1"})
	if err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerCustom(t, &Server{
		Addr:             pickFreeAddr(t),
		Store:            st,
		Client:           newFakeRESTClient(t),
		Namespace:        testRESTNamespace,
		SnapshotTakeWait: wait,
	})
	t.Cleanup(stop)

	return base
}

// settleAfterTake plays the controller: once the snapshot shows up, it
// waits takeDelay and then records the given flag, or deletes the snapshot
// when flag is empty.
func settleAfterTake(t *testing.T, st store.Store, flag string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	done := make(chan struct{})

	t.Cleanup(func() {
		cancel()
		<-done
	})

	go func() {
		defer close(done)

		for ctx.Err() == nil {
			snap, err := st.Snapshots().Get(ctx, "pvc-1", "snap-1")
			if err != nil {
				time.Sleep(10 * time.Millisecond)

				continue
			}

			time.Sleep(takeDelay)

			if flag == "" {
				_ = st.Snapshots().Delete(ctx, "pvc-1", "snap-1")

				return
			}

			snap.Flags = append(snap.Flags, flag)
			_ = st.Snapshots().Update(ctx, &snap)

			return
		}
	}()
}

// viewFlags reads snap-1's flags from the cluster-wide snapshot view, the
// read linstor-csi makes right after its create and then caches.
func viewFlags(t *testing.T, base string) []string {
	t.Helper()

	resp := httpGet(t, base+"/v1/view/snapshots")
	defer func() { _ = resp.Body.Close() }()

	var view []apiv1.Snapshot

	err := json.NewDecoder(resp.Body).Decode(&view)
	if err != nil {
		t.Fatalf("decode view: %v", err)
	}

	for i := range view {
		if view[i].Name == "snap-1" {
			return view[i].Flags
		}
	}

	t.Fatalf("snap-1 missing from the view: %+v", view)

	return nil
}

func postCreate(t *testing.T, base, path string) time.Duration {
	t.Helper()

	body := []byte(`{"name":"snap-1"}`)
	if path == multiCreatePath {
		body = []byte(`{"snapshots":[{"resource_name":"pvc-1","name":"snap-1"}]}`)
	}

	// A create that never answers fails here, not at the test binary's
	// timeout.
	ctx, cancel := context.WithTimeout(t.Context(), postDeadline)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s after %v: %v", path, time.Since(start), err)
	}
	defer func() { _ = resp.Body.Close() }()

	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST %s: got %d, want 201", path, resp.StatusCode)
	}

	var rcs []apiv1.APICallRc

	err = json.NewDecoder(resp.Body).Decode(&rcs)
	if err != nil {
		t.Fatalf("decode rcs: %v", err)
	}

	if len(rcs) != 1 || rcs[0].RetCode&apiCallRcError != 0 {
		t.Fatalf("POST %s: want one success rc, got %+v", path, rcs)
	}

	return elapsed
}

const (
	multiCreatePath = "/v1/actions/snapshot/multi"
	rdCreatePath    = "/v1/resource-definitions/pvc-1/snapshots"
)

// TestSnapshotCreateAnswersAfterTheTake pins LINSTOR's answer point: the
// create returns once the take recorded SUCCESSFUL, so the view read right
// after it, which linstor-csi caches for 30s, already says ready.
func TestSnapshotCreateAnswersAfterTheTake(t *testing.T) {
	t.Parallel()

	for _, path := range []string{multiCreatePath, rdCreatePath} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			base := startTakeWaitServer(t, st, 10*time.Second)
			settleAfterTake(t, st, apiv1.SnapshotFlagSuccessful)

			elapsed := postCreate(t, base, path)
			if elapsed > 5*time.Second {
				t.Errorf("create answered after %v, want once SUCCESSFUL was recorded (~%v)", elapsed, takeDelay)
			}

			if flags := viewFlags(t, base); !slices.Contains(flags, apiv1.SnapshotFlagSuccessful) {
				t.Errorf("view right after the create: flags %v, want SUCCESSFUL", flags)
			}
		})
	}
}

// TestSnapshotCreateAnswersOnEveryEndOfTheTake: a failed or deleted take
// ends the wait too, long before its bound.
func TestSnapshotCreateAnswersOnEveryEndOfTheTake(t *testing.T) {
	t.Parallel()

	ends := map[string]string{
		"failed":            snapshotFlagFailed,
		"failed-deployment": apiv1.SnapshotFlagFailedDeployment,
		"failed-disconnect": apiv1.SnapshotFlagFailedDisconnect,
		"deleted":           "",
	}

	for name, flag := range ends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			base := startTakeWaitServer(t, st, 20*time.Second)
			settleAfterTake(t, st, flag)

			elapsed := postCreate(t, base, multiCreatePath)
			if elapsed < takeDelay || elapsed > 10*time.Second {
				t.Errorf("create answered after %v, want once the take ended (~%v)", elapsed, takeDelay)
			}
		})
	}
}

// TestSnapshotCreateWaitIsBounded: a take that never settles still gets
// its answer once the wait runs out.
func TestSnapshotCreateWaitIsBounded(t *testing.T) {
	t.Parallel()

	const wait = 400 * time.Millisecond

	st := store.NewInMemory()
	base := startTakeWaitServer(t, st, wait)

	elapsed := postCreate(t, base, multiCreatePath)
	if elapsed < wait || elapsed > 10*time.Second {
		t.Errorf("create answered after %v, want about the %v bound", elapsed, wait)
	}
}

// trailingCacheStore hides a snapshot from Get for a while after its create, the
// way the informer cache trails a write.
type trailingCacheStore struct {
	store.Store

	snaps *trailingCacheSnapshots
}

func (l trailingCacheStore) Snapshots() store.SnapshotStore { return l.snaps }

type trailingCacheSnapshots struct {
	store.SnapshotStore

	created time.Time
}

func (l *trailingCacheSnapshots) Create(ctx context.Context, snap *apiv1.Snapshot) error {
	l.created = time.Now()

	return l.SnapshotStore.Create(ctx, snap) //nolint:wrapcheck // pass-through test double
}

func (l *trailingCacheSnapshots) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	if !l.created.IsZero() && time.Since(l.created) < takeDelay {
		return apiv1.Snapshot{}, store.ErrNotFound
	}

	return l.SnapshotStore.Get(ctx, rdName, snapName) //nolint:wrapcheck // pass-through test double
}

// TestSnapshotCreateWaitsOutACacheThatTrailsTheCreate: a snapshot the read
// cannot find yet is still being taken, not deleted.
func TestSnapshotCreateWaitsOutACacheThatTrailsTheCreate(t *testing.T) {
	t.Parallel()

	inner := store.NewInMemory()
	st := trailingCacheStore{Store: inner, snaps: &trailingCacheSnapshots{SnapshotStore: inner.Snapshots()}}
	base := startTakeWaitServer(t, st, 10*time.Second)
	settleAfterTake(t, inner, apiv1.SnapshotFlagSuccessful)

	postCreate(t, base, multiCreatePath)

	if flags := viewFlags(t, base); !slices.Contains(flags, apiv1.SnapshotFlagSuccessful) {
		t.Errorf("view right after the create: flags %v, want SUCCESSFUL", flags)
	}
}

// TestSnapshotCreateMultiAnswersARefusalAtOnce: a batch with a refused
// entry never assembles its group, so no take starts before the
// controller's suspend deadline. The refusal is answered at once, not
// after the wait, which a CSI sidecar gives up on first.
func TestSnapshotCreateMultiAnswersARefusalAtOnce(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	base := startTakeWaitServer(t, st, 10*time.Second)

	body := []byte(`{"snapshots":[
		{"resource_name":"pvc-1","name":"snap-1"},
		{"resource_name":"pvc-missing","name":"snap-1"}
	]}`)

	ctx, cancel := context.WithTimeout(t.Context(), postDeadline)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+multiCreatePath, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST after %v: %v", time.Since(start), err)
	}
	defer func() { _ = resp.Body.Close() }()

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("refused batch answered after %v, want at once", elapsed)
	}

	var rcs []apiv1.APICallRc

	err = json.NewDecoder(resp.Body).Decode(&rcs)
	if err != nil {
		t.Fatalf("decode rcs: %v", err)
	}

	if !slices.ContainsFunc(rcs, func(rc apiv1.APICallRc) bool { return rc.RetCode&apiCallRcError != 0 }) {
		t.Errorf("want the refusal in the answer, got %+v", rcs)
	}
}
