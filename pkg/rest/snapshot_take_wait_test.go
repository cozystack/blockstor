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
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// takeDelay is how long the stand-in controller takes a snapshot for.
const takeDelay = 300 * time.Millisecond

// testAbortReason is the abort the stand-in controller records with a
// failure flag.
const testAbortReason = "replica on n1 is not UpToDate"

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
			if flag != apiv1.SnapshotFlagSuccessful {
				snap.AbortReason = testAbortReason
			}

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

// createAnswer is what a create answered and how long it took.
type createAnswer struct {
	elapsed time.Duration
	status  int
	rcs     []apiv1.APICallRc
}

func postCreateBody(t *testing.T, base, path string, body []byte) createAnswer {
	t.Helper()

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

	answer := createAnswer{elapsed: time.Since(start), status: resp.StatusCode}

	err = json.NewDecoder(resp.Body).Decode(&answer.rcs)
	if err != nil {
		t.Fatalf("decode rcs: %v", err)
	}

	return answer
}

// postSnap1 creates snap-1 on pvc-1 through path.
func postSnap1(t *testing.T, base, path string) createAnswer {
	t.Helper()

	body := []byte(`{"name":"snap-1"}`)
	if path == multiCreatePath {
		body = []byte(`{"snapshots":[{"resource_name":"pvc-1","name":"snap-1"}]}`)
	}

	return postCreateBody(t, base, path, body)
}

// postCreate creates snap-1 and wants the success answer.
func postCreate(t *testing.T, base, path string) time.Duration {
	t.Helper()

	answer := postSnap1(t, base, path)

	if answer.status != http.StatusCreated {
		t.Fatalf("POST %s: got %d, want 201", path, answer.status)
	}

	if len(answer.rcs) != 1 || answer.rcs[0].RetCode&apiCallRcError != 0 {
		t.Fatalf("POST %s: want one success rc, got %+v", path, answer.rcs)
	}

	return answer.elapsed
}

func hasErrorRc(rcs []apiv1.APICallRc, text string) bool {
	return slices.ContainsFunc(rcs, func(rc apiv1.APICallRc) bool {
		return rc.RetCode&apiCallRcError != 0 && strings.Contains(rc.Message, text)
	})
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
// ends the wait too, long before its bound, and is answered the way
// LINSTOR answers an aborted create flux: 500 with the abort's error
// after the responses already collected.
func TestSnapshotCreateAnswersOnEveryEndOfTheTake(t *testing.T) {
	t.Parallel()

	ends := map[string]struct {
		flag string
		text string
	}{
		"failed":            {snapshotFlagFailed, testAbortReason},
		"failed-deployment": {apiv1.SnapshotFlagFailedDeployment, testAbortReason},
		"failed-disconnect": {apiv1.SnapshotFlagFailedDisconnect, testAbortReason},
		"deleted":           {"", "deleted before its take ended"},
		"deleting":          {apiv1.SnapshotFlagDelete, "deleted before its take ended"},
	}

	for name, end := range ends {
		for _, path := range []string{multiCreatePath, rdCreatePath} {
			t.Run(name+path, func(t *testing.T) {
				t.Parallel()

				st := store.NewInMemory()
				base := startTakeWaitServer(t, st, 20*time.Second)
				settleAfterTake(t, st, end.flag)

				answer := postSnap1(t, base, path)
				if answer.elapsed < takeDelay || answer.elapsed > 10*time.Second {
					t.Errorf("create answered after %v, want once the take ended (~%v)", answer.elapsed, takeDelay)
				}

				if answer.status != http.StatusInternalServerError {
					t.Errorf("status %d, want 500 for a take that did not succeed", answer.status)
				}

				if !hasErrorRc(answer.rcs, end.text) {
					t.Errorf("want an error rc saying %q, got %+v", end.text, answer.rcs)
				}

				if !slices.ContainsFunc(answer.rcs, func(rc apiv1.APICallRc) bool { return rc.RetCode&maskInfo != 0 }) {
					t.Errorf("want the create's own rc kept before the failure, got %+v", answer.rcs)
				}

				if end.flag != "" {
					resp := httpGet(t, base+"/v1/view/snapshots")
					defer func() { _ = resp.Body.Close() }()

					raw, err := io.ReadAll(resp.Body)
					if err != nil {
						t.Fatalf("read view: %v", err)
					}

					if bytes.Contains(raw, []byte(testAbortReason)) {
						t.Errorf("the abort reason reached the snapshot view, which LINSTOR's DTO has no field for: %s", raw)
					}
				}
			})
		}
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

	answer := postCreateBody(t, base, multiCreatePath, []byte(`{"snapshots":[
		{"resource_name":"pvc-1","name":"snap-1"},
		{"resource_name":"pvc-missing","name":"snap-1"}
	]}`))

	if answer.elapsed > 2*time.Second {
		t.Errorf("refused batch answered after %v, want at once", answer.elapsed)
	}

	if !hasErrorRc(answer.rcs, "pvc-missing") {
		t.Errorf("want the refusal in the answer, got %+v", answer.rcs)
	}
}
