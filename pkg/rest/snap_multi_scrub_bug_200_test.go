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

// Bug 200 (P2) — `createOneFromMulti` (in snapshot_multi.go) builds
// `APICallRc{Message: entry.ResourceName + "/" + entry.Name + ": " +
// err.Error()}` envelopes inline and returns them through
// `writeJSON`, NOT `writeError`. Bug 199's fix wraps `writeError` with
// `scrubImplDetails` — but the multi-create batch path emits
// per-entry envelopes via `writeJSON`, so the scrub guard is
// bypassed. A `Snapshots().Create` failure carrying "etcdserver:",
// "apimachinery", "k8s.io" or "controllerconfigs.blockstor.io"
// leaks the backend identity through `POST /v1/actions/snapshot/multi`.
//
// v12 specifically flagged `snapshot_multi.go:110, 118` as suspect.
// Bug 199 didn't reach them.
//
// The fix wraps every `err.Error()` slot in `createOneFromMulti` with
// `scrubImplDetails` (or, equivalently, extracts an
// `apiCallRcFromErr` helper that always scrubs). Caller-supplied
// literal strings ("snapshot X already exists", "not found") pass
// through unchanged — `scrubImplDetails` is a no-op on operator-
// friendly text.
//
// Tests pinned here:
//
//   - TestBug200SnapshotMultiScrubsEtcdLeak: a `Snapshots().Create`
//     returning "etcdserver: request is too large" → body envelope's
//     Message contains `<backend>`, NOT "etcdserver" / "etcd".
//   - TestBug200SnapshotMultiScrubsAPIMachineryLeak: same shape for
//     "controllerconfigs.blockstor.io.blockstor.io" / "apimachinery"
//     / "k8s.io".
//   - TestBug200SnapshotMultiPreservesLiteralMessages: an operator-
//     friendly literal ("snapshot X already exists") passes through
//     unchanged. The scrub MUST be a no-op on plain strings.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Static error fixtures synthesise the exact wire-shape a K8s-backed
// store surfaces verbatim when etcd / apimachinery faults bubble up.
// err113 prefers static errors over inline errors.New, and the AST
// guards in writeerror_scrub_bug_199_test.go pin the exact substrings
// the scrub regex must catch — so the fixtures live as package-level
// vars rather than being constructed per-test.
var (
	errBug200EtcdTooLarge = errors.New("etcdserver: request is too large")
	//nolint:staticcheck // ST1005: mirrors apimachinery's literal
	// "Operation cannot be fulfilled on …" capitalised message so the
	// scrub test sees the exact byte sequence emitted in production.
	errBug200APIMachineryConflict = errors.New(
		`Operation cannot be fulfilled on controllerconfigs.blockstor.io.blockstor.io ` +
			`"default": the object has been modified; please apply your changes to the latest ` +
			`version and try again (via apimachinery/k8s.io/api/...)`)
	errBug200LiteralAlreadyExists   = errors.New("snapshot snap-x already exists")
	errBug200LiteralRDNotFound      = errors.New("resource definition not found")
	errBug200LiteralNameReservation = errors.New("snapshot name conflicts with existing reservation")
)

// errInjectingSnapshots wraps an inner SnapshotStore and replaces the
// Create error with a fixed synthetic err. Mirrors the shape of a
// K8s-backed store surfacing an etcd or apimachinery error verbatim.
//
// Only Create is overridden — every other surface passes through, so
// the multi-create handler's hydrate path still sees a valid RD on
// the inner store.
type errInjectingSnapshots struct {
	inner     store.SnapshotStore
	createErr error
}

func (s *errInjectingSnapshots) List(ctx context.Context) ([]apiv1.Snapshot, error) {
	return s.inner.List(ctx) //nolint:wrapcheck // test helper
}

func (s *errInjectingSnapshots) ListByDefinition(
	ctx context.Context, rdName string,
) ([]apiv1.Snapshot, error) {
	return s.inner.ListByDefinition(ctx, rdName) //nolint:wrapcheck // test helper
}

func (s *errInjectingSnapshots) Get(
	ctx context.Context, rdName, snapName string,
) (apiv1.Snapshot, error) {
	return s.inner.Get(ctx, rdName, snapName) //nolint:wrapcheck // test helper
}

func (s *errInjectingSnapshots) Create(_ context.Context, _ *apiv1.Snapshot) error {
	return s.createErr
}

func (s *errInjectingSnapshots) Update(ctx context.Context, snap *apiv1.Snapshot) error {
	return s.inner.Update(ctx, snap) //nolint:wrapcheck // test helper
}

func (s *errInjectingSnapshots) Delete(
	ctx context.Context, rdName, snapName string,
) error {
	return s.inner.Delete(ctx, rdName, snapName) //nolint:wrapcheck // test helper
}

// errInjectingSnapshotStore wraps an inmemory store and swaps in the
// errInjectingSnapshots wrapper for the Snapshots() surface.
type errInjectingSnapshotStore struct {
	inner     *store.InMemory
	snapshots *errInjectingSnapshots
}

func newErrInjectingSnapshotStore(createErr error) *errInjectingSnapshotStore {
	inner := store.NewInMemory()

	return &errInjectingSnapshotStore{
		inner: inner,
		snapshots: &errInjectingSnapshots{
			inner:     inner.Snapshots(),
			createErr: createErr,
		},
	}
}

func (s *errInjectingSnapshotStore) Nodes() store.NodeStore { return s.inner.Nodes() }
func (s *errInjectingSnapshotStore) StoragePools() store.StoragePoolStore {
	return s.inner.StoragePools()
}

func (s *errInjectingSnapshotStore) ResourceGroups() store.ResourceGroupStore {
	return s.inner.ResourceGroups()
}

func (s *errInjectingSnapshotStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return s.inner.ResourceDefinitions()
}

func (s *errInjectingSnapshotStore) Resources() store.ResourceStore {
	return s.inner.Resources()
}

func (s *errInjectingSnapshotStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return s.inner.VolumeDefinitions()
}

func (s *errInjectingSnapshotStore) Snapshots() store.SnapshotStore { return s.snapshots }

func (s *errInjectingSnapshotStore) PhysicalDevices() store.PhysicalDeviceStore {
	return s.inner.PhysicalDevices()
}

func (s *errInjectingSnapshotStore) ControllerProps() store.ControllerPropsStore {
	return s.inner.ControllerProps()
}

// seedRDForMultiSnapshot stamps an RD onto the inner inmemory store so
// the multi-create handler's hydrate path succeeds and the test reaches
// the Snapshots().Create call where the synthesised err fires.
func seedRDForMultiSnapshot(t *testing.T, st *errInjectingSnapshotStore, rd string) {
	t.Helper()

	ctx := t.Context()

	err := st.inner.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd})
	if err != nil {
		t.Fatalf("seed RD %q: %v", rd, err)
	}

	err = st.inner.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: "n1"})
	if err != nil {
		t.Fatalf("seed resource %q: %v", rd, err)
	}
}

// postMultiCreateOne builds a single-entry multi-snapshot create body
// for `(rd, snap)` and POSTs it to the multi endpoint.
func postMultiCreateOne(t *testing.T, base, rd, snap string) *http.Response {
	t.Helper()

	body := []byte(`{"snapshots":[{"resource_name":"` + rd + `","name":"` + snap + `"}]}`)

	return httpPost(t, base+"/v1/actions/snapshot/multi", body)
}

// TestBug200SnapshotMultiScrubsEtcdLeak pins the etcd flavour. A
// Snapshots().Create returning "etcdserver: request is too large"
// must reach the wire scrubbed — the multi-create envelope sits
// behind writeJSON, so Bug 199's writeError-level wrap does NOT
// cover it. Without the fix, the body literally contains
// "etcdserver:" / "etcd".
func TestBug200SnapshotMultiScrubsEtcdLeak(t *testing.T) {
	t.Parallel()

	st := newErrInjectingSnapshotStore(errBug200EtcdTooLarge)
	seedRDForMultiSnapshot(t, st, "pvc-a")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postMultiCreateOne(t, base, "pvc-a", "snap-a")
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	low := strings.ToLower(string(body))
	for _, leak := range []string{"etcdserver", "etcd"} {
		if strings.Contains(low, leak) {
			t.Errorf("body leaks impl detail %q: %s", leak, body)
		}
	}

	// envelope MUST still be the LINSTOR `[]APICallRc` shape so the
	// python CLI / golinstor decode the failure correctly.
	var rcs []apiv1.APICallRc
	if jErr := json.Unmarshal(body, &rcs); jErr != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", jErr, body)
	}

	if len(rcs) != 1 {
		t.Fatalf("ApiCallRc count: got %d, want 1\nbody: %s", len(rcs), body)
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("rc[0] not error-marked: %#v", rcs[0])
	}

	// `<backend>` is the canonical opaque token scrubImplDetails uses
	// — its presence tells us the scrub ran rather than the message
	// being stripped wholesale.
	if !strings.Contains(rcs[0].Message, "<backend>") {
		t.Errorf("scrubbed message lacks <backend> token: %q", rcs[0].Message)
	}

	// The per-entry "rd/snap: " prefix is operator context and MUST
	// survive — only the backend identifiers get rewritten.
	if !strings.Contains(rcs[0].Message, "pvc-a/snap-a") {
		t.Errorf("scrubbed message lost per-entry prefix: %q", rcs[0].Message)
	}
}

// TestBug200SnapshotMultiScrubsAPIMachineryLeak pins the apimachinery
// flavour. A Snapshots().Create returning a NewConflict-shaped string
// carrying "controllerconfigs.blockstor.io.blockstor.io" /
// "apimachinery" / "k8s.io" must reach the wire scrubbed.
func TestBug200SnapshotMultiScrubsAPIMachineryLeak(t *testing.T) {
	t.Parallel()

	st := newErrInjectingSnapshotStore(errBug200APIMachineryConflict)
	seedRDForMultiSnapshot(t, st, "pvc-b")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postMultiCreateOne(t, base, "pvc-b", "snap-b")
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	low := strings.ToLower(string(body))
	for _, leak := range []string{
		"controllerconfigs.blockstor.io",
		"apimachinery",
		"k8s.io",
		"etcd",
	} {
		if strings.Contains(low, strings.ToLower(leak)) {
			t.Errorf("body leaks impl detail %q: %s", leak, body)
		}
	}

	var rcs []apiv1.APICallRc
	if jErr := json.Unmarshal(body, &rcs); jErr != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", jErr, body)
	}

	if len(rcs) != 1 {
		t.Fatalf("ApiCallRc count: got %d, want 1\nbody: %s", len(rcs), body)
	}

	if !strings.Contains(rcs[0].Message, "pvc-b/snap-b") {
		t.Errorf("scrubbed message lost per-entry prefix: %q", rcs[0].Message)
	}
}

// TestBug200SnapshotMultiPreservesLiteralMessages pins that the scrub
// wrap is a no-op on operator-friendly literal strings. A caller-
// supplied "snapshot X already exists"-shaped error must reach the
// wire byte-for-byte (modulo the per-entry "rd/snap: " prefix the
// handler adds). If scrubImplDetails ever grows a rule that mangles a
// plain string here, this test fires.
func TestBug200SnapshotMultiPreservesLiteralMessages(t *testing.T) {
	t.Parallel()

	for _, fixture := range []error{
		errBug200LiteralAlreadyExists,
		errBug200LiteralRDNotFound,
		errBug200LiteralNameReservation,
	} {
		t.Run(fixture.Error(), func(t *testing.T) {
			t.Parallel()

			st := newErrInjectingSnapshotStore(fixture)
			seedRDForMultiSnapshot(t, st, "pvc-c")

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := postMultiCreateOne(t, base, "pvc-c", "snap-c")
			defer func() { _ = resp.Body.Close() }()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			var rcs []apiv1.APICallRc
			if jErr := json.Unmarshal(body, &rcs); jErr != nil {
				t.Fatalf("decode envelope: %v\nbody: %s", jErr, body)
			}

			if len(rcs) != 1 {
				t.Fatalf("ApiCallRc count: got %d, want 1\nbody: %s", len(rcs), body)
			}

			want := "pvc-c/snap-c: " + fixture.Error()
			if rcs[0].Message != want {
				t.Errorf("literal mangled:\n got: %q\nwant: %q", rcs[0].Message, want)
			}
		})
	}
}
