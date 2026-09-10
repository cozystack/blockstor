// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr/funcr"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// errSnapshotParentRead stands in for whatever breaks the read that finds the
// orphan: a timeout, a throttled API server, a selector the cluster cannot
// serve.
var errSnapshotParentRead = errors.New("list snapshots by definition failed")

type failingSnapshotList struct {
	store.SnapshotStore
}

func (f failingSnapshotList) ListByDefinition(context.Context, string) ([]apiv1.Snapshot, error) {
	return nil, errSnapshotParentRead
}

type failingSnapshotListStore struct {
	store.Store
}

func (f failingSnapshotListStore) Snapshots() store.SnapshotStore {
	return failingSnapshotList{f.Store.Snapshots()}
}

// sweepCaptureContext hands the sweep a logger that buffers every entry, so a
// test can assert on what reached the operator rather than on what the code
// meant to say. The package already declares a type named logr, so the logger
// is built inline rather than returned.
func sweepCaptureContext(t *testing.T, buf *bytes.Buffer) context.Context {
	t.Helper()

	return ctrllog.IntoContext(t.Context(), funcr.New(func(prefix, args string) {
		buf.WriteString(prefix)
		buf.WriteString(" ")
		buf.WriteString(args)
		buf.WriteString("\n")
	}, funcr.Options{Verbosity: 1}))
}

// The sweep is best-effort by design, but the read it skips on is the one that
// finds the orphan. The operator has already been told the delete succeeded,
// so a silent return leaves the Bug 180 row alive with nothing anywhere saying
// the mop-up did not run.
func TestOrphanSweepSaysSoWhenItsReadFails(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	ctx := sweepCaptureContext(t, &buf)
	srv := &Server{Store: failingSnapshotListStore{store.NewInMemory()}}

	srv.sweepOrphanSnapshotsAfterRDDelete(ctx, "pvc-swept")

	if !strings.Contains(buf.String(), "orphan-snapshot sweep skipped") {
		t.Errorf("a failed parent read left no trace; log = %q", buf.String())
	}

	if !strings.Contains(buf.String(), "pvc-swept") {
		t.Errorf("the log does not name the definition; log = %q", buf.String())
	}
}

// The control: the ordinary path stays quiet. Without it the assertion above
// is satisfied by a function that logs on every call, which would bury the
// signal in the noise of every successful delete.
func TestOrphanSweepStaysQuietWhenItsReadSucceeds(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	ctx := sweepCaptureContext(t, &buf)
	srv := &Server{Store: store.NewInMemory()}

	srv.sweepOrphanSnapshotsAfterRDDelete(ctx, "pvc-clean")

	if buf.Len() != 0 {
		t.Errorf("a clean sweep logged %q", buf.String())
	}
}
