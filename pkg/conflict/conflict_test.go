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

package conflict_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/cozystack/blockstor/pkg/conflict"
)

var (
	errBroken   = errors.New("apiserver refused the write")
	errModified = errors.New("the object has been modified")
)

// logLevels records the verbosity of every Info line and counts Error
// lines.
type logLevels struct {
	info   []int
	errors int
}

func capture(levels *logLevels) logr.Logger {
	return logr.New(&levelSink{levels: levels})
}

type levelSink struct{ levels *logLevels }

func (*levelSink) Init(logr.RuntimeInfo) {}
func (*levelSink) Enabled(int) bool      { return true }
func (s *levelSink) Info(level int, _ string, _ ...any) {
	s.levels.info = append(s.levels.info, level)
}
func (s *levelSink) Error(error, string, ...any)    { s.levels.errors++ }
func (s *levelSink) WithValues(...any) logr.LogSink { return s }
func (s *levelSink) WithName(string) logr.LogSink   { return s }

func run(t *testing.T, result reconcile.Result, err error) (logLevels, reconcile.Result, error) {
	t.Helper()

	var levels logLevels

	ctx := logf.IntoContext(context.Background(), capture(&levels))

	var retrier conflict.Retrier

	got, gotErr := retrier.Reconcile(ctx, reconcile.Request{}, func(context.Context, reconcile.Request) (reconcile.Result, error) {
		return result, err
	})

	return levels, got, gotErr //nolint:wrapcheck // the result under test
}

// A pass that lost an optimistic-concurrency race comes back through the
// rate limiter, logged below the default verbosity, without an error for
// controller-runtime to log as "Reconciler error" at ERROR. The conflict
// arrives wrapped, the way the reconcilers return it.
func TestConflictIsRetriedQuietly(t *testing.T) {
	t.Parallel()

	conflictErr := apierrors.NewConflict(schema.GroupResource{Group: "blockstor.org", Resource: "resources"}, "pvc-c.n1", errModified)

	levels, got, err := run(t, reconcile.Result{}, errors.Wrap(conflictErr, "add finalizer"))
	if err != nil {
		t.Fatalf("error %v, want none", err)
	}

	if !got.Requeue { //nolint:staticcheck // the rate-limited retry an error would get
		t.Errorf("result %+v, want a requeue", got)
	}

	if levels.errors != 0 || len(levels.info) != 1 || levels.info[0] < 1 {
		t.Errorf("logged %d errors and info at levels %v, want one line at V(1)", levels.errors, levels.info)
	}
}

// Any other failure is still returned, so it is still logged at ERROR.
func TestOtherFailuresAreReturned(t *testing.T) {
	t.Parallel()

	for name, err := range map[string]error{
		"plain":     errBroken,
		"not found": apierrors.NewNotFound(schema.GroupResource{Group: "blockstor.org", Resource: "resources"}, "pvc-c.n1"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, _, got := run(t, reconcile.Result{}, err)
			if !errors.Is(got, err) {
				t.Fatalf("error %v, want %v", got, err)
			}
		})
	}
}

// A pass that succeeded keeps its own result.
func TestSuccessKeepsItsResult(t *testing.T) {
	t.Parallel()

	want := reconcile.Result{RequeueAfter: 5 * time.Second}

	levels, got, err := run(t, want, nil)
	if err != nil || got != want {
		t.Fatalf("got %+v, %v, want %+v", got, err, want)
	}

	if len(levels.info) != 0 || levels.errors != 0 {
		t.Errorf("logged %v / %d errors for a clean pass", levels.info, levels.errors)
	}
}

// passOn returns a pass that fails with err, and the request for the
// object name.
func passOn(name string, err error) (reconcile.Request, reconcile.Func) {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name}},
		func(context.Context, reconcile.Request) (reconcile.Result, error) { return reconcile.Result{}, err }
}

func newConflict(name string) error {
	return apierrors.NewConflict(schema.GroupResource{Group: "blockstor.org", Resource: "resources"}, name, errModified)
}

// Two writers that keep overwriting the same object would otherwise
// retry silently forever: the error is gone from the log and from the
// reconcile-error metric. A run of LoudAfter conflicts on one object is
// logged at the default level, and earlier ones are not.
func TestLongConflictRunIsLogged(t *testing.T) {
	t.Parallel()

	var (
		levels  logLevels
		retrier conflict.Retrier
	)

	ctx := logf.IntoContext(context.Background(), capture(&levels))
	req, pass := passOn("pvc-c.n1", newConflict("pvc-c.n1"))

	for range conflict.LoudAfter - 1 {
		if _, err := retrier.Reconcile(ctx, req, pass); err != nil {
			t.Fatalf("error %v, want none", err)
		}
	}

	for i, level := range levels.info {
		if level < 1 {
			t.Fatalf("conflict %d logged at V(%d), want V(1) before %d in a row", i+1, level, conflict.LoudAfter)
		}
	}

	if _, err := retrier.Reconcile(ctx, req, pass); err != nil {
		t.Fatalf("error %v, want none", err)
	}

	if last := levels.info[len(levels.info)-1]; last != 0 {
		t.Errorf("conflict %d logged at V(%d), want the default level", conflict.LoudAfter, last)
	}
}

// The run is per object and ends with any pass that did not conflict,
// so churn spread over many objects, or a conflict now and then on one,
// stays below the default level.
func TestConflictRunResets(t *testing.T) {
	t.Parallel()

	var (
		levels  logLevels
		retrier conflict.Retrier
	)

	ctx := logf.IntoContext(context.Background(), capture(&levels))
	req, conflicted := passOn("pvc-a.n1", newConflict("pvc-a.n1"))
	_, clean := passOn("pvc-a.n1", nil)
	other, otherConflicted := passOn("pvc-b.n1", newConflict("pvc-b.n1"))

	for range conflict.LoudAfter - 1 {
		_, _ = retrier.Reconcile(ctx, req, conflicted)
		_, _ = retrier.Reconcile(ctx, other, otherConflicted)
	}

	_, _ = retrier.Reconcile(ctx, req, clean)

	for range conflict.LoudAfter - 1 {
		_, _ = retrier.Reconcile(ctx, req, conflicted)
	}

	for i, level := range levels.info {
		if level < 1 {
			t.Fatalf("line %d logged at V(%d), want every line at V(1)", i+1, level)
		}
	}
}

// A side write that failed next to a pass's own outcome never reaches
// the Retrier, so LogFailure itself keeps a lost race out of the ERROR
// log and still reports any other failure there.
func TestLogFailureLogsAConflictAtV1AndAnythingElseAtError(t *testing.T) {
	t.Parallel()

	modified := apierrors.NewConflict(schema.GroupResource{Group: "blockstor.org", Resource: "snapshots"}, "s", errModified)

	for _, tc := range []struct {
		name       string
		err        error
		wantErrors int
		wantInfo   []int
	}{
		{name: "conflict", err: errors.Wrap(modified, "stamp"), wantInfo: []int{1}},
		{name: "other", err: errBroken, wantErrors: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var levels logLevels

			conflict.LogFailure(logf.IntoContext(context.Background(), capture(&levels)), tc.err, "record failure")

			if levels.errors != tc.wantErrors || !slices.Equal(levels.info, tc.wantInfo) {
				t.Errorf("logged %d errors and info at %v, want %d errors and info at %v",
					levels.errors, levels.info, tc.wantErrors, tc.wantInfo)
			}
		})
	}
}
