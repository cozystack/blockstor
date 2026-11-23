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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

	got, gotErr := conflict.Reconcile(ctx, reconcile.Request{}, func(context.Context, reconcile.Request) (reconcile.Result, error) {
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
