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

package controllers

import (
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// An apply that keeps failing is retried with a back-off that doubles
// up to applyFailureMaxRequeue, not every applyFailureRequeue for as long
// as it fails, and an apply that goes through starts the back-off over.
func TestAFailingApplyIsRetriedWithBackOff(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD("STORAGE")
	res := memberReplica()
	r := newPassphraseTestReconciler(t, luksGroup(), rd, res)

	provider := &flakyCreateProvider{failsRemaining: 8}
	r.Config.Exec = storage.NewFakeExec()
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": provider},
		NodeName:  "node-a",
	})

	run := func() time.Duration {
		t.Helper()

		result, err := r.runApply(t.Context(), res, logr.Discard())
		if err != nil {
			t.Fatalf("runApply: %v", err)
		}

		return result.RequeueAfter
	}

	want := []time.Duration{
		5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second,
		80 * time.Second, 160 * time.Second, 5 * time.Minute, 5 * time.Minute,
	}
	for i, w := range want {
		if got := run(); got != w {
			t.Fatalf("retry %d after %v, want %v", i+1, got, w)
		}
	}

	if got := run(); got != 0 {
		t.Fatalf("a storage-only apply that went through requeued after %v, want no requeue", got)
	}

	provider.failsRemaining = 1

	if got := run(); got != applyFailureRequeue {
		t.Fatalf("the first failure after a success retried after %v, want %v", got, applyFailureRequeue)
	}
}
