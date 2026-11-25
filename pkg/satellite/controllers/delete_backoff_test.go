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
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

var errZvolBusy = errors.New("zfs destroy tank/pvc-del_00000: dataset is busy")

// busyProvider refuses every volume delete, as ZFS does for a zvol it
// still counts as open right after `drbdsetup down`.
type busyProvider struct {
	orderingProvider
}

func (p *busyProvider) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	_ = p.orderingProvider.DeleteVolume(ctx, vol)

	return errZvolBusy
}

// A delete the storage refuses is retried after a back-off that doubles
// with each retry that fails, and an event that reaches the replica before
// the retry is due does not run the delete again: one busy zvol was
// otherwise deleted seventeen times in six seconds.
func TestFailedDeleteIsRetriedAfterABackoff(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-delete"
		rdName = "pvc-del"
	)

	baseCli, _, _, step := newDeleteRaceFixture(t)
	provider := &busyProvider{orderingProvider: orderingProvider{step: step}}

	clock := time.Unix(1_700_000_000, 0)

	reconciler := &ResourceReconciler{
		Client: baseCli,
		Config: Config{
			NodeName: node,
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{"lvm-thin": provider},
				NodeName:  node,
			}),
			Exec:      storage.NewFakeExec(),
			APIReader: baseCli,
		},
	}
	reconciler.deleteRetries.now = func() time.Time { return clock }

	reconcile := func() time.Duration {
		t.Helper()

		result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: rdName + "." + node},
		})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		return result.RequeueAfter
	}

	if got := reconcile(); got != deleteFailureRequeue {
		t.Errorf("first failed delete requeued after %v, want %v", got, deleteFailureRequeue)
	}

	clock = clock.Add(deleteFailureRequeue / 2)

	if got := reconcile(); got != deleteFailureRequeue/2 {
		t.Errorf("event before the retry is due requeued after %v, want the %v left", got, deleteFailureRequeue/2)
	}

	if got := len(provider.deletesSnapshot()); got != 1 {
		t.Errorf("an event before the retry was due ran the delete again: %d deletes, want 1", got)
	}

	clock = clock.Add(deleteFailureRequeue / 2)

	if got := reconcile(); got != 2*deleteFailureRequeue {
		t.Errorf("second failed delete requeued after %v, want %v", got, 2*deleteFailureRequeue)
	}

	if got := len(provider.deletesSnapshot()); got != 2 {
		t.Errorf("the due retry ran %d deletes in all, want 2", got)
	}
}
