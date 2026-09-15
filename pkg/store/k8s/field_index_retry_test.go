// SPDX-License-Identifier: Apache-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	errDiscoveryBlip    = errors.New("failed to get server groups: connection refused")
	errIndexerConflict  = errors.New("indexer conflict")
	errNeverRegistering = errors.New("still unreachable")
)

// flakyIndexer refuses one field once, as a discovery blip does, and refuses a
// second indexer under a name it already holds, as an informer does.
type flakyIndexer struct {
	failOnce   string
	registered map[string]int
}

func (f *flakyIndexer) IndexField(_ context.Context, obj ctrlclient.Object, field string, _ ctrlclient.IndexerFunc) error {
	key := fmt.Sprintf("%T/%s", obj, field)

	if key == f.failOnce {
		f.failOnce = ""

		return errDiscoveryBlip
	}

	if f.registered[key] > 0 {
		return fmt.Errorf("%w: %s", errIndexerConflict, key)
	}

	f.registered[key]++

	return nil
}

// A retry after a partial success must ask only for what failed. Retrying the
// whole set asks the informer for an index it already holds, which it refuses,
// so the construction that should have recovered spends its budget failing on
// the part that worked.
func TestIndexRegistrationRetriesOnlyWhatFailed(t *testing.T) {
	t.Parallel()

	indexer := &flakyIndexer{
		failOnce:   fmt.Sprintf("%T/%s", fieldIndexes()[2].object, fieldIndexes()[2].field),
		registered: map[string]int{},
	}

	err := registerFieldIndexesWithin(10*time.Second, indexer)
	if err != nil {
		t.Fatalf("registration after one blip on the third index: %v", err)
	}

	for _, index := range fieldIndexes() {
		key := fmt.Sprintf("%T/%s", index.object, index.field)
		if indexer.registered[key] != 1 {
			t.Errorf("%s registered %d time(s), want 1", key, indexer.registered[key])
		}
	}
}

type deadIndexer struct{}

func (deadIndexer) IndexField(context.Context, ctrlclient.Object, string, ctrlclient.IndexerFunc) error {
	return errNeverRegistering
}

func TestIndexRegistrationReportsTheLastErrorWhenTheBudgetRunsOut(t *testing.T) {
	t.Parallel()

	err := registerFieldIndexesWithin(300*time.Millisecond, deadIndexer{})
	if !errors.Is(err, errNeverRegistering) {
		t.Fatalf("err = %v, want it to carry the registration error", err)
	}
}

// hangingIndexer never answers, the way discovery does when its connection
// goes into a dropped route and waits out the dial timeout.
type hangingIndexer struct {
	release chan struct{}
}

func (h hangingIndexer) IndexField(context.Context, ctrlclient.Object, string, ctrlclient.IndexerFunc) error {
	<-h.release

	return errNeverRegistering
}

// The budget bounds the wait, not an attempt. An attempt that hangs has to be
// abandoned when the budget runs out, or the kill the budget was sized to beat
// arrives anyway, as long after it as the hung call takes to fail.
func TestIndexRegistrationAbandonsAnAttemptThatOutlivesTheBudget(t *testing.T) {
	t.Parallel()

	indexer := hangingIndexer{release: make(chan struct{})}
	t.Cleanup(func() { close(indexer.release) })

	returned := make(chan error, 1)

	go func() { returned <- registerFieldIndexesWithin(200*time.Millisecond, indexer) }()

	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("registration over an indexer that never answered returned no error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("registration was still waiting on a hung attempt 5s into a 200ms budget")
	}
}
