// SPDX-License-Identifier: Apache-2.0

package satellite

import (
	"fmt"
	"sync"
	"testing"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// registryProvider stands in for a real backend; the lookups under test
// never call into it.
type registryProvider struct{ storage.Provider }

// The StoragePool controller registers pools while several Resource
// workers apply at once, so the pool lookups those workers make must not
// read the registry unguarded. Run with -race: an unguarded read is a data
// race the detector reports, and outside it a fatal map error.
func TestProviderLookupsTolerateConcurrentRegistration(t *testing.T) {
	t.Parallel()

	rec := NewReconciler(ReconcilerConfig{NodeName: "n1"})
	rec.RegisterProvider("thin1", registryProvider{})
	rec.rememberPool("pvc-a", "thin1")

	dr := &intent.DesiredResource{
		Name:    "pvc-a",
		Volumes: []*intent.DesiredVolume{{VolumeNumber: 0, StoragePool: "thin1"}},
	}

	const rounds = 2000

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range rounds {
			rec.RegisterProvider(fmt.Sprintf("pool-%d", i), registryProvider{})
		}
	})

	wg.Go(func() {
		for range rounds {
			if _, err := rec.providerForResource("pvc-a"); err != nil {
				t.Errorf("providerForResource: %v", err)

				return
			}

			rec.resizeAssumeClean(dr)
		}
	})

	wg.Wait()
}
