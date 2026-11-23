// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// definitionReadRacesARollback hands out the definition it read, and once,
// after reading it, lets a rollback write its mark through the API server: the
// claim's mutate then decides on a view the server has already moved past.
type definitionReadRacesARollback struct {
	ctrlclient.Client

	rollback func()
	fired    *atomic.Bool
}

func (d definitionReadRacesARollback) Get(
	ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption,
) error {
	err := d.Client.Get(ctx, key, obj, opts...)
	if _, ok := obj.(*crdv1alpha1.ResourceDefinition); ok && err == nil && d.rollback != nil &&
		d.fired.CompareAndSwap(false, true) {
		d.rollback()
	}

	return err //nolint:wrapcheck // pass-through test double
}

// The claim's refusal over a rollback is decided inside the patch's mutate, so
// what keeps a claim from landing over a rollback that wrote its mark after the
// mutate's read is the patch's optimistic lock alone: the patch is refused with
// a conflict, the retry reads the mark, and the claim stands down.
func TestAClaimDoesNotLandOverARollbackThatRacedItsRead(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	for _, tc := range []struct {
		name  string
		races bool
	}{
		{name: "rollback writes between the read and the patch", races: true},
		{name: "no rollback: the claim lands"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			seed := k8s.New(fixture.client)
			name := "pvc-claim-race"

			if !tc.races {
				name = "pvc-claim-clean"
			}

			if err := seed.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: name}); err != nil {
				t.Fatalf("seed definition: %v", err)
			}

			t.Cleanup(func() { _ = seed.ResourceDefinitions().Delete(context.Background(), name) })

			racing := definitionReadRacesARollback{Client: fixture.client, fired: new(atomic.Bool)}
			if tc.races {
				racing.rollback = func() {
					err := seed.ResourceDefinitions().PatchResourceDefinitionSpec(context.Background(), name,
						func(rd *apiv1.ResourceDefinition) error {
							if rd.Props == nil {
								rd.Props = map[string]string{}
							}

							rd.Props[store.RollbackAbandonedProp] = store.RollbackInProgress

							return nil
						})
					if err != nil {
						t.Errorf("the racing rollback's mark: %v", err)
					}
				}
			}

			st := k8s.NewWithAPIReader(racing, fixture.client)
			err := store.ClaimAdoptedLeftover(ctx, st, name, nil)

			got, readErr := seed.ResourceDefinitions().GetUncached(ctx, name)
			if readErr != nil {
				t.Fatalf("read back: %v", readErr)
			}

			mark := got.Props[store.RestoreAdoptedProp]

			if !tc.races {
				if err != nil || mark == "" {
					t.Fatalf("an uncontended claim = %v, mark %q; want it accepted and marked", err, mark)
				}

				return
			}

			if !errors.Is(err, store.ErrAdoptedLeftoverRollingBack) {
				t.Errorf("a claim racing a rollback = %v, want ErrAdoptedLeftoverRollingBack", err)
			}

			if mark != "" {
				t.Errorf("a claim racing a rollback landed its mark %q over the rollback's", mark)
			}
		})
	}
}
