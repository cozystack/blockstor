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
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Release-gate regression (TestGroupFRActivateDeactivate, CI job
// 81060268741): `POST .../resources/{node}/activate` answered
// `409 conflict: store object was modified, retry the request` when a
// controller reconciler updated the Resource CRD between the
// handler's read and write. Upstream LINSTOR never returns 409 on
// activate/deactivate and the Python CLI does not retry it, so the
// conflict leaked straight to the operator and failed the suite.
//
// The fixture below reproduces the race deterministically against the
// CRD-backed store: a fake-client interceptor performs an out-of-band
// "reconciler" write on the same Resource CRD immediately before the
// handler's first persist attempt, so the optimistic-lock write is
// guaranteed stale. The handler must absorb the conflict via the
// store's retry-on-conflict patch path and answer 200; the
// reconciler's concurrent write must survive (no lost update).
func TestActivateDeactivateRetriesOnStoreConflict(t *testing.T) {
	t.Parallel()

	cases := []struct {
		action    string
		seedFlags []string
		wantFlag  bool // INACTIVE present after the call
	}{
		{action: "activate", seedFlags: []string{"INACTIVE"}, wantFlag: false},
		{action: "deactivate", seedFlags: nil, wantFlag: true},
	}

	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			t.Parallel()

			cli, injected := newConflictInjectingResourceClient(t)
			st := k8s.New(cli)
			ctx := t.Context()

			if err := st.Resources().Create(ctx, &apiv1.Resource{
				Name:     "pvc-conflict",
				NodeName: "n1",
				Flags:    tc.seedFlags,
			}); err != nil {
				t.Fatalf("seed Resource: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPost(t,
				base+"/v1/resource-definitions/pvc-conflict/resources/n1/"+tc.action, nil)
			defer func() { _ = resp.Body.Close() }()

			if !injected.Load() {
				t.Fatalf("fixture rot: no conflicting write was injected — the test proves nothing")
			}

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200 — the handler leaked the store conflict to the operator", resp.StatusCode)
			}

			var rc []apiv1.APICallRc

			if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			if len(rc) == 0 || rc[0].RetCode < 0 {
				t.Fatalf("envelope: got %+v, want non-empty MASK_INFO success", rc)
			}

			got, err := st.Resources().Get(ctx, "pvc-conflict", "n1")
			if err != nil {
				t.Fatalf("Get after %s: %v", tc.action, err)
			}

			if gotFlag := slices.Contains(got.Flags, "INACTIVE"); gotFlag != tc.wantFlag {
				t.Errorf("INACTIVE flag after %s: got %v (flags %v), want %v",
					tc.action, gotFlag, got.Flags, tc.wantFlag)
			}

			// The injected "reconciler" write must not have been
			// clobbered by the retried handler write.
			var crd crdv1alpha1.Resource
			if err := cli.Get(ctx, types.NamespacedName{Name: resourceConflictCRDName(t, cli)}, &crd); err != nil {
				t.Fatalf("get Resource CRD: %v", err)
			}

			if crd.Labels[conflictTouchLabel] != "1" {
				t.Errorf("concurrent reconciler write lost: label %q missing on the CRD", conflictTouchLabel)
			}
		})
	}
}

// conflictTouchLabel marks the out-of-band "reconciler" write the
// interceptor injects between the handler's read and write.
const conflictTouchLabel = "test.blockstor.io/reconciler-touch"

// newConflictInjectingResourceClient builds a fake controller-runtime
// client that simulates a controller reconciler racing the REST
// handler: immediately before the FIRST write (Update or Patch) that
// targets a Resource CRD, it performs a side-write on the live object
// through the un-intercepted inner client — bumping resourceVersion so
// the handler's optimistic-lock write fails with a 409 conflict. Every
// subsequent write passes through untouched, so a retry that re-reads
// fresh state succeeds. The returned flag reports whether the
// injection fired (guards against fixture rot).
func newConflictInjectingResourceClient(t *testing.T) (ctrlclient.WithWatch, *atomic.Bool) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	injected := &atomic.Bool{}

	sideWrite := func(ctx context.Context, inner ctrlclient.WithWatch, name string) error {
		var live crdv1alpha1.Resource

		if err := inner.Get(ctx, types.NamespacedName{Name: name}, &live); err != nil {
			return errors.Wrapf(err, "side-write get Resource %q", name)
		}

		if live.Labels == nil {
			live.Labels = map[string]string{}
		}

		live.Labels[conflictTouchLabel] = "1"

		return errors.Wrapf(inner.Update(ctx, &live), "side-write update Resource %q", name)
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&crdv1alpha1.Resource{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, inner ctrlclient.WithWatch, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
				if _, isRes := obj.(*crdv1alpha1.Resource); isRes && injected.CompareAndSwap(false, true) {
					if err := sideWrite(ctx, inner, obj.GetName()); err != nil {
						return err
					}
				}

				return inner.Update(ctx, obj, opts...)
			},
			Patch: func(ctx context.Context, inner ctrlclient.WithWatch, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
				if _, isRes := obj.(*crdv1alpha1.Resource); isRes && injected.CompareAndSwap(false, true) {
					if err := sideWrite(ctx, inner, obj.GetName()); err != nil {
						return err
					}
				}

				return inner.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	return cli, injected
}

// resourceConflictCRDName resolves the single Resource CRD's
// metadata.name without depending on the store's internal naming
// scheme — the test only ever seeds one Resource.
func resourceConflictCRDName(t *testing.T, cli ctrlclient.Client) string {
	t.Helper()

	var list crdv1alpha1.ResourceList

	if err := cli.List(t.Context(), &list); err != nil {
		t.Fatalf("list Resource CRDs: %v", err)
	}

	if len(list.Items) != 1 {
		t.Fatalf("expected exactly 1 Resource CRD, got %d", len(list.Items))
	}

	return list.Items[0].Name
}
