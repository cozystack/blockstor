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

package controller

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestFailedResizeStampAsksForARetry: the resize-pending stamp on a
// replica is written over the cached copy and often meets a conflict
// with the satellite's own write. The five second requeue used to retry
// it; now the pass has to ask, and one that stamped asks for nothing.
func TestFailedResizeStampAsksForARetry(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-grow"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 2048}},
		},
	}

	for _, tc := range []struct {
		name      string
		updateErr error
		want      time.Duration
	}{
		{
			name:      "conflict",
			updateErr: apierrors.NewConflict(schema.GroupResource{Resource: "resources"}, "pvc-grow.n1", nil),
			want:      rdReconcileRequeue,
		},
		{name: "stamped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			replica := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-grow.n1"},
				Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1", ResourceDefinitionName: "pvc-grow"},
			}

			cli := fake.NewClientBuilder().WithScheme(bug048Scheme(t)).WithObjects(rd.DeepCopy(), replica).
				WithInterceptorFuncs(interceptor.Funcs{
					Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
						if tc.updateErr != nil {
							return tc.updateErr
						}

						return c.Update(ctx, obj, opts...)
					},
				}).Build()

			var retry time.Duration

			ctx := context.WithValue(context.Background(), rdRetryKey{}, &retry)

			err := (&ResourceDefinitionReconciler{Client: cli}).stampResizePending(ctx, rd)
			if err != nil {
				t.Fatalf("stampResizePending: %v", err)
			}

			if retry != tc.want {
				t.Errorf("asked to come back after %v, want %v", retry, tc.want)
			}
		})
	}
}
