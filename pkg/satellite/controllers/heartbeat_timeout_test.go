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

package controllers_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
)

// TestHeartbeatGivesASlowAPIServerAPeriod: an API server that is up
// but slow to commit, etcd catching up after it lost a member, must
// still get the stamp in. kubelet allows a node status update a full
// period; cutting a stamp shorter marks a node OFFLINE that a slower
// write would have kept ONLINE.
func TestHeartbeatGivesASlowAPIServerAPeriod(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	budget := make(chan time.Duration, 1)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, _ client.Client, _ string, _ client.Object, _ client.Patch,
				_ ...client.SubResourcePatchOption,
			) error {
				deadline, ok := ctx.Deadline()
				if !ok {
					deadline = time.Now().Add(time.Hour)
				}

				select {
				case budget <- time.Until(deadline):
				default:
				}

				return nil
			},
		}).
		Build()

	hb := &controllers.HeartbeatRunnable{Client: cli, NodeName: "worker-1", Period: time.Hour}

	// No deadline of its own: the stamp's budget must be the
	// heartbeat's, not the test's.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- hb.Start(ctx) }()

	select {
	case got := <-budget:
		if got < controllers.HeartbeatPeriod-time.Second {
			t.Errorf("a stamp gets %s before it is cut off; want a full %s heartbeat period",
				got.Round(time.Second), controllers.HeartbeatPeriod)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no stamp was sent")
	}

	cancel()

	err := <-done
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
}
