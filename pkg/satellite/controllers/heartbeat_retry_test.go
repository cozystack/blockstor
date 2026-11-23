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
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
)

// heartbeatAttempts runs a heartbeat for run whose every stamp fails
// with stampErr and returns how many stamps it sent and how many times
// it dropped its connections.
func heartbeatAttempts(t *testing.T, stampErr error, run time.Duration) (int64, int64) {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	var attempts, resets atomic.Int64

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch,
				...client.SubResourcePatchOption,
			) error {
				attempts.Add(1)

				return stampErr
			},
		}).
		Build()

	hb := &controllers.HeartbeatRunnable{
		Client:           cli,
		NodeName:         "worker-1",
		Period:           time.Second,
		RetryPeriod:      10 * time.Millisecond,
		ResetConnections: func() { resets.Add(1) },
	}

	ctx, cancel := context.WithTimeout(t.Context(), run)
	defer cancel()

	err := hb.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	return attempts.Load(), resets.Load()
}

// TestHeartbeatBacksOffWhenTheAPIServerRefuses: an API server that
// answers a stamp with an error is reachable; a new connection gets
// the same answer. Every satellite redialling and resending each
// retry period would pile handshakes onto an API server that may be
// shedding load already, so the stamp keeps its connection and backs
// off.
func TestHeartbeatBacksOffWhenTheAPIServerRefuses(t *testing.T) {
	t.Parallel()

	gr := schema.GroupResource{Group: blockstoriov1alpha1.GroupVersion.Group, Resource: "nodes"}

	for name, refusal := range map[string]error{
		"429 overloaded":       apierrors.NewTooManyRequests("priority and fairness", 1),
		"403 grant revoked":    apierrors.NewForbidden(gr, "worker-1", nil),
		"422 schema mismatch":  apierrors.NewInvalid(schema.GroupKind{Group: gr.Group, Kind: "Node"}, "worker-1", nil),
		"500 internal failure": apierrors.NewInternalError(context.Canceled),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// 10, 20, 40, 80, 160 ms: six stamps fit in 300 ms; a
			// fixed 10 ms retry would send about thirty.
			attempts, resets := heartbeatAttempts(t, refusal, 300*time.Millisecond)

			if resets != 0 {
				t.Errorf("dropped the connections %d times on an answer from the API server", resets)
			}

			if attempts > 8 {
				t.Errorf("%d stamps in 300 ms against an API server that refuses them; want a backoff", attempts)
			}
		})
	}
}

// TestHeartbeatRedialsWhenTheAPIServerIsUnreachable: a stamp that got
// no answer at all may be stuck on a connection to an API server that
// is gone; drop it and retry soon on a new one.
func TestHeartbeatRedialsWhenTheAPIServerIsUnreachable(t *testing.T) {
	t.Parallel()

	for name, failure := range map[string]error{
		"stamp timed out":    context.DeadlineExceeded,
		"connection refused": &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		"504 gateway":        apierrors.NewTimeoutError("etcd", 1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			attempts, resets := heartbeatAttempts(t, failure, 300*time.Millisecond)

			if attempts < 10 {
				t.Errorf("%d stamps in 300 ms with a 10 ms retry; want quick retries", attempts)
			}

			if resets != attempts {
				t.Errorf("dropped the connections %d times for %d failed stamps; want every time", resets, attempts)
			}
		})
	}
}
