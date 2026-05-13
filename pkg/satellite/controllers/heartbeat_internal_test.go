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
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// nodeNotFoundOnStatusPatch returns a SubResourcePatch interceptor
// that mimics the real apiserver's behaviour: Status patches against
// a missing parent Node CRD fail with NotFound. The fake client
// otherwise silently CREATEs the parent on an SSA-Apply, which would
// mask the very bug (auto-resurrection on satellite restart) we
// guard against.
func nodeNotFoundOnStatusPatch(base client.WithWatch) client.Client {
	return interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string,
			obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			if subResourceName == "status" {
				key := client.ObjectKey{Name: obj.GetName(), Namespace: obj.GetNamespace()}
				probe := &blockstoriov1alpha1.Node{}

				err := c.Get(ctx, key, probe)
				if apierrors.IsNotFound(err) {
					gr := schema.GroupResource{
						Group:    blockstoriov1alpha1.GroupVersion.Group,
						Resource: "nodes",
					}

					return apierrors.NewNotFound(gr, obj.GetName())
				}
			}

			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	})
}

// captureLogger returns a logr.Logger that buffers every Error
// + Info entry to buf. Lightweight stand-in for klog/zap that
// keeps the assertion surface to a single bytes.Buffer.
func captureLogger(buf *bytes.Buffer) logr.Logger {
	return funcr.New(func(_, args string) {
		buf.WriteString(args)
		buf.WriteString("\n")
	}, funcr.Options{Verbosity: 1})
}

// TestHelloRejectsUnregisteredNode (Bug 20) pins the satellite-side
// of the "Hello"-equivalent contract: a heartbeat stamp against a
// Node CRD that does not exist MUST NOT auto-create the CRD. The
// satellite is REJECTED at the controller boundary (NotFound from
// SSA-Patch on the Status subresource) and must surface that
// rejection to the operator with an actionable error.
//
// Phase 10.6 retired the gRPC Hello RPC — Hello is now the very
// first heartbeat stamp the satellite makes against its own Node
// CRD. The semantic the upstream Hello enforced ("controller
// refuses to register a satellite the operator did not opt in
// to") is preserved here by SSA-Patch refusing to create a Node
// CRD on a Status subresource write.
func TestHelloRejectsUnregisteredNode(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	// Empty client — no Node CRD for "worker-3" exists. Mirrors the
	// post-`linstor node lost worker-3` cluster state where the
	// DaemonSet then schedules a fresh satellite pod onto the host.
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		Build()

	cli := nodeNotFoundOnStatusPatch(base)

	hb := &HeartbeatRunnable{
		Client:   cli,
		NodeName: "worker-3",
	}

	var buf bytes.Buffer

	logger := captureLogger(&buf)

	err := hb.stamp(context.Background(), logger)
	if err != nil {
		t.Fatalf("stamp must not propagate NotFound as a fatal error: %v", err)
	}

	// Critical: the satellite must NOT have created a Node CRD as a
	// side-effect of the heartbeat. Auto-resurrection would silently
	// undo the operator's `node lost`.
	var nodes blockstoriov1alpha1.NodeList
	if err := base.List(context.Background(), &nodes); err != nil {
		t.Fatalf("list Nodes: %v", err)
	}

	if len(nodes.Items) != 0 {
		t.Fatalf("satellite auto-created Node CRD on NotFound — violates `node lost` "+
			"semantic (Bug 20); items=%d", len(nodes.Items))
	}

	// And the rejection must be logged with the actionable hint so
	// operators reading `kubectl logs ds/blockstor-satellite` can
	// see why the node never came online.
	logged := buf.String()
	if !strings.Contains(logged, nodeMissingHint) {
		t.Fatalf("missing actionable hint in log; got:\n%s", logged)
	}

	if !strings.Contains(logged, "worker-3") {
		t.Errorf("log must name the offending node; got:\n%s", logged)
	}

	if hb.missCount != 1 {
		t.Errorf("first NotFound must bump missCount to 1; got %d", hb.missCount)
	}
}

// TestSatelliteHelloLoopHandlesUnregisteredGracefully (Bug 20)
// pins the satellite-side caller behaviour: when the controller
// rejects with NotFound, the loop logs once at ERROR, then
// throttles re-logs at UnregisteredLogEvery cadence, and crucially
// keeps ticking so the operator may re-register the node WITHOUT
// restarting the pod. The successful recovery stamp resets the
// throttle counter and emits an INFO breadcrumb.
func TestSatelliteHelloLoopHandlesUnregisteredGracefully(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).
		Build()

	cli := nodeNotFoundOnStatusPatch(base)

	hb := &HeartbeatRunnable{
		Client:   cli,
		NodeName: "worker-3",
	}

	var buf bytes.Buffer

	logger := captureLogger(&buf)

	// Drive UnregisteredLogEvery+2 consecutive misses. The hint must
	// be logged at i==0 and i==UnregisteredLogEvery (i.e. twice),
	// NOT every tick.
	for i := range UnregisteredLogEvery + 2 {
		err := hb.stamp(context.Background(), logger)
		if err != nil {
			t.Fatalf("iteration %d: stamp returned fatal error: %v", i, err)
		}
	}

	hintCount := strings.Count(buf.String(), nodeMissingHint)
	if hintCount != 2 {
		t.Errorf("rate-limit broken: expected exactly 2 hint emissions over "+
			"%d misses, got %d", UnregisteredLogEvery+2, hintCount)
	}

	if hb.missCount != UnregisteredLogEvery+2 {
		t.Errorf("missCount: got %d, want %d", hb.missCount, UnregisteredLogEvery+2)
	}

	// Operator re-registers the node. Next stamp succeeds; loop
	// emits an INFO recovery log and resets missCount.
	node := &blockstoriov1alpha1.Node{}
	node.Name = "worker-3"
	node.Spec.Type = "SATELLITE"

	if err := base.Create(context.Background(), node); err != nil {
		t.Fatalf("create Node post-rejection: %v", err)
	}

	buf.Reset()

	err := hb.stamp(context.Background(), logger)
	if err != nil {
		t.Fatalf("recovery stamp: %v", err)
	}

	if hb.missCount != 0 {
		t.Errorf("recovery must reset missCount; got %d", hb.missCount)
	}

	if !strings.Contains(buf.String(), "Node CRD reappeared") {
		t.Errorf("recovery breadcrumb missing; got:\n%s", buf.String())
	}
}
