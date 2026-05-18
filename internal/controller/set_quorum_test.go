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

package controller_test

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestSetQuorumWritesValue: setQuorum writes the
// `DrbdOptions/Resource/quorum` prop onto the RD's Spec.Props,
// initialising the map if nil. The underlying CRD reflects the
// change after the call returns.
func TestSetQuorumWritesValue(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		// Props intentionally nil — exercises the lazy-init branch.
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	if err := rec.SetQuorum(context.Background(), rd, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop: got %q, want majority", got.Spec.Props["DrbdOptions/Resource/quorum"])
	}
}

// TestSetQuorumIdempotent: calling setQuorum with the same value
// must NOT issue a redundant Update — protects against requeue
// storms when the RD reconciler runs back-to-back. Detected by
// asserting ResourceVersion stays unchanged.
func TestSetQuorumIdempotent(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				"DrbdOptions/Resource/quorum": "majority",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	pre := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, pre); err != nil {
		t.Fatalf("get pre: %v", err)
	}

	if err := rec.SetQuorum(context.Background(), pre, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	post := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, post); err != nil {
		t.Fatalf("get post: %v", err)
	}

	if pre.ResourceVersion != post.ResourceVersion {
		t.Errorf("ResourceVersion changed after no-op SetQuorum: %s → %s",
			pre.ResourceVersion, post.ResourceVersion)
	}
}

// TestSetQuorumReplacesExistingValue: changing from "off" to
// "majority" overwrites the prop in place. Ensures the RD reconciler
// can flip the quorum mode when a witness lands or evaporates.
func TestSetQuorumReplacesExistingValue(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				"DrbdOptions/Resource/quorum": "off",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	if err := rec.SetQuorum(context.Background(), rd, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop: got %q, want majority", got.Spec.Props["DrbdOptions/Resource/quorum"])
	}
}

// TestSetQuorumSeedsOnNoQuorumForMajority pins the Bug 297 invariant:
// when setQuorum stamps `majority`, it must also seed
// `DrbdOptions/Resource/on-no-quorum=suspend-io` if the operator
// hasn't pinned a value. Without this companion, DRBD-9 falls back
// to its built-in `io-error` policy and the minority replica freezes
// in an ENODATA state that survives partition heal — `drbdadm primary`
// then fails on auto-promote and dd opens with "No data available"
// (observed live on the network-partition.sh e2e). The REST POST
// handler's seedAutoQuorumDefaults already stamps this on POST-created
// RDs, but kubectl-apply on the CRD directly (e2e, GitOps) bypasses
// that path — so the controller has to seed on every code path that
// produces a `quorum=majority` RD.
func TestSetQuorumSeedsOnNoQuorumForMajority(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		// Props intentionally nil — exercises the lazy-init branch
		// AND the seed-from-empty path that the e2e network-partition
		// reproducer hits (kubectl-applied RD with no REST-side seeding).
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	if err := rec.SetQuorum(context.Background(), rd, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop: got %q, want majority", got.Spec.Props["DrbdOptions/Resource/quorum"])
	}

	if got.Spec.Props["DrbdOptions/Resource/on-no-quorum"] != "suspend-io" {
		t.Errorf("on-no-quorum prop: got %q, want suspend-io (Bug 297 seed)",
			got.Spec.Props["DrbdOptions/Resource/on-no-quorum"])
	}
}

// TestSetQuorumPreservesOperatorOnNoQuorum pins the "operator wins"
// half of the Bug 297 fix: when the RD already carries an explicit
// `DrbdOptions/Resource/on-no-quorum` value, setQuorum must leave it
// alone. Silently overriding it would undo scenario 7.W01's manual
// quorum-policy contract from the other direction. Mirrors the same
// guarantee `seedAutoQuorumDefaults` documents on the REST path.
func TestSetQuorumPreservesOperatorOnNoQuorum(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				// Operator-pinned io-error — must survive.
				"DrbdOptions/Resource/on-no-quorum": "io-error",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	if err := rec.SetQuorum(context.Background(), rd, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Spec.Props["DrbdOptions/Resource/on-no-quorum"] != "io-error" {
		t.Errorf("on-no-quorum prop: got %q, want io-error (operator-pinned must survive)",
			got.Spec.Props["DrbdOptions/Resource/on-no-quorum"])
	}
}

// TestSetQuorumDoesNotSeedOnOffPolicy pins the narrow scope of the
// Bug 297 seed: `on-no-quorum` is only consulted by DRBD-9 when
// quorum is enabled. Stamping a value alongside `quorum=off` would
// be noise and would churn ResourceVersion on every reconcile of a
// 1-replica RD. Negative-space guard so a refactor that broadened
// the seed scope would surface here.
func TestSetQuorumDoesNotSeedOnOffPolicy(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	if err := rec.SetQuorum(context.Background(), rd, "off"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Spec.Props["DrbdOptions/Resource/quorum"] != "off" {
		t.Errorf("quorum prop: got %q, want off", got.Spec.Props["DrbdOptions/Resource/quorum"])
	}

	if _, present := got.Spec.Props["DrbdOptions/Resource/on-no-quorum"]; present {
		t.Errorf("on-no-quorum stamped on off policy: got %q, want absent",
			got.Spec.Props["DrbdOptions/Resource/on-no-quorum"])
	}
}

// TestSetQuorumIdempotentWithSeed pins the Bug 297 idempotency
// guarantee: once both `quorum=majority` and the seeded
// `on-no-quorum=suspend-io` are present, a follow-up SetQuorum call
// must NOT re-Update the RD. Without this, the reconciler would
// churn ResourceVersion on every requeue and the conflict-retry
// budget would burn against itself under fan-out load.
func TestSetQuorumIdempotentWithSeed(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				"DrbdOptions/Resource/quorum":       "majority",
				"DrbdOptions/Resource/on-no-quorum": "suspend-io",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	pre := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, pre); err != nil {
		t.Fatalf("get pre: %v", err)
	}

	if err := rec.SetQuorum(context.Background(), pre, "majority"); err != nil {
		t.Fatalf("SetQuorum: %v", err)
	}

	post := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, post); err != nil {
		t.Fatalf("get post: %v", err)
	}

	if pre.ResourceVersion != post.ResourceVersion {
		t.Errorf("ResourceVersion changed after no-op SetQuorum (Bug 297 idempotency): %s → %s",
			pre.ResourceVersion, post.ResourceVersion)
	}
}

// TestSetQuorumRetriesOnConflict pins the conflict-retry loop:
// when the apiserver returns a Conflict error on Update (because
// another reconciler bumped resourceVersion in flight), setQuorum
// must refetch the RD and retry. Without this, two concurrent
// reconcilers would race each other to fail the quorum-prop write
// and leave the RD stuck in the wrong quorum state.
func TestSetQuorumRetriesOnConflict(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
	}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	var updateCalls int

	cli := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updateCalls++
			if updateCalls == 1 {
				gr := schema.GroupResource{Group: blockstoriov1alpha1.GroupVersion.Group, Resource: "resourcedefinitions"}

				return apierrors.NewConflict(gr, obj.GetName(), nil)
			}

			return c.Update(ctx, obj, opts...)
		},
	})

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	if err := rec.SetQuorum(context.Background(), rd, "majority"); err != nil {
		t.Fatalf("SetQuorum: got %v, want nil after retry", err)
	}

	if updateCalls < 2 {
		t.Errorf("Update calls: got %d, want >=2 (retry must happen)", updateCalls)
	}

	final := &blockstoriov1alpha1.ResourceDefinition{}
	if err := base.Get(context.Background(), types.NamespacedName{Name: "pvc-1"}, final); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if final.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop after retry: got %q, want majority", final.Spec.Props["DrbdOptions/Resource/quorum"])
	}
}

// TestSetQuorumGivesUpAfterThreeConflicts: a permanently-conflicting
// apiserver makes setQuorum return Conflict after the third attempt
// rather than looping forever. Pins the bounded-retry budget so a
// hot-reconcile loop can't melt CPU.
func TestSetQuorumGivesUpAfterThreeConflicts(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"},
	}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	var updateCalls int

	cli := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
			updateCalls++
			gr := schema.GroupResource{Group: blockstoriov1alpha1.GroupVersion.Group, Resource: "resourcedefinitions"}

			return apierrors.NewConflict(gr, obj.GetName(), nil)
		},
	})

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme}

	err := rec.SetQuorum(context.Background(), rd, "majority")
	if err == nil {
		t.Fatalf("SetQuorum: got nil, want bounded-retry Conflict error")
	}

	if !apierrors.IsConflict(err) {
		t.Errorf("error kind: got %v, want Conflict (bounded retry)", err)
	}

	if updateCalls != 3 {
		t.Errorf("Update calls: got %d, want exactly 3 (bounded retry budget)", updateCalls)
	}
}
