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

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// EnqueueResourcesForRD exposes the internal RD-watch fan-out for
// the *_test.go suite. Pure forward; the production wiring (in
// SetupWithManager) keeps using the unexported method.
func (r *ResourceReconciler) EnqueueResourcesForRD(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.enqueueResourcesForRD(ctx, obj)
}

// EnqueueSiblings exposes the internal sibling fan-out for tests.
func (r *ResourceReconciler) EnqueueSiblings(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.enqueueSiblings(ctx, obj)
}

// EnqueueRDForResource exposes the RD-side parent lookup for tests.
// Production wiring stays unexported via SetupWithManager.
func (r *ResourceDefinitionReconciler) EnqueueRDForResource(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.enqueueRDForResource(ctx, obj)
}

// AlreadyExists exposes the wrapped-error keyword check the RD
// reconciler uses to tolerate kube-apiserver "already exists"
// races. Tests pin both the positive and the false-positive paths.
func AlreadyExists(err error) bool {
	return alreadyExists(err)
}

// ResolveLayerStack exposes the four-tier layer-stack resolver:
// RD spec → RG spec → nil (dispatcher default). Tests pin each
// tier so a refactor that swapped precedence (RG over RD, say)
// would surface immediately.
func (r *ResourceReconciler) ResolveLayerStack(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) []string {
	return r.resolveLayerStack(ctx, rd)
}

// ResolveEffectiveProps exposes the four-tier prop resolver: cluster
// ControllerProps → RG → RD → Resource. Tests pin each tier and the
// soft-fail-on-missing-RG path.
func (r *ResourceReconciler) ResolveEffectiveProps(ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) (map[string]string, error) {
	return r.resolveEffectiveProps(ctx, target, rd)
}

// FirstAvailablePool exposes the auto-diskful pool selector so the
// test suite can pin its diskless-skip + node-filter rules.
func (r *ResourceReconciler) FirstAvailablePool(ctx context.Context, nodeName string) (string, error) {
	return r.firstAvailablePool(ctx, nodeName)
}

// IsAutoTieBreakerEnabled exposes the prop-driven default for the
// auto-quorum-tiebreaker logic. The default is on; an explicit
// "false" override (case-insensitive) is the only way to disable it.
func IsAutoTieBreakerEnabled(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	return isAutoTieBreakerEnabled(rd)
}

// SetQuorum exposes the conflict-retry quorum prop writer for tests.
func (r *ResourceDefinitionReconciler) SetQuorum(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition, value string) error {
	return r.setQuorum(ctx, rd, value)
}

// ControllerProps exposes the cluster-KV ControllerProps reader.
// Tests pin the instance-filter so KVEntry rows belonging to other
// instances (e.g. csi-volumes) don't leak into the prop bag the
// dispatcher hands the satellite.
func (r *ResourceReconciler) ControllerProps(ctx context.Context) (map[string]string, error) {
	return r.controllerProps(ctx)
}

// LookupRD exposes the soft-fail RD fetcher. Tests pin the
// (nil, nil) return on NotFound so the dispatcher can still set up
// connection state when the RD is being deleted concurrently.
func (r *ResourceReconciler) LookupRD(ctx context.Context, name string) (*blockstoriov1alpha1.ResourceDefinition, error) {
	return r.lookupRD(ctx, name)
}

// PtrEqI32 exposes the nil-aware *int32 equality helper. Tests pin
// every branch of the three-way nil check so a refactor that flipped
// the nil-vs-non-nil case wouldn't silently make ID-equality tests
// always-false (which would trigger spurious Status updates on
// every reconcile).
func PtrEqI32(a, b *int32) bool {
	return ptrEqI32(a, b)
}

// RangeProp exposes the Node-prop range parser the per-node port
// and minor allocators use. Tests pin the three-tier fallback
// (missing node → defaults; missing prop → defaults; bad format →
// error) without spinning up the full allocator path.
func (r *ResourceReconciler) RangeProp(ctx context.Context, nodeName, prop string, defLow, defHigh int32) (int32, int32, error) {
	return r.rangeProp(ctx, nodeName, prop, defLow, defHigh)
}

// QuorumPolicy exposes upstream-LINSTOR's isQuorumFeasible
// decision: 2 diskful + ≥1 diskless OR ≥3 diskful → majority,
// else off. Tests pin every (diskful, diskless) combination.
func QuorumPolicy(diskful, diskless int) string {
	return quorumPolicy(diskful, diskless)
}

// SplitByDiskless exposes the disk-class partitioner the quorum
// helper feeds. A regression that swapped which slice a Resource
// lands in would silently flip the quorum decision for every RD.
func SplitByDiskless(replicas []apiv1.Resource) ([]apiv1.Resource, []apiv1.Resource) {
	return splitByDiskless(replicas)
}

// FilterTieBreaker exposes the TIE_BREAKER subset filter — pins
// "regular diskless mixed with witnesses" → only-witnesses shape
// the witness-create / witness-remove decision relies on.
func FilterTieBreaker(diskless []apiv1.Resource) []apiv1.Resource {
	return filterTieBreaker(diskless)
}

// PickTiebreakerNode exposes the witness-node selector for tests.
// Production wiring stays unexported via createWitness.
func (r *ResourceDefinitionReconciler) PickTiebreakerNode(ctx context.Context, hostingReplica map[string]bool) (string, error) {
	return r.pickTiebreakerNode(ctx, hostingReplica)
}

// IsDisabledNode exposes the EVICTED/LOST flag check used by both
// the placer and the RD-level tiebreaker path. Pins the "drain
// signal" flag set so the witness path doesn't pin a dying node.
func IsDisabledNode(node *apiv1.Node) bool {
	return isDisabledNode(node)
}

// RemoveWitnesses exposes the witness-cleanup helper for tests.
// Production wiring stays unexported via applyWitnessDecision.
func (r *ResourceDefinitionReconciler) RemoveWitnesses(ctx context.Context, rdName string, witnesses []apiv1.Resource) error {
	return r.removeWitnesses(ctx, rdName, witnesses)
}

// ApplyWitnessDecision exposes the witness create-or-remove gate
// for tests. Pinning the want=true/witnesses=0 → create branch and
// the want=false/witnesses>0 → remove branch keeps the auto-quorum
// reconcile logic from drifting.
func (r *ResourceDefinitionReconciler) ApplyWitnessDecision(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
	replicas, diskless, witness []apiv1.Resource,
	wantWitness bool,
) ([]apiv1.Resource, error) {
	return r.applyWitnessDecision(ctx, rd, replicas, diskless, witness, wantWitness)
}

// EnsureTiebreaker exposes the full ensure-tiebreaker pipeline
// (decision + quorum prop write) for tests. The wired-up version
// fires from RD reconcile; tests pin the boundary cases (witness
// auto-add on 2-replica, quorum off on 1-replica, quorum majority
// on 3-replica) without reconstructing the full Reconcile call.
func (r *ResourceDefinitionReconciler) EnsureTiebreaker(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	return r.ensureTiebreaker(ctx, rd)
}
