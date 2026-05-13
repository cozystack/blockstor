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
	"maps"
	"strings"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// LabelSyncFieldOwner is the SSA field-manager the label-sync
// reconciler uses when writing the `Aux/<full-label-key>` slice of
// `Spec.Props` on the blockstor Node CRD.
//
// Kept distinct from the satellite's heartbeat manager and from the
// node-watchdog manager so the apiserver merges cleanly: each owns
// the slice of fields it actually authors, and a re-org of one path
// never accidentally steps on the other.
const LabelSyncFieldOwner = "blockstor.io/label-sync"

// nodeKind is the TypeMeta Kind string for the blockstor Node CRD,
// shared across SSA patch constructors in this package.
const nodeKind = "Node"

// syncedLabelPrefixes is the allow-list of corev1.Node label
// prefixes the reconciler manages on the blockstor Node CRD. Labels
// outside this list are NOT mirrored — operator-owned `Aux/...` keys
// stay untouched.
//
// The trailing "/" matters: prefix matches anchor on full namespace
// segments, so `topology.kubernetes.io/` matches
// `topology.kubernetes.io/zone` but NOT a hypothetical
// `topology.kubernetes.iotest/zone`.
//
// Bare keys (no slash) live in syncedLabelExactKeys below.
var syncedLabelPrefixes = []string{
	"topology.kubernetes.io/",
	"node-role.kubernetes.io/",
}

// syncedLabelExactKeys is the allow-list of exact corev1.Node label
// keys (no prefix matching) that get mirrored. kubelet supplies these
// directly with no `/`-bearing namespace beyond what's already in the
// key, e.g. `kubernetes.io/hostname`.
var syncedLabelExactKeys = map[string]struct{}{
	"kubernetes.io/hostname": {},
}

// NodeLabelSyncReconciler watches corev1.Node objects and propagates
// kubelet-supplied labels (e.g. `topology.kubernetes.io/zone`) into
// the matching blockstor `Node` CRD's `Spec.Props["Aux/<full-key>"]`.
//
// Without this, an RG / StorageClass selector like
// `replicasOnSame: "topology.kubernetes.io/zone=z1"` matches no
// nodes and the placer silently picks topology-blind replicas —
// the operator-perceived "the StorageClass setting doesn't work"
// bug (scenario 2.13 in tests/scenarios/02-placement.md).
//
// # Precedence design — labels-win-under-synced-prefix-only
//
// Two writers can touch `Node.Spec.Props`:
//
//  1. The label-sync reconciler (this controller), driven by
//     corev1.Node labels under syncedLabelPrefixes /
//     syncedLabelExactKeys.
//  2. The operator, setting arbitrary `Aux/...` props via the REST
//     API or `kubectl edit`.
//
// We resolve the overlap by scoping ownership: this controller
// only touches `Aux/<full-label-key>` slots for keys that match an
// allow-list entry. The SSA write uses field-manager
// `LabelSyncFieldOwner` with `ForceOwnership` so kubelet labels
// always win for those specific Aux keys. Operator-written Aux
// keys OUTSIDE the allow-list (e.g. `Aux/deploy-zone` from
// scenario 2.14) stay completely untouched — different field
// manager, different ownership slot, no conflict.
//
// This is intentionally distinct from
// pkg/store/k8s/nodes.go.foldTopologyLabels, which is the inverse
// shim: it copies labels set on the **blockstor Node CRD itself**
// (under prefix `topology.blockstor.io/`) into Props on GET. That
// path operates on the CRD's own labels and only on the wire; it
// never reads corev1.Node and never persists to Spec.Props.
type NodeLabelSyncReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile pulls the corev1.Node's labels into the matching
// blockstor Node CRD's `Spec.Props["Aux/<full-key>"]` under the
// allow-list. Missing corev1.Node or missing blockstor Node are
// both no-ops — when the satellite exists for a k8s node, it'll
// show up here; when the k8s node is being deleted, the satellite
// teardown path takes over.
func (r *NodeLabelSyncReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("node", req.Name)

	// Fetch the corev1.Node. NotFound → either the node was just
	// deleted (the blockstor Node teardown drives via a separate
	// path) or the controller saw a delete event with a tombstone.
	// Either way, nothing to sync.
	var kubeNode corev1.Node

	err := r.Get(ctx, req.NamespacedName, &kubeNode)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get corev1.Node")
	}

	// Fetch the blockstor Node CRD with the same name. If absent,
	// there's no satellite yet — Node CRDs land via the REST
	// register path; once one appears the apiserver event will
	// drive a re-reconcile here on the next label edit (or on
	// satellite re-creation if the kube-node is re-labeled).
	var blockstorNode blockstoriov1alpha1.Node

	err = r.Get(ctx, req.NamespacedName, &blockstorNode)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get blockstor Node")
	}

	desired := desiredAuxFromLabels(kubeNode.Labels)
	current := currentSyncedAux(blockstorNode.Spec.Props)

	if mapsEqual(desired, current) {
		// Common steady-state path: nothing to do. Skip the SSA
		// write so we don't churn resourceVersion on every
		// Node status update (kubelet stamps Status fields
		// every few seconds even when labels don't change).
		return ctrl.Result{}, nil
	}

	// Build the merged Props: take everything we don't own
	// (anything NOT under syncedLabelPrefixes /
	// syncedLabelExactKeys), then overlay the desired Aux keys.
	// Even with SSA field-owner scoping, sending the full Props
	// map keeps the on-the-wire payload self-contained for
	// debuggability.
	merged := mergeProps(blockstorNode.Spec.Props, desired)

	apply := &blockstoriov1alpha1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind:       nodeKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: blockstorNode.Name},
		Spec: blockstoriov1alpha1.NodeSpec{
			// Type is required by the CRD schema; carry the
			// existing value so SSA validation passes. We do
			// not author Type — another field manager owns it.
			Type:  blockstorNode.Spec.Type,
			Props: merged,
		},
	}

	err = r.Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner(LabelSyncFieldOwner),
		client.ForceOwnership)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "ssa label-sync for node %q", blockstorNode.Name)
	}

	log.Info("synced k8s node labels to blockstor Node Aux props",
		"keys", len(desired))

	return ctrl.Result{}, nil
}

// SetupWithManager wires the reconciler against corev1.Node with a
// label-change predicate — without it, every kubelet status update
// (heartbeat, capacity, conditions) would trigger a reconcile and
// hammer the apiserver with no-op SSA writes.
func (r *NodeLabelSyncReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		Named("node-label-sync").
		For(&corev1.Node{},
			builder.WithPredicates(predicate.Or(
				predicate.LabelChangedPredicate{},
				// Create / Delete still fire — LabelChangedPredicate
				// only filters Update. Create gives us the initial
				// sync; Delete is a no-op (the Get returns NotFound).
				predicate.Funcs{
					CreateFunc:  func(_ event.CreateEvent) bool { return true },
					DeleteFunc:  func(_ event.DeleteEvent) bool { return false },
					GenericFunc: func(_ event.GenericEvent) bool { return false },
				},
			))).
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register NodeLabelSyncReconciler")
	}

	return nil
}

// desiredAuxFromLabels projects the kubelet-supplied labels under
// syncedLabelPrefixes / syncedLabelExactKeys into the
// `Aux/<full-label-key>` shape consumed by LINSTOR autoplacer
// auxKey lookups.
func desiredAuxFromLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))

	for k, v := range labels {
		if !isSyncedLabel(k) {
			continue
		}

		out["Aux/"+k] = v
	}

	return out
}

// currentSyncedAux extracts the slice of an existing Props map that
// represents prior label-sync output — Aux keys whose suffix matches
// a synced-label prefix / exact key. Used to detect drift and skip
// no-op writes.
func currentSyncedAux(props map[string]string) map[string]string {
	out := make(map[string]string)

	for k, v := range props {
		suffix, ok := strings.CutPrefix(k, "Aux/")
		if !ok {
			continue
		}

		if !isSyncedLabel(suffix) {
			continue
		}

		out[k] = v
	}

	return out
}

// mergeProps returns a new Props map: every entry from existing
// that is NOT a synced Aux slot is carried over, then every entry
// in desired is overlaid. The result is what the SSA write sends.
func mergeProps(existing, desired map[string]string) map[string]string {
	merged := make(map[string]string, len(existing)+len(desired))

	for k, v := range existing {
		if suffix, ok := strings.CutPrefix(k, "Aux/"); ok && isSyncedLabel(suffix) {
			// Drop — desired authoritative for these slots.
			continue
		}

		merged[k] = v
	}

	maps.Copy(merged, desired)

	return merged
}

// isSyncedLabel reports whether a corev1.Node label key falls under
// the allow-list this reconciler manages.
func isSyncedLabel(key string) bool {
	if _, ok := syncedLabelExactKeys[key]; ok {
		return true
	}

	for _, prefix := range syncedLabelPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	return false
}

// mapsEqual is a shallow equality check on two string-keyed maps.
// `reflect.DeepEqual` would work, but it's reflective and this hot
// path runs on every Node event.
func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}

	return true
}
