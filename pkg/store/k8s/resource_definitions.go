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

package k8s

import (
	"context"
	"maps"
	"sort"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

type resourceDefinitions struct {
	c ctrlclient.Client
}

func (s *resourceDefinitions) List(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	var crdList crdv1alpha1.ResourceDefinitionList

	err := s.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list ResourceDefinition CRDs")
	}

	out := make([]apiv1.ResourceDefinition, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireRD(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *resourceDefinitions) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	var crd crdv1alpha1.ResourceDefinition

	err := s.c.Get(ctx, types.NamespacedName{Name: Name(name)}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.ResourceDefinition{}, errors.Wrapf(store.ErrNotFound, "resource definition %q", name)
		}

		return apiv1.ResourceDefinition{}, errors.Wrapf(err, "get ResourceDefinition %q", name)
	}

	return crdToWireRD(&crd), nil
}

func (s *resourceDefinitions) Create(ctx context.Context, in *apiv1.ResourceDefinition) error {
	if in == nil {
		return errors.New("nil ResourceDefinition")
	}

	crd := wireToCRDRD(in)

	err := s.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "resource definition %q", in.Name)
		}

		return errors.Wrapf(err, "create ResourceDefinition %q", in.Name)
	}

	return nil
}

func (s *resourceDefinitions) Update(ctx context.Context, in *apiv1.ResourceDefinition) error {
	if in == nil {
		return errors.New("nil ResourceDefinition")
	}

	// RetryOnConflict wraps the get-modify-update cycle: the RD
	// reconciler writes Status / finalizer concurrently with the
	// REST shim's spec updates (linstor-csi DeleteVolume runs a
	// finaliser-strip + spec patch under apiserver load). A bare
	// Update racy-conflicts and surfaces as "the object has been
	// modified" in csi-sanity's AfterEach cleanup — re-fetch
	// before retrying is the apiserver-recommended pattern.
	return errors.Wrapf(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var existing crdv1alpha1.ResourceDefinition

		err := s.c.Get(ctx, types.NamespacedName{Name: Name(in.Name)}, &existing)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.Wrapf(store.ErrNotFound, "resource definition %q", in.Name)
			}

			return errors.Wrapf(err, "get ResourceDefinition %q", in.Name)
		}

		// VolumeDefinitions live inline on the CRD spec but have
		// no counterpart on the wire `ResourceDefinition` shape —
		// upstream LINSTOR addresses VDs through a separate
		// `/v1/.../volume-definitions` endpoint family.
		// `wireToCRDRDSpec` builds a fresh spec from the wire
		// shape, so a naïve replace would silently wipe the
		// inline VolumeDefinitions on every RD update (linstor rd
		// set-property, RG-rebind, etc.). Carry them across
		// explicitly. Status volumes are on a subresource and
		// aren't touched by spec writes.
		//
		// The typed Encryption pointer is the same shape of
		// problem (Bug 209): operator-only, no wire counterpart,
		// silently wiped on every routine RD modify, which
		// downgrades subsequent volumes to unencrypted. Carry it
		// across alongside VolumeDefinitions.
		prevVDs := existing.Spec.VolumeDefinitions
		prevEncryption := existing.Spec.Encryption
		existing.Spec = wireToCRDRDSpec(in)
		existing.Spec.VolumeDefinitions = prevVDs
		existing.Spec.Encryption = prevEncryption

		mergeUserAnnotationsInto(&existing.ObjectMeta, in.Annotations)

		return s.c.Update(ctx, &existing)
	}), "update ResourceDefinition %q", in.Name)
}

// mergeUserAnnotationsInto applies the wire-side annotation map
// onto existing.Annotations. Behavior:
//   - nil wire input: no-op (preserves whatever the controller-side
//     reconciler may have stamped under blockstor.io/* keys).
//   - non-nil: replaces ALL non-blockstor.io annotations with the
//     wire set; blockstor.io/* keys (the OriginalName store-internal
//     marker) survive.
//
// Phase 10.4: lets the REST KV-store handler write
// `blockstor.io/csi-volume-data` via Update without losing the
// store-internal `blockstor.io/original-name`.
func mergeUserAnnotationsInto(meta *metav1.ObjectMeta, wire map[string]string) {
	if wire == nil {
		return
	}

	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}

	// Drop existing user keys.
	for k := range meta.Annotations {
		if k == AnnotationLinstorName {
			continue
		}

		delete(meta.Annotations, k)
	}

	maps.Copy(meta.Annotations, wire)

	if len(meta.Annotations) == 0 {
		meta.Annotations = nil
	}
}

// PatchResourceDefinitionSpec runs `mutate` against the freshly-fetched
// current wire-shape ResourceDefinition and persists the mutated value
// via a typed-Patch (JSON-merge-patch). On 409 the cycle re-runs against
// fresh state, with `mutate` re-applied to the live wire value — so
// concurrent disjoint edits converge instead of clobbering one another.
//
// Distinct from `Update`, whose `RetryOnConflict` loop replays the
// stale wire-side snapshot the caller built once before the loop and
// therefore re-applies the same stale write on every retry (Bug 204b).
// As with `Update`, the parent RD's inline Spec.VolumeDefinitions are
// preserved across the spec rebuild (VDs have no wire-side counterpart)
// and user annotations are merged through `mergeUserAnnotationsInto`.
func (s *resourceDefinitions) PatchResourceDefinitionSpec(ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return errors.Wrapf(retry.RetryOnConflict(patchRetryBackoff(), func() error {
		var existing crdv1alpha1.ResourceDefinition

		err := s.c.Get(ctx, types.NamespacedName{Name: Name(name)}, &existing)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.Wrapf(store.ErrNotFound, "resource definition %q", name)
			}

			return errors.Wrapf(err, "get ResourceDefinition %q", name)
		}

		base := existing.DeepCopy()

		// Surface as wire shape so the closure runs in the same
		// vocabulary as REST handlers.
		wire := crdToWireRD(&existing)

		err = mutate(&wire)
		if err != nil {
			return err
		}

		// Preserve inline VolumeDefinitions and the typed Encryption
		// pointer — both are operator-only with no wire-side
		// counterpart and would be wiped by a naïve spec rebuild.
		// Mirrors the carry-across in `Update` (Bug 206 / Bug 209).
		prevVDs := existing.Spec.VolumeDefinitions
		prevEncryption := existing.Spec.Encryption
		existing.Spec = wireToCRDRDSpec(&wire)
		existing.Spec.VolumeDefinitions = prevVDs
		existing.Spec.Encryption = prevEncryption

		mergeUserAnnotationsInto(&existing.ObjectMeta, wire.Annotations)

		return s.c.Patch(ctx, &existing, ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}))
	}), "patch ResourceDefinition %q", name)
}

func (s *resourceDefinitions) Delete(ctx context.Context, name string) error {
	crd := &crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: Name(name)}}

	err := s.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource definition %q", name)
		}

		return errors.Wrapf(err, "delete ResourceDefinition %q", name)
	}

	return nil
}

func crdToWireRD(crd *crdv1alpha1.ResourceDefinition) apiv1.ResourceDefinition {
	// Re-emit typed DRBDOptions + ExtraProps back as a flat Props
	// bag so golinstor (which only knows the upstream
	// `props["DrbdOptions/..."]` shape) keeps working unmodified
	// across the typed-fields migration.
	props := mergeProps(crd.Spec.Props, typedToProps(crd.Spec.DRBDOptions, crd.Spec.ExtraProps))

	out := apiv1.ResourceDefinition{
		Name:              OriginalName(&crd.ObjectMeta),
		ExternalName:      crd.Spec.ExternalName,
		ResourceGroupName: crd.Spec.ResourceGroupName,
		Props:             props,
		Annotations:       userAnnotations(crd.Annotations),
		Flags:             crd.Spec.Flags,
		LayerStack:        crd.Spec.LayerStack,
		UUID:              string(crd.UID),
	}

	return out
}

// userAnnotations strips the store-internal annotation keys
// (`blockstor.io/original-name` etc.) from a CRD's metadata.annotations
// before surfacing it on the wire — only operator/CSI-managed keys
// (e.g. `blockstor.io/csi-volume-data`) reach the REST layer.
// Returns nil when no user-facing annotation survives the filter.
func userAnnotations(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))

	for k, v := range in {
		if k == AnnotationLinstorName {
			continue
		}

		out[k] = v
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func wireToCRDRD(in *apiv1.ResourceDefinition) *crdv1alpha1.ResourceDefinition {
	crd := &crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:        Name(in.Name),
			Annotations: cloneAnnotations(in.Annotations),
		},
		Spec: wireToCRDRDSpec(in),
	}
	SetOriginalName(&crd.ObjectMeta, in.Name)

	return crd
}

// cloneAnnotations returns a defensive copy of the wire annotation
// map. nil → nil. Used by Create/Update so callers can mutate the
// input apiv1.ResourceDefinition without us silently writing through.
func cloneAnnotations(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	return maps.Clone(in)
}

func wireToCRDRDSpec(in *apiv1.ResourceDefinition) crdv1alpha1.ResourceDefinitionSpec {
	// Split the wire Props bag into typed DRBDOptions (recognised
	// keys → admission-validated structured fields) + ExtraProps
	// (unknown DrbdOptions/* keys, kept as forward-compat shim).
	// Non-DRBD props (StorPoolName, Aux/zone, …) stay in Spec.Props
	// for now — Phase 10.3 only owns the DRBD slice; Phase 10.4
	// drops Spec.Props entirely.
	typed, extras := propsToTyped(in.Props)
	residual := stripDRBDProps(in.Props)

	return crdv1alpha1.ResourceDefinitionSpec{
		ExternalName:      in.ExternalName,
		ResourceGroupName: in.ResourceGroupName,
		Props:             residual,
		Flags:             in.Flags,
		LayerStack:        in.LayerStack,
		DRBDOptions:       typed,
		ExtraProps:        extras,
	}
}
