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

type resourceGroups struct {
	c ctrlclient.Client
}

func (s *resourceGroups) List(ctx context.Context) ([]apiv1.ResourceGroup, error) {
	var crdList crdv1alpha1.ResourceGroupList

	err := s.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list ResourceGroup CRDs")
	}

	out := make([]apiv1.ResourceGroup, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireRG(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *resourceGroups) Get(ctx context.Context, name string) (apiv1.ResourceGroup, error) {
	var crd crdv1alpha1.ResourceGroup

	err := s.c.Get(ctx, types.NamespacedName{Name: Name(name)}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.ResourceGroup{}, errors.Wrapf(store.ErrNotFound, "resource group %q", name)
		}

		return apiv1.ResourceGroup{}, errors.Wrapf(err, "get ResourceGroup %q", name)
	}

	return crdToWireRG(&crd), nil
}

func (s *resourceGroups) Create(ctx context.Context, in *apiv1.ResourceGroup) error {
	if in == nil {
		return errors.New("nil ResourceGroup")
	}

	crd := wireToCRDRG(in)

	err := s.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "resource group %q", in.Name)
		}

		return errors.Wrapf(err, "create ResourceGroup %q", in.Name)
	}

	return nil
}

func (s *resourceGroups) Update(ctx context.Context, in *apiv1.ResourceGroup) error {
	if in == nil {
		return errors.New("nil ResourceGroup")
	}

	// RetryOnConflict mirrors the RD store: the RG reconciler
	// writes Status concurrently with REST `linstor rg modify` /
	// linstor-csi's `sc-XXX` provisioning patch, which racy-conflicts
	// with "the object has been modified". Same Get-modify-Update
	// pattern, re-fetched on each retry.
	return errors.Wrapf(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var existing crdv1alpha1.ResourceGroup

		err := s.c.Get(ctx, types.NamespacedName{Name: Name(in.Name)}, &existing)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.Wrapf(store.ErrNotFound, "resource group %q", in.Name)
			}

			return errors.Wrapf(err, "get ResourceGroup %q", in.Name)
		}

		existing.Spec = wireToCRDRGSpec(in)

		mergeUserAnnotationsInto(&existing.ObjectMeta, in.Annotations)

		return s.c.Update(ctx, &existing)
	}), "update ResourceGroup %q", in.Name)
}

func (s *resourceGroups) Delete(ctx context.Context, name string) error {
	crd := &crdv1alpha1.ResourceGroup{ObjectMeta: metav1.ObjectMeta{Name: Name(name)}}

	err := s.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource group %q", name)
		}

		return errors.Wrapf(err, "delete ResourceGroup %q", name)
	}

	return nil
}

func crdToWireRG(crd *crdv1alpha1.ResourceGroup) apiv1.ResourceGroup {
	props := mergeProps(crd.Spec.Props, typedToProps(crd.Spec.DRBDOptions, crd.Spec.ExtraProps))

	out := apiv1.ResourceGroup{
		Name:        OriginalName(&crd.ObjectMeta),
		Description: crd.Spec.Description,
		Props:       props,
		Annotations: userAnnotations(crd.Annotations),
		PeerSlots:   crd.Spec.PeerSlots,
		UUID:        string(crd.UID),
	}

	out.SelectFilter = apiv1.AutoSelectFilter{
		PlaceCount:              apiv1.LaxInt32(crd.Spec.SelectFilter.PlaceCount),
		StoragePool:             crd.Spec.SelectFilter.StoragePool,
		StoragePoolList:         crd.Spec.SelectFilter.StoragePoolList,
		StoragePoolDisklessList: crd.Spec.SelectFilter.StoragePoolDisklessList,
		NodeNameList:            crd.Spec.SelectFilter.NodeNameList,
		ReplicasOnSame:          crd.Spec.SelectFilter.ReplicasOnSame,
		ReplicasOnDifferent:     crd.Spec.SelectFilter.ReplicasOnDifferent,
		NotPlaceWithRsc:         crd.Spec.SelectFilter.NotPlaceWithRsc,
		NotPlaceWithRscRegex:    crd.Spec.SelectFilter.NotPlaceWithRscRegex,
		LayerStack:              crd.Spec.SelectFilter.LayerStack,
		ProviderList:            crd.Spec.SelectFilter.ProviderList,
		DisklessOnRemaining:     crd.Spec.SelectFilter.DisklessOnRemaining,
	}

	if len(crd.Spec.VolumeGroups) > 0 {
		out.VolumeGroups = make([]apiv1.VolumeGroup, 0, len(crd.Spec.VolumeGroups))
		for i := range crd.Spec.VolumeGroups {
			vg := &crd.Spec.VolumeGroups[i]
			out.VolumeGroups = append(out.VolumeGroups, apiv1.VolumeGroup{
				VolumeNumber: vg.VolumeNumber,
				Props:        vg.Props,
				Flags:        vg.Flags,
			})
		}
	}

	return out
}

func wireToCRDRG(in *apiv1.ResourceGroup) *crdv1alpha1.ResourceGroup {
	crd := &crdv1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        Name(in.Name),
			Annotations: cloneAnnotations(in.Annotations),
		},
		Spec: wireToCRDRGSpec(in),
	}
	SetOriginalName(&crd.ObjectMeta, in.Name)

	return crd
}

func wireToCRDRGSpec(in *apiv1.ResourceGroup) crdv1alpha1.ResourceGroupSpec {
	typed, extras := propsToTyped(in.Props)
	residual := stripDRBDProps(in.Props)

	spec := crdv1alpha1.ResourceGroupSpec{
		Description: in.Description,
		Props:       residual,
		DRBDOptions: typed,
		ExtraProps:  extras,
		PeerSlots:   in.PeerSlots,
		SelectFilter: crdv1alpha1.ResourceGroupSelectFilter{
			PlaceCount:              int32(in.SelectFilter.PlaceCount),
			StoragePool:             in.SelectFilter.StoragePool,
			StoragePoolList:         in.SelectFilter.StoragePoolList,
			StoragePoolDisklessList: in.SelectFilter.StoragePoolDisklessList,
			NodeNameList:            in.SelectFilter.NodeNameList,
			ReplicasOnSame:          in.SelectFilter.ReplicasOnSame,
			ReplicasOnDifferent:     in.SelectFilter.ReplicasOnDifferent,
			NotPlaceWithRsc:         in.SelectFilter.NotPlaceWithRsc,
			NotPlaceWithRscRegex:    in.SelectFilter.NotPlaceWithRscRegex,
			LayerStack:              in.SelectFilter.LayerStack,
			ProviderList:            in.SelectFilter.ProviderList,
			DisklessOnRemaining:     in.SelectFilter.DisklessOnRemaining,
		},
	}

	if len(in.VolumeGroups) > 0 {
		spec.VolumeGroups = make([]crdv1alpha1.ResourceGroupVolumeGroup, 0, len(in.VolumeGroups))
		for i := range in.VolumeGroups {
			vg := &in.VolumeGroups[i]
			spec.VolumeGroups = append(spec.VolumeGroups, crdv1alpha1.ResourceGroupVolumeGroup{
				VolumeNumber: vg.VolumeNumber,
				Props:        vg.Props,
				Flags:        vg.Flags,
			})
		}
	}

	return spec
}
