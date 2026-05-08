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

	var existing crdv1alpha1.ResourceDefinition

	err := s.c.Get(ctx, types.NamespacedName{Name: Name(in.Name)}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource definition %q", in.Name)
		}

		return errors.Wrapf(err, "get ResourceDefinition %q", in.Name)
	}

	existing.Spec = wireToCRDRDSpec(in)

	err = s.c.Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "update ResourceDefinition %q", in.Name)
	}

	return nil
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
	out := apiv1.ResourceDefinition{
		Name:              OriginalName(&crd.ObjectMeta),
		ExternalName:      crd.Spec.ExternalName,
		ResourceGroupName: crd.Spec.ResourceGroupName,
		Props:             crd.Spec.Props,
		Flags:             crd.Spec.Flags,
		UUID:              string(crd.UID),
	}

	return out
}

func wireToCRDRD(in *apiv1.ResourceDefinition) *crdv1alpha1.ResourceDefinition {
	crd := &crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: Name(in.Name)},
		Spec:       wireToCRDRDSpec(in),
	}
	SetOriginalName(&crd.ObjectMeta, in.Name)

	return crd
}

func wireToCRDRDSpec(in *apiv1.ResourceDefinition) crdv1alpha1.ResourceDefinitionSpec {
	return crdv1alpha1.ResourceDefinitionSpec{
		ExternalName:      in.ExternalName,
		ResourceGroupName: in.ResourceGroupName,
		Props:             in.Props,
		Flags:             in.Flags,
	}
}
