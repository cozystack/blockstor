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
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// volumeDefinitions stores VolumeDefinition objects inline on the parent
// ResourceDefinition CRD's spec.volumeDefinitions array. There is no
// separate VolumeDefinition CRD: upstream LINSTOR addresses VDs through the
// RD anyway, and a single CRD makes ownership/reclamation trivially correct.
type volumeDefinitions struct {
	c ctrlclient.Client
}

func (s *volumeDefinitions) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	rd, err := s.fetchRD(ctx, rdName)
	if err != nil {
		return nil, err
	}

	out := make([]apiv1.VolumeDefinition, 0, len(rd.Spec.VolumeDefinitions))
	for i := range rd.Spec.VolumeDefinitions {
		out = append(out, crdToWireVD(&rd.Spec.VolumeDefinitions[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VolumeNumber < out[j].VolumeNumber })

	return out, nil
}

func (s *volumeDefinitions) Get(ctx context.Context, rdName string, volumeNumber int32) (apiv1.VolumeDefinition, error) {
	rd, err := s.fetchRD(ctx, rdName)
	if err != nil {
		return apiv1.VolumeDefinition{}, err
	}

	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].VolumeNumber == volumeNumber {
			return crdToWireVD(&rd.Spec.VolumeDefinitions[i]), nil
		}
	}

	return apiv1.VolumeDefinition{}, errors.Wrapf(store.ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
}

func (s *volumeDefinitions) Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if vd == nil {
		return errors.New("nil VolumeDefinition")
	}

	rd, err := s.fetchRD(ctx, rdName)
	if err != nil {
		return err
	}

	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].VolumeNumber == vd.VolumeNumber {
			return errors.Wrapf(store.ErrAlreadyExists, "volume %d on resource definition %q", vd.VolumeNumber, rdName)
		}
	}

	rd.Spec.VolumeDefinitions = append(rd.Spec.VolumeDefinitions, wireToCRDVD(vd))

	err = s.c.Update(ctx, rd)
	if err != nil {
		return errors.Wrapf(err, "update RD %q to add volume %d", rdName, vd.VolumeNumber)
	}

	return nil
}

func (s *volumeDefinitions) Update(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if vd == nil {
		return errors.New("nil VolumeDefinition")
	}

	rd, err := s.fetchRD(ctx, rdName)
	if err != nil {
		return err
	}

	idx := -1

	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].VolumeNumber == vd.VolumeNumber {
			idx = i

			break
		}
	}

	if idx == -1 {
		return errors.Wrapf(store.ErrNotFound, "volume %d on resource definition %q", vd.VolumeNumber, rdName)
	}

	rd.Spec.VolumeDefinitions[idx] = wireToCRDVD(vd)

	err = s.c.Update(ctx, rd)
	if err != nil {
		return errors.Wrapf(err, "update RD %q for volume %d", rdName, vd.VolumeNumber)
	}

	return nil
}

func (s *volumeDefinitions) Delete(ctx context.Context, rdName string, volumeNumber int32) error {
	rd, err := s.fetchRD(ctx, rdName)
	if err != nil {
		return err
	}

	idx := -1

	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].VolumeNumber == volumeNumber {
			idx = i

			break
		}
	}

	if idx == -1 {
		return errors.Wrapf(store.ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
	}

	rd.Spec.VolumeDefinitions = append(rd.Spec.VolumeDefinitions[:idx], rd.Spec.VolumeDefinitions[idx+1:]...)

	err = s.c.Update(ctx, rd)
	if err != nil {
		return errors.Wrapf(err, "update RD %q to remove volume %d", rdName, volumeNumber)
	}

	return nil
}

func (s *volumeDefinitions) fetchRD(ctx context.Context, rdName string) (*crdv1alpha1.ResourceDefinition, error) {
	var rd crdv1alpha1.ResourceDefinition

	err := s.c.Get(ctx, types.NamespacedName{Name: Name(rdName)}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(store.ErrNotFound, "resource definition %q", rdName)
		}

		return nil, errors.Wrapf(err, "get ResourceDefinition %q", rdName)
	}

	return &rd, nil
}

func crdToWireVD(vd *crdv1alpha1.ResourceDefinitionVolume) apiv1.VolumeDefinition {
	return apiv1.VolumeDefinition{
		VolumeNumber: vd.VolumeNumber,
		SizeKib:      vd.SizeKib,
		Props:        vd.Props,
		Flags:        vd.Flags,
	}
}

func wireToCRDVD(vd *apiv1.VolumeDefinition) crdv1alpha1.ResourceDefinitionVolume {
	return crdv1alpha1.ResourceDefinitionVolume{
		VolumeNumber: vd.VolumeNumber,
		SizeKib:      vd.SizeKib,
		Props:        vd.Props,
		Flags:        vd.Flags,
	}
}
