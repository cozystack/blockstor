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

// LabelSnapshot is the label that lets us List snapshots by parent RD.
const LabelSnapshot = "blockstor.io/snapshot-name"

type snapshots struct {
	c ctrlclient.Client
}

func snapshotCRDName(rdName, snapName string) string {
	return Name(rdName + "." + snapName)
}

func (s *snapshots) List(ctx context.Context) ([]apiv1.Snapshot, error) {
	var crdList crdv1alpha1.SnapshotList

	err := s.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list Snapshot CRDs")
	}

	out := make([]apiv1.Snapshot, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireSnapshot(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ResourceName != out[j].ResourceName {
			return out[i].ResourceName < out[j].ResourceName
		}

		return out[i].Name < out[j].Name
	})

	return out, nil
}

func (s *snapshots) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	var crdList crdv1alpha1.SnapshotList

	err := s.c.List(ctx, &crdList,
		ctrlclient.MatchingLabels{LabelResourceDefinition: rdName})
	if err != nil {
		return nil, errors.Wrapf(err, "list Snapshot CRDs for RD %q", rdName)
	}

	out := make([]apiv1.Snapshot, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireSnapshot(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *snapshots) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	var crd crdv1alpha1.Snapshot

	err := s.c.Get(ctx, types.NamespacedName{Name: snapshotCRDName(rdName, snapName)}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.Snapshot{}, errors.Wrapf(store.ErrNotFound, "snapshot %q on RD %q", snapName, rdName)
		}

		return apiv1.Snapshot{}, errors.Wrapf(err, "get Snapshot %s/%s", rdName, snapName)
	}

	return crdToWireSnapshot(&crd), nil
}

func (s *snapshots) Create(ctx context.Context, in *apiv1.Snapshot) error {
	if in == nil {
		return errors.New("nil Snapshot")
	}

	crd := wireToCRDSnapshot(in)

	err := s.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "snapshot %q on RD %q", in.Name, in.ResourceName)
		}

		return errors.Wrapf(err, "create Snapshot %s/%s", in.ResourceName, in.Name)
	}

	return nil
}

func (s *snapshots) Update(ctx context.Context, in *apiv1.Snapshot) error {
	if in == nil {
		return errors.New("nil Snapshot")
	}

	var existing crdv1alpha1.Snapshot

	key := types.NamespacedName{Name: snapshotCRDName(in.ResourceName, in.Name)}

	err := s.c.Get(ctx, key, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "snapshot %q on RD %q", in.Name, in.ResourceName)
		}

		return errors.Wrapf(err, "get Snapshot %s/%s", in.ResourceName, in.Name)
	}

	existing.Spec = wireToCRDSnapshotSpec(in)

	err = s.c.Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "update Snapshot %s/%s", in.ResourceName, in.Name)
	}

	return nil
}

func (s *snapshots) Delete(ctx context.Context, rdName, snapName string) error {
	crd := &crdv1alpha1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: snapshotCRDName(rdName, snapName)}}

	err := s.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "snapshot %q on RD %q", snapName, rdName)
		}

		return errors.Wrapf(err, "delete Snapshot %s/%s", rdName, snapName)
	}

	return nil
}

func crdToWireSnapshot(crd *crdv1alpha1.Snapshot) apiv1.Snapshot {
	out := apiv1.Snapshot{
		Name:         crd.Spec.SnapshotName,
		ResourceName: crd.Spec.ResourceDefinitionName,
		Nodes:        crd.Spec.Nodes,
		Props:        crd.Spec.Props,
		UUID:         string(crd.UID),
	}

	if len(crd.Spec.VolumeDefinitions) > 0 {
		out.VolumeDefinitions = make([]apiv1.SnapshotVolumeDef, 0, len(crd.Spec.VolumeDefinitions))
		for i := range crd.Spec.VolumeDefinitions {
			out.VolumeDefinitions = append(out.VolumeDefinitions, apiv1.SnapshotVolumeDef{
				VolumeNumber: crd.Spec.VolumeDefinitions[i].VolumeNumber,
				SizeKib:      crd.Spec.VolumeDefinitions[i].SizeKib,
			})
		}
	}

	if len(crd.Status.NodeStatus) > 0 {
		out.Snapshots = make([]apiv1.SnapshotPerNode, 0, len(crd.Status.NodeStatus))
		for i := range crd.Status.NodeStatus {
			out.Snapshots = append(out.Snapshots, apiv1.SnapshotPerNode{
				SnapshotName:    crd.Spec.SnapshotName,
				NodeName:        crd.Status.NodeStatus[i].NodeName,
				CreateTimestamp: crd.Status.NodeStatus[i].CreateTimestamp,
			})
		}
	}

	return out
}

func wireToCRDSnapshot(in *apiv1.Snapshot) *crdv1alpha1.Snapshot {
	return &crdv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapshotCRDName(in.ResourceName, in.Name),
			Labels: map[string]string{
				LabelResourceDefinition: in.ResourceName,
				LabelSnapshot:           in.Name,
			},
		},
		Spec: wireToCRDSnapshotSpec(in),
	}
}

func wireToCRDSnapshotSpec(in *apiv1.Snapshot) crdv1alpha1.SnapshotSpec {
	spec := crdv1alpha1.SnapshotSpec{
		ResourceDefinitionName: in.ResourceName,
		SnapshotName:           in.Name,
		Nodes:                  in.Nodes,
		Props:                  in.Props,
	}

	if len(in.VolumeDefinitions) > 0 {
		spec.VolumeDefinitions = make([]crdv1alpha1.SnapshotVolumeRef, 0, len(in.VolumeDefinitions))
		for i := range in.VolumeDefinitions {
			spec.VolumeDefinitions = append(spec.VolumeDefinitions, crdv1alpha1.SnapshotVolumeRef{
				VolumeNumber: in.VolumeDefinitions[i].VolumeNumber,
				SizeKib:      in.VolumeDefinitions[i].SizeKib,
			})
		}
	}

	return spec
}
