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

	parents, err := s.collectParentRDs(ctx, crdList.Items)
	if err != nil {
		return nil, err
	}

	out := make([]apiv1.Snapshot, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireSnapshot(&crdList.Items[i], parents[crdList.Items[i].Spec.ResourceDefinitionName]))
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

	parent, _ := s.getParentRD(ctx, rdName)

	out := make([]apiv1.Snapshot, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireSnapshot(&crdList.Items[i], parent))
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

	parent, _ := s.getParentRD(ctx, crd.Spec.ResourceDefinitionName)

	return crdToWireSnapshot(&crd, parent), nil
}

// getParentRD fetches the parent ResourceDefinition for a Snapshot,
// returning nil + the not-found error when the parent has already
// been deleted (orphan Snapshot — the view layer still has to render
// the snapshot row without panicking on nil-deref).
func (s *snapshots) getParentRD(ctx context.Context, rdName string) (*crdv1alpha1.ResourceDefinition, error) {
	if rdName == "" {
		return nil, nil //nolint:nilnil // empty name == no parent to look up
	}

	var rd crdv1alpha1.ResourceDefinition

	err := s.c.Get(ctx, types.NamespacedName{Name: Name(rdName)}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil //nolint:nilnil // orphan snapshot — no parent
		}

		return nil, errors.Wrapf(err, "get parent RD %q for Snapshot", rdName)
	}

	return &rd, nil
}

// collectParentRDs batches the parent-RD lookups for a List call so
// the view of 50 snapshots-on-the-same-RD doesn't trigger 50 separate
// API server GETs. Missing parents (orphan Snapshots) silently
// produce a nil entry in the map — the wire-shape conversion folds
// nil into empty maps.
func (s *snapshots) collectParentRDs(
	ctx context.Context, snaps []crdv1alpha1.Snapshot,
) (map[string]*crdv1alpha1.ResourceDefinition, error) {
	out := make(map[string]*crdv1alpha1.ResourceDefinition, len(snaps))

	for i := range snaps {
		rdName := snaps[i].Spec.ResourceDefinitionName

		if _, seen := out[rdName]; seen {
			continue
		}

		rd, err := s.getParentRD(ctx, rdName)
		if err != nil {
			return nil, err
		}

		out[rdName] = rd
	}

	return out, nil
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

	// RetryOnConflict mirrors the RD/RG store fixes (ee2f4af, bfa98f5):
	// the satellite reconciler stamps `Status.NodeStatus` concurrently
	// with REST snapshot-prop patches, which racy-conflict with "the
	// object has been modified". Re-fetch + retry handles it.
	return errors.Wrapf(retry.RetryOnConflict(retry.DefaultRetry, func() error {
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
		mergeUserAnnotationsInto(&existing.ObjectMeta, in.Annotations)

		return s.c.Update(ctx, &existing)
	}), "update Snapshot %s/%s", in.ResourceName, in.Name)
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

// crdToWireSnapshot converts the Snapshot CRD into the wire DTO.
// `parent` is the parent ResourceDefinition CRD at conversion time —
// nil when the parent has been deleted (orphan Snapshot). The parent
// supplies the `resource_definition_props` map and per-volume
// `volume_definition_props` blocks F20 added to the snapshot DTO.
func crdToWireSnapshot(
	crd *crdv1alpha1.Snapshot, parent *crdv1alpha1.ResourceDefinition,
) apiv1.Snapshot {
	out := apiv1.Snapshot{
		Name:                    crd.Spec.SnapshotName,
		ResourceName:            crd.Spec.ResourceDefinitionName,
		Nodes:                   crd.Spec.Nodes,
		Props:                   crd.Spec.Props,
		Annotations:             userAnnotations(crd.Annotations),
		UUID:                    string(crd.UID),
		SnapshotDefinitionProps: crd.Spec.Props,
	}

	// Parent-RD-derived fields — guarded by nil-check so an orphan
	// Snapshot whose RD has already been deleted still renders.
	var vdPropsByNumber map[int32]map[string]string

	if parent != nil {
		out.ResourceDefinitionProps = parent.Spec.Props

		vdPropsByNumber = make(map[int32]map[string]string, len(parent.Spec.VolumeDefinitions))
		for i := range parent.Spec.VolumeDefinitions {
			vd := &parent.Spec.VolumeDefinitions[i]
			if len(vd.Props) > 0 {
				vdPropsByNumber[vd.VolumeNumber] = vd.Props
			}
		}
	}

	if len(crd.Spec.VolumeDefinitions) > 0 {
		out.VolumeDefinitions = make([]apiv1.SnapshotVolumeDef, 0, len(crd.Spec.VolumeDefinitions))
		for i := range crd.Spec.VolumeDefinitions {
			out.VolumeDefinitions = append(out.VolumeDefinitions, apiv1.SnapshotVolumeDef{
				VolumeNumber:          crd.Spec.VolumeDefinitions[i].VolumeNumber,
				SizeKib:               crd.Spec.VolumeDefinitions[i].SizeKib,
				VolumeDefinitionProps: vdPropsByNumber[crd.Spec.VolumeDefinitions[i].VolumeNumber],
			})
		}
	}

	switch {
	case len(crd.Status.NodeStatus) > 0:
		out.Snapshots = make([]apiv1.SnapshotPerNode, 0, len(crd.Status.NodeStatus))
		for i := range crd.Status.NodeStatus {
			out.Snapshots = append(out.Snapshots, apiv1.SnapshotPerNode{
				SnapshotName:    crd.Spec.SnapshotName,
				NodeName:        crd.Status.NodeStatus[i].NodeName,
				CreateTimestamp: crd.Status.NodeStatus[i].CreateTimestamp,
				SnapshotVolumes: snapshotVolumesFromVDs(crd.Spec.VolumeDefinitions),
			})
		}
	case len(crd.Spec.Nodes) > 0:
		// Status.NodeStatus is satellite-reported and lands after the
		// satellite reconciler picks up the new Snapshot CRD. The
		// REST shim's view of "where the snapshot landed" needs to
		// be visible immediately after CreateSnapshot — linstor-csi
		// hard-fails ListSnapshots with "missing snapshots" when
		// the per-node Snapshots[] is empty. Synthesise one
		// SnapshotPerNode entry per Spec.Nodes target so the wire
		// shape matches upstream LINSTOR's "all replicas have a
		// SnapshotNode entry once the controller commits the
		// definition" semantic.
		out.Snapshots = make([]apiv1.SnapshotPerNode, 0, len(crd.Spec.Nodes))
		for _, node := range crd.Spec.Nodes {
			out.Snapshots = append(out.Snapshots, apiv1.SnapshotPerNode{
				SnapshotName:    crd.Spec.SnapshotName,
				NodeName:        node,
				SnapshotVolumes: snapshotVolumesFromVDs(crd.Spec.VolumeDefinitions),
			})
		}
	}

	return out
}

// snapshotVolumesFromVDs derives the per-node `snapshot_volumes[]`
// array from the snapshot's volume definitions. Each VD slot
// surfaces as one SnapshotVolume entry — upstream's CLI reads the
// `vlm_nr` column from this. `state` is left blank: blockstor does
// not yet track per-volume per-node snapshot state, but the slot
// is still emitted so the CLI table renders the volume_number
// column without an empty list.
func snapshotVolumesFromVDs(vds []crdv1alpha1.SnapshotVolumeRef) []apiv1.SnapshotVolume {
	if len(vds) == 0 {
		return nil
	}

	out := make([]apiv1.SnapshotVolume, 0, len(vds))
	for i := range vds {
		out = append(out, apiv1.SnapshotVolume{
			VolumeNumber: vds[i].VolumeNumber,
		})
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
			Annotations: cloneAnnotations(in.Annotations),
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
