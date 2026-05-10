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

// Labels used to index Resource CRDs by their composite key.
const (
	LabelResourceDefinition = "blockstor.io/resource-definition"
)

type resources struct {
	c ctrlclient.Client
}

func resourceCRDName(rd, node string) string {
	return Name(rd + "." + node)
}

func (s *resources) List(ctx context.Context) ([]apiv1.Resource, error) {
	var crdList crdv1alpha1.ResourceList

	err := s.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list Resource CRDs")
	}

	out := make([]apiv1.Resource, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireResource(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}

		return out[i].NodeName < out[j].NodeName
	})

	return out, nil
}

func (s *resources) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	var crdList crdv1alpha1.ResourceList

	// List all Resources and filter by Spec.ResourceDefinitionName
	// in-process. We used to use a label selector, but Resources
	// created directly via kubectl/manifests (rather than through
	// the REST handler that sets labels) wouldn't be matched. Cluster
	// resource counts are small enough that the in-process filter is
	// the right tradeoff for correctness over hashed-label lookups.
	err := s.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrapf(err, "list Resource CRDs for RD %q", rdName)
	}

	out := make([]apiv1.Resource, 0, len(crdList.Items))

	for i := range crdList.Items {
		if crdList.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		out = append(out, crdToWireResource(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].NodeName < out[j].NodeName })

	return out, nil
}

func (s *resources) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	var crd crdv1alpha1.Resource

	err := s.c.Get(ctx, types.NamespacedName{Name: resourceCRDName(rdName, node)}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.Resource{}, errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return apiv1.Resource{}, errors.Wrapf(err, "get Resource %s/%s", rdName, node)
	}

	return crdToWireResource(&crd), nil
}

func (s *resources) Create(ctx context.Context, in *apiv1.Resource) error {
	if in == nil {
		return errors.New("nil Resource")
	}

	crd := wireToCRDResource(in)

	err := s.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "resource %q on node %q", in.Name, in.NodeName)
		}

		return errors.Wrapf(err, "create Resource %s/%s", in.Name, in.NodeName)
	}

	return nil
}

func (s *resources) Update(ctx context.Context, in *apiv1.Resource) error {
	if in == nil {
		return errors.New("nil Resource")
	}

	var existing crdv1alpha1.Resource

	key := types.NamespacedName{Name: resourceCRDName(in.Name, in.NodeName)}

	err := s.c.Get(ctx, key, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", in.Name, in.NodeName)
		}

		return errors.Wrapf(err, "get Resource %s/%s", in.Name, in.NodeName)
	}

	existing.Spec = wireToCRDResourceSpec(in)

	err = s.c.Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "update Resource %s/%s", in.Name, in.NodeName)
	}

	return nil
}

func (s *resources) Delete(ctx context.Context, rdName, node string) error {
	crd := &crdv1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: resourceCRDName(rdName, node)}}

	err := s.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return errors.Wrapf(err, "delete Resource %s/%s", rdName, node)
	}

	return nil
}

// SatelliteFieldOwner is the SSA field-manager identity the
// satellite uses for its observed-state writes (DrbdState,
// per-volume DiskState/CurrentGi). The controller uses
// `ControllerFieldOwner` for its allocator outputs (DRBDPort,
// DRBDMinor, DRBDNodeID). Distinct field owners let SSA's merge
// algorithm preserve each side's writes when the two collide on
// the same Status subresource. Phase 10.2.
const (
	SatelliteFieldOwner  = "blockstor-satellite"
	ControllerFieldOwner = "blockstor-controller"
)

// SetState writes the runtime-observed state via Server-Side Apply
// on the Status subresource. Called from the satellite's events2
// observer path when role/disk state changes — these must not race
// concurrent Spec mutations (auto-diskful, resize, evacuation) nor
// the controller's allocator writes to DRBDPort/Minor/NodeID.
//
// SSA with `FieldOwner=blockstor-satellite` makes the merge
// per-field: only the fields the satellite explicitly sets in the
// apply object are claimed; the controller's allocator outputs
// (`DRBDPort`, `DRBDMinor`, `DRBDNodeID`) stay untouched even
// across racing Status writes. Phase 10.2.
//
// state.DrbdState lands on Status.DrbdState; per-volume
// DiskState/CurrentGi land on Status.Volumes[i] (the listMapKey is
// `volumeNumber`, so SSA matches up entries correctly).
func (s *resources) SetState(ctx context.Context, rdName, node string, state apiv1.ResourceState, volumes []apiv1.VolumeObservation) error {
	name := resourceCRDName(rdName, node)

	// Verify the Resource exists before applying — apiserver Apply
	// would happily create a half-formed Resource if it didn't,
	// and we'd rather surface NotFound to callers (events2
	// observer treats it as a convergence-pending case and skips).
	var existing crdv1alpha1.Resource

	err := s.c.Get(ctx, types.NamespacedName{Name: name}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return errors.Wrapf(err, "get Resource %s/%s", rdName, node)
	}

	apply := &crdv1alpha1.Resource{
		TypeMeta:   metav1.TypeMeta{Kind: "Resource", APIVersion: crdv1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: crdv1alpha1.ResourceStatus{
			InUse:     state.InUse,
			DrbdState: state.DrbdState,
			Volumes:   buildVolumeStatusForApply(volumes),
		},
	}

	// Note: client.Apply is the Patch-type-based SSA path. The
	// newer Client.Apply / SubResource.Apply methods need
	// applyconfiguration-gen output for our CRDs, which we don't
	// produce yet — sticking to the patch-type API keeps the
	// dependency tree shallow.
	err = s.c.Status().Patch(ctx, apply,
		ctrlclient.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		ctrlclient.FieldOwner(SatelliteFieldOwner),
		ctrlclient.ForceOwnership)
	if err != nil {
		return errors.Wrapf(err, "ssa apply Resource Status %s/%s", rdName, node)
	}

	return nil
}

// buildVolumeStatusForApply turns a slice of per-volume
// observations into the SSA-shaped Status.Volumes payload. Only
// non-empty fields land in the apply object so SSA doesn't claim
// ownership of fields the satellite didn't explicitly set.
//
// The Status.Volumes slice is a `+listType=map +listMapKey=volumeNumber`
// list, so the apiserver merges the apply against the existing
// state by volume number — a frame that only carries DiskState
// for vol 0 leaves vol 1's CurrentGi alone.
func buildVolumeStatusForApply(observations []apiv1.VolumeObservation) []crdv1alpha1.ResourceVolumeStatus {
	if len(observations) == 0 {
		return nil
	}

	out := make([]crdv1alpha1.ResourceVolumeStatus, 0, len(observations))

	for _, vol := range observations {
		out = append(out, crdv1alpha1.ResourceVolumeStatus{
			VolumeNumber: vol.VolumeNumber,
			DiskState:    vol.State.DiskState,
			CurrentGi:    vol.State.CurrentGi,
		})
	}

	return out
}

func crdToWireResource(crd *crdv1alpha1.Resource) apiv1.Resource {
	props := mergeProps(crd.Spec.Props, typedToProps(crd.Spec.DRBDOptions, crd.Spec.ExtraProps))

	// Phase 10.3 step: re-emit typed StoragePool back into Props
	// so golinstor + the dispatcher's legacy fallback see the
	// upstream-compatible shape on GET.
	if crd.Spec.StoragePool != "" {
		if props == nil {
			props = map[string]string{}
		}

		props["StorPoolName"] = crd.Spec.StoragePool
	}

	return apiv1.Resource{
		Name:     crd.Spec.ResourceDefinitionName,
		NodeName: crd.Spec.NodeName,
		Props:    props,
		Flags:    crd.Spec.Flags,
		State: apiv1.ResourceState{
			InUse:     crd.Status.InUse,
			DrbdState: crd.Status.DrbdState,
		},
		UUID: string(crd.UID),
	}
}

func wireToCRDResource(in *apiv1.Resource) *crdv1alpha1.Resource {
	return &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceCRDName(in.Name, in.NodeName),
			Labels: map[string]string{
				LabelResourceDefinition: in.Name,
				LabelNodeName:           in.NodeName,
			},
		},
		Spec: wireToCRDResourceSpec(in),
	}
}

func wireToCRDResourceSpec(in *apiv1.Resource) crdv1alpha1.ResourceSpec {
	typed, extras := propsToTyped(in.Props)
	residual := stripDRBDProps(in.Props)

	return crdv1alpha1.ResourceSpec{
		ResourceDefinitionName: in.Name,
		NodeName:               in.NodeName,
		Props:                  residual,
		DRBDOptions:            typed,
		ExtraProps:             extras,
		Flags:                  in.Flags,
		// Phase 10.3 step: lift Props["StorPoolName"] into the
		// typed slot. The legacy key stays in residual Props (it's
		// non-DRBD, so stripDRBDProps left it alone) for forward-
		// compat — readers still consulting Props will see it.
		StoragePool: in.Props["StorPoolName"],
	}
}
