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

type physicalDevices struct {
	c ctrlclient.Client
}

// PhysicalDeviceCRDName encodes the (node, stable-id) composite
// key into a single CRD name. Uses the same `<node>.<key>` shape
// as `crdName` (StoragePool), `resourceCRDName` (Resource), and
// `snapshotCRDName` (Snapshot) — every composite-key CRD in the
// project follows this convention so operators can grep for
// `node.something` and find every related CRD across kinds.
//
// Run through `Name()` to slugify: stable identifiers carry
// upstream udev shapes (`wwn-0x...`, `scsi-SATA_<vendor>_...`)
// with characters k8s names reject; `Name()` lowercases,
// substitutes invalid runes with `-`, and prepends an 8-char
// SHA256 prefix when slugification was lossy. Phase 10.7.
func PhysicalDeviceCRDName(nodeName, stableID string) string {
	return Name(nodeName + "." + stableID)
}

func (s *physicalDevices) List(ctx context.Context) ([]apiv1.PhysicalDevice, error) {
	var crdList crdv1alpha1.PhysicalDeviceList

	err := s.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list PhysicalDevice CRDs")
	}

	out := make([]apiv1.PhysicalDevice, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWirePhysicalDevice(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *physicalDevices) ListForNode(ctx context.Context, nodeName string) ([]apiv1.PhysicalDevice, error) {
	var crdList crdv1alpha1.PhysicalDeviceList

	err := s.c.List(ctx, &crdList,
		ctrlclient.MatchingLabels{crdv1alpha1.PhysicalDeviceLabelNode: nodeName})
	if err != nil {
		return nil, errors.Wrapf(err, "list PhysicalDevice CRDs for node %q", nodeName)
	}

	out := make([]apiv1.PhysicalDevice, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWirePhysicalDevice(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *physicalDevices) Get(ctx context.Context, name string) (apiv1.PhysicalDevice, error) {
	var crd crdv1alpha1.PhysicalDevice

	err := s.c.Get(ctx, types.NamespacedName{Name: name}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.PhysicalDevice{}, errors.Wrapf(store.ErrNotFound, "physical device %q", name)
		}

		return apiv1.PhysicalDevice{}, errors.Wrapf(err, "get PhysicalDevice %q", name)
	}

	return crdToWirePhysicalDevice(&crd), nil
}

func (s *physicalDevices) Create(ctx context.Context, dev *apiv1.PhysicalDevice) error {
	if dev == nil {
		return errors.New("nil PhysicalDevice")
	}

	crd := wireToCRDPhysicalDevice(dev)

	err := s.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "physical device %q", dev.Name)
		}

		return errors.Wrapf(err, "create PhysicalDevice %q", dev.Name)
	}

	return nil
}

func (s *physicalDevices) Update(ctx context.Context, dev *apiv1.PhysicalDevice) error {
	if dev == nil {
		return errors.New("nil PhysicalDevice")
	}

	var existing crdv1alpha1.PhysicalDevice

	err := s.c.Get(ctx, types.NamespacedName{Name: dev.Name}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "physical device %q", dev.Name)
		}

		return errors.Wrapf(err, "get PhysicalDevice %q", dev.Name)
	}

	// CAS guard for the attach race (Phase 10.7 race-matrix line
	// 767): two concurrent CDP requests both pass `pickFreeDevice`
	// because each sees `AttachTo=nil`. Without this check the
	// last writer would silently overwrite the first and both
	// callers would think they'd won. Refusing the second
	// attach with `ErrAlreadyExists` lets the REST handler
	// surface a 409 to the loser.
	if dev.AttachTo != nil && existing.Spec.AttachTo != nil {
		return errors.Wrapf(store.ErrAlreadyExists, "physical device %q already attached", dev.Name)
	}

	existing.Spec = wireToCRDPhysicalDeviceSpec(dev)

	// Reflect typed-but-Spec.AttachTo-related label maintenance
	// here; CRD's labels carry the node binding.
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}

	if dev.NodeName != "" {
		existing.Labels[crdv1alpha1.PhysicalDeviceLabelNode] = dev.NodeName
	}

	err = s.c.Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "update PhysicalDevice %q", dev.Name)
	}

	return nil
}

func (s *physicalDevices) Delete(ctx context.Context, name string) error {
	crd := &crdv1alpha1.PhysicalDevice{ObjectMeta: metav1.ObjectMeta{Name: name}}

	err := s.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "physical device %q", name)
		}

		return errors.Wrapf(err, "delete PhysicalDevice %q", name)
	}

	return nil
}

func crdToWirePhysicalDevice(crd *crdv1alpha1.PhysicalDevice) apiv1.PhysicalDevice {
	out := apiv1.PhysicalDevice{
		Name:           crd.Name,
		NodeName:       crd.Status.NodeName,
		StableID:       crd.Status.StableID,
		DevicePath:     crd.Status.DevicePath,
		CurrentDevPath: crd.Status.CurrentDevPath,
		SizeBytes:      crd.Status.SizeBytes,
		Model:          crd.Status.Model,
		Serial:         crd.Status.Serial,
		Rotational:     crd.Status.Rotational,
		Transport:      crd.Status.Transport,
		Phase:          crd.Status.Phase,
	}

	// Fall back to the label when Status.NodeName isn't populated
	// yet — discovery runs Status updates separately from the
	// initial Create that stamps the label.
	if out.NodeName == "" {
		out.NodeName = crd.Labels[crdv1alpha1.PhysicalDeviceLabelNode]
	}

	// Bug 89: surface the satellite-stamped Free condition on the
	// wire shape so the REST `ps cdp` handler can refuse attaches
	// on devices the `ps l` endpoint already filters out. The
	// condition is the source of truth — `pkg/satellite/controllers/
	// physicaldevice_discovery.go::buildDiscoveryStatus` writes
	// True/False with Reason=FreeBlockDevice/SignatureFound on
	// every scan tick.
	for i := range crd.Status.Conditions {
		cond := &crd.Status.Conditions[i]
		if cond.Type != crdv1alpha1.PhysicalDeviceConditionFree {
			continue
		}

		free := cond.Status == metav1.ConditionTrue
		out.Free = &free
		out.FreeReason = cond.Reason
		out.FreeMessage = cond.Message

		break
	}

	if crd.Spec.AttachTo != nil {
		out.AttachTo = &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: crd.Spec.AttachTo.StoragePoolName,
			ProviderKind:    crd.Spec.AttachTo.ProviderKind,
			VGName:          crd.Spec.AttachTo.VGName,
			ThinPoolName:    crd.Spec.AttachTo.ThinPoolName,
			ZPoolName:       crd.Spec.AttachTo.ZPoolName,
			Directory:       crd.Spec.AttachTo.Directory,
			Wipe:            crd.Spec.AttachTo.Wipe,
		}
	}

	return out
}

func wireToCRDPhysicalDevice(in *apiv1.PhysicalDevice) *crdv1alpha1.PhysicalDevice {
	labels := map[string]string{}
	if in.NodeName != "" {
		labels[crdv1alpha1.PhysicalDeviceLabelNode] = in.NodeName
	}

	return &crdv1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{Name: in.Name, Labels: labels},
		Spec:       wireToCRDPhysicalDeviceSpec(in),
	}
}

func wireToCRDPhysicalDeviceSpec(in *apiv1.PhysicalDevice) crdv1alpha1.PhysicalDeviceSpec {
	if in.AttachTo == nil {
		return crdv1alpha1.PhysicalDeviceSpec{}
	}

	return crdv1alpha1.PhysicalDeviceSpec{
		AttachTo: &crdv1alpha1.AttachToPool{
			StoragePoolName: in.AttachTo.StoragePoolName,
			ProviderKind:    in.AttachTo.ProviderKind,
			VGName:          in.AttachTo.VGName,
			ThinPoolName:    in.AttachTo.ThinPoolName,
			ZPoolName:       in.AttachTo.ZPoolName,
			Directory:       in.AttachTo.Directory,
			Wipe:            in.AttachTo.Wipe,
		},
	}
}
