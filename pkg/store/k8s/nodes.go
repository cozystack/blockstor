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

// nodes implements store.NodeStore against the Node CRD.
type nodes struct {
	c ctrlclient.Client
}

// List returns all Node CRDs as wire-shape apiv1.Node values, sorted by name.
func (n *nodes) List(ctx context.Context) ([]apiv1.Node, error) {
	var crdList crdv1alpha1.NodeList

	err := n.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list Node CRDs")
	}

	out := make([]apiv1.Node, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireNode(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// Get returns the named Node CRD as an apiv1.Node, or ErrNotFound.
func (n *nodes) Get(ctx context.Context, name string) (apiv1.Node, error) {
	var crd crdv1alpha1.Node

	err := n.c.Get(ctx, types.NamespacedName{Name: name}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.Node{}, errors.Wrapf(store.ErrNotFound, "node %q", name)
		}

		return apiv1.Node{}, errors.Wrapf(err, "get Node %q", name)
	}

	return crdToWireNode(&crd), nil
}

// Create persists a new Node CRD from an apiv1.Node value.
func (n *nodes) Create(ctx context.Context, in *apiv1.Node) error {
	if in == nil {
		return errors.New("nil Node")
	}

	crd := wireToCRDNode(in)

	err := n.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "node %q", in.Name)
		}

		return errors.Wrapf(err, "create Node %q", in.Name)
	}

	return nil
}

// Update overwrites the spec of an existing Node CRD with the value supplied.
// Status is not touched here — reconcilers own status.
func (n *nodes) Update(ctx context.Context, in *apiv1.Node) error {
	if in == nil {
		return errors.New("nil Node")
	}

	var existing crdv1alpha1.Node

	err := n.c.Get(ctx, types.NamespacedName{Name: in.Name}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "node %q", in.Name)
		}

		return errors.Wrapf(err, "get Node %q", in.Name)
	}

	existing.Spec = wireToCRDNodeSpec(in)

	err = n.c.Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "update Node %q", in.Name)
	}

	return nil
}

// SetConnectionStatus updates the Node CRD's
// `.status.connectionStatus` via the Status subresource. Survives
// subsequent Spec Update calls (which would otherwise overwrite
// nothing here, but the dedicated subresource is the kubebuilder-
// idiomatic place to land observed state).
func (n *nodes) SetConnectionStatus(ctx context.Context, name, status string) error {
	var existing crdv1alpha1.Node

	err := n.c.Get(ctx, types.NamespacedName{Name: name}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "node %q", name)
		}

		return errors.Wrapf(err, "get Node %q", name)
	}

	existing.Status.ConnectionStatus = status

	err = n.c.Status().Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "status update Node %q", name)
	}

	return nil
}

// Delete removes the named Node CRD.
func (n *nodes) Delete(ctx context.Context, name string) error {
	crd := &crdv1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}

	err := n.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "node %q", name)
		}

		return errors.Wrapf(err, "delete Node %q", name)
	}

	return nil
}

// crdToWireNode flattens a Node CRD into the LINSTOR REST shape.
func crdToWireNode(crd *crdv1alpha1.Node) apiv1.Node {
	out := apiv1.Node{
		Name:             crd.Name,
		Type:             crd.Spec.Type,
		Props:            crd.Spec.Props,
		ConnectionStatus: crd.Status.ConnectionStatus,
	}

	flags := make([]string, 0, len(crd.Spec.Flags)+len(crd.Status.Flags))
	flags = append(flags, crd.Spec.Flags...)
	flags = append(flags, crd.Status.Flags...)

	if len(flags) > 0 {
		out.Flags = flags
	}

	if len(crd.Spec.NetInterfaces) > 0 {
		out.NetInterfaces = make([]apiv1.NetInterface, 0, len(crd.Spec.NetInterfaces))
		for i := range crd.Spec.NetInterfaces {
			ni := &crd.Spec.NetInterfaces[i]
			out.NetInterfaces = append(out.NetInterfaces, apiv1.NetInterface{
				Name:                    ni.Name,
				Address:                 ni.Address,
				SatellitePort:           int(ni.SatellitePort),
				SatelliteEncryptionType: ni.SatelliteEncryptionType,
			})
		}
	}

	return out
}

// wireToCRDNode builds a fresh Node CRD from an apiv1.Node — used by Create.
func wireToCRDNode(in *apiv1.Node) *crdv1alpha1.Node {
	return &crdv1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: in.Name},
		Spec:       wireToCRDNodeSpec(in),
	}
}

// wireToCRDNodeSpec is the spec-only converter used by both Create and Update.
func wireToCRDNodeSpec(in *apiv1.Node) crdv1alpha1.NodeSpec {
	spec := crdv1alpha1.NodeSpec{
		Type:  in.Type,
		Props: in.Props,
		Flags: in.Flags,
	}

	if len(in.NetInterfaces) > 0 {
		spec.NetInterfaces = make([]crdv1alpha1.NodeNetInterface, 0, len(in.NetInterfaces))
		for i := range in.NetInterfaces {
			ni := &in.NetInterfaces[i]
			spec.NetInterfaces = append(spec.NetInterfaces, crdv1alpha1.NodeNetInterface{
				Name:                    ni.Name,
				Address:                 ni.Address,
				SatellitePort:           int32(ni.SatellitePort), //nolint:gosec // upstream LINSTOR ports fit in int32
				SatelliteEncryptionType: ni.SatelliteEncryptionType,
			})
		}
	}

	return spec
}
