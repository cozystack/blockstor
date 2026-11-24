// SPDX-License-Identifier: Apache-2.0

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

package k8s_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

func peerSlotsStore(t *testing.T, objs ...client.Object) (*k8s.Store, client.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&crdv1alpha1.ResourceDefinition{}, &crdv1alpha1.Resource{}).
		Build()

	return k8s.New(cli), cli
}

// A definition's peer slots round-trip, and a write that names none, as
// a full-shape PUT built from scratch does, keeps them: dropping them
// would make the definition read as the 15 a missing count means.
func TestRDPeerSlotsSurviveAWriteThatNamesNone(t *testing.T) {
	t.Parallel()

	st, cli := peerSlotsStore(t)
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-a", PeerSlots: 7}); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-a")
	if err != nil || got.PeerSlots != 7 {
		t.Fatalf("get: %d, %v; want 7", got.PeerSlots, err)
	}

	if err := st.ResourceDefinitions().Update(ctx, &apiv1.ResourceDefinition{Name: "pvc-a", Props: map[string]string{"Aux/a": "b"}}); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "pvc-a", func(rd *apiv1.ResourceDefinition) error {
		rd.PeerSlots = 0

		return nil
	}); err != nil {
		t.Fatalf("patch: %v", err)
	}

	var crd crdv1alpha1.ResourceDefinition
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-a"}, &crd); err != nil {
		t.Fatalf("get crd: %v", err)
	}

	if crd.Spec.PeerSlots == nil || *crd.Spec.PeerSlots != 7 {
		t.Errorf("peer slots after writes naming none: %v, want 7", crd.Spec.PeerSlots)
	}

	if err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "pvc-a", func(rd *apiv1.ResourceDefinition) error {
		rd.PeerSlots = 12

		return nil
	}); err != nil {
		t.Fatalf("patch: %v", err)
	}

	if got, _ := st.ResourceDefinitions().Get(ctx, "pvc-a"); got.PeerSlots != 12 {
		t.Errorf("peer slots after a patch setting 12: %d", got.PeerSlots)
	}
}

// A replica's peer slots are the controller's, set once: a REST write of
// the replica keeps them, and reads report them, on the DRBD layer too.
func TestReplicaPeerSlotsSurviveAWireWrite(t *testing.T) {
	t.Parallel()

	seven := int32(7)
	rd := &crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-a"}}
	res := &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a.n1", Labels: map[string]string{}},
		Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-a", NodeName: "n1", PeerSlots: &seven},
	}
	legacy := &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a.n2", Labels: map[string]string{}},
		Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-a", NodeName: "n2"},
	}

	st, cli := peerSlotsStore(t, rd, res, legacy)
	ctx := t.Context()

	got, err := st.Resources().Get(ctx, "pvc-a", "n1")
	if err != nil || got.PeerSlots != 7 || got.LayerObject == nil || got.LayerObject.Drbd == nil ||
		got.LayerObject.Drbd.PeerSlots != 7 {
		t.Fatalf("get: %+v, %v; want 7 recorded and reported", got, err)
	}

	got.PeerSlots = 0
	if err := st.Resources().Update(ctx, &got); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := st.Resources().PatchResourceSpec(ctx, "pvc-a", "n1", func(r *apiv1.Resource) error {
		r.PeerSlots = 0

		return nil
	}); err != nil {
		t.Fatalf("patch: %v", err)
	}

	var crd crdv1alpha1.Resource
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-a.n1"}, &crd); err != nil {
		t.Fatalf("get crd: %v", err)
	}

	if crd.Spec.PeerSlots == nil || *crd.Spec.PeerSlots != 7 {
		t.Errorf("peer slots after wire writes: %v, want 7", crd.Spec.PeerSlots)
	}

	old, err := st.Resources().Get(ctx, "pvc-a", "n2")
	if err != nil || old.PeerSlots != 0 || old.LayerObject.Drbd.PeerSlots != 15 {
		t.Errorf("legacy replica: recorded %d, reported %+v, %v; want none recorded and 15 reported",
			old.PeerSlots, old.LayerObject.Drbd, err)
	}
}
