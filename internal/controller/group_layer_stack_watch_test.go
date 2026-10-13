// SPDX-License-Identifier: Apache-2.0

package controller_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

func layeredGroup(name string, stack ...string) *blockstoriov1alpha1.ResourceGroup {
	rg := &blockstoriov1alpha1.ResourceGroup{ObjectMeta: metav1.ObjectMeta{Name: name}}
	rg.Spec.SelectFilter.LayerStack = stack

	return rg
}

// A group that comes to set a layer stack re-runs the pin on the definitions
// that leave their stack to it, and on nothing else: a definition with a stack
// of its own is already decided, and one under another group is not its.
func TestAGroupGainingAStackEnqueuesTheDefinitionsLeftToIt(t *testing.T) {
	t.Parallel()

	rd := func(name, group string, stack ...string) client.Object {
		return &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: group, LayerStack: stack},
		}
	}

	cli := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(
		rd("left", "grp"), rd("pinned", "grp", "DRBD", "STORAGE"), rd("elsewhere", "other"),
	).Build()

	r := &controllerpkg.ResourceDefinitionReconciler{Client: cli}

	reqs := r.EnqueueRDsLeftToGroupStack(t.Context(), layeredGroup("grp", "DRBD", "LUKS", "STORAGE"))
	if len(reqs) != 1 || reqs[0].Name != "left" {
		t.Errorf("enqueued %v, want only the definition that leaves its stack to the group", reqs)
	}
}

// The watch fires when a group comes to set a stack it did not, and not on
// every edit to it.
func TestGroupLayerStackSetFiresOnAStackChange(t *testing.T) {
	t.Parallel()

	p := controllerpkg.GroupLayerStackSet()
	luks := layeredGroup("grp", "DRBD", "LUKS", "STORAGE")
	plain := layeredGroup("grp")

	if !p.Create(event.CreateEvent{Object: luks}) || p.Create(event.CreateEvent{Object: plain}) {
		t.Error("create: want it only for a group that sets a stack")
	}

	if !p.Update(event.UpdateEvent{ObjectOld: plain, ObjectNew: luks}) {
		t.Error("update that adds a stack did not fire")
	}

	if p.Update(event.UpdateEvent{ObjectOld: luks, ObjectNew: layeredGroup("grp", "DRBD", "LUKS", "STORAGE")}) {
		t.Error("update that keeps the stack fired")
	}

	if p.Update(event.UpdateEvent{ObjectOld: luks, ObjectNew: plain}) {
		t.Error("update that drops the stack fired: nothing is left undecided by it")
	}
}
