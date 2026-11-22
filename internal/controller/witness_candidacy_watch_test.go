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

package controller

import (
	"strconv"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

func candidacyNode(mutate func(*blockstoriov1alpha1.Node)) *blockstoriov1alpha1.Node {
	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n3"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/rack": "a"},
		},
	}

	if mutate != nil {
		mutate(node)
	}

	return node
}

// With no periodic RD pass, a node edit that changes where a witness
// may go has to bring the RDs back, and one that does not must not fan
// out to every RD.
func TestWitnessCandidacyChangedFiresOnWhatThePickReads(t *testing.T) {
	t.Parallel()

	pred := witnessCandidacyChanged()

	if !pred.Create(event.CreateEvent{Object: candidacyNode(nil)}) {
		t.Error("a node that joined did not bring the RDs back")
	}

	if !pred.Delete(event.DeleteEvent{Object: candidacyNode(nil)}) {
		t.Error("a node that went did not bring the RDs back")
	}

	relevant := map[string]func(*blockstoriov1alpha1.Node){
		"evicted": func(n *blockstoriov1alpha1.Node) { n.Spec.Flags = []string{apiv1.NodeFlagEvicted} },
		"lost":    func(n *blockstoriov1alpha1.Node) { n.Status.Flags = []string{apiv1.NodeFlagLost} },
		"opted out of autoplace": func(n *blockstoriov1alpha1.Node) {
			n.Spec.Props[apiv1.PropAutoplaceTarget] = strconv.FormatBool(false)
		},
		"type": func(n *blockstoriov1alpha1.Node) { n.Spec.Type = apiv1.NodeTypeController },
	}

	for name, mutate := range relevant {
		if !pred.Update(event.UpdateEvent{ObjectOld: candidacyNode(nil), ObjectNew: candidacyNode(mutate)}) {
			t.Errorf("%s: the RDs were not brought back", name)
		}
	}

	unrelated := candidacyNode(func(n *blockstoriov1alpha1.Node) { n.Spec.Props["Aux/rack"] = "b" })
	if pred.Update(event.UpdateEvent{ObjectOld: candidacyNode(nil), ObjectNew: unrelated}) {
		t.Error("an unrelated prop edit brought every RD back")
	}
}

// An RD can name a group before the group exists; the group's creation
// is the event that pins it.
func TestGroupCreationBringsItsRDsBack(t *testing.T) {
	t.Parallel()

	if !groupLayerStackChanged().Create(event.CreateEvent{Object: watchGroup("DRBD", "STORAGE")}) {
		t.Error("a group's creation did not bring its RDs back")
	}
}

// TestGroupDeletionBringsItsRDsBack: an RD whose stack lacks a layer of
// its group's carries the unapplied-group annotation, and loses it once
// the group is gone. The RD's own pass is what drops it, and with no
// periodic pass left only the group's deletion brings the RD back.
func TestGroupDeletionBringsItsRDsBack(t *testing.T) {
	t.Parallel()

	if !groupLayerStackChanged().Delete(event.DeleteEvent{Object: watchGroup("LUKS", "DRBD", "STORAGE")}) {
		t.Error("a group's deletion did not bring its RDs back")
	}
}
