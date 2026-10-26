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

package controller_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var (
	luksStack = []string{"DRBD", "LUKS", "STORAGE"}

	errRead = errors.New("apiserver unavailable")
)

func pinGroup(stack ...string) *blockstoriov1alpha1.ResourceGroup {
	return &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "rg-pin"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{LayerStack: stack},
		},
	}
}

func pinRD(group string, stack ...string) *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-pin"},
		Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: group, LayerStack: stack},
	}
}

func pinReplica(rd, node string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rd + "-" + node},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: node},
	}
}

// withMetadata marks a replica whose DRBD metadata was created.
func withMetadata(res *blockstoriov1alpha1.Resource) *blockstoriov1alpha1.Resource {
	res.Status.Conditions = []metav1.Condition{{
		Type: blockstoriov1alpha1.ConditionMetadataCreated, Status: metav1.ConditionTrue,
		Reason: "Created", LastTransitionTime: metav1.Now(),
	}}

	return res
}

// withDevice marks a replica whose satellite reported a volume device.
func withDevice(res *blockstoriov1alpha1.Resource) *blockstoriov1alpha1.Resource {
	res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, DevicePath: "/dev/drbd1000"}}

	return res
}

type pinResult struct {
	result ctrl.Result
	rd     blockstoriov1alpha1.ResourceDefinition
	events []string
	err    error
}

func reconcilePin(t *testing.T, cli client.Client) pinResult {
	t.Helper()

	recorder := events.NewFakeRecorder(10)
	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client:   cli,
		Scheme:   newScheme(t),
		Store:    store.NewInMemory(),
		Recorder: recorder,
	}

	result, err := rec.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-pin"}})

	var out pinResult

	out.result, out.err = result, err

	getErr := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-pin"}, &out.rd)
	if getErr != nil {
		t.Fatalf("read RD back: %v", getErr)
	}

	close(recorder.Events)

	for e := range recorder.Events {
		out.events = append(out.events, e)
	}

	return out
}

func pinClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()

	return fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).Build()
}

// An RD with nothing on disk yet takes its group's stack, LUKS
// included, so the satellite builds it encrypted from the start.
func TestPinLayerStackGivesAFreshRDItsGroupsStack(t *testing.T) {
	t.Parallel()

	got := reconcilePin(t, pinClient(t, pinGroup(luksStack...), pinRD("rg-pin"), pinReplica("pvc-pin", "n1")))
	if got.err != nil {
		t.Fatalf("reconcile: %v", got.err)
	}

	if !slices.Equal(got.rd.Spec.LayerStack, luksStack) {
		t.Fatalf("stack %v, want %v", got.rd.Spec.LayerStack, luksStack)
	}

	if _, ok := got.rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation]; ok {
		t.Error("a fresh RD carries the unapplied-stack annotation")
	}

	if len(got.events) != 0 {
		t.Errorf("events %v, want none", got.events)
	}
}

// Nothing watches a group's stack on an RD's behalf: an RD whose group
// has no stack yet picks one up only on its next reconcile, which the
// controller schedules every time.
func TestPinLayerStackComesBackForAGroupWithoutAStack(t *testing.T) {
	t.Parallel()

	got := reconcilePin(t, pinClient(t, pinGroup(), pinRD("rg-pin")))
	if got.err != nil {
		t.Fatalf("reconcile: %v", got.err)
	}

	if got.result.RequeueAfter != controllerpkg.RDReconcileRequeue {
		t.Errorf("requeue after %v, want %v", got.result.RequeueAfter, controllerpkg.RDReconcileRequeue)
	}
}

// LINSTOR names are case-insensitive. An RD that names its group as the
// operator spelled it (`--resource-group RG-PIN`) is pinned from the
// group's CR, which is stored under the folded name.
func TestPinLayerStackFindsAGroupNamedInAnotherCase(t *testing.T) {
	t.Parallel()

	got := reconcilePin(t, pinClient(t, pinGroup(luksStack...), pinRD("RG-Pin"), pinReplica("pvc-pin", "n1")))
	if got.err != nil {
		t.Fatalf("reconcile: %v", got.err)
	}

	if !slices.Equal(got.rd.Spec.LayerStack, luksStack) {
		t.Fatalf("stack %v, want the group's %v", got.rd.Spec.LayerStack, luksStack)
	}
}

// An RD that already went to disk under the default stack keeps it:
// the group's LUKS stack over that data would luksFormat it. The RD
// says what it is, and the operator is told.
func TestPinLayerStackKeepsTheAppliedStackOfAnRDThatHoldsData(t *testing.T) {
	t.Parallel()

	for name, replica := range map[string]*blockstoriov1alpha1.Resource{
		"metadata created": withMetadata(pinReplica("pvc-pin", "n1")),
		"device reported":  withDevice(pinReplica("pvc-pin", "n1")),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := reconcilePin(t, pinClient(t, pinGroup(luksStack...), pinRD("rg-pin"), replica, pinReplica("pvc-pin", "n2")))
			if got.err != nil {
				t.Fatalf("reconcile: %v", got.err)
			}

			if !slices.Equal(got.rd.Spec.LayerStack, apiv1.DefaultLayerStack()) {
				t.Fatalf("stack %v, want the applied default %v", got.rd.Spec.LayerStack, apiv1.DefaultLayerStack())
			}

			if a := got.rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation]; a != "DRBD,LUKS,STORAGE" {
				t.Errorf("annotation %q, want the group's stack", a)
			}

			if len(got.events) != 1 || !strings.HasPrefix(got.events[0], "Warning LayerStackNotInherited") ||
				!strings.Contains(got.events[0], "DRBD,LUKS,STORAGE") {
				t.Errorf("events %v, want one LayerStackNotInherited warning naming the group's stack", got.events)
			}
		})
	}
}

// Data under the default stack in a group whose stack is the default
// is no conflict: pinned as is, without a warning.
func TestPinLayerStackDataUnderTheGroupsOwnStackIsNoConflict(t *testing.T) {
	t.Parallel()

	got := reconcilePin(t, pinClient(t, pinGroup("DRBD", "STORAGE"), pinRD("rg-pin"), withMetadata(pinReplica("pvc-pin", "n1"))))
	if got.err != nil {
		t.Fatalf("reconcile: %v", got.err)
	}

	if !slices.Equal(got.rd.Spec.LayerStack, []string{"DRBD", "STORAGE"}) {
		t.Fatalf("stack %v", got.rd.Spec.LayerStack)
	}

	if _, ok := got.rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation]; ok || len(got.events) != 0 {
		t.Errorf("annotations %v, events %v, want neither", got.rd.Annotations, got.events)
	}
}

// The group's stack is a set: a group spelling it in another order or case
// asks for the same layers, and the data under them is no conflict.
func TestPinLayerStackReadsTheGroupsStackAsASet(t *testing.T) {
	t.Parallel()

	for _, groupStack := range [][]string{{"drbd", "storage"}, {"STORAGE", "DRBD"}} {
		got := reconcilePin(t, pinClient(t, pinGroup(groupStack...), pinRD("rg-pin"), withMetadata(pinReplica("pvc-pin", "n1"))))
		if got.err != nil {
			t.Fatalf("reconcile under %v: %v", groupStack, got.err)
		}

		if _, ok := got.rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation]; ok || len(got.events) != 0 {
			t.Errorf("group %v: annotations %v, events %v, want neither", groupStack, got.rd.Annotations, got.events)
		}
	}
}

// Data under more layers than the group asks for is not short of it, and is
// not flagged.
func TestPinLayerStackFlagsOnlyAStackShortOfTheGroup(t *testing.T) {
	t.Parallel()

	got := reconcilePin(t, pinClient(t, pinGroup("STORAGE"), pinRD("rg-pin"), withMetadata(pinReplica("pvc-pin", "n1"))))
	if got.err != nil {
		t.Fatalf("reconcile: %v", got.err)
	}

	if _, ok := got.rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation]; ok || len(got.events) != 0 {
		t.Errorf("annotations %v, events %v, want neither", got.rd.Annotations, got.events)
	}
}

// Data on another RD's replica says nothing about this one.
func TestPinLayerStackLooksOnlyAtItsOwnReplicas(t *testing.T) {
	t.Parallel()

	got := reconcilePin(t, pinClient(t, pinGroup(luksStack...), pinRD("rg-pin"), withMetadata(pinReplica("pvc-other", "n1"))))
	if got.err != nil {
		t.Fatalf("reconcile: %v", got.err)
	}

	if !slices.Equal(got.rd.Spec.LayerStack, luksStack) {
		t.Fatalf("stack %v, want %v", got.rd.Spec.LayerStack, luksStack)
	}
}

// Every shape that has nothing to inherit is left exactly as it is. An RD
// that set its own stack short of its group is flagged instead, see
// TestPinnedRDsUnappliedGroupStackFollowsTheGroup.
func TestPinLayerStackLeavesRDsWithNothingToInherit(t *testing.T) {
	t.Parallel()

	cases := map[string][]client.Object{
		"RD sets its own stack": {pinGroup(luksStack...), pinRD("rg-pin", luksStack...)},
		"RD in no group":        {pinGroup(luksStack...), pinRD("")},
		"group gone":            {pinRD("rg-pin")},
		"group sets no stack":   {pinGroup(), pinRD("rg-pin")},
	}

	for name, objs := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var before blockstoriov1alpha1.ResourceDefinition

			patched := false
			cli := interceptor.NewClient(pinClient(t, objs...), interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if _, ok := obj.(*blockstoriov1alpha1.ResourceDefinition); ok {
						patched = true
					}

					return c.Patch(ctx, obj, patch, opts...)
				},
			})
			if err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-pin"}, &before); err != nil {
				t.Fatalf("read RD: %v", err)
			}

			got := reconcilePin(t, cli)
			if got.err != nil {
				t.Fatalf("reconcile: %v", got.err)
			}

			if !slices.Equal(got.rd.Spec.LayerStack, before.Spec.LayerStack) || len(got.rd.Annotations) != 0 {
				t.Errorf("stack %v annotations %v, want %v and none", got.rd.Spec.LayerStack, got.rd.Annotations, before.Spec.LayerStack)
			}

			if patched {
				t.Error("the RD was written although there was nothing to pin")
			}
		})
	}
}

// Neither an unreadable group nor an unreadable replica list is a
// licence to pin: either could hide the answer.
func TestPinLayerStackFailsClosedOnAReadError(t *testing.T) {
	t.Parallel()

	for name, funcs := range map[string]interceptor.Funcs{
		"group": {Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*blockstoriov1alpha1.ResourceGroup); ok {
				return errRead
			}

			return c.Get(ctx, key, obj, opts...)
		}},
		"replicas": {List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*blockstoriov1alpha1.ResourceList); ok {
				return errRead
			}

			return c.List(ctx, list, opts...)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cli := interceptor.NewClient(pinClient(t, pinGroup(luksStack...), pinRD("rg-pin")), funcs)

			got := reconcilePin(t, cli)
			if !errors.Is(got.err, errRead) {
				t.Fatalf("reconcile: %v, want the read error", got.err)
			}

			if len(got.rd.Spec.LayerStack) != 0 {
				t.Errorf("stack %v pinned on a read error", got.rd.Spec.LayerStack)
			}
		})
	}
}

// The pin only fills an empty stack. A writer that set one first (the
// REST autoplace stamp) wins; the pin must not overwrite it from a
// stale copy.
func TestPinLayerStackDoesNotOverwriteARacingWriter(t *testing.T) {
	t.Parallel()

	base := pinClient(t, pinGroup(luksStack...), pinRD("rg-pin"))
	raced := false

	cli := interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if !raced {
				raced = true

				var live blockstoriov1alpha1.ResourceDefinition
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &live); err != nil {
					return err
				}

				live.Spec.LayerStack = []string{"DRBD", "STORAGE"}
				if err := c.Update(ctx, &live); err != nil {
					return err
				}
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	})

	got := reconcilePin(t, cli)
	if got.err == nil {
		t.Fatal("reconcile succeeded over a racing writer")
	}

	if !slices.Equal(got.rd.Spec.LayerStack, []string{"DRBD", "STORAGE"}) {
		t.Fatalf("stack %v, want the racing writer's", got.rd.Spec.LayerStack)
	}
}

// The Initialized latch is data too, before any replica has reported:
// an RD adopted by linstor-migrate carries it from the start.
func TestPinLayerStackTreatsTheInitializedLatchAsData(t *testing.T) {
	t.Parallel()

	initialized := true
	rd := pinRD("rg-pin")
	rd.Spec.Initialized = &initialized

	got := reconcilePin(t, pinClient(t, pinGroup(luksStack...), rd, pinReplica("pvc-pin", "n1")))
	if got.err != nil {
		t.Fatalf("reconcile: %v", got.err)
	}

	if !slices.Equal(got.rd.Spec.LayerStack, apiv1.DefaultLayerStack()) {
		t.Fatalf("stack %v, want the applied default", got.rd.Spec.LayerStack)
	}
}

// The annotation on a pinned RD follows its group both ways: it goes once
// the group no longer asks for a layer the RD lacks, or the group is gone,
// and names the group's current stack while the RD is short of it, whether
// or not it was flagged before.
func TestPinnedRDsUnappliedGroupStackFollowsTheGroup(t *testing.T) {
	t.Parallel()

	flagged := func(value string) *blockstoriov1alpha1.ResourceDefinition {
		rd := pinRD("rg-pin", "DRBD", "STORAGE")
		rd.Annotations = map[string]string{apiv1.UnappliedGroupLayerStackAnnotation: value}

		return rd
	}

	for name, tc := range map[string]struct {
		objs []client.Object
		want string
	}{
		"group dropped the layer": {objs: []client.Object{pinGroup("DRBD", "STORAGE"), flagged("DRBD,LUKS,STORAGE")}},
		"group gone":              {objs: []client.Object{flagged("DRBD,LUKS,STORAGE")}},
		"group still asks":        {objs: []client.Object{pinGroup(luksStack...), flagged("DRBD,LUKS,STORAGE")}, want: "DRBD,LUKS,STORAGE"},
		"group changed its ask":   {objs: []client.Object{pinGroup("LUKS", "STORAGE"), flagged("DRBD,LUKS,STORAGE")}, want: "LUKS,STORAGE"},
		"group gained a layer":    {objs: []client.Object{pinGroup(luksStack...), pinRD("rg-pin", "DRBD", "STORAGE")}, want: "DRBD,LUKS,STORAGE"},
		"never short":             {objs: []client.Object{pinGroup("DRBD", "STORAGE"), pinRD("rg-pin", "DRBD", "STORAGE")}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := reconcilePin(t, pinClient(t, tc.objs...))
			if got.err != nil {
				t.Fatalf("reconcile: %v", got.err)
			}

			a, ok := got.rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation]
			if a != tc.want || ok != (tc.want != "") {
				t.Errorf("annotation %q (present %v), want %q", a, ok, tc.want)
			}

			if !slices.Equal(got.rd.Spec.LayerStack, []string{"DRBD", "STORAGE"}) {
				t.Errorf("the pinned stack changed to %v", got.rd.Spec.LayerStack)
			}
		})
	}
}

// A group that drops a layer and takes it back flags its RD again, with a
// warning Event: the mark is not lost to the flap.
func TestUnappliedGroupStackSurvivesAGroupFlap(t *testing.T) {
	t.Parallel()

	rd := pinRD("rg-pin", "DRBD", "STORAGE")
	rd.Annotations = map[string]string{apiv1.UnappliedGroupLayerStackAnnotation: "DRBD,LUKS,STORAGE"}
	group := pinGroup(luksStack...)
	cli := pinClient(t, group, rd)

	setGroupStack := func(stack ...string) {
		t.Helper()

		var live blockstoriov1alpha1.ResourceGroup
		if err := cli.Get(context.Background(), client.ObjectKey{Name: "rg-pin"}, &live); err != nil {
			t.Fatalf("get group: %v", err)
		}

		live.Spec.SelectFilter.LayerStack = stack
		if err := cli.Update(context.Background(), &live); err != nil {
			t.Fatalf("update group: %v", err)
		}
	}

	setGroupStack("DRBD", "STORAGE")

	if got := reconcilePin(t, cli); got.rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation] != "" {
		t.Fatalf("still flagged after the group dropped LUKS: %v", got.rd.Annotations)
	}

	setGroupStack(luksStack...)

	got := reconcilePin(t, cli)
	if a := got.rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation]; a != "DRBD,LUKS,STORAGE" {
		t.Fatalf("annotation %q after the group took LUKS back, want the group's stack", a)
	}

	if len(got.events) != 1 || !strings.HasPrefix(got.events[0], "Warning LayerStackNotInherited") {
		t.Errorf("events %v, want one LayerStackNotInherited warning", got.events)
	}
}
