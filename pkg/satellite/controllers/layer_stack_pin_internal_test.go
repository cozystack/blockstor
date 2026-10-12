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

package controllers

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

var errGroupRead = errors.New("apiserver unavailable")

// luksGroup is a resource group whose layer stack carries LUKS.
func luksGroup() *blockstoriov1alpha1.ResourceGroup {
	return &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "rg-luks"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{LayerStack: []string{"DRBD", "LUKS", "STORAGE"}},
		},
	}
}

// groupMemberRD is an RD in rg-luks that sets no layer stack of its own.
func groupMemberRD(stack ...string) *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-member"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "rg-luks",
			LayerStack:        stack,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 4 * 1024},
			},
		},
	}
}

func memberReplica() *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-member-node-a"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-member",
			NodeName:               "node-a",
			StoragePool:            "pool-a",
		},
	}
}

// An RD that leaves its layer stack to its group must not be built into
// an intent from its own empty stack: that intent has no LUKS layer and
// carries no passphrase, so the volume would go to disk unencrypted
// while everything else reads the group's stack and calls it encrypted.
func TestAnRDLeftToItsGroupsLUKSStackIsNotBuiltPlaintext(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD()
	r := newPassphraseTestReconciler(t, luksGroup(), rd, memberReplica(), passphraseSecret("pass-member"))

	desired, err := r.buildDesiredFromCRD(t.Context(), memberReplica(), rd, nil)
	if err == nil {
		if !slices.Contains(desired.GetLayerStack(), "LUKS") || desired.GetProps()["LuksPassphrase"] == "" {
			t.Fatalf("intent for an RD in a LUKS group: stack %v, passphrase set %v; want LUKS with its passphrase, or no intent",
				desired.GetLayerStack(), desired.GetProps()["LuksPassphrase"] != "")
		}

		return
	}

	if !errors.Is(err, errLayerStackNotPinned) {
		t.Fatalf("build: %v, want errLayerStackNotPinned", err)
	}
}

// Once the controller has written the group's stack onto the RD, the
// intent carries LUKS and the passphrase.
func TestAPinnedGroupStackBuildsLUKSWithItsPassphrase(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD("DRBD", "LUKS", "STORAGE")
	r := newPassphraseTestReconciler(t, luksGroup(), rd, memberReplica(), passphraseSecret("pass-member"))

	desired, err := r.buildDesiredFromCRD(t.Context(), memberReplica(), rd, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if !slices.Contains(desired.GetLayerStack(), "LUKS") {
		t.Errorf("stack %v, want LUKS", desired.GetLayerStack())
	}

	if desired.GetProps()["LuksPassphrase"] != "pass-member" {
		t.Error("the passphrase did not reach the intent")
	}
}

// The wait applies only to an RD whose group sets a stack it does not
// carry yet. Every other shape goes on with the stack it has.
func TestCheckLayerStackPinned(t *testing.T) {
	t.Parallel()

	noStackGroup := luksGroup()
	noStackGroup.Spec.SelectFilter.LayerStack = nil

	outsideGroup := groupMemberRD()
	outsideGroup.Spec.ResourceGroupName = ""

	// LINSTOR names are case-insensitive: the RD keeps the operator's
	// spelling, the group's CR is stored under the folded one.
	mixedCase := groupMemberRD()
	mixedCase.Spec.ResourceGroupName = "RG-LUKS"

	cases := []struct {
		name string
		rd   *blockstoriov1alpha1.ResourceDefinition
		objs []client.Object
		want error
	}{
		{name: "group stack not on the RD", rd: groupMemberRD(), objs: []client.Object{luksGroup()}, want: errLayerStackNotPinned},
		{name: "group named in another case", rd: mixedCase, objs: []client.Object{luksGroup()}, want: errLayerStackNotPinned},
		{name: "RD carries its stack", rd: groupMemberRD("DRBD", "STORAGE"), objs: []client.Object{luksGroup()}},
		{name: "RD in no group", rd: outsideGroup},
		{name: "group gone", rd: groupMemberRD()},
		{name: "group sets no stack", rd: groupMemberRD(), objs: []client.Object{noStackGroup}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := newPassphraseTestReconciler(t, tc.objs...)

			err := r.checkLayerStackPinned(t.Context(), tc.rd)
			if tc.want == nil && err != nil {
				t.Fatalf("check: %v, want nil", err)
			}

			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("check: %v, want %v", err, tc.want)
			}
		})
	}
}

// A group that cannot be read is no licence to apply the default stack.
func TestCheckLayerStackPinnedFailsClosedOnAnUnreadableGroup(t *testing.T) {
	t.Parallel()

	r := newPassphraseTestReconciler(t, luksGroup())
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			if _, ok := obj.(*blockstoriov1alpha1.ResourceGroup); ok {
				return errGroupRead
			}

			return nil
		},
	})

	err := r.checkLayerStackPinned(t.Context(), groupMemberRD())
	if !errors.Is(err, errGroupRead) || errors.Is(err, errLayerStackNotPinned) {
		t.Fatalf("check: %v, want the read error", err)
	}
}

// The reconcile waits for the pin quietly, on a short requeue, rather
// than surfacing an error and backing off, and builds nothing meanwhile.
func TestRunApplyWaitsForTheLayerStackPin(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD()
	res := memberReplica()
	r := newPassphraseTestReconciler(t, luksGroup(), rd, res)

	provider := &flakyCreateProvider{}
	exec := storage.NewFakeExec()
	r.Config.Exec = exec
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": provider},
		NodeName:  "node-a",
	})

	result, err := r.runApply(t.Context(), res, logr.Discard())
	if err != nil {
		t.Fatalf("runApply: %v", err)
	}

	if result.RequeueAfter != layerStackPinRequeue {
		t.Fatalf("requeue after %v, want %v", result.RequeueAfter, layerStackPinRequeue)
	}

	if n := atomic.LoadInt32(&provider.created); n != 0 {
		t.Errorf("%d volumes created before the pin, want none", n)
	}

	if cmds := exec.CommandLines(); len(cmds) != 0 {
		t.Errorf("commands run before the pin: %v, want none", cmds)
	}
}
