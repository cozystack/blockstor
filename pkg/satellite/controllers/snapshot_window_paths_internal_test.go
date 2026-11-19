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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

const suspendCmd = "drbdadm suspend-io " + windowTestResource

// A STORAGE-only resource on a satellite that runs DRBD for others has
// nothing to suspend: the take runs without a window, after a flush of
// the storage device, instead of a suspend-io that fails the snapshot.
func TestSnapshotOfAResourceWithoutDRBDOnADRBDSatellite(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	err := rig.cli.Create(context.Background(), &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: windowTestResource},
		Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{LayerStack: []string{"STORAGE"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	rig.reconcile()

	if !rig.node().SuspendIOAcked || rig.node().Failed {
		t.Fatalf("suspend phase: %+v", rig.node())
	}

	rig.controllerSets(true, true)
	rig.reconcile()

	if !rig.node().Ready || rig.provider.takeCount() != 1 {
		t.Errorf("take: node=%+v takes=%d", rig.node(), rig.provider.takeCount())
	}

	cmds := rig.kernel.commands()
	if slices.Contains(cmds, suspendCmd) {
		t.Errorf("suspend-io on a resource without DRBD: %v", cmds)
	}

	if !slices.Contains(cmds, "blockdev --flushbufs /dev/zvol/pool/"+windowTestResource+"_00000") {
		t.Errorf("storage device not flushed before the take: %v", cmds)
	}
}
