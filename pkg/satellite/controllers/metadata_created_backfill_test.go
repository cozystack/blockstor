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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// dumpMdFound is what drbdadm prints for a volume carrying metadata.
const dumpMdFound = "version \"v09\";\n"

// backfillFixture is one replica on n1 with no MetadataCreated
// Condition, of a DRBD definition with the given volumes.
type backfillFixture struct {
	rd      *blockstoriov1alpha1.ResourceDefinition
	replica *blockstoriov1alpha1.Resource
}

func newBackfillFixture(rdName string, volumes ...int32) backfillFixture {
	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: naming.Key(rdName)}}
	k8s.SetOriginalName(&rd.ObjectMeta, rdName)
	rd.Spec.LayerStack = []string{"DRBD", "STORAGE"}

	for _, vol := range volumes {
		rd.Spec.VolumeDefinitions = append(rd.Spec.VolumeDefinitions,
			blockstoriov1alpha1.ResourceDefinitionVolume{VolumeNumber: vol, SizeKib: 1024})
	}

	replica := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName(rdName, "n1")},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rdName, NodeName: "n1"},
	}

	return backfillFixture{rd: rd, replica: replica}
}

// run starts the backfill against fx and returns the replica's
// MetadataCreated Condition, nil when none was stamped.
func (f backfillFixture) run(t *testing.T, fx *storage.FakeExec) *metav1.Condition {
	t.Helper()

	cli := fake.NewClientBuilder().WithScheme(crNameScheme(t)).WithObjects(f.rd, f.replica).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	backfill := &MetadataCreatedBackfillRunnable{
		Client:   cli,
		Adm:      drbd.NewAdm(fx),
		Stamper:  &MetadataCreatedStamper{Client: cli},
		NodeName: "n1",
	}

	if err := backfill.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var got blockstoriov1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: f.replica.Name}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}

	return meta.FindStatusCondition(got.Status.Conditions, blockstoriov1alpha1.ConditionMetadataCreated)
}

// dumpMdTargets lists the targets drbdadm dump-md was called with.
func dumpMdTargets(fx *storage.FakeExec) []string {
	var out []string

	for _, call := range fx.CallsSnapshot() {
		if call.Name == "drbdadm" && len(call.Args) == 2 && call.Args[0] == "dump-md" {
			out = append(out, call.Args[1])
		}
	}

	return out
}

// newDumpMdExec answers dump-md the way drbdadm does: metadata on the
// volumes in found, and "not defined" for any name the .res files do
// not carry, such as the Resource CR name.
func newDumpMdExec(found ...string) *storage.FakeExec {
	fx := storage.NewFakeExec()

	for _, target := range found {
		fx.Responses["drbdadm dump-md "+target] = storage.FakeResponse{Stdout: []byte(dumpMdFound)}
	}

	return fx
}

// The startup backfill probes the DRBD resource the .res file defines,
// per volume, and stamps MetadataFound. It used to pass the Resource CR
// name, `<rd>.<node>`, which drbdadm answers with "not defined in your
// config", so no replica was ever stamped.
func TestTheBackfillProbesTheDRBDResourceNotTheCRName(t *testing.T) {
	t.Parallel()

	for _, rdName := range []string{"pvc-1", "My_Res"} {
		f := newBackfillFixture(rdName, 0, 1)
		fx := newDumpMdExec(naming.Key(rdName)+"/0", naming.Key(rdName)+"/1")
		fx.Responses["drbdadm dump-md "+f.replica.Name] = storage.FakeResponse{
			Err: errors.New("'" + f.replica.Name + "' not defined in your config (for this host).: exit status 1"),
		}

		cond := f.run(t, fx)

		if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != blockstoriov1alpha1.MetadataCreatedReasonFound {
			t.Errorf("%s: MetadataCreated = %+v, want True/%s; dump-md targets %v",
				rdName, cond, blockstoriov1alpha1.MetadataCreatedReasonFound, dumpMdTargets(fx))
		}

		want := []string{naming.Key(rdName) + "/0", naming.Key(rdName) + "/1"}
		if got := dumpMdTargets(fx); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: dump-md targets = %v, want %v", rdName, got, want)
		}
	}
}

// A volume without metadata leaves the Condition unstamped, even when
// another volume of the replica has it: a resource-wide dump-md stops
// at the first volume and cannot say that.
func TestTheBackfillStampsOnlyWhenEveryVolumeHasMetadata(t *testing.T) {
	t.Parallel()

	f := newBackfillFixture("pvc-2", 0, 1)
	// The resource-wide probe answers for volume 0 alone, as drbdadm
	// does when that volume is attached.
	fx := newDumpMdExec("pvc-2/0", "pvc-2")
	fx.Responses["drbdadm dump-md pvc-2/1"] = storage.FakeResponse{
		Err: errors.New("No valid meta data found: exit status 255"),
	}

	if cond := f.run(t, fx); cond != nil {
		t.Errorf("MetadataCreated = %+v, want none while volume 1 has no metadata", cond)
	}
}

// Replicas with no lower disk to probe, or no .res to probe it
// through, are left alone rather than probed into an error on every
// satellite start.
func TestTheBackfillSkipsReplicasWithoutDRBDMetadata(t *testing.T) {
	t.Parallel()

	diskless := newBackfillFixture("pvc-3", 0)
	diskless.replica.Spec.Flags = []string{apiv1.ResourceFlagDiskless}

	storageOnly := newBackfillFixture("pvc-4", 0)
	storageOnly.rd.Spec.LayerStack = []string{"STORAGE"}

	for name, f := range map[string]backfillFixture{"diskless": diskless, "storage-only": storageOnly} {
		fx := newDumpMdExec(naming.Key(f.rd.Name) + "/0")

		if cond := f.run(t, fx); cond != nil {
			t.Errorf("%s: MetadataCreated = %+v, want none", name, cond)
		}

		if got := dumpMdTargets(fx); len(got) != 0 {
			t.Errorf("%s: dump-md called with %v, want no probe", name, got)
		}
	}
}

// An empty layer stack is the default DRBD-over-storage stack: the
// controller leaves it unset when the group names none, as on a
// definition adopted from LINSTOR in the default group, and the apply
// chain renders a .res for it. The backfill probes such a replica like
// any other DRBD one.
func TestTheBackfillProbesADefinitionWithoutALayerStack(t *testing.T) {
	t.Parallel()

	f := newBackfillFixture("pvc-5", 0, 1)
	f.rd.Spec.LayerStack = nil
	fx := newDumpMdExec("pvc-5/0", "pvc-5/1")

	cond := f.run(t, fx)

	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != blockstoriov1alpha1.MetadataCreatedReasonFound {
		t.Errorf("MetadataCreated = %+v, want True/%s", cond, blockstoriov1alpha1.MetadataCreatedReasonFound)
	}

	if got, want := dumpMdTargets(fx), []string{"pvc-5/0", "pvc-5/1"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("dump-md targets = %v, want %v", got, want)
	}
}
