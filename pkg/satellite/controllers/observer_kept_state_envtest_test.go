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
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// keptStateFixture is one replica on a real apiserver and an observer
// writing its Status, fed events2 lines one by one.
type keptStateFixture struct {
	t   *testing.T
	cli client.Client
	o   *ObserverRunnable
	adm *drbd.Adm
}

func newKeptStateFixture(t *testing.T) *keptStateFixture {
	t.Helper()

	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (needs envtest apiserver)")
	}

	cli, err := client.New(startRestartEnv(t), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	err = cli.Create(t.Context(), &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName("pvc-kept", "n1")},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-kept", NodeName: "n1"},
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	fx := storage.NewFakeExec()

	return &keptStateFixture{
		t: t, cli: cli,
		o:   &ObserverRunnable{Client: cli, Exec: fx, NodeName: "n1"},
		adm: drbd.NewAdm(fx),
	}
}

func (f *keptStateFixture) feed(lines ...string) {
	f.t.Helper()

	for _, line := range lines {
		ev, err := drbd.ParseEvent(line)
		if err != nil {
			f.t.Fatalf("parse %q: %v", line, err)
		}

		obs, ok := translateEvent(ev)
		if !ok {
			f.t.Fatalf("translate %q", line)
		}

		f.o.handleObservation(f.t.Context(), f.adm, &obs)
	}
}

func (f *keptStateFixture) status() blockstoriov1alpha1.ResourceStatus {
	f.t.Helper()

	var got blockstoriov1alpha1.Resource

	err := f.cli.Get(f.t.Context(), client.ObjectKey{Name: k8s.ResourceCRName("pvc-kept", "n1")}, &got)
	if err != nil {
		f.t.Fatalf("get resource: %v", err)
	}

	return got.Status
}

// read is the replica as the REST surface and the CLI see it.
func (f *keptStateFixture) read() apiv1.Resource {
	f.t.Helper()

	resources, err := k8s.New(f.cli).Resources().List(f.t.Context())
	if err != nil {
		f.t.Fatalf("list resources: %v", err)
	}

	if len(resources) != 1 {
		f.t.Fatalf("read %d replicas; want 1", len(resources))
	}

	return resources[0]
}

// keptStateUp is what events2 prints for the running replica.
func keptStateUp() []string {
	return []string{
		"exists resource name:pvc-kept role:Secondary suspended:no",
		"exists connection name:pvc-kept peer-node-id:1 conn-name:n2 connection:Connected role:Secondary",
		"exists device name:pvc-kept volume:0 minor:1000 disk:UpToDate client:no quorum:yes",
	}
}

func diskStateOf(status *blockstoriov1alpha1.ResourceStatus) string {
	for _, vol := range status.Volumes {
		if vol.VolumeNumber == 0 {
			return vol.DiskState
		}
	}

	return ""
}

// A frame that carries no volumes or links, such as a role change,
// must not erase the ones the observer wrote before: Status is
// written with server-side apply, which removes what the observer
// leaves out, and every read until the next resync showed the
// replica with no state.
func TestObserverFrameWithoutVolumesKeepsStoredState(t *testing.T) {
	f := newKeptStateFixture(t)
	f.feed(keptStateUp()...)

	f.feed("change resource name:pvc-kept role:Primary")

	st := f.status()
	if got := diskStateOf(&st); got != "UpToDate" {
		t.Errorf("disk state after a role change = %q; want UpToDate", got)
	}

	if len(st.Connections) != 1 {
		t.Errorf("%d peer links after a role change; want 1", len(st.Connections))
	}

	f.feed("change connection name:pvc-kept peer-node-id:1 conn-name:n2 connection:Connecting")

	st = f.status()
	if got := diskStateOf(&st); got != "UpToDate" {
		t.Errorf("disk state after a link change = %q; want UpToDate", got)
	}
}

// A KernelLoaded=False left from before the replica came back must not
// make a running replica read gone when a device frame that changed
// nothing arrives. Resource frames stamp the Condition again, so the
// frame here is one that does not.
func TestObserverStaleKernelLoadedFalseRunningReplicaReadsPresent(t *testing.T) {
	f := newKeptStateFixture(t)
	f.feed(keptStateUp()...)

	err := f.o.stampKernelLoaded(t.Context(), "pvc-kept", false)
	if err != nil {
		t.Fatalf("stamp stale False: %v", err)
	}

	f.feed("change device name:pvc-kept volume:0 minor:1000 disk:UpToDate client:no quorum:yes")

	if f.read().KernelSlotDestroyed {
		t.Error("a running replica with a stale KernelLoaded=False reads as gone after an unchanged device frame")
	}
}

// What `drbdsetup down` prints, written by the observer, reads as a
// replica whose kernel resource is gone.
func TestObserverDownFramesReadKernelResourceGone(t *testing.T) {
	f := newKeptStateFixture(t)
	f.feed(keptStateUp()...)

	if f.read().KernelSlotDestroyed {
		t.Fatal("a running replica reads as gone")
	}

	f.feed(
		"destroy device name:pvc-kept volume:0 minor:1000",
		"destroy connection name:pvc-kept peer-node-id:1 conn-name:n2",
		"destroy resource name:pvc-kept",
	)

	st := f.status()
	if !meta.IsStatusConditionFalse(st.Conditions, blockstoriov1alpha1.ConditionKernelLoaded) {
		t.Errorf("KernelLoaded after down = %+v; want False", st.Conditions)
	}

	if !f.read().KernelSlotDestroyed {
		t.Errorf("replica after down does not read as gone; status %+v", st)
	}
}
