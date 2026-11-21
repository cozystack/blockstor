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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// A satellite restart on the same kernel boot must keep the incarnation:
// the DRBD devices are untouched, so the state the previous process
// reported is still current, and a new name would hide every replica of
// the node until the observer had rewritten each one. A reboot drops the
// devices and must name a new one.
func TestIncarnationFollowsTheKernelBoot(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "boot_id")
	writeBootID(t, path, "boot-a\n")

	first, restarted := bootIncarnation(path), bootIncarnation(path)
	if first != "boot-a" || restarted != first {
		t.Fatalf("incarnations before and after a satellite restart = %q, %q; want boot-a twice", first, restarted)
	}

	writeBootID(t, path, "boot-b\n")

	if rebooted := bootIncarnation(path); rebooted != "boot-b" {
		t.Errorf("incarnation after a reboot = %q, want boot-b", rebooted)
	}
}

// Without a readable boot id each start still names a new process, so a
// report is hidden rather than shown when the kernel may no longer back it.
func TestIncarnationWithoutBootIDIsPerProcess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	writeBootID(t, empty, " \n")

	for _, path := range []string{filepath.Join(dir, "missing"), empty} {
		first, second := bootIncarnation(path), bootIncarnation(path)
		if first == "" || first == second {
			t.Errorf("%s: incarnations %q and %q; want two distinct non-empty ones", filepath.Base(path), first, second)
		}
	}

	preset := &Config{Incarnation: "given"}
	ensureWiredDefaults(preset)

	if preset.Incarnation != "given" {
		t.Errorf("a given incarnation was replaced with %q", preset.Incarnation)
	}
}

// The manager names its satellite after the host's kernel boot, so two
// wirings on one boot agree. Only a Linux host has the file to check.
func TestManagerIncarnationIsTheKernelBoot(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(kernelBootIDPath)
	if err != nil {
		t.Skipf("no kernel boot id here: %v", err)
	}

	first, second := &Config{}, &Config{}
	ensureWiredDefaults(first)
	ensureWiredDefaults(second)

	want := strings.TrimSpace(string(raw))
	if first.Incarnation != want || second.Incarnation != want {
		t.Errorf("incarnations %q and %q; want the boot id %q", first.Incarnation, second.Incarnation, want)
	}
}

func writeBootID(t *testing.T, path, id string) {
	t.Helper()

	err := os.WriteFile(path, []byte(id), 0o600)
	if err != nil {
		t.Fatal(err)
	}
}

// The heartbeat and the observer the manager runs carry the same
// incarnation: the heartbeat names the running process on the Node, the
// observer signs every report with it, and a read compares the two.
func TestManagerRunnablesCarryTheIncarnation(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		NodeName:    "worker-1",
		Incarnation: "inc-2",
		Apply:       satellite.NewReconciler(satellite.ReconcilerConfig{StateDir: t.TempDir()}),
	}

	if got := newHeartbeatRunnableFromConfig(nil, cfg).Incarnation; got != "inc-2" {
		t.Errorf("heartbeat incarnation = %q, want inc-2", got)
	}

	if got := newObserverRunnableFromConfig(nil, cfg).Incarnation; got != "inc-2" {
		t.Errorf("observer incarnation = %q, want inc-2", got)
	}
}

func incarnationScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	return scheme
}

func TestHeartbeatNamesTheRunningSatellite(t *testing.T) {
	t.Parallel()

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Status:     blockstoriov1alpha1.NodeStatus{SatelliteIncarnation: "inc-1"},
	}
	cli := fake.NewClientBuilder().WithScheme(incarnationScheme(t)).WithObjects(node).
		WithStatusSubresource(&blockstoriov1alpha1.Node{}).Build()

	hb := &HeartbeatRunnable{Client: cli, NodeName: "worker-1", Incarnation: "inc-2", Period: time.Hour}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	if err := hb.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-1"}, &got); err != nil {
		t.Fatalf("get node: %v", err)
	}

	if got.Status.SatelliteIncarnation != "inc-2" {
		t.Errorf("node incarnation = %q, want the running satellite's inc-2", got.Status.SatelliteIncarnation)
	}
}

// The observer signs the state it writes with its own incarnation, so a
// replica reported since the restart reads as current again.
func TestObserverSignsTheStateItWrites(t *testing.T) {
	t.Parallel()

	replica := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName("pvc-x", "worker-1")},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-x", NodeName: "worker-1"},
	}
	cli := fake.NewClientBuilder().WithScheme(incarnationScheme(t)).WithObjects(replica).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	// The satellite before the reboot, then the one after it.
	for _, run := range []struct{ incarnation, state string }{{"inc-1", "UpToDate"}, {"inc-2", "Inconsistent"}} {
		o := &ObserverRunnable{Client: cli, NodeName: "worker-1", Incarnation: run.incarnation}

		err := o.writeStatus(t.Context(), &observation{ResourceName: "pvc-x", DrbdState: run.state, Role: "Secondary"})
		if err != nil {
			t.Fatalf("writeStatus by %s: %v", run.incarnation, err)
		}
	}

	var got blockstoriov1alpha1.Resource
	if err := cli.Get(t.Context(), client.ObjectKey{Name: replica.Name}, &got); err != nil {
		t.Fatalf("get replica: %v", err)
	}

	if got.Status.SatelliteIncarnation != "inc-2" || got.Status.DrbdState != "Inconsistent" {
		t.Errorf("status = incarnation %q state %q; want inc-2 with the new state",
			got.Status.SatelliteIncarnation, got.Status.DrbdState)
	}
}
