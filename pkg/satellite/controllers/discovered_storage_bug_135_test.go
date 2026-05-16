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

package controllers_test

import (
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Bug 135 — satellite-side discovered-storage publisher.
//
// The apiserver-side Bug 135 fix (commit 82e67e78e) refuses
// `linstor sp c lvmthin <node> <pool> /garbage/path` when the
// requested VG / zpool isn't in the satellite's stamped discovery
// set on the Node CRD. The reader keys are:
//
//   - Node.Spec.Props["Aux/DiscoveredVGs"]    — comma-joined VG list.
//   - Node.Spec.Props["Aux/DiscoveredZPools"] — comma-joined zpool list.
//
// Without the satellite half, both keys stay unset and the
// apiserver's permissive fall-through admits garbage. The tests
// here pin the satellite's contract for stamping those props.

// vgsCmdline is the exact FakeExec key for the satellite's
// `vgs --config <ConfigFilter> --noheadings -o vg_name` invocation.
// The `--config` flag is the same upstream-mirroring guard every
// LVM CLI call carries (rejects /dev/drbd, /dev/zd) and is part of
// the FakeExec command-line match.
const vgsCmdline = "vgs --config " + lvm.ConfigFilter + " --noheadings -o vg_name"

// zpoolCmdline is the FakeExec key for the satellite's
// `zpool list -H -o name` invocation. `-H` strips the header,
// `-o name` projects only the pool-name column.
const zpoolCmdline = "zpool list -H -o name"

// TestBug135SatellitePublishesDiscoveredVGs: one tick of the
// runnable enumerates VGs via `vgs --noheadings -o vg_name` and
// stamps `Aux/DiscoveredVGs="myvg"` on the Node CRD's Spec.Props.
// The apiserver's Bug 135 pre-flight reads that prop via the Node
// store's wire-shape projection; without this stamp every pool
// create slips past the permissive fall-through.
func TestBug135SatellitePublishesDiscoveredVGs(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect(vgsCmdline, storage.FakeResponse{Stdout: []byte("  myvg\n")})
	fx.Expect(zpoolCmdline, storage.FakeResponse{Stdout: []byte("")})

	r := &controllers.DiscoveredStorageRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "worker-1",
	}

	err := controllers.DiscoveryTickForTest(t.Context(), r, logr.Discard())
	if err != nil {
		t.Fatalf("DiscoveryTickForTest: %v", err)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-1"}, &got); err != nil {
		t.Fatalf("Get post-tick: %v", err)
	}

	if got.Spec.Props["Aux/DiscoveredVGs"] != "myvg" {
		t.Errorf("Aux/DiscoveredVGs: got %q, want %q",
			got.Spec.Props["Aux/DiscoveredVGs"], "myvg")
	}

	// ZFS probe returned an empty list — the key is still
	// advertised (set-to-empty) so the apiserver's checkAdvertised
	// can refuse ZFS pool creates rather than fall through to the
	// satellite. The "absent" semantic is reserved for "satellite
	// hasn't ticked yet"; an empty list is "satellite ticked,
	// nothing here".
	v, ok := got.Spec.Props["Aux/DiscoveredZPools"]
	if !ok {
		t.Errorf("Aux/DiscoveredZPools: key not stamped after tick (want set-to-empty)")
	}

	if v != "" {
		t.Errorf("Aux/DiscoveredZPools: got %q, want \"\"", v)
	}
}

// TestBug135SatellitePublishesDiscoveredZPools: ZFS probe surfaces
// one zpool ("mypool"); the runnable stamps it under
// `Aux/DiscoveredZPools`. Pin the ZFS half of the contract; the
// apiserver's checkAdvertised path for StoragePoolKindZFS /
// StoragePoolKindZFSThin reads this key.
func TestBug135SatellitePublishesDiscoveredZPools(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect(vgsCmdline, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect(zpoolCmdline, storage.FakeResponse{Stdout: []byte("mypool\n")})

	r := &controllers.DiscoveredStorageRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "worker-1",
	}

	err := controllers.DiscoveryTickForTest(t.Context(), r, logr.Discard())
	if err != nil {
		t.Fatalf("DiscoveryTickForTest: %v", err)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-1"}, &got); err != nil {
		t.Fatalf("Get post-tick: %v", err)
	}

	if got.Spec.Props["Aux/DiscoveredZPools"] != "mypool" {
		t.Errorf("Aux/DiscoveredZPools: got %q, want %q",
			got.Spec.Props["Aux/DiscoveredZPools"], "mypool")
	}
}

// TestBug135SatelliteUpdatesOnVGAddRemove: drive three ticks back
// to back with different VG lists ([a] → [a,b] → [b]) and confirm
// the Node CRD prop tracks. This is the live-cluster behaviour the
// apiserver depends on — operators run `vgcreate / vgremove` and
// the next satellite tick must refresh the advertised set so the
// apiserver pre-flight reflects reality.
func TestBug135SatelliteUpdatesOnVGAddRemove(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect(zpoolCmdline, storage.FakeResponse{Stdout: []byte("")})

	r := &controllers.DiscoveredStorageRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "worker-1",
	}

	type tickCase struct {
		vgsOutput string
		wantProp  string
	}

	cases := []tickCase{
		{vgsOutput: "a\n", wantProp: "a"},
		{vgsOutput: "a\nb\n", wantProp: "a,b"},
		{vgsOutput: "b\n", wantProp: "b"},
	}

	for i, tc := range cases {
		fx.Expect(vgsCmdline, storage.FakeResponse{Stdout: []byte(tc.vgsOutput)})

		err := controllers.DiscoveryTickForTest(t.Context(), r, logr.Discard())
		if err != nil {
			t.Fatalf("tick %d DiscoveryTickForTest: %v", i, err)
		}

		var got blockstoriov1alpha1.Node
		if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-1"}, &got); err != nil {
			t.Fatalf("tick %d Get: %v", i, err)
		}

		if got.Spec.Props["Aux/DiscoveredVGs"] != tc.wantProp {
			t.Errorf("tick %d Aux/DiscoveredVGs: got %q, want %q",
				i, got.Spec.Props["Aux/DiscoveredVGs"], tc.wantProp)
		}
	}
}

// TestBug135MultiValueCommaJoined: probe surfaces three VGs; the
// stamped prop must be the comma-joined form the apiserver's
// `strings.SplitSeq(raw, ",")` parser expects. Order is the order
// `vgs` printed (no sort) so operators reading the prop see the
// same listing as `vgs` on the host. The apiserver pre-flight only
// asks "is `want` in this set?", so order is operator-facing only.
func TestBug135MultiValueCommaJoined(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect(vgsCmdline, storage.FakeResponse{Stdout: []byte("vg1\nvg2\nvg3\n")})
	fx.Expect(zpoolCmdline, storage.FakeResponse{Stdout: []byte("zp1\nzp2\n")})

	r := &controllers.DiscoveredStorageRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "worker-1",
	}

	err := controllers.DiscoveryTickForTest(t.Context(), r, logr.Discard())
	if err != nil {
		t.Fatalf("DiscoveryTickForTest: %v", err)
	}

	var got blockstoriov1alpha1.Node
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "worker-1"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Spec.Props["Aux/DiscoveredVGs"] != "vg1,vg2,vg3" {
		t.Errorf("Aux/DiscoveredVGs: got %q, want %q",
			got.Spec.Props["Aux/DiscoveredVGs"], "vg1,vg2,vg3")
	}

	if got.Spec.Props["Aux/DiscoveredZPools"] != "zp1,zp2" {
		t.Errorf("Aux/DiscoveredZPools: got %q, want %q",
			got.Spec.Props["Aux/DiscoveredZPools"], "zp1,zp2")
	}
}
