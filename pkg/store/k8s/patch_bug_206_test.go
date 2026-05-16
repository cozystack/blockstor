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
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Bug 206: wireToCRDResourceSpec builds a fresh CRD Spec from the
// wire shape, which omits Spec.Volumes — the controller-stamped slice
// carrying SeedFromGi and per-volume DRBD layout. Both `Resources.Update`
// and `Resources.PatchResourceSpec` (Bug 204b) wholesale-assigned the
// returned Spec, wiping Volumes on every routine REST modify
// (`r modify --property foo=bar`, layer-stack change, flag toggle).
// DRBD then did a full initial-sync over the wire instead of the
// seeded fast path. These witnesses pin the carry-across behaviour for
// both code paths.

// TestBug206_PatchResourceSpecWipesSpecVolumes proves that a routine
// `r modify` after the controller has populated Spec.Volumes[i].SeedFromGi
// causes the SeedFromGi to be silently wiped, forcing a full DRBD
// initial-sync on next satellite reconcile.
func TestBug206_PatchResourceSpecWipesSpecVolumes(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	// Resource on which the controller has pre-stamped SeedFromGi.
	seed := crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd1.n1"},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "rd1",
			NodeName:               "n1",
			Volumes: []crdv1alpha1.ResourceVolumeSpec{{
				VolumeNumber: 0,
				SeedFromGi:   "78A0DDDABCDEF000",
			}},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.Resource{}).
		Build()

	s := k8s.New(cli)

	// REST handler does a routine prop bump (e.g. `r modify --property foo=bar`).
	err := s.Resources().PatchResourceSpec(context.Background(), "rd1", "n1", func(r *apiv1.Resource) error {
		if r.Props == nil {
			r.Props = map[string]string{}
		}
		r.Props["foo"] = "bar"
		return nil
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	// Verify SeedFromGi survives.
	var got crdv1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "rd1.n1"}, &got); err != nil {
		t.Fatalf("get post-patch: %v", err)
	}

	if len(got.Spec.Volumes) == 0 {
		t.Fatalf("Spec.Volumes was WIPED by the routine prop bump (Bug 206); SeedFromGi lost — DRBD will full-sync")
	}

	if got.Spec.Volumes[0].SeedFromGi != "78A0DDDABCDEF000" {
		t.Fatalf("SeedFromGi clobbered: got %q want %q", got.Spec.Volumes[0].SeedFromGi, "78A0DDDABCDEF000")
	}
}

// TestBug206_UpdateWipesSpecVolumes is the sibling witness for the
// legacy `Resources.Update` path. Same root cause as PatchResourceSpec
// — `existing.Spec = wireToCRDResourceSpec(in)` clobbers Spec.Volumes
// because the wire shape doesn't carry controller-stamped fields.
func TestBug206_UpdateWipesSpecVolumes(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd1.n1"},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "rd1",
			NodeName:               "n1",
			Volumes: []crdv1alpha1.ResourceVolumeSpec{{
				VolumeNumber: 0,
				SeedFromGi:   "78A0DDDABCDEF000",
			}},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.Resource{}).
		Build()

	s := k8s.New(cli)

	// Round-trip through the wire shape — the wire `Resource` carries
	// no Volumes spec field, so a vanilla read-modify-write loses the
	// controller-stamped slice on the wholesale Update replace.
	cur, err := s.Resources().Get(context.Background(), "rd1", "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if cur.Props == nil {
		cur.Props = map[string]string{}
	}
	cur.Props["foo"] = "bar"

	if err := s.Resources().Update(context.Background(), &cur); err != nil {
		t.Fatalf("update: %v", err)
	}

	var got crdv1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "rd1.n1"}, &got); err != nil {
		t.Fatalf("get post-update: %v", err)
	}

	if len(got.Spec.Volumes) == 0 {
		t.Fatalf("Spec.Volumes was WIPED by the routine Update (Bug 206); SeedFromGi lost — DRBD will full-sync")
	}

	if got.Spec.Volumes[0].SeedFromGi != "78A0DDDABCDEF000" {
		t.Fatalf("SeedFromGi clobbered: got %q want %q", got.Spec.Volumes[0].SeedFromGi, "78A0DDDABCDEF000")
	}
}
