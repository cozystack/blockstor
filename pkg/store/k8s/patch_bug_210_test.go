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

// Bug 210: Resources.Update and Resources.PatchResourceSpec wholesale-
// replace ObjectMeta.Annotations, wiping the satellite-stamped
// `blockstor.io/volume-numbers` key (Bug 107) on every routine REST
// modify. A concurrent RD cascade-delete that hits `lookupVolumeNumbers`
// during the harm window leaks the backing LV/zvol.
//
// The race: REST handler Get returns a snapshot without the annotation,
// satellite stamps `blockstor.io/volume-numbers`, REST Update wipes it
// via the wholesale write. Self-healing within ≤5s (observerResyncInterval)
// but the harm window is real and uniquely exposed via the cascade-delete
// path that depends on the annotation as a fallback record.
//
// Asymmetry vs RD/RG/Snapshot stores: they all use
// `mergeUserAnnotationsInto` which preserves internal `blockstor.io/*`
// keys. Resources is the lone holdout using wholesale replace.

const resourceVolumeNumbersAnnotation = "blockstor.io/volume-numbers"

func TestBug210_UpdatePreservesVolumeNumbersWhenOperatorOmits(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	// Seed Resource CRD with satellite-stamped `volume-numbers`
	// annotation. Simulates the steady-state shape: the satellite's
	// successful apply pass has recorded the per-volume number list
	// so the Bug-107 cascade-delete fallback has something to read.
	seed := crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rd-x.node-a",
			Annotations: map[string]string{
				resourceVolumeNumbersAnnotation: "0,1",
			},
			Labels: map[string]string{
				k8s.LabelResourceDefinition: "rd-x",
				k8s.LabelNodeName:           "node-a",
			},
		},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "rd-x",
			NodeName:               "node-a",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.Resource{}).
		Build()

	s := k8s.New(cli)

	// Simulate the race: REST handler did Get BEFORE the satellite
	// stamped `blockstor.io/volume-numbers`, so the wire snapshot
	// carries a non-nil but stale annotation set (e.g. the
	// `blockstor.io/peer-changed` bump from Bug 67). The satellite
	// then stamps `volume-numbers` between Get and Update. The
	// REST handler's Update with the stale snapshot must NOT wipe
	// the satellite-stamped key.
	in := apiv1.Resource{
		Name:     "rd-x",
		NodeName: "node-a",
		Annotations: map[string]string{
			"blockstor.io/peer-changed": "2026-05-16T12:00:00Z",
		},
	}

	if err := s.Resources().Update(context.Background(), &in); err != nil {
		t.Fatalf("update: %v", err)
	}

	var got crdv1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "rd-x.node-a"}, &got); err != nil {
		t.Fatalf("get post-update: %v", err)
	}

	if got.Annotations[resourceVolumeNumbersAnnotation] != "0,1" {
		t.Fatalf("Bug 210: `blockstor.io/volume-numbers` annotation was WIPED by Resources.Update; "+
			"got %q want %q. RD cascade-delete fallback (Bug 107) now leaks the backing LV/zvol.",
			got.Annotations[resourceVolumeNumbersAnnotation], "0,1")
	}
}

func TestBug210_UpdateMergesOperatorAnnotationsPreservingInternal(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rd-y.node-a",
			Annotations: map[string]string{
				resourceVolumeNumbersAnnotation: "0,1",
			},
			Labels: map[string]string{
				k8s.LabelResourceDefinition: "rd-y",
				k8s.LabelNodeName:           "node-a",
			},
		},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "rd-y",
			NodeName:               "node-a",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.Resource{}).
		Build()

	s := k8s.New(cli)

	// Operator pushes a user-facing annotation. The internal
	// `blockstor.io/volume-numbers` key must NOT be in the wire
	// payload (REST handlers only know about user-managed keys);
	// the store-side merge must preserve it.
	in := apiv1.Resource{
		Name:     "rd-y",
		NodeName: "node-a",
		Annotations: map[string]string{
			"myorg.com/team": "storage",
		},
	}

	if err := s.Resources().Update(context.Background(), &in); err != nil {
		t.Fatalf("update: %v", err)
	}

	var got crdv1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "rd-y.node-a"}, &got); err != nil {
		t.Fatalf("get post-update: %v", err)
	}

	if got.Annotations["myorg.com/team"] != "storage" {
		t.Fatalf("user annotation `myorg.com/team` not persisted: got %q want %q",
			got.Annotations["myorg.com/team"], "storage")
	}
	if got.Annotations[resourceVolumeNumbersAnnotation] != "0,1" {
		t.Fatalf("Bug 210: internal `blockstor.io/volume-numbers` WIPED on user-annotation merge; "+
			"got %q want %q",
			got.Annotations[resourceVolumeNumbersAnnotation], "0,1")
	}
}

func TestBug210_PatchResourceSpecPreservesVolumeNumbersWhenClosureOmits(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rd-z.node-a",
			Annotations: map[string]string{
				resourceVolumeNumbersAnnotation: "0,1",
			},
			Labels: map[string]string{
				k8s.LabelResourceDefinition: "rd-z",
				k8s.LabelNodeName:           "node-a",
			},
		},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "rd-z",
			NodeName:               "node-a",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.Resource{}).
		Build()

	s := k8s.New(cli)

	// Closure mutates an unrelated wire field and explicitly clears
	// the annotations the wire saw (simulating an operator action
	// that drops all user annotations). The internal
	// `blockstor.io/volume-numbers` key must survive — it lives on
	// the CRD, not the wire surface that the closure operates on.
	err := s.Resources().PatchResourceSpec(context.Background(), "rd-z", "node-a", func(live *apiv1.Resource) error {
		live.Annotations = nil

		return nil
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	var got crdv1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "rd-z.node-a"}, &got); err != nil {
		t.Fatalf("get post-patch: %v", err)
	}

	if got.Annotations[resourceVolumeNumbersAnnotation] != "0,1" {
		t.Fatalf("Bug 210: `blockstor.io/volume-numbers` annotation was WIPED by Resources.PatchResourceSpec; "+
			"got %q want %q",
			got.Annotations[resourceVolumeNumbersAnnotation], "0,1")
	}
}

func TestBug210_PatchResourceSpecMergesOperatorAnnotationsPreservingInternal(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rd-w.node-a",
			Annotations: map[string]string{
				resourceVolumeNumbersAnnotation: "0,1",
			},
			Labels: map[string]string{
				k8s.LabelResourceDefinition: "rd-w",
				k8s.LabelNodeName:           "node-a",
			},
		},
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: "rd-w",
			NodeName:               "node-a",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.Resource{}).
		Build()

	s := k8s.New(cli)

	err := s.Resources().PatchResourceSpec(context.Background(), "rd-w", "node-a", func(live *apiv1.Resource) error {
		if live.Annotations == nil {
			live.Annotations = map[string]string{}
		}

		live.Annotations["myorg.com/team"] = "storage"
		// Closure also strips the internal key it observed on the
		// wire — proves the merge restores it from the CRD.
		delete(live.Annotations, resourceVolumeNumbersAnnotation)

		return nil
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	var got crdv1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "rd-w.node-a"}, &got); err != nil {
		t.Fatalf("get post-patch: %v", err)
	}

	if got.Annotations["myorg.com/team"] != "storage" {
		t.Fatalf("user annotation `myorg.com/team` not persisted: got %q want %q",
			got.Annotations["myorg.com/team"], "storage")
	}
	if got.Annotations[resourceVolumeNumbersAnnotation] != "0,1" {
		t.Fatalf("Bug 210: internal `blockstor.io/volume-numbers` WIPED by Resources.PatchResourceSpec; "+
			"got %q want %q",
			got.Annotations[resourceVolumeNumbersAnnotation], "0,1")
	}
}
