//go:build integration

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

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// The size bound lives on the CRD, and it only works there because
// volumeDefinitions is keyed on volumeNumber.
//
// On an atomic list the API server compares the whole slice, so ratcheting
// cannot spare an element the update did not touch: a volume written before
// the bound existed rejects every later write to its definition, including
// the controller's own, since it rewrites the list to stamp drbdMinor. Keyed,
// an untouched element is not re-validated.
//
// Both halves are asserted against a real apiserver, because the whole
// difference between them is apiserver behaviour and cannot be read off the
// markers.
func TestVDSizeBoundRatchetsOnAKeyedList(t *testing.T) {
	stack := harness.StartStack(t)
	ctx := context.Background()

	// A volume BELOW the floor, which is the only value that exercises
	// ratcheting. Seeded at the floor the object is valid under any schema,
	// so the test passes on an atomic list and on a keyed one alike, and
	// proves nothing about either.
	//
	// Getting one below the floor means writing it while the bound is not
	// there, which is exactly how an in-place upgrade produces it: the
	// cluster ran without the bound, then the new CRD arrives over the top.
	crds := crdClient(t, stack)
	withSizeBound(t, ctx, crds, stack.Env.Client, false)

	legacy := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "ratchet-legacy"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	err := stack.Env.Client.Create(ctx, legacy)
	if err != nil {
		t.Fatalf("seed the pre-upgrade definition: %v", err)
	}

	withSizeBound(t, ctx, crds, stack.Env.Client, true)

	// The upgrade has landed. Appending a second, in-range volume is the
	// controller's own shape — it rewrites this list to stamp drbdMinor. On
	// an atomic list the API server re-validates every element and rejects
	// this naming volume 0, which locks the definition against every later
	// write including the controller's.
	var fetched blockstoriov1alpha1.ResourceDefinition

	err = stack.Env.Client.Get(ctx, types.NamespacedName{Name: "ratchet-legacy"}, &fetched)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	fetched.Spec.VolumeDefinitions = append(fetched.Spec.VolumeDefinitions,
		blockstoriov1alpha1.ResourceDefinitionVolume{VolumeNumber: 1, SizeKib: 8 * 1024})

	err = stack.Env.Client.Update(ctx, &fetched)
	if err != nil {
		t.Fatalf("appending an in-range volume was rejected, so the bound poisons "+
			"writes that do not touch the offending element: %v", err)
	}

	// And volume 0 is still there, untouched, at its grandfathered size.
	err = stack.Env.Client.Get(ctx, types.NamespacedName{Name: "ratchet-legacy"}, &fetched)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}

	if got := fetched.Spec.VolumeDefinitions[0].SizeKib; got != 1024 {
		t.Errorf("grandfathered volume = %d KiB, want 1024", got)
	}

	// The bound still holds for anything new.
	fresh := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "ratchet-fresh"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	err = stack.Env.Client.Create(ctx, fresh)
	if err == nil {
		t.Fatal("a fresh definition below the floor was accepted; the bound is gone")
	}

	if !strings.Contains(err.Error(), "sizeKib") {
		t.Errorf("the refusal does not name the field: %v", err)
	}
}

// crdClient talks to the apiextensions group, which the harness scheme does
// not carry.
func crdClient(t *testing.T, stack *harness.Stack) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("register apiextensions scheme: %v", err)
	}

	c, err := client.New(stack.Env.Cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("apiextensions client: %v", err)
	}

	return c
}

// withSizeBound adds or removes the sizeKib minimum on the live CRD, and
// waits until the API server actually enforces the new schema — the update
// returns before the served schema catches up.
func withSizeBound(
	t *testing.T, ctx context.Context, crds, objects client.Client, want bool,
) {
	t.Helper()

	const crdName = "resourcedefinitions.blockstor.cozystack.io"

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var crd apiextensionsv1.CustomResourceDefinition

		if err := crds.Get(ctx, types.NamespacedName{Name: crdName}, &crd); err != nil {
			return err
		}

		for i := range crd.Spec.Versions {
			props := crd.Spec.Versions[i].Schema.OpenAPIV3Schema.
				Properties["spec"].Properties["volumeDefinitions"].Items.Schema.Properties["sizeKib"]

			if want {
				props.Minimum = ptr(float64(vdBoundsMinSizeKib))
			} else {
				props.Minimum = nil
			}

			crd.Spec.Versions[i].Schema.OpenAPIV3Schema.
				Properties["spec"].Properties["volumeDefinitions"].Items.Schema.Properties["sizeKib"] = props
		}

		return crds.Update(ctx, &crd)
	})
	if err != nil {
		t.Fatalf("set sizeKib bound to %v: %v", want, err)
	}

	// Poll the served behaviour rather than the object: the apiserver
	// rebuilds its validator asynchronously after the CRD write.
	probe := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "ratchet-probe"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		probeErr := objects.Create(ctx, probe.DeepCopy())
		refused := probeErr != nil

		if refused == want {
			if !refused {
				_ = objects.Delete(ctx, probe)
			}

			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("the API server never started enforcing bound=%v", want)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func ptr[T any](v T) *T { return &v }

// Zero is the value the field's own comment says must never reach the
// satellite, because it loops on drbdadm create-md rather than failing. With
// the bound off the CRD it persisted through any writer that is neither the
// CLI nor REST.
func TestVDSizeZeroIsRefusedByTheAPIServer(t *testing.T) {
	stack := harness.StartStack(t)
	ctx := context.Background()

	for name, size := range map[string]int64{
		"zero":     0,
		"negative": -1,
	} {
		rd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: "ratchet-" + name},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
					{VolumeNumber: 0, SizeKib: size},
				},
			},
		}

		if err := stack.Env.Client.Create(ctx, rd); err == nil {
			t.Errorf("%s size (%d) was accepted and persisted", name, size)
		}
	}
}
