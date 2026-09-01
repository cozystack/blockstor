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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

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

	// A definition carrying a volume below the floor, the way an in-place
	// upgrade leaves one behind. It is written through the store rather than
	// the REST path, which has its own gate.
	legacy := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "ratchet-legacy"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: vdBoundsMinSizeKib},
			},
		},
	}

	err := stack.Env.Client.Create(ctx, legacy)
	if err != nil {
		t.Fatalf("seed the legacy definition: %v", err)
	}

	// Appending a second, in-range volume is the controller's own shape. On
	// an atomic list this was rejected naming volume 0.
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
