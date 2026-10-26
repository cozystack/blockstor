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

package k8s_test

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestTheAPIServerRefusesToEmptyADecidedLayerStack: the store keeps a
// pinned stack across a stale write-back, and the CRD's transition rule
// holds the same line for every other writer (kubectl, another
// controller). Setting a stack and replacing it stay allowed.
func TestTheAPIServerRefusesToEmptyADecidedLayerStack(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()
	rd := &crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-cel-pin"}}

	if err := fixture.client.Create(ctx, rd); err != nil {
		t.Fatalf("create with no stack: %v", err)
	}

	rd.Spec.LayerStack = []string{"DRBD", "LUKS", "STORAGE"}
	if err := fixture.client.Update(ctx, rd); err != nil {
		t.Fatalf("set the stack: %v", err)
	}

	rd.Spec.LayerStack = []string{"DRBD", "STORAGE"}
	if err := fixture.client.Update(ctx, rd); err != nil {
		t.Fatalf("replace the stack: %v", err)
	}

	rd.Spec.LayerStack = nil

	err := fixture.client.Update(ctx, rd)
	if err == nil {
		t.Fatal("emptying a decided stack: want the transition rule's refusal, got nil")
	}

	if !strings.Contains(err.Error(), "never emptied") {
		t.Fatalf("emptying a decided stack: want the transition rule's message, got %v", err)
	}
}
