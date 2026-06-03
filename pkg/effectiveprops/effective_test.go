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

package effectiveprops_test

import (
	"context"
	"testing"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
)

func int32Ptr(v int32) *int32 { return &v }

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

// TestResolve_CloserScopeWinsOverController is the C2 "closer to the
// resource wins" regression for the controller tier. `controller
// drbd-options --max-buffers` lands in ControllerConfig.Spec.ExtraProps
// (raw, untyped — see pkg/rest/controller_props.go). A more-specific
// `rd drbd-options --max-buffers` is transcoded into the RD's typed
// Spec.DRBDOptions. The effective render must honour the RD value;
// the controller's cluster-wide default must NOT win over a closer
// scope's explicit override.
func TestResolve_CloserScopeWinsOverController(t *testing.T) {
	scheme := newScheme(t)

	ctrlCfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: v1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			// `controller drbd-options --max-buffers=36864` path.
			ExtraProps: map[string]string{
				"DrbdOptions/Net/max-buffers": "36864",
			},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: v1.ObjectMeta{Name: "rd1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			// `rd drbd-options --max-buffers=8192` → typed.
			DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
				Net: &blockstoriov1alpha1.DRBDNetOptions{
					MaxBuffers: int32Ptr(8192),
				},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: v1.ObjectMeta{Name: "rd1-node1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ctrlCfg).
		Build()

	got, err := effectiveprops.Resolve(context.Background(), client.Reader(cli), res, rd)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if got["DrbdOptions/Net/max-buffers"] != "8192" {
		t.Fatalf("closer scope (RD) must win over controller: got max-buffers=%q, want 8192",
			got["DrbdOptions/Net/max-buffers"])
	}
}

// TestResolve_ControllerExtraPropsInheritedWhenUnset confirms the
// controller value DOES survive when no closer scope overrides it —
// the C1 retroactive-inheritance question: a controller-level
// drbd-option must reach the effective render of a resource that sets
// nothing of its own.
func TestResolve_ControllerExtraPropsInheritedWhenUnset(t *testing.T) {
	scheme := newScheme(t)

	ctrlCfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: v1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				"DrbdOptions/Net/max-buffers": "36864",
			},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: v1.ObjectMeta{Name: "rd1"},
		Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: v1.ObjectMeta{Name: "rd1-node1"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ctrlCfg).
		Build()

	got, err := effectiveprops.Resolve(context.Background(), client.Reader(cli), res, rd)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if got["DrbdOptions/Net/max-buffers"] != "36864" {
		t.Fatalf("controller-level drbd-option must be inherited when unset closer: got %q, want 36864",
			got["DrbdOptions/Net/max-buffers"])
	}
}

// TestResolve_RGOverridesController_RDOverridesRG walks the full
// Controller < RG < RD precedence chain for a single key, each scope
// carrying the value through whichever store representation the CLI
// produces (controller → ExtraProps, RG/RD → typed). Confirms the
// closer scope wins at every hop (C2 controller-vs-RG and RG-vs-RD).
func TestResolve_RGOverridesController_RDOverridesRG(t *testing.T) {
	scheme := newScheme(t)

	ctrlCfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: v1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{"DrbdOptions/Net/max-buffers": "1000"},
		},
	}

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: v1.ObjectMeta{Name: "rg1"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
				Net: &blockstoriov1alpha1.DRBDNetOptions{MaxBuffers: int32Ptr(2000)},
			},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: v1.ObjectMeta{Name: "rd1"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "rg1",
			DRBDOptions: &blockstoriov1alpha1.DRBDOptions{
				Net: &blockstoriov1alpha1.DRBDNetOptions{MaxBuffers: int32Ptr(3000)},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{ObjectMeta: v1.ObjectMeta{Name: "rd1-node1"}}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ctrlCfg, rg).
		Build()

	got, err := effectiveprops.Resolve(context.Background(), client.Reader(cli), res, rd)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if got["DrbdOptions/Net/max-buffers"] != "3000" {
		t.Fatalf("RD (closest) must win the chain: got max-buffers=%q, want 3000",
			got["DrbdOptions/Net/max-buffers"])
	}

	// Drop the RD override → RG (2000) must now win over controller (1000).
	rd.Spec.DRBDOptions = nil

	got, err = effectiveprops.Resolve(context.Background(), client.Reader(cli), res, rd)
	if err != nil {
		t.Fatalf("Resolve (no RD): %v", err)
	}

	if got["DrbdOptions/Net/max-buffers"] != "2000" {
		t.Fatalf("RG must win over controller when RD is unset: got %q, want 2000",
			got["DrbdOptions/Net/max-buffers"])
	}
}

// TestResolve_ControllerNonDRBDPropDropped pins that a controller-level
// non-DRBD ExtraProp (something outside the DrbdOptions/ namespace) does
// NOT leak onto the resource's effective props — only the resource's own
// non-DRBD raw Spec.Props survive. Mirrors drbd.ResolveOptions'
// controller-non-DRBD-drop contract through the effective resolver.
func TestResolve_ControllerNonDRBDPropDropped(t *testing.T) {
	scheme := newScheme(t)

	ctrlCfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: v1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{"Aux/zone": "from-controller"},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: v1.ObjectMeta{Name: "rd1"}}
	res := &blockstoriov1alpha1.Resource{ObjectMeta: v1.ObjectMeta{Name: "rd1-node1"}}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ctrlCfg).
		Build()

	got, err := effectiveprops.Resolve(context.Background(), client.Reader(cli), res, rd)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if v, ok := got["Aux/zone"]; ok {
		t.Fatalf("controller non-DRBD ExtraProp leaked onto resource: Aux/zone=%q", v)
	}
}
