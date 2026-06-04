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

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Corner-case I2: property inheritance is a reference, not a copy.
//
// UG9 §"Setting QoS using LINSTOR sysfs properties" (~4563-4588):
//
//	IMPORTANT: Settings made to a group or definition will affect both
//	existing and new resources created from the group or definition.
//	NOTE: As the QoS properties are inherited and not copied, you will
//	not see the property listed in any "child" objects that have been
//	spawned from the "parent" group or definition.
//
// blockstor's behaviour, pinned here:
//
//  1. Inheritance is a REFERENCE (MATCHES upstream). An RG property set
//     AFTER the RD exists is reflected on the next read — effective
//     props resolve at read time (effectivePropsForRD), not at spawn.
//     Changing the value re-resolves to the new value.
//
//  2. Inherited keys ARE shown inline in `rd lp` (DELIBERATE DELTA,
//     known-deltas row 67). Upstream's NOTE says the inherited key is
//     NOT listed in the child; blockstor inlines Controller/RG-scope
//     keys into the RD's `props` map (stampRDEffectiveProps) so the
//     python CLI's `rd lp` table renders the effective value. The
//     inherited key carries scope=RESOURCE_GROUP in effective_props so
//     a scope-aware client can still tell it apart from an RD-local key.
func TestCornerI2_InheritanceIsReferenceAndShownInline(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const qosKey = "sys/fs/blkio_throttle_write"

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:  "rg-1",
		Props: map[string]string{qosKey: "1048576"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	// RD spawned from rg-1 carries NO local copy of the QoS key.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd-1",
		ResourceGroupName: "rg-1",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	getRD := func() apiv1.ResourceDefinition {
		t.Helper()

		resp := httpGet(t, base+"/v1/resource-definitions/rd-1")
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET rd-1: status %d", resp.StatusCode)
		}

		var rd apiv1.ResourceDefinition
		if err := json.NewDecoder(resp.Body).Decode(&rd); err != nil {
			t.Fatalf("decode rd: %v", err)
		}

		return rd
	}

	// (2) DELIBERATE DELTA: the inherited RG key is inlined into the
	// RD's bare `props` so `rd lp` shows it.
	rd := getRD()
	if rd.Props[qosKey] != "1048576" {
		t.Errorf("inherited RG prop not inlined in rd.props (rd lp would render empty): props=%v", rd.Props)
	}

	// The effective_props scope annotation still marks it as RG-scope so
	// a scope-aware client can distinguish inherited from RD-local.
	if entry, ok := rd.EffectiveProps[qosKey]; !ok ||
		entry.Scope != apiv1.EffectivePropScopeResourceGroup {
		t.Errorf("effective_props[%s] scope wrong: %+v (want RESOURCE_GROUP)", qosKey, rd.EffectiveProps[qosKey])
	}

	// (1) REFERENCE not copy: mutate the RG prop AFTER the RD exists;
	// the next read of the RD MUST reflect the new value.
	if err := st.ResourceGroups().PatchResourceGroup(ctx, "rg-1", func(rg *apiv1.ResourceGroup) error {
		if rg.Props == nil {
			rg.Props = map[string]string{}
		}

		rg.Props[qosKey] = "2097152"

		return nil
	}); err != nil {
		t.Fatalf("patch RG prop: %v", err)
	}

	rd2 := getRD()
	if rd2.Props[qosKey] != "2097152" {
		t.Errorf("inheritance is not a live reference: post-change rd.props=%v, want %s=2097152",
			rd2.Props, qosKey)
	}
}
