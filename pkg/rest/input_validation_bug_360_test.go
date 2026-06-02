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
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 360: long RD names (>63 chars) bypassed the wire-side gate
// (which capped at 253 — the k8s metadata.name regime) and then
// exploded on the FIRST `r c` because the k8s store writes the
// RD-name into `metadata.labels` (LabelResourceDefinition in
// pkg/store/k8s/resources.go). k8s label VALUES are bounded to 63
// chars — anything longer leaks the raw apimachinery error to the
// operator and leaves a zombie RD that accepts `vd c` but never
// accepts a replica.
//
// Fix: cap maxLinstorName at 48 (matches upstream LINSTOR's
// DRBD_RES_NAME_MAX). The wire gate now fires at `rd c` before any
// partial state lands.
//
// Reproduction on dev stand (HEAD 6f69c5678, 2026-06-01):
//
//	$ linstor rd c $(printf 'a%.0s' {1..150})
//	SUCCESS: resource definition created
//	$ linstor vd c $LONG_NAME 64M
//	SUCCESS: volume definition created
//	$ linstor r c dev-worker-1 $LONG_NAME --storage-pool stand
//	ERROR: store error: create Resource …/dev-worker-1: <backend>
//	  "<150-a-name>.dev-worker-1" is invalid:
//	  metadata.labels: Invalid value: "aaa…aaa": must be no more
//	  than 63 characters

// TestBug360RDCreateRefusedOnNameOver48 pins the upper bound: an
// RD name strictly over 48 chars must be refused at `rd c` time with
// a LINSTOR envelope naming the rule, NOT a k8s apimachinery error
// surfacing through later. We exercise sizes that previously slipped
// past the 253-char cap (64, 150, 250) plus the boundary (49).
func TestBug360RDCreateRefusedOnNameOver48(t *testing.T) {
	t.Parallel()

	cases := []int{49, 64, 150, 250}

	for _, length := range cases {
		t.Run(makeLenLabel(length), func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()

			base, stop := startServerWithStore(t, st)
			defer stop()

			name := strings.Repeat("a", length)
			body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
				ResourceDefinition: apiv1.ResourceDefinition{Name: name},
			})

			resp := httpPost(t, base+"/v1/resource-definitions", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode < 400 || resp.StatusCode >= 500 {
				t.Fatalf("status: got %d, want 4xx (Bug 360: name >%d chars must be refused)",
					resp.StatusCode, maxLinstorName)
			}

			got, _ := readAllBody(resp)
			gotStr := string(got)
			// Envelope must name the offending length, not leak a k8s error.
			if !strings.Contains(gotStr, "exceeds") {
				t.Errorf("envelope should name the violated rule (exceeds %d chars); got %s",
					maxLinstorName, gotStr)
			}

			if strings.Contains(gotStr, "metadata.labels") {
				t.Errorf("envelope must NOT leak k8s apimachinery error; got %s", gotStr)
			}

			// Store must remain clean: zombie RD is the exact symptom
			// Bug 360 fixes.
			if _, err := st.ResourceDefinitions().Get(t.Context(), name); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("RD %q must NOT be persisted on refusal; got err=%v", name, err)
			}
		})
	}
}

// TestBug360RDCreateAcceptedAtBoundary pins the lower bound: 48-char
// names (the new cap) must still be accepted. Guards against the cap
// being one-off in the wrong direction.
func TestBug360RDCreateAcceptedAtBoundary(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	base, stop := startServerWithStore(t, st)
	defer stop()

	name := strings.Repeat("a", 48)
	body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: name},
	})

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 for 48-char name. Body: %s",
			resp.StatusCode, got)
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), name); err != nil {
		t.Errorf("RD %q not persisted at boundary: %v", name, err)
	}
}

// TestBug360RGSpawnRefusedOnLongRDName covers the second entry point:
// `linstor rg spawn rg-foo $LONG_NAME` should also be refused, sharing
// the same gate as `rd c` (the rd-name-validation-bulk fix in PR #56
// added validateLinstorName to spawn handler).
func TestBug360RGSpawnRefusedOnLongRDName(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed an RG so the spawn at least reaches the name-validation
	// gate (without the RG, the handler short-circuits earlier).
	rgName := "rg-bug360"
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: rgName}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	longName := strings.Repeat("b", 64)
	body, _ := json.Marshal(map[string]string{
		"resource_definition_name": longName,
	})

	resp := httpPost(t, base+"/v1/resource-groups/"+rgName+"/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		t.Fatalf("status: got %d, want 4xx (Bug 360 via spawn)", resp.StatusCode)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "exceeds") {
		t.Errorf("envelope should name the violated rule; got %s", got)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, longName); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("RD %q must NOT be persisted via spawn refusal; err=%v", longName, err)
	}
}

// makeLenLabel formats a subtest label like "len-49" so the
// parallel run report stays readable for the matrix of lengths.
func makeLenLabel(n int) string {
	switch n {
	case 49:
		return "len-49"
	case 64:
		return "len-64"
	case 150:
		return "len-150"
	case 250:
		return "len-250"
	default:
		return "len-other"
	}
}
