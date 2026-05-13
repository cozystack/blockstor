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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestValidateLayerStack_AllowsSupportedStacks pins scenario 6.9's
// allow-table: the four shapes blockstor supports must pass.
func TestValidateLayerStack_AllowsSupportedStacks(t *testing.T) {
	t.Parallel()

	cases := [][]string{
		nil, // empty → DefaultLayerStack() at the caller
		{},
		{"DRBD", "LUKS", "STORAGE"},
		{"DRBD", "STORAGE"},
		{"LUKS", "STORAGE"},
		{"STORAGE"},
		// Mixed-case input — upstream LINSTOR accepts it.
		{"drbd", "storage"},
		{"Drbd", "Luks", "Storage"},
	}

	for _, in := range cases {
		if err := validateLayerStack(in); err != nil {
			t.Errorf("validateLayerStack(%v) = %v, want nil", in, err)
		}
	}
}

// TestValidateLayerStack_RejectsUnsupportedLayers pins scenario 6.9's
// reject-table (unsupported layers): CACHE / WRITECACHE / NVME must
// return ErrUnsupportedLayer with the offending token in the message.
func TestValidateLayerStack_RejectsUnsupportedLayers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in    []string
		token string
	}{
		{[]string{"CACHE", "DRBD", "STORAGE"}, "CACHE"},
		{[]string{"DRBD", "WRITECACHE", "STORAGE"}, "WRITECACHE"},
		{[]string{"NVME", "STORAGE"}, "NVME"},
		{[]string{"OPENFLEX", "STORAGE"}, "OPENFLEX"},
	}

	for _, tc := range cases {
		err := validateLayerStack(tc.in)
		if err == nil {
			t.Errorf("validateLayerStack(%v) = nil; want error", tc.in)

			continue
		}

		if !errors.Is(err, ErrUnsupportedLayer) {
			t.Errorf("validateLayerStack(%v) = %v; want ErrUnsupportedLayer", tc.in, err)
		}

		if !strings.Contains(err.Error(), tc.token) {
			t.Errorf("error %q should mention offending token %q", err, tc.token)
		}
	}
}

// TestValidateLayerStack_RejectsBadOrdering pins scenario 6.9's
// reject-table (ordering): LUKS-before-DRBD, STORAGE-not-last, and
// duplicate-layer all return ErrInvalidLayerOrder.
func TestValidateLayerStack_RejectsBadOrdering(t *testing.T) {
	t.Parallel()

	cases := [][]string{
		{"LUKS", "DRBD", "STORAGE"}, // LUKS above DRBD
		{"STORAGE", "DRBD"},         // STORAGE not last
		{"DRBD", "STORAGE", "LUKS"}, // STORAGE in middle
		{"STORAGE", "LUKS"},         // STORAGE not last
		{"DRBD", "DRBD", "STORAGE"}, // duplicate
		{"STORAGE", "STORAGE"},      // duplicate
	}

	for _, in := range cases {
		err := validateLayerStack(in)
		if err == nil {
			t.Errorf("validateLayerStack(%v) = nil; want error", in)

			continue
		}

		if !errors.Is(err, ErrInvalidLayerOrder) {
			t.Errorf("validateLayerStack(%v) = %v; want ErrInvalidLayerOrder", in, err)
		}
	}
}

// TestRGCreateRejectsBadLayerStack drives the validator through the
// REST handler — POST /v1/resource-groups with a CACHE / wrong-order
// layer_stack returns 400 with the validation text in the body.
func TestRGCreateRejectsBadLayerStack(t *testing.T) {
	cases := []struct {
		name     string
		layers   []string
		wantText string
	}{
		{"cache", []string{"CACHE", "DRBD", "STORAGE"}, "unsupported layer"},
		{"writecache", []string{"DRBD", "WRITECACHE", "STORAGE"}, "unsupported layer"},
		{"nvme", []string{"NVME", "STORAGE"}, "unsupported layer"},
		{"luks-above-drbd", []string{"LUKS", "DRBD", "STORAGE"}, "invalid layer order"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, stop := startServerWithStore(t, store.NewInMemory())
			defer stop()

			body, _ := json.Marshal(apiv1.ResourceGroup{
				Name: "rg-" + tc.name,
				SelectFilter: apiv1.AutoSelectFilter{
					LayerStack: tc.layers,
				},
			})

			resp := httpPost(t, base+"/v1/resource-groups", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status: got %d, want 400 for %v", resp.StatusCode, tc.layers)
			}

			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			if !strings.Contains(strings.ToLower(string(raw)), tc.wantText) {
				t.Errorf("body should contain %q; got %s", tc.wantText, raw)
			}
		})
	}
}

// TestRGCreateAcceptsSupportedLayerStack mirrors the positive half:
// each allowed shape comes back as a 201 Created.
func TestRGCreateAcceptsSupportedLayerStack(t *testing.T) {
	cases := []struct {
		name   string
		layers []string
	}{
		{"drbd-luks-storage", []string{"DRBD", "LUKS", "STORAGE"}},
		{"drbd-storage", []string{"DRBD", "STORAGE"}},
		{"luks-storage", []string{"LUKS", "STORAGE"}},
		{"storage-only", []string{"STORAGE"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, stop := startServerWithStore(t, store.NewInMemory())
			defer stop()

			body, _ := json.Marshal(apiv1.ResourceGroup{
				Name: "rg-" + tc.name,
				SelectFilter: apiv1.AutoSelectFilter{
					LayerStack: tc.layers,
				},
			})

			resp := httpPost(t, base+"/v1/resource-groups", body)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusCreated {
				t.Errorf("status for %v: got %d, want 201", tc.layers, resp.StatusCode)
			}
		})
	}
}

// TestRDCreateRejectsBadLayerStack mirrors the RG-create test against
// the RD create handler — the same validator must apply on POST
// /v1/resource-definitions so callers that skip the RG path can't
// sneak a CACHE / WRITECACHE / NVME stack through.
func TestRDCreateRejectsBadLayerStack(t *testing.T) {
	cases := []struct {
		name     string
		layers   []string
		wantText string
	}{
		{"cache", []string{"CACHE", "DRBD", "STORAGE"}, "unsupported layer"},
		{"luks-above-drbd", []string{"LUKS", "DRBD", "STORAGE"}, "invalid layer order"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, stop := startServerWithStore(t, store.NewInMemory())
			defer stop()

			body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
				ResourceDefinition: apiv1.ResourceDefinition{
					Name:       "rd-" + tc.name,
					LayerStack: tc.layers,
				},
			})

			resp := httpPost(t, base+"/v1/resource-definitions", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status: got %d, want 400 for %v", resp.StatusCode, tc.layers)
			}

			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			if !strings.Contains(strings.ToLower(string(raw)), tc.wantText) {
				t.Errorf("body should contain %q; got %s", tc.wantText, raw)
			}
		})
	}
}
