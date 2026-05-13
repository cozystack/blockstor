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
	"errors"
	"fmt"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Static sentinel errors for layer-stack validation. Per scenario 6.9
// (tests/scenarios/06-storage-backends.md), the REST handler rejects
// unsupported layers and bad ordering at the wire boundary. Callers
// wrap these with the offending input via fmt.Errorf("%w: %s", ...).
var (
	// ErrUnsupportedLayer fires for any layer outside the
	// {DRBD, LUKS, STORAGE} allowlist (CACHE, WRITECACHE, NVME, etc.).
	ErrUnsupportedLayer = errors.New("unsupported layer")

	// ErrInvalidLayerOrder fires when DRBD isn't first / STORAGE
	// isn't last / LUKS appears in the wrong place / a layer repeats.
	ErrInvalidLayerOrder = errors.New("invalid layer order")
)

// validateLayerStack enforces blockstor's narrowed layer set per scenario
// 6.9 (tests/scenarios/06-storage-backends.md). Upstream LINSTOR ships
// CACHE, WRITECACHE, NVMe-oF, and NVMe-TCP layers; cozystack does not
// support any of them (rationale in 6.11) so the REST handler rejects
// them at the wire boundary with a 400 + clear text.
//
// Rules:
//   - Empty stack is allowed (the caller defaults to DefaultLayerStack()).
//   - Allowed layers: DRBD, LUKS, STORAGE. Comparison is case-insensitive
//     on input — upstream LINSTOR accepts mixed case.
//   - Ordering (top to bottom): DRBD if present must be first; STORAGE
//     must be last; LUKS, if present, sits between DRBD and STORAGE.
//     LUKS-before-DRBD is rejected — LUKS must be a child of DRBD so
//     DRBD replicates ciphertext, not plaintext.
//
// Returns nil on success; a non-nil error suitable for surfacing as
// the 400 body otherwise.
func validateLayerStack(layers []string) error {
	if len(layers) == 0 {
		return nil
	}

	normalized, err := normalizeLayerStack(layers)
	if err != nil {
		return err
	}

	return validateLayerStackOrder(normalized)
}

// normalizeLayerStack upper-cases each entry and rejects anything outside
// the {DRBD, LUKS, STORAGE} allowlist. The original (un-normalized) token
// is surfaced in the error so operators can grep their request body for
// the offending entry verbatim.
func normalizeLayerStack(layers []string) ([]string, error) {
	out := make([]string, 0, len(layers))

	for _, raw := range layers {
		layer := strings.ToUpper(strings.TrimSpace(raw))

		switch layer {
		case apiv1.LayerKindDRBD, apiv1.LayerKindLUKS, apiv1.LayerKindStorage:
			out = append(out, layer)
		default:
			return nil, fmt.Errorf("%w: %s (blockstor supports DRBD, LUKS, STORAGE)",
				ErrUnsupportedLayer, raw)
		}
	}

	return out, nil
}

// validateLayerStackOrder enforces:
//   - STORAGE must be the terminal (last) layer.
//   - DRBD, if present, must be index 0.
//   - LUKS, if present alongside DRBD, must be a child (i.e. below) of DRBD.
//   - No layer appears twice.
//
// Allowed shapes: [STORAGE], [LUKS,STORAGE], [DRBD,STORAGE], [DRBD,LUKS,STORAGE].
func validateLayerStackOrder(normalized []string) error {
	joined := strings.Join(normalized, ",")

	if normalized[len(normalized)-1] != apiv1.LayerKindStorage {
		return fmt.Errorf("%w: STORAGE must be the terminal (last) layer; got %s",
			ErrInvalidLayerOrder, joined)
	}

	drbdIdx := -1
	luksIdx := -1

	for i, layer := range normalized {
		switch layer {
		case apiv1.LayerKindDRBD:
			drbdIdx = i
		case apiv1.LayerKindLUKS:
			luksIdx = i
		}
	}

	if drbdIdx > 0 {
		return fmt.Errorf("%w: DRBD must be the first layer when present; got %s",
			ErrInvalidLayerOrder, joined)
	}

	if luksIdx >= 0 && drbdIdx >= 0 && luksIdx < drbdIdx {
		// Unreachable with current rule set (DRBD-must-be-0), but
		// pinned explicitly so the intent survives a future refactor:
		// LUKS-above-DRBD means DRBD replicates plaintext.
		return fmt.Errorf("%w: LUKS must be a child of DRBD, not parent; got %s",
			ErrInvalidLayerOrder, joined)
	}

	seen := map[string]bool{}
	for _, layer := range normalized {
		if seen[layer] {
			return fmt.Errorf("%w: layer %s appears more than once in %s",
				ErrInvalidLayerOrder, layer, joined)
		}

		seen[layer] = true
	}

	return nil
}
