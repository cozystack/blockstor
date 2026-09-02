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

// Package validate holds the invariants that must hold for every writer,
// whatever door it came through.
//
// blockstor has two of those doors: the REST shim that upstream LINSTOR
// clients speak to, and the native CLI, which writes the CRDs directly and
// never goes near the REST server. An invariant implemented on one door only
// is not an invariant — it is a habit of that door, and the other one becomes
// a way around it. Several rounds of review on the CLI found exactly that,
// one rule at a time, so the rules live here now and both callers delegate.
//
// What belongs here: checks that are pure functions of the input and of state
// the caller has already fetched. What does not: anything needing a store, a
// cluster round trip, or REST-specific wire shapes.
package validate

import (
	"errors"
	"fmt"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

var (
	// ErrUnsupportedLayer fires for any layer outside the
	// {DRBD, LUKS, STORAGE} allowlist (CACHE, WRITECACHE, NVME, etc.).
	ErrUnsupportedLayer = errors.New("unsupported layer")

	// ErrInvalidLayerOrder fires when DRBD isn't first / STORAGE isn't
	// last / LUKS appears in the wrong place / a layer repeats.
	ErrInvalidLayerOrder = errors.New("invalid layer order")
)

// LayerStack enforces blockstor's narrowed layer set. Upstream LINSTOR ships
// CACHE, WRITECACHE, NVMe-oF and NVMe-TCP layers; cozystack supports none of
// them.
//
// The rules read as ergonomics until you look at what the satellite does with
// the result. Its predicates ask whether the stack CONTAINS a layer, so an
// unrecognised token is not a loud error downstream — it is a silently absent
// layer. `DRDB,STORAGE` renders no .res file and brings the volume up as a
// single local copy while the operator believes it is replicated, and
// `DRBD,LUSK,STORAGE` writes plaintext while the operator believes it is
// encrypted. A one-character typo is the whole distance between those states,
// which is why an unknown layer is refused rather than ignored.
//
// Rules:
//   - An empty stack is allowed; the caller defaults it.
//   - Allowed layers: DRBD, LUKS, STORAGE. Input is matched
//     case-insensitively, as upstream LINSTOR accepts mixed case.
//   - Ordering, top to bottom: DRBD first if present, STORAGE last, LUKS
//     between them. LUKS above DRBD is refused — DRBD must replicate
//     ciphertext, not plaintext.
func LayerStack(layers []string) error {
	if len(layers) == 0 {
		return nil
	}

	normalized, err := NormalizeLayerStack(layers)
	if err != nil {
		return err
	}

	return layerStackOrder(normalized)
}

// NormalizeLayerStack upper-cases each entry and rejects anything outside the
// allowlist. The original token is surfaced in the error so an operator can
// grep their input for the offending entry verbatim.
func NormalizeLayerStack(layers []string) ([]string, error) {
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

// layerStackOrder enforces the shape rules on an already-normalized stack.
//
// Allowed shapes: [STORAGE], [LUKS,STORAGE], [DRBD,STORAGE], [DRBD,LUKS,STORAGE].
func layerStackOrder(normalized []string) error {
	joined := strings.Join(normalized, ",")

	// Duplicate detection runs FIRST so a stack like `DRBD,DRBD,STORAGE` is
	// reported as a duplicate rather than tripping the position check below:
	// the second DRBD lands at index 1 and would otherwise be reported as
	// "DRBD must be the first layer", which names the wrong cause.
	seen := map[string]bool{}
	for _, layer := range normalized {
		if seen[layer] {
			return fmt.Errorf("%w: layer %s appears more than once in %s",
				ErrInvalidLayerOrder, layer, joined)
		}

		seen[layer] = true
	}

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
		// Unreachable with the current rule set (DRBD must be index 0), but
		// pinned explicitly so the intent survives a refactor: LUKS above
		// DRBD means DRBD replicates plaintext.
		return fmt.Errorf("%w: LUKS must be a child of DRBD, not parent; got %s",
			ErrInvalidLayerOrder, joined)
	}

	return nil
}
