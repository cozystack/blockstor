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
	"github.com/cozystack/blockstor/pkg/validate"
)

// Static sentinel errors for layer-stack validation. Per scenario 6.9
// (tests/scenarios/06-storage-backends.md), the REST handler rejects
// unsupported layers and bad ordering at the wire boundary. Callers
// wrap these with the offending input via fmt.Errorf("%w: %s", ...).
//
// The rules themselves live in pkg/validate so the native CLI, which writes
// the CRDs directly and never reaches this package, enforces the same ones.
// A layer rule that held only here would not be a rule: the CLI would be the
// way around it.
var (
	// ErrUnsupportedLayer fires for any layer outside the
	// {DRBD, LUKS, STORAGE} allowlist (CACHE, WRITECACHE, NVME, etc.).
	ErrUnsupportedLayer = validate.ErrUnsupportedLayer

	// ErrInvalidLayerOrder fires when DRBD isn't first / STORAGE
	// isn't last / LUKS appears in the wrong place / a layer repeats.
	ErrInvalidLayerOrder = validate.ErrInvalidLayerOrder
)

// validateLayerStack enforces blockstor's narrowed layer set per scenario
// 6.9 (tests/scenarios/06-storage-backends.md).
//
// Rules, rationale and the allowed shapes are documented on
// validate.LayerStack.
//
// The returned error is this project's own sentinel, already carrying the
// offending input, so it is passed through rather than wrapped: wrapping
// would duplicate the message the handler formats into the 400 body.
//
//nolint:wrapcheck // see above
func validateLayerStack(layers []string) error {
	return validate.LayerStack(layers)
}
