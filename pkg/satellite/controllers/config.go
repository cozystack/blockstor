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

package controllers

import (
	"github.com/cozystack/blockstor/pkg/satellite"
)

// Config carries the bits the four reconcilers need beyond what
// the controller-runtime manager wires for them automatically.
// Mirrors the existing `pkg/satellite.ReconcilerConfig` shape:
// the apply-chain on a Resource event ultimately invokes the
// pre-existing `pkg/satellite.Reconciler.applyOne` body via the
// `Apply` shim, so we hold one and reuse it.
type Config struct {
	// NodeName is the satellite's own name. The Resource +
	// StoragePool reconcilers filter `Spec.NodeName == NodeName`
	// at the predicate layer; the Snapshot reconciler checks
	// `Spec.Nodes ∋ NodeName` inside its reconcile body.
	NodeName string

	// Apply is the existing satellite reconciler — the one that
	// today serves the gRPC `ApplyResources` handler and owns
	// the storage + DRBD + LUKS chain. Phase 10.1 reuses it so
	// the controller-runtime path doesn't duplicate logic.
	Apply *satellite.Reconciler
}
