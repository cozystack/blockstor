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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
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

	// Exec is the shell-out interface the StoragePool reconciler
	// hands to `satellite.NewProviderFromKind` when materialising
	// a Provider instance from a StoragePool CRD. Production
	// wires `storage.RealExec{}`; tests inject `storage.FakeExec`.
	Exec storage.Exec

	// APIReader is a direct apiserver reader that bypasses the
	// informer cache. Bug 65: handleDelete must re-read the
	// Resource's finalizer slice right before stripping ours so
	// concurrent finalizer edits (controller force-strip, external
	// actors) are preserved rather than clobbered by an Update
	// built off a cache-trailed snapshot. Production is wired from
	// `mgr.GetAPIReader()` in NewManager; unit tests can leave it
	// nil and the reconciler falls back to the cached client.
	APIReader client.Reader

	// HealthProbeBindAddress is the address the manager's healthz /
	// readyz HTTP endpoints bind to. Empty disables the probe
	// server (controller-runtime default). Production wires
	// `:8081` from cmd/satellite/main.go; tests typically leave it
	// empty so the probe server doesn't race port bindings between
	// parallel runs.
	HealthProbeBindAddress string

	// ReconcileTrigger is the channel the ObserverRunnable emits
	// `event.GenericEvent` onto whenever a kernel-state change for
	// a local Resource lands (resource lifecycle, role, disk, conn,
	// repl). The ResourceReconciler attaches it as an additional
	// WatchesRawSource input so satellite-side recovery decisions
	// (Phase 11.7) wake on observed state even when no apiserver
	// write fires a Generation bump.
	//
	// Production wires it from `NewManager.ensureWiredDefaults`
	// with a buffered channel of `reconcileTriggerBuffer` slots
	// shared between the observer (producer) and the reconciler
	// (consumer). Unit tests can leave it nil and the wiring
	// short-circuits — neither Watches consumer nor producer fires.
	ReconcileTrigger chan event.GenericEvent
}
