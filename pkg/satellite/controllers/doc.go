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

// Package controllers is the satellite-side controller-runtime
// migration target. Phase 10.1 of the architectural overhaul:
// the satellite stops being a gRPC server that consumes
// `ApplyResources` from the controller and instead watches the
// apiserver directly for the CRDs it acts on.
//
// Four reconcilers live here, one per CRD type the satellite
// cares about:
//
//   - `ResourceReconciler` — Resources where
//     `Spec.NodeName == cfg.NodeName`. Replaces the gRPC
//     `ApplyResources` consumer.
//   - `ResourceDefinitionReconciler` — RDs. No own write path;
//     just feeds the cache (the Resource reconciler does
//     `client.Get(rd)` when computing the desired state).
//   - `SnapshotReconciler` — Snapshots whose `Spec.Nodes`
//     contains the satellite's name. Replaces the gRPC
//     `CreateSnapshot` / `DeleteSnapshot` consumer.
//   - `StoragePoolReconciler` — StoragePools where
//     `Spec.NodeName == cfg.NodeName`. Replaces the gRPC
//     `ApplyStoragePools` consumer. Phase 10.5's dynamic
//     `Reconciler.RegisterProvider` plugs in here.
//
// `NewManager` builds the controller-runtime manager and
// registers all four reconcilers. Once Phase 10.1 lands, the
// satellite's `agent.Run` becomes "build manager, register
// reconcilers, start manager" — mirroring `cmd/controller/main.go` on
// the controller side. Until then the gRPC server keeps
// running; this package's reconcilers are scaffolded but not
// yet wired into the agent.
//
// Reconciler bodies are intentionally minimal in this initial
// skeleton — they fetch the CRD, log what they see, and exit.
// Subsequent commits fill in the actual apply logic by
// delegating to the existing `pkg/satellite.Reconciler`
// methods, which already cover the storage + DRBD + LUKS
// chain.
package controllers
