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
	"net/http"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerPhysicalStorage wires the `linstor physical-storage`
// endpoints. Phase 10.7: the GET endpoints now surface
// PhysicalDevice CRDs from the store; the cluster-wide list groups
// devices by (node, size, rotational) per upstream LINSTOR's
// PhysicalStorage shape. The POST create-device-pool path stays
// 501 until the satellite-side reconciler lands — operators can
// already trigger attach by editing PhysicalDevice.Spec.AttachTo
// directly via `kubectl edit`.
func (s *Server) registerPhysicalStorage(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/physical-storage",
		s.requireStore(s.handlePhysicalStorageList))
	mux.HandleFunc("GET /v1/nodes/{node}/physical-storage",
		s.requireStore(s.handlePhysicalStorageListForNode))
	mux.HandleFunc("POST /v1/physical-storage/{node}",
		handlePhysicalStorageCreateNotImplemented)
}

// physicalStorageEntry is the envelope golinstor expects on
// GET /v1/physical-storage — devices grouped by attributes (size,
// rotational), with each group carrying the per-node list of
// devices in that bucket.
type physicalStorageEntry struct {
	Size       int64                                            `json:"size,omitempty"`
	Rotational *bool                                            `json:"rotational,omitempty"`
	Nodes      map[string][]physicalStorageDeviceWireRepetition `json:"nodes,omitempty"`
}

// physicalStorageDeviceWireRepetition mirrors upstream's
// PhysicalStorageDevice (slimmer than our internal apiv1.PhysicalDevice).
type physicalStorageDeviceWireRepetition struct {
	Device string `json:"device,omitempty"`
	Model  string `json:"model,omitempty"`
	Serial string `json:"serial,omitempty"`
	WWN    string `json:"wwn,omitempty"`
}

// handlePhysicalStorageList groups all available PhysicalDevices
// (Phase=Available, AttachTo=nil) by (size, rotational) and
// returns them in the upstream-LINSTOR envelope shape.
func (s *Server) handlePhysicalStorageList(w http.ResponseWriter, r *http.Request) {
	devs, err := s.Store.PhysicalDevices().List(r.Context())
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, groupPhysicalDevices(filterAvailable(devs)))
}

// handlePhysicalStorageListForNode returns the per-node devices
// list — the bucket map for a single node, flattened. piraeus-
// operator uses this to know which devices a satellite has free.
func (s *Server) handlePhysicalStorageListForNode(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	devs, err := s.Store.PhysicalDevices().ListForNode(r.Context(), node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	out := make([]physicalStorageDeviceWireRepetition, 0, len(devs))

	for i := range devs {
		if devs[i].Phase != "" && devs[i].Phase != crdv1alpha1.PhysicalDevicePhaseAvailable {
			continue
		}

		if devs[i].AttachTo != nil {
			continue
		}

		out = append(out, physicalStorageDeviceWireRepetition{
			Device: devs[i].DevicePath,
			Model:  devs[i].Model,
			Serial: devs[i].Serial,
			WWN:    devs[i].StableID,
		})
	}

	writeJSON(w, http.StatusOK, out)
}

// filterAvailable drops PhysicalDevices that are Attaching/Failed
// or already have an AttachTo target. Available + AttachTo=nil is
// the "free for new pool" signal upstream cares about.
func filterAvailable(devs []apiv1.PhysicalDevice) []apiv1.PhysicalDevice {
	out := make([]apiv1.PhysicalDevice, 0, len(devs))

	for i := range devs {
		if devs[i].Phase != "" && devs[i].Phase != crdv1alpha1.PhysicalDevicePhaseAvailable {
			continue
		}

		if devs[i].AttachTo != nil {
			continue
		}

		out = append(out, devs[i])
	}

	return out
}

// groupPhysicalDevices buckets devices by (size, rotational) and
// emits each bucket as a physicalStorageEntry. The shape matches
// upstream's grouping so `linstor physical-storage list` displays
// the same per-bucket counts blockstor users would see in vanilla
// LINSTOR.
func groupPhysicalDevices(devs []apiv1.PhysicalDevice) []physicalStorageEntry {
	type bucketKey struct {
		size       int64
		rotational bool
		hasRotaSet bool
	}

	buckets := map[bucketKey]*physicalStorageEntry{}

	for i := range devs {
		key := bucketKey{size: devs[i].SizeBytes}
		if devs[i].Rotational != nil {
			key.rotational = *devs[i].Rotational
			key.hasRotaSet = true
		}

		entry, ok := buckets[key]
		if !ok {
			entry = &physicalStorageEntry{Size: devs[i].SizeBytes, Nodes: map[string][]physicalStorageDeviceWireRepetition{}}

			if key.hasRotaSet {
				rota := key.rotational
				entry.Rotational = &rota
			}

			buckets[key] = entry
		}

		entry.Nodes[devs[i].NodeName] = append(entry.Nodes[devs[i].NodeName],
			physicalStorageDeviceWireRepetition{
				Device: devs[i].DevicePath,
				Model:  devs[i].Model,
				Serial: devs[i].Serial,
				WWN:    devs[i].StableID,
			})
	}

	out := make([]physicalStorageEntry, 0, len(buckets))
	for _, entry := range buckets {
		out = append(out, *entry)
	}

	return out
}

// handlePhysicalStorageCreateNotImplemented surfaces 501 with a
// LINSTOR-shaped ApiCallRc body explaining the boundary. piraeus-
// operator's `LinstorSatelliteConfiguration.spec.storagePools` would
// otherwise retry the call indefinitely.
//
// The Phase 10.7 design will replace this with a handler that flips
// `PhysicalDevice.Spec.AttachTo` on a free device matching the
// request — once the satellite-side reconciler is wired up.
func handlePhysicalStorageCreateNotImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented,
		"physical-storage create is out of scope for blockstor; "+
			"provision storage pools via Talos extensions / static node config")
}
