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

package drbd

import (
	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// ResolveDRBDOptions is the typed-fields equivalent of ResolveOptions.
// It walks the upstream LINSTOR override hierarchy
// (Controller → ResourceGroup → ResourceDefinition → Resource) and
// merges each lower scope's non-nil fields into the accumulator.
//
// nil/missing scopes are skipped. Within each scope, only non-empty
// strings and non-nil pointer fields override — nil pointers (`*bool`,
// `*int32`) signal "not overridden at this scope, inherit from
// parent". This matches the design summary in PLAN.md Phase 10.3.
//
// Returns a freshly-allocated *DRBDOptions; the caller may mutate it
// without disturbing inputs. Returns nil only if all four inputs are
// nil — useful for short-circuiting "RD has no DRBD config and never
// inherits any" paths.
func ResolveDRBDOptions(
	controller, rg, rd, resource *blockstoriov1alpha1.DRBDOptions,
) *blockstoriov1alpha1.DRBDOptions {
	scopes := []*blockstoriov1alpha1.DRBDOptions{controller, rg, rd, resource}

	if !anyNonNil(scopes) {
		return nil
	}

	out := &blockstoriov1alpha1.DRBDOptions{}

	for _, src := range scopes {
		if src == nil {
			continue
		}

		mergeNet(out, src.Net)
		mergeDisk(out, src.Disk)
		mergePeerDevice(out, src.PeerDevice)
		mergeResource(out, src.Resource)
		mergeHandlers(out, src.Handlers)
	}

	return out
}

// anyNonNil reports whether the slice contains at least one non-nil
// pointer. Tiny helper kept inline so the resolver doesn't allocate
// anything on the all-nil short-circuit path.
func anyNonNil(scopes []*blockstoriov1alpha1.DRBDOptions) bool {
	for _, s := range scopes {
		if s != nil {
			return true
		}
	}

	return false
}

// mergeNet folds src.Net into out.Net field-by-field, only overriding
// when src has a non-empty / non-nil value. Allocates out.Net on
// first non-nil src.
func mergeNet(out *blockstoriov1alpha1.DRBDOptions, src *blockstoriov1alpha1.DRBDNetOptions) {
	if src == nil {
		return
	}

	if out.Net == nil {
		out.Net = &blockstoriov1alpha1.DRBDNetOptions{}
	}

	if src.Protocol != "" {
		out.Net.Protocol = src.Protocol
	}

	if src.SharedSecretRef != nil {
		out.Net.SharedSecretRef = src.SharedSecretRef
	}

	if src.AllowTwoPrimaries != nil {
		out.Net.AllowTwoPrimaries = src.AllowTwoPrimaries
	}

	if src.MaxBuffers != nil {
		out.Net.MaxBuffers = src.MaxBuffers
	}

	if src.AfterSb0Pri != "" {
		out.Net.AfterSb0Pri = src.AfterSb0Pri
	}

	if src.AfterSb1Pri != "" {
		out.Net.AfterSb1Pri = src.AfterSb1Pri
	}

	if src.AfterSb2Pri != "" {
		out.Net.AfterSb2Pri = src.AfterSb2Pri
	}
}

func mergeDisk(out *blockstoriov1alpha1.DRBDOptions, src *blockstoriov1alpha1.DRBDDiskOptions) {
	if src == nil {
		return
	}

	if out.Disk == nil {
		out.Disk = &blockstoriov1alpha1.DRBDDiskOptions{}
	}

	if src.OnIOError != "" {
		out.Disk.OnIOError = src.OnIOError
	}

	if src.ALExtents != nil {
		out.Disk.ALExtents = src.ALExtents
	}
}

func mergePeerDevice(out *blockstoriov1alpha1.DRBDOptions, src *blockstoriov1alpha1.DRBDPeerDeviceOptions) {
	if src == nil {
		return
	}

	if out.PeerDevice == nil {
		out.PeerDevice = &blockstoriov1alpha1.DRBDPeerDeviceOptions{}
	}

	if src.CMaxRate != "" {
		out.PeerDevice.CMaxRate = src.CMaxRate
	}
}

func mergeResource(out *blockstoriov1alpha1.DRBDOptions, src *blockstoriov1alpha1.DRBDResourceOptions) {
	if src == nil {
		return
	}

	if out.Resource == nil {
		out.Resource = &blockstoriov1alpha1.DRBDResourceOptions{}
	}

	if src.AutoPromote != nil {
		out.Resource.AutoPromote = src.AutoPromote
	}

	if src.Quorum != "" {
		out.Resource.Quorum = src.Quorum
	}

	if src.OnNoQuorum != "" {
		out.Resource.OnNoQuorum = src.OnNoQuorum
	}
}

// mergeHandlers folds src handler entries into out.Handlers. Per-key
// override semantics — a non-empty value overrides any previous,
// empty string from a lower scope deletes the entry (mirrors what
// `linstor c sp DrbdOptions/Handlers/fence-peer ""` would do
// upstream).
func mergeHandlers(out *blockstoriov1alpha1.DRBDOptions, src map[string]string) {
	if len(src) == 0 {
		return
	}

	if out.Handlers == nil {
		out.Handlers = map[string]string{}
	}

	for handler, script := range src {
		if script == "" {
			delete(out.Handlers, handler)

			continue
		}

		out.Handlers[handler] = script
	}
}
