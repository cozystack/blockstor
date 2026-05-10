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

package k8s

import (
	"maps"
	"strconv"
	"strings"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// propsKVPrefix is the upstream-LINSTOR prefix used by every DRBD-
// related property key. Anything outside this prefix is left alone in
// the wire `Props` and never folded into typed DRBDOptions.
const propsKVPrefix = "DrbdOptions/"

// LINSTOR-native bool literals — golinstor sends `yes`/`no` over the
// wire, so we round-trip in the same form rather than Go's
// `true`/`false`. The Go-style spellings are accepted on input only.
const (
	wireBoolYes      = "yes"
	wireBoolNo       = "no"
	goStyleBoolTrue  = "true"
	goStyleBoolFalse = "false"
)

// propsToTyped splits a wire-format Props bag into typed DRBDOptions
// (recognised keys) and a leftover ExtraProps map (unknown
// `DrbdOptions/...` keys plus all non-DRBD keys). Phase 10.3.
//
// We mirror upstream LINSTOR's flat-property-bag schema on the wire
// but persist a typed structure inside the CRD so admission validation
// catches typos and the `.res` renderer can read structured config.
//
// An empty input returns (nil, nil) so callers can short-circuit on
// "no DRBD config to persist" without dereferencing.
func propsToTyped(props map[string]string) (*crdv1alpha1.DRBDOptions, map[string]string) {
	if len(props) == 0 {
		return nil, nil
	}

	var (
		opts   crdv1alpha1.DRBDOptions
		extras = map[string]string{}
	)

	for key, val := range props {
		if !strings.HasPrefix(key, propsKVPrefix) {
			extras[key] = val

			continue
		}

		if !applyTypedKey(&opts, key, val) {
			// Unknown DRBD key — keep in ExtraProps so a later
			// release can either type it or drop it explicitly,
			// without losing the user's intent in the meantime.
			extras[key] = val
		}
	}

	if len(extras) == 0 {
		extras = nil
	}

	if isEmptyDRBDOptions(&opts) {
		return nil, extras
	}

	return &opts, extras
}

// typedToProps re-emits typed DRBDOptions + ExtraProps as a flat wire
// Props map so golinstor (and existing REST clients) keep seeing the
// upstream LINSTOR shape on GET responses. Inverse of propsToTyped.
func typedToProps(opts *crdv1alpha1.DRBDOptions, extras map[string]string) map[string]string {
	if opts == nil && len(extras) == 0 {
		return nil
	}

	out := map[string]string{}

	maps.Copy(out, extras)

	if opts != nil {
		emitNetProps(out, opts.Net)
		emitDiskProps(out, opts.Disk)
		emitPeerDeviceProps(out, opts.PeerDevice)
		emitResourceProps(out, opts.Resource)
		emitHandlersProps(out, opts.Handlers)
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

// applyTypedKey routes one `DrbdOptions/...` key into the matching
// typed slot. Returns true when the key was recognised; false leaves
// it for ExtraProps. Each section is its own helper to keep this
// function inside golangci-lint's funlen budget.
func applyTypedKey(opts *crdv1alpha1.DRBDOptions, key, val string) bool {
	rest := strings.TrimPrefix(key, propsKVPrefix)

	switch {
	case strings.HasPrefix(rest, "Net/"):
		return applyNetKey(opts, strings.TrimPrefix(rest, "Net/"), val)
	case strings.HasPrefix(rest, "Disk/"):
		return applyDiskKey(opts, strings.TrimPrefix(rest, "Disk/"), val)
	case strings.HasPrefix(rest, "PeerDevice/"):
		return applyPeerDeviceKey(opts, strings.TrimPrefix(rest, "PeerDevice/"), val)
	case strings.HasPrefix(rest, "Resource/"):
		return applyResourceKey(opts, strings.TrimPrefix(rest, "Resource/"), val)
	case strings.HasPrefix(rest, "Handlers/"):
		return applyHandlerKey(opts, strings.TrimPrefix(rest, "Handlers/"), val)
	}

	// Section-less DRBD keys — upstream LINSTOR uses
	// `DrbdOptions/<Key>` (no inner `<Section>/`) for a handful of
	// controller-level toggles consumed by the controller, not by
	// drbdadm. Route the few we care about into typed slots.
	return applySectionlessKey(opts, rest, val)
}

// applySectionlessKey routes section-less `DrbdOptions/<Key>` props
// into typed slots. AutoAddQuorumTiebreaker is the only one we
// currently type — it gates the RD reconciler's witness creation.
// Phase 10.3.
func applySectionlessKey(opts *crdv1alpha1.DRBDOptions, name, val string) bool {
	if name == "AutoAddQuorumTiebreaker" {
		b, ok := parseBool(val)
		if !ok {
			return false
		}

		if opts.Resource == nil {
			opts.Resource = &crdv1alpha1.DRBDResourceOptions{}
		}

		opts.Resource.AutoTieBreaker = &b

		return true
	}

	return false
}

func applyNetKey(opts *crdv1alpha1.DRBDOptions, name, val string) bool {
	if opts.Net == nil {
		opts.Net = &crdv1alpha1.DRBDNetOptions{}
	}

	switch name {
	case "protocol":
		opts.Net.Protocol = val
	case "max-buffers":
		n, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return false
		}

		parsed := int32(n)
		opts.Net.MaxBuffers = &parsed
	case "allow-two-primaries":
		if b, ok := parseBool(val); ok {
			opts.Net.AllowTwoPrimaries = &b
		} else {
			return false
		}
	case "after-sb-0pri":
		opts.Net.AfterSb0Pri = val
	case "after-sb-1pri":
		opts.Net.AfterSb1Pri = val
	case "after-sb-2pri":
		opts.Net.AfterSb2Pri = val
	default:
		return false
	}

	return true
}

func applyDiskKey(opts *crdv1alpha1.DRBDOptions, name, val string) bool {
	if opts.Disk == nil {
		opts.Disk = &crdv1alpha1.DRBDDiskOptions{}
	}

	switch name {
	case "on-io-error":
		opts.Disk.OnIOError = val
	case "al-extents":
		n, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return false
		}

		parsed := int32(n)
		opts.Disk.ALExtents = &parsed
	default:
		return false
	}

	return true
}

func applyPeerDeviceKey(opts *crdv1alpha1.DRBDOptions, name, val string) bool {
	if opts.PeerDevice == nil {
		opts.PeerDevice = &crdv1alpha1.DRBDPeerDeviceOptions{}
	}

	switch name {
	case "c-max-rate":
		opts.PeerDevice.CMaxRate = val
	default:
		return false
	}

	return true
}

func applyResourceKey(opts *crdv1alpha1.DRBDOptions, name, val string) bool {
	if opts.Resource == nil {
		opts.Resource = &crdv1alpha1.DRBDResourceOptions{}
	}

	switch name {
	case "auto-promote":
		if b, ok := parseBool(val); ok {
			opts.Resource.AutoPromote = &b
		} else {
			return false
		}
	case "quorum":
		opts.Resource.Quorum = val
	case "on-no-quorum":
		opts.Resource.OnNoQuorum = val
	default:
		return false
	}

	return true
}

func applyHandlerKey(opts *crdv1alpha1.DRBDOptions, name, val string) bool {
	if opts.Handlers == nil {
		opts.Handlers = map[string]string{}
	}

	opts.Handlers[name] = val

	return true
}

func emitNetProps(out map[string]string, net *crdv1alpha1.DRBDNetOptions) {
	if net == nil {
		return
	}

	if net.Protocol != "" {
		out["DrbdOptions/Net/protocol"] = net.Protocol
	}

	if net.MaxBuffers != nil {
		out["DrbdOptions/Net/max-buffers"] = strconv.FormatInt(int64(*net.MaxBuffers), 10)
	}

	if net.AllowTwoPrimaries != nil {
		out["DrbdOptions/Net/allow-two-primaries"] = formatBool(*net.AllowTwoPrimaries)
	}

	if net.AfterSb0Pri != "" {
		out["DrbdOptions/Net/after-sb-0pri"] = net.AfterSb0Pri
	}

	if net.AfterSb1Pri != "" {
		out["DrbdOptions/Net/after-sb-1pri"] = net.AfterSb1Pri
	}

	if net.AfterSb2Pri != "" {
		out["DrbdOptions/Net/after-sb-2pri"] = net.AfterSb2Pri
	}
}

func emitDiskProps(out map[string]string, disk *crdv1alpha1.DRBDDiskOptions) {
	if disk == nil {
		return
	}

	if disk.OnIOError != "" {
		out["DrbdOptions/Disk/on-io-error"] = disk.OnIOError
	}

	if disk.ALExtents != nil {
		out["DrbdOptions/Disk/al-extents"] = strconv.FormatInt(int64(*disk.ALExtents), 10)
	}
}

func emitPeerDeviceProps(out map[string]string, peerDev *crdv1alpha1.DRBDPeerDeviceOptions) {
	if peerDev == nil {
		return
	}

	if peerDev.CMaxRate != "" {
		out["DrbdOptions/PeerDevice/c-max-rate"] = peerDev.CMaxRate
	}
}

func emitResourceProps(out map[string]string, res *crdv1alpha1.DRBDResourceOptions) {
	if res == nil {
		return
	}

	if res.AutoPromote != nil {
		out["DrbdOptions/Resource/auto-promote"] = formatBool(*res.AutoPromote)
	}

	if res.Quorum != "" {
		out["DrbdOptions/Resource/quorum"] = res.Quorum
	}

	if res.OnNoQuorum != "" {
		out["DrbdOptions/Resource/on-no-quorum"] = res.OnNoQuorum
	}

	if res.AutoTieBreaker != nil {
		// Upstream uses the section-less spelling for this controller-
		// level toggle; round-trip preserves it so golinstor sees the
		// same key it sent in.
		out["DrbdOptions/AutoAddQuorumTiebreaker"] = formatBool(*res.AutoTieBreaker)
	}
}

func emitHandlersProps(out, handlers map[string]string) {
	for name, script := range handlers {
		out["DrbdOptions/Handlers/"+name] = script
	}
}

// isEmptyDRBDOptions returns true when no field was populated. Saves
// us from persisting a vacuous `drbdOptions: {}` in the CRD and
// keeps the wire-format round-trip stable for clients that send no
// DRBD-shaped props at all.
func isEmptyDRBDOptions(opts *crdv1alpha1.DRBDOptions) bool {
	return opts.Net == nil && opts.Disk == nil && opts.PeerDevice == nil && opts.Resource == nil && len(opts.Handlers) == 0
}

// parseBool accepts every form upstream LINSTOR + golinstor are known
// to push: "yes"/"no" (LINSTOR-native), "true"/"false" (Go-style),
// "1"/"0" (numeric). Any other value returns ok=false so the caller
// can fall back to ExtraProps without silently coercing.
func parseBool(val string) (bool, bool) {
	switch strings.ToLower(val) {
	case wireBoolYes, goStyleBoolTrue, "1":
		return true, true
	case wireBoolNo, goStyleBoolFalse, "0":
		return false, true
	}

	return false, false
}

// formatBool emits the LINSTOR-native form (`yes`/`no`) so golinstor
// sees the same shape it sent on the way back out.
func formatBool(b bool) string {
	if b {
		return wireBoolYes
	}

	return wireBoolNo
}

// stripDRBDProps returns a copy of props with every `DrbdOptions/...`
// key removed, leaving only the residual non-DRBD keys
// (`StorPoolName`, `Aux/zone`, …) that still belong on
// Spec.Props until Phase 10.4 lands and we type those too.
func stripDRBDProps(props map[string]string) map[string]string {
	if len(props) == 0 {
		return nil
	}

	out := map[string]string{}

	for k, v := range props {
		if strings.HasPrefix(k, propsKVPrefix) {
			continue
		}

		out[k] = v
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

// mergeProps combines residual non-DRBD props (kept on Spec.Props)
// with the re-emitted typed-DRBD props so the wire `Props` field
// stays the unified flat bag golinstor expects on GET. Right-hand
// keys win on conflict; in practice the two maps are disjoint
// because stripDRBDProps removes everything that typedToProps
// re-emits.
func mergeProps(residual, drbd map[string]string) map[string]string {
	if len(residual) == 0 && len(drbd) == 0 {
		return nil
	}

	out := map[string]string{}

	maps.Copy(out, residual)
	maps.Copy(out, drbd)

	return out
}
