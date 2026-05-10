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
	"testing"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestPropsToTypedRecognisesEachSection pins the per-section
// prop→typed mapping for all currently-modelled DrbdOptions/* keys.
// A regression here either drops a known key into ExtraProps
// (silently disabling typed validation) or — worse — coerces it into
// the wrong field (silently changing user intent).
func TestPropsToTypedRecognisesEachSection(t *testing.T) {
	t.Parallel()

	in := map[string]string{
		"DrbdOptions/Net/protocol":            "B",
		"DrbdOptions/Net/max-buffers":         "8000",
		"DrbdOptions/Net/allow-two-primaries": "yes",
		"DrbdOptions/Net/after-sb-0pri":       "discard-zero-changes",
		"DrbdOptions/Disk/on-io-error":        "detach",
		"DrbdOptions/Disk/al-extents":         "1237",
		"DrbdOptions/PeerDevice/c-max-rate":   "100M",
		"DrbdOptions/Resource/auto-promote":   "yes",
		"DrbdOptions/Resource/quorum":         "majority",
		"DrbdOptions/Resource/on-no-quorum":   "io-error",
		"DrbdOptions/Handlers/fence-peer":     "/usr/bin/fence",
	}

	typed, extras := propsToTyped(in)

	if typed == nil {
		t.Fatalf("typed: got nil, want populated DRBDOptions")
	}

	if typed.Net.Protocol != "B" {
		t.Errorf("Net.Protocol: got %q, want B", typed.Net.Protocol)
	}

	if typed.Net.MaxBuffers == nil || *typed.Net.MaxBuffers != 8000 {
		t.Errorf("Net.MaxBuffers: got %v, want 8000", typed.Net.MaxBuffers)
	}

	if typed.Net.AllowTwoPrimaries == nil || !*typed.Net.AllowTwoPrimaries {
		t.Errorf("Net.AllowTwoPrimaries: got %v, want true", typed.Net.AllowTwoPrimaries)
	}

	if typed.Net.AfterSb0Pri != "discard-zero-changes" {
		t.Errorf("Net.AfterSb0Pri: got %q, want discard-zero-changes", typed.Net.AfterSb0Pri)
	}

	if typed.Disk.OnIOError != "detach" {
		t.Errorf("Disk.OnIOError: got %q, want detach", typed.Disk.OnIOError)
	}

	if typed.Disk.ALExtents == nil || *typed.Disk.ALExtents != 1237 {
		t.Errorf("Disk.ALExtents: got %v, want 1237", typed.Disk.ALExtents)
	}

	if typed.PeerDevice.CMaxRate != "100M" {
		t.Errorf("PeerDevice.CMaxRate: got %q, want 100M", typed.PeerDevice.CMaxRate)
	}

	if typed.Resource.AutoPromote == nil || !*typed.Resource.AutoPromote {
		t.Errorf("Resource.AutoPromote: got %v, want true", typed.Resource.AutoPromote)
	}

	if typed.Resource.Quorum != "majority" {
		t.Errorf("Resource.Quorum: got %q, want majority", typed.Resource.Quorum)
	}

	if typed.Resource.OnNoQuorum != "io-error" {
		t.Errorf("Resource.OnNoQuorum: got %q, want io-error", typed.Resource.OnNoQuorum)
	}

	if typed.Handlers["fence-peer"] != "/usr/bin/fence" {
		t.Errorf("Handlers[fence-peer]: got %q, want /usr/bin/fence", typed.Handlers["fence-peer"])
	}

	if len(extras) != 0 {
		t.Errorf("extras: got %v, want empty (every key was recognised)", extras)
	}
}

// TestPropsToTypedUnknownDrbdKeyToExtras pins the forward-compat
// shim: a `DrbdOptions/<unrecognised>` key MUST land in ExtraProps
// rather than silently dropping. golinstor pushes evolving keys; we
// can only type them on a release cadence, so unrecognised ones
// have to round-trip without loss.
func TestPropsToTypedUnknownDrbdKeyToExtras(t *testing.T) {
	t.Parallel()

	in := map[string]string{
		"DrbdOptions/Net/some-future-knob": "42",
	}

	typed, extras := propsToTyped(in)

	if typed != nil && typed.Net != nil && typed.Net.Protocol != "" {
		t.Errorf("typed.Net.Protocol shouldn't be set: got %q", typed.Net.Protocol)
	}

	if extras["DrbdOptions/Net/some-future-knob"] != "42" {
		t.Errorf("extras: got %v, want some-future-knob=42 preserved", extras)
	}
}

// TestPropsToTypedNonDRBDKeysGoToExtras pins that keys outside the
// `DrbdOptions/` namespace (StorPoolName, Aux/zone) bypass the
// typed-fields mapping entirely and stay in ExtraProps. The k8s
// store separately writes those into Spec.Props (residual) — but
// the transcoder treats them as extras for now.
func TestPropsToTypedNonDRBDKeysGoToExtras(t *testing.T) {
	t.Parallel()

	in := map[string]string{
		"StorPoolName":             "thin1",
		"Aux/zone":                 "us-east-1a",
		"DrbdOptions/Net/protocol": "C",
	}

	typed, extras := propsToTyped(in)

	if typed == nil || typed.Net.Protocol != "C" {
		t.Errorf("typed.Net.Protocol: got %v, want C", typed)
	}

	if extras["StorPoolName"] != "thin1" {
		t.Errorf("extras[StorPoolName]: got %q, want thin1", extras["StorPoolName"])
	}

	if extras["Aux/zone"] != "us-east-1a" {
		t.Errorf("extras[Aux/zone]: got %q, want us-east-1a", extras["Aux/zone"])
	}
}

// TestPropsToTypedEmptyShortCircuits pins the nil-input nil-output
// optimisation — the k8s store relies on this so a wire request
// that omits Props doesn't persist a vacuous DRBDOptions=&{} that
// would round-trip as a non-empty Props map on GET.
func TestPropsToTypedEmptyShortCircuits(t *testing.T) {
	t.Parallel()

	typed, extras := propsToTyped(nil)
	if typed != nil {
		t.Errorf("typed: got %+v, want nil", typed)
	}

	if extras != nil {
		t.Errorf("extras: got %v, want nil", extras)
	}
}

// TestPropsToTypedInvalidIntFallsToExtras pins the parsing-error
// path: a `max-buffers` value that's not parseable as int32 must
// NOT silently set MaxBuffers=0 (a regression that did
// `if n, _ := strconv.ParseInt(...); ...` would coerce garbage to
// zero and admission validation would never see it).
func TestPropsToTypedInvalidIntFallsToExtras(t *testing.T) {
	t.Parallel()

	in := map[string]string{
		"DrbdOptions/Net/max-buffers": "garbage",
	}

	typed, extras := propsToTyped(in)

	if typed != nil && typed.Net != nil && typed.Net.MaxBuffers != nil {
		t.Errorf("MaxBuffers: must be nil on parse failure, got %v", typed.Net.MaxBuffers)
	}

	if extras["DrbdOptions/Net/max-buffers"] != "garbage" {
		t.Errorf("extras: garbage value must be preserved, got %v", extras)
	}
}

// TestPropsToTypedRoundTrip pins lossless round-trip for every
// recognised key: typedToProps(propsToTyped(p)) ≡ p (modulo bool
// canonicalisation — "true" round-trips as "yes" because that's the
// LINSTOR-native form on the wire).
func TestPropsToTypedRoundTrip(t *testing.T) {
	t.Parallel()

	in := map[string]string{
		"DrbdOptions/Net/protocol":            "B",
		"DrbdOptions/Net/max-buffers":         "8000",
		"DrbdOptions/Net/allow-two-primaries": "yes",
		"DrbdOptions/Disk/on-io-error":        "detach",
		"DrbdOptions/Resource/quorum":         "majority",
		"DrbdOptions/Handlers/fence-peer":     "/cluster-fence",
	}

	typed, extras := propsToTyped(in)

	out := typedToProps(typed, extras)

	for k, v := range in {
		if out[k] != v {
			t.Errorf("round-trip key %q: got %q, want %q", k, out[k], v)
		}
	}
}

// TestPropsToTypedBoolGoForms pins that golinstor's various bool
// spellings ("true" / "1" / "yes") all parse to the same typed
// pointer-to-bool. A regression that only handled "yes" would
// silently drop AllowTwoPrimaries when a Go-style client used "true".
func TestPropsToTypedBoolGoForms(t *testing.T) {
	t.Parallel()

	for _, val := range []string{"true", "yes", "1", "TRUE", "Yes"} {
		in := map[string]string{"DrbdOptions/Net/allow-two-primaries": val}
		typed, _ := propsToTyped(in)

		if typed == nil || typed.Net == nil || typed.Net.AllowTwoPrimaries == nil {
			t.Errorf("val %q: AllowTwoPrimaries missing", val)

			continue
		}

		if !*typed.Net.AllowTwoPrimaries {
			t.Errorf("val %q: got false, want true", val)
		}
	}
}

// TestStripDRBDPropsKeepsResidual pins that `StorPoolName`,
// `Aux/...` and other non-DrbdOptions keys survive the strip while
// every `DrbdOptions/` key is removed (since those go into typed
// fields).
func TestStripDRBDPropsKeepsResidual(t *testing.T) {
	t.Parallel()

	in := map[string]string{
		"StorPoolName":                 "thin1",
		"Aux/zone":                     "us-east-1a",
		"DrbdOptions/Net/protocol":     "C",
		"DrbdOptions/Disk/on-io-error": "detach",
	}

	out := stripDRBDProps(in)

	if out["StorPoolName"] != "thin1" {
		t.Errorf("StorPoolName: got %q, want thin1", out["StorPoolName"])
	}

	if out["Aux/zone"] != "us-east-1a" {
		t.Errorf("Aux/zone: got %q, want us-east-1a", out["Aux/zone"])
	}

	for k := range out {
		if k == "DrbdOptions/Net/protocol" || k == "DrbdOptions/Disk/on-io-error" {
			t.Errorf("residual still has %q — should be in typed fields", k)
		}
	}
}

// TestTypedToPropsHandlersAreEmitted pins handler emission — the
// only map-shaped field in DRBDOptions, easy to skip if someone
// adds a new field and forgets to plumb it through emitHandlersProps.
func TestTypedToPropsHandlersAreEmitted(t *testing.T) {
	t.Parallel()

	in := &crdv1alpha1.DRBDOptions{
		Handlers: map[string]string{
			"fence-peer":           "/cluster-fence",
			"before-resync-target": "/cluster-bsr",
		},
	}

	out := typedToProps(in, nil)

	if out["DrbdOptions/Handlers/fence-peer"] != "/cluster-fence" {
		t.Errorf("fence-peer: got %q, want /cluster-fence", out["DrbdOptions/Handlers/fence-peer"])
	}

	if out["DrbdOptions/Handlers/before-resync-target"] != "/cluster-bsr" {
		t.Errorf("before-resync-target: got %q, want /cluster-bsr", out["DrbdOptions/Handlers/before-resync-target"])
	}
}
