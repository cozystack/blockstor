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

package k8s_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// Bug 206 / Bug 208 / Bug 209 are all instances of the same canonical
// class: a store-side `wireToCRDXxxSpec` rebuilds a CRD Spec from the
// REST wire shape, the Update/Patch path then wholesale-assigns
// `existing.Spec = wireToCRDXxxSpec(in)`, and any typed-pointer
// (or otherwise operator-only) field that has NO wire-side counterpart
// gets silently wiped on every routine REST modify.
//
// The fix is uniform — copy the operator-only fields aside before the
// rebuild and re-stamp them after. The risk is that someone adds a
// new typed field to a Spec, forgets the carry-across, and ships
// Bug 210.
//
// This file is the type-system tripwire: for every CRD Spec that has
// a corresponding `wireToCRDXxxSpec` rebuild path, every field MUST
// be explicitly classified as either:
//
//   - "wire-derived" — the wire converter populates it from the
//     `apiv1.Xxx` input, so the wholesale assign is the right thing.
//   - "must-carry-across" — operator-only, no wire counterpart; the
//     store-side path MUST copy it aside before the rebuild and
//     re-stamp it after.
//
// Adding a new field to any Spec without classifying it here is a
// build-stop test failure with an actionable error message pointing
// at exactly the unclassified field. That's the contract that turns
// "we found three of these in the v14/v16 audits" into "we cannot
// ship a fourth without explicitly opting out".
//
// Status subresource fields are out of scope: status writes go through
// `Status().Update(...)` and never touch the Spec rebuild path.

type specClassification struct {
	// kind is a human-readable label for failure messages.
	kind string
	// wireDerived enumerates field names the corresponding
	// wireToCRDXxxSpec populates from the wire input. Listing a
	// field here documents "this is set on the rebuild, so the
	// wholesale-replace is correct".
	wireDerived map[string]bool
	// mustCarryAcross enumerates operator-only fields with no wire
	// counterpart. Every Update / Patch path that does
	// `existing.Spec = wireToCRDXxxSpec(in)` MUST copy these aside
	// first and re-stamp them after.
	mustCarryAcross map[string]bool
}

// classifications is the source of truth for what every CRD Spec
// field must be on the wire-rebuild path. Keyed by the Go type's
// `reflect.Type.Name()`.
//
// To add a new Spec field:
//  1. Decide whether the wire shape sets it (→ wireDerived) or it's
//     operator-only (→ mustCarryAcross).
//  2. Add the field name to the appropriate set below.
//  3. If mustCarryAcross: extend the store-side Update/Patch path to
//     copy the field across the spec rebuild (see Bug 206/208/209
//     fixes in nodes.go and resource_definitions.go for the pattern).
//
// To add a new Spec kind (new CRD with a wireToCRDXxxSpec rebuild):
//  1. Add a new entry below with the classification.
//  2. Add the type to the specsUnderTest list below.
var classifications = map[string]specClassification{ //nolint:gochecknoglobals // package-level test data
	"NodeSpec": {
		kind: "NodeSpec (pkg/store/k8s/nodes.go::wireToCRDNodeSpec)",
		wireDerived: map[string]bool{
			"Type":              true,
			"Props":             true,
			"Flags":             true,
			"NetInterfaces":     true,
			"SatelliteEndpoint": true, // derived from Props["SatelliteEndpoint"]
		},
		mustCarryAcross: map[string]bool{
			// Bug 208: operator-only typed pointers, no wire
			// counterpart on apiv1.Node. Wiping these on every
			// routine `n set-property` drops new replicas back to
			// the default 7000-7999 / 1000-1099 ranges.
			"DRBDPortRange":  true,
			"DRBDMinorRange": true,
		},
	},
	"ResourceDefinitionSpec": {
		kind: "ResourceDefinitionSpec (pkg/store/k8s/resource_definitions.go::wireToCRDRDSpec)",
		wireDerived: map[string]bool{
			"ExternalName":      true,
			"ResourceGroupName": true,
			"Props":             true,
			"Flags":             true,
			"LayerStack":        true,
			"DRBDOptions":       true, // typed-split out of Props
			"ExtraProps":        true, // forward-compat shim from Props
		},
		mustCarryAcross: map[string]bool{
			// Bug 206: addressed via the separate
			// /v1/.../volume-definitions endpoint family on the
			// wire; the parent RD's spec rebuild would otherwise
			// wipe the inline list.
			"VolumeDefinitions": true,
			// Bug 209: operator-only typed pointer carrying the
			// LUKS passphrase Secret ref. Wiping it silently
			// downgrades subsequent volumes to unencrypted.
			"Encryption": true,
		},
	},
	"ResourceGroupSpec": {
		kind: "ResourceGroupSpec (pkg/store/k8s/resource_groups.go::wireToCRDRGSpec)",
		wireDerived: map[string]bool{
			"Description":  true,
			"Props":        true,
			"PeerSlots":    true,
			"SelectFilter": true,
			"VolumeGroups": true,
			"DRBDOptions":  true,
			"ExtraProps":   true,
		},
		mustCarryAcross: map[string]bool{},
	},
	"ResourceSpec": {
		kind: "ResourceSpec (pkg/store/k8s/resources.go::wireToCRDResourceSpec)",
		wireDerived: map[string]bool{
			"ResourceDefinitionName": true,
			"NodeName":               true,
			"Props":                  true,
			"Flags":                  true,
			"DRBDOptions":            true,
			"ExtraProps":             true,
			"StoragePool":            true, // lifted from Props["StorPoolName"]
			"ToggleDiskCancel":       true, // Bug 40 — wire-side flag
		},
		mustCarryAcross: map[string]bool{
			// Bug 206: controller-stamped per-volume seed
			// (SeedFromGi). No wire counterpart on apiv1.Resource.
			"Volumes": true,
		},
	},
	"StoragePoolSpec": {
		kind: "StoragePoolSpec (pkg/store/k8s/storage_pools.go::wireToCRDStoragePoolSpec)",
		wireDerived: map[string]bool{
			"NodeName":      true,
			"PoolName":      true,
			"ProviderKind":  true,
			"SharedSpaceID": true,
			"Props":         true,
		},
		mustCarryAcross: map[string]bool{},
	},
	"SnapshotSpec": {
		kind: "SnapshotSpec (pkg/store/k8s/snapshots.go::wireToCRDSnapshotSpec)",
		wireDerived: map[string]bool{
			"ResourceDefinitionName": true,
			"SnapshotName":           true,
			"Nodes":                  true,
			"Props":                  true,
			"VolumeDefinitions":      true,
		},
		mustCarryAcross: map[string]bool{},
	},
	"PhysicalDeviceSpec": {
		kind: "PhysicalDeviceSpec (pkg/store/k8s/physicaldevices.go::wireToCRDPhysicalDeviceSpec)",
		wireDerived: map[string]bool{
			"AttachTo": true,
		},
		mustCarryAcross: map[string]bool{},
	},
}

// specsUnderTest lists every CRD Spec whose Update / Patch path
// wholesale-rebuilds the spec from the wire shape via a
// `wireToCRDXxxSpec` helper. Driven by `reflect.TypeOf(zeroValue)`
// so the regression guard can introspect the struct fields without
// hard-coding them — anyone adding a field gets a test failure
// pointing at the new unclassified field.
//
// ControllerConfigSpec is intentionally NOT here: its store path
// (pkg/store/k8s/controller_config.go) uses field-scoped patches
// (PatchControllerExtraProps / PatchControllerNodeConnections) that
// never do `existing.Spec = ...`, so the carry-across hazard does
// not apply.
//
// ResourceDefinitionVolume is the inline element-shape of
// ResourceDefinitionSpec.VolumeDefinitions and is itself rebuilt by
// `wireToCRDVD`, but every field there has a wire counterpart on
// apiv1.VolumeDefinition (volumeNumber, sizeKib, props, flags) — no
// operator-only fields to carry across.
func specsUnderTest() []reflect.Type {
	return []reflect.Type{
		reflect.TypeOf(crdv1alpha1.NodeSpec{}),
		reflect.TypeOf(crdv1alpha1.ResourceDefinitionSpec{}),
		reflect.TypeOf(crdv1alpha1.ResourceGroupSpec{}),
		reflect.TypeOf(crdv1alpha1.ResourceSpec{}),
		reflect.TypeOf(crdv1alpha1.StoragePoolSpec{}),
		reflect.TypeOf(crdv1alpha1.SnapshotSpec{}),
		reflect.TypeOf(crdv1alpha1.PhysicalDeviceSpec{}),
	}
}

// TestSpecFieldsClassified is the regression guard: walk every Spec
// under test, look up its classification, and assert every field is
// in exactly one of the wire-derived / must-carry-across sets. A new
// field that's not classified yields a clear failure naming the field
// and the kind, with instructions on how to extend the table.
//
// If this test fails with "field X not classified", the author of
// the new field MUST:
//
//   - Decide whether the wire shape carries it (wire-derived) or it
//     is operator-only (must-carry-across).
//   - If must-carry-across: extend the matching `Update` / `Patch`
//     path in the store package to copy the field across the spec
//     rebuild, mirroring the Bug 206 / 208 / 209 pattern.
//   - Add the field name to the appropriate set in
//     `classifications` above.
func TestSpecFieldsClassified(t *testing.T) {
	t.Parallel()

	for _, specType := range specsUnderTest() {
		typeName := specType.Name()

		class, ok := classifications[typeName]
		if !ok {
			t.Errorf("Spec type %q is listed in specsUnderTest but has no entry in classifications — "+
				"add a specClassification entry pointing at the wireToCRD<Kind>Spec helper.",
				typeName)

			continue
		}

		// Collect all actual field names on the Spec via reflect.
		actual := map[string]bool{}

		for i := range specType.NumField() {
			actual[specType.Field(i).Name] = true
		}

		// Every actual field must appear in exactly one set.
		for name := range actual {
			inWire := class.wireDerived[name]
			inCarry := class.mustCarryAcross[name]

			switch {
			case !inWire && !inCarry:
				t.Errorf("%s: field %q is not classified.\n"+
					"  Add it to classifications[%q].wireDerived if the wire converter sets it from apiv1.%s,\n"+
					"  or to classifications[%q].mustCarryAcross if it is operator-only with no wire counterpart\n"+
					"  (and then extend the store-side Update + Patch paths to carry it across, mirroring\n"+
					"  the Bug 206 / 208 / 209 fixes).",
					class.kind, name, typeName, strings.TrimSuffix(typeName, "Spec"), typeName)
			case inWire && inCarry:
				t.Errorf("%s: field %q is in BOTH wireDerived and mustCarryAcross — "+
					"pick one. A field is either rebuilt from the wire or carried across; "+
					"never both.", class.kind, name)
			}
		}

		// Every entry in the classification table must correspond
		// to a real field — keeps the table from drifting when a
		// field is removed.
		for name := range class.wireDerived {
			if !actual[name] {
				t.Errorf("%s: classifications.wireDerived references nonexistent field %q — remove the stale entry.",
					class.kind, name)
			}
		}

		for name := range class.mustCarryAcross {
			if !actual[name] {
				t.Errorf("%s: classifications.mustCarryAcross references nonexistent field %q — remove the stale entry.",
					class.kind, name)
			}
		}
	}
}

// TestSpecClassificationsCover ensures every entry in
// `classifications` matches an entry in `specsUnderTest` — a stale
// classification for a removed Spec would otherwise go unnoticed.
func TestSpecClassificationsCover(t *testing.T) {
	t.Parallel()

	covered := map[string]bool{}
	for _, specType := range specsUnderTest() {
		covered[specType.Name()] = true
	}

	stale := []string{}

	for name := range classifications {
		if !covered[name] {
			stale = append(stale, name)
		}
	}

	sort.Strings(stale)

	for _, name := range stale {
		t.Errorf("classifications has entry %q but specsUnderTest does not — "+
			"either re-add it to specsUnderTest, or drop the classification.", name)
	}
}
