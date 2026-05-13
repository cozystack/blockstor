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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cozystack/blockstor/pkg/store/k8s"
)

func TestK8sName_PassThroughForValidNames(t *testing.T) {
	t.Parallel()

	cases := []string{
		"vol-3a8c1d4e",
		"node-1",
		"pvc-bc01.fff",
		"a",
	}
	for _, in := range cases {
		if got := k8s.Name(in); got != in {
			t.Errorf("Name(%q) = %q, want pass-through", in, got)
		}
	}
}

func TestK8sName_SlugifiesInvalidNames(t *testing.T) {
	t.Parallel()

	cases := []string{
		"DeleteSnapshot-volume-1-target",
		"Foo_Bar",
		"weird name with spaces",
		"UPPER",
	}
	for _, in := range cases {
		got := k8s.Name(in)
		if got == in {
			t.Errorf("Name(%q) = %q, expected slugified", in, got)
		}

		if strings.ToLower(got) != got {
			t.Errorf("Name(%q) = %q, expected all lowercase", in, got)
		}

		if len(got) > 253 {
			t.Errorf("Name(%q) = %q (%d chars), expected ≤253", in, got, len(got))
		}
	}
}

func TestK8sName_DistinctInputsProduceDistinctOutputs(t *testing.T) {
	t.Parallel()

	a := k8s.Name("Foo-Bar")
	b := k8s.Name("foo_bar")
	c := k8s.Name("FOO BAR")

	if a == b || b == c || a == c {
		t.Errorf("collision: a=%q b=%q c=%q", a, b, c)
	}
}

func TestK8sName_DeterministicForSameInput(t *testing.T) {
	t.Parallel()

	in := "DeleteSnapshot-volume-1-source"
	first := k8s.Name(in)
	second := k8s.Name(in)

	if first != second {
		t.Errorf("Name(%q) non-deterministic: %q vs %q", in, first, second)
	}
}

// TestSetAndOriginalName_RoundTrip pins the slugify-fallback case:
// when the input has characters rfc1123 rejects (here a slash),
// Name() builds a hashed slug and SetOriginalName preserves the
// original through the annotation so OriginalName can recover it.
// Case-only differences (e.g. `DfltRscGrp` → `dfltrscgrp`) ARE
// preserved too — see TestSetOriginalName_CaseOnlyDifference (Bug 57).
func TestSetAndOriginalName_RoundTrip(t *testing.T) {
	t.Parallel()

	original := "needs/slugify"
	meta := metav1.ObjectMeta{Name: k8s.Name(original)}
	k8s.SetOriginalName(&meta, original)

	if got := k8s.OriginalName(&meta); got != original {
		t.Errorf("OriginalName = %q, want %q", got, original)
	}
}

// TestSetOriginalName_CaseOnlyDifference pins Bug 57: mixed-case input
// that lowercases to a valid rfc1123 name (the upstream LINSTOR
// `DfltRscGrp` case) DOES round-trip through the annotation. meta.Name
// stays the lowercased k8s-addressable slug (so `kubectl get
// resourcegroup dfltrscgrp` keeps working), while OriginalName()
// returns the canonical CamelCase for wire output. linstor-csi's
// `defaultResourceGroup` constant and operator runbooks grep for the
// exact `DfltRscGrp` literal — silently lowercasing it on the wire
// breaks those callers.
func TestSetOriginalName_CaseOnlyDifference(t *testing.T) {
	t.Parallel()

	original := "DfltRscGrp"
	meta := metav1.ObjectMeta{Name: k8s.Name(original)}
	k8s.SetOriginalName(&meta, original)

	if meta.Name != "dfltrscgrp" {
		t.Errorf("Name() = %q, want %q (k8s-addressable slug)", meta.Name, "dfltrscgrp")
	}

	if meta.Annotations[k8s.AnnotationLinstorName] != original {
		t.Errorf("annotation: got %q, want %q (case must round-trip)",
			meta.Annotations[k8s.AnnotationLinstorName], original)
	}

	if got := k8s.OriginalName(&meta); got != original {
		t.Errorf("OriginalName = %q, want %q (canonical CamelCase)", got, original)
	}
}

func TestSetOriginalName_PassThroughDoesNotAnnotate(t *testing.T) {
	t.Parallel()

	original := "vol-3a8c1d4e"
	meta := metav1.ObjectMeta{Name: k8s.Name(original)}
	k8s.SetOriginalName(&meta, original)

	if _, ok := meta.Annotations[k8s.AnnotationLinstorName]; ok {
		t.Errorf("expected no annotation for pass-through name; got %v", meta.Annotations)
	}

	if got := k8s.OriginalName(&meta); got != original {
		t.Errorf("OriginalName = %q, want %q", got, original)
	}
}

// TestPhysicalDeviceCRDNameUsesDotSeparator pins the cross-CRD
// naming convention: every composite-key CRD in the project
// (StoragePool, Resource, Snapshot, PhysicalDevice) uses
// `<key1>.<key2>` and routes through `Name()` for slug
// discipline. Operators rely on this — `kubectl get … | grep
// '<node>\.' ` works across kinds. A regression that switched
// PhysicalDevice to a hyphen separator would silently break that
// grep workflow.
func TestPhysicalDeviceCRDNameUsesDotSeparator(t *testing.T) {
	t.Parallel()

	// RFC1123-clean inputs pass through verbatim with the dot.
	got := k8s.PhysicalDeviceCRDName("n1", "wwn-0xabcdef0123456789")
	if got != "n1.wwn-0xabcdef0123456789" {
		t.Errorf("got %q, want n1.wwn-0xabcdef0123456789", got)
	}

	// Inputs with characters k8s rejects (uppercase, underscore,
	// at-sign) get slugified by Name() — same path other
	// composite-key CRDs travel through.
	got = k8s.PhysicalDeviceCRDName("N1-Worker", "scsi-SATA_Samsung_SSD_980_PRO_S6BUNS0R")
	if !strings.Contains(got, "n1-worker") {
		t.Errorf("got %q, want substring n1-worker (slugified node)", got)
	}
}
