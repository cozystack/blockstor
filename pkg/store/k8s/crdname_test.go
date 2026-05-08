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

func TestSetAndOriginalName_RoundTrip(t *testing.T) {
	t.Parallel()

	original := "DeleteSnapshot-volume-1-target"
	meta := metav1.ObjectMeta{Name: k8s.Name(original)}
	k8s.SetOriginalName(&meta, original)

	if got := k8s.OriginalName(&meta); got != original {
		t.Errorf("OriginalName = %q, want %q", got, original)
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
