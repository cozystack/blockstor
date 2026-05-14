//go:build integration

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

package harness

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// Eventually polls predicate until it returns true or the timeout
// elapses. Fails the test via t.Fatalf on timeout. The default
// poll interval is 100ms — short enough that a 30s budget gives
// 300 attempts, long enough that the apiserver isn't hammered.
func Eventually(t *testing.T, timeout time.Duration, predicate func() bool, msg string) {
	t.Helper()

	const pollInterval = 100 * time.Millisecond

	deadline := time.Now().Add(timeout)

	for {
		if predicate() {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("Eventually timed out after %s: %s", timeout, msg)
		}

		time.Sleep(pollInterval)
	}
}

// MustList returns the .Items slice of the given list-type. The
// caller passes a pointer to an empty list (e.g. &blockstoriov1alpha1.NodeList{})
// and a function that extracts the Items. We keep the interface
// explicit rather than reflection-based: every CRD has a slightly
// different Items field type, so any "generic" wrapper still needs
// a per-kind extractor.
//
// Example:
//
//	nodes := MustList(t, c, &blockstoriov1alpha1.NodeList{},
//	  func(l *blockstoriov1alpha1.NodeList) []blockstoriov1alpha1.Node { return l.Items })
func MustList[L client.ObjectList, T any](
	t *testing.T,
	c client.Client,
	list L,
	items func(L) []T,
	opts ...client.ListOption,
) []T {
	t.Helper()

	err := c.List(context.Background(), list, opts...)
	if err != nil {
		t.Fatalf("List %T: %v", list, err)
	}

	return items(list)
}

// MustGet fetches the named cluster-scoped object into `into`. For
// namespaced objects callers should fall back to the raw
// client.Client.Get — the integration suite is overwhelmingly
// cluster-scoped (CRDs are scope=Cluster), so this terse form
// covers the common case.
func MustGet[T client.Object](t *testing.T, c client.Client, name string, into T) T {
	t.Helper()

	err := c.Get(context.Background(), types.NamespacedName{Name: name}, into)
	if err != nil {
		t.Fatalf("Get %s/%s: %v", typeNameOf(into), name, err)
	}

	return into
}

// WaitForDRBDState polls until the named Resource on the given
// node has Status.DrbdState == want. The satellite mock fills
// this in on each tick — see harness/satellite.go.
func WaitForDRBDState(t *testing.T, stack *Stack, rd, node, want string) {
	t.Helper()

	const drbdStateTimeout = 15 * time.Second

	// Resource.metadata.name follows `<rd>.<node>` (CEL-pinned).
	resourceName := rd + "." + node

	Eventually(t, drbdStateTimeout, func() bool {
		var r blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: resourceName}, &r)
		if err != nil {
			return false
		}

		return r.Status.DrbdState == want
	}, "Resource "+resourceName+" DrbdState != "+want)
}

func typeNameOf(obj client.Object) string {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" {
		return gvk.Kind
	}
	// Fallback: Go type name. Concrete types unset GVK until
	// the scheme decodes them, which never happens on a hand-
	// allocated `&Foo{}` test object.
	switch obj.(type) {
	case *blockstoriov1alpha1.Node:
		return "Node"
	case *blockstoriov1alpha1.StoragePool:
		return "StoragePool"
	case *blockstoriov1alpha1.ResourceGroup:
		return "ResourceGroup"
	case *blockstoriov1alpha1.ResourceDefinition:
		return "ResourceDefinition"
	case *blockstoriov1alpha1.Resource:
		return "Resource"
	case *blockstoriov1alpha1.Snapshot:
		return "Snapshot"
	default:
		return "Object"
	}
}
