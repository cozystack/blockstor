// SPDX-License-Identifier: Apache-2.0

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
	"context"
	"slices"

	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store"
)

// LINSTOR names are case-insensitive, and the store addresses one object
// through the folded CRD slug, so `PVC-X` and `pvc-x` are the same
// definition. A replica or snapshot keeps its definition's name in spec,
// though, and a field selector compares that value exactly. A listing asked
// in one spelling then missed rows stored in another, and on a delete that is
// the destructive kind of short answer: the snapshot gate passed over
// snapshots it could not see, and the cascade left replicas with live DRBD
// under a definition that was gone.
//
// So a scoped listing asks under every spelling the name can reasonably have
// been stored in: the one given, the folded one, and the one the definition
// (or node) itself was registered under. Writes store the registered
// spelling (canonicalRDName), so rows written through the store always fall
// under the last.

// canonicalRDName is the spelling a replica or snapshot of rdName is stored
// under: the definition's own registered spelling when it exists, the name as
// given otherwise.
func canonicalRDName(ctx context.Context, reader ctrlclient.Reader, rdName string) string {
	if registered, ok := registeredRDName(ctx, reader, rdName); ok {
		return registered
	}

	return rdName
}

// canonicalNodeName is the spelling a replica on node is stored under: the
// node's registered spelling when it is registered, the name as given
// otherwise. `r c NODE-MIX pvc` over a node registered as `Node-Mix` stored
// the replica under a third spelling no listing of the node asked.
func canonicalNodeName(ctx context.Context, reader ctrlclient.Reader, node string) string {
	if registered, ok := registeredNodeName(ctx, reader, node); ok {
		return registered
	}

	return node
}

func registeredRDName(ctx context.Context, reader ctrlclient.Reader, rdName string) (string, bool) {
	var crd crdv1alpha1.ResourceDefinition

	err := reader.Get(ctx, types.NamespacedName{Name: Name(rdName)}, &crd)
	if err != nil {
		return "", false
	}

	return OriginalName(&crd.ObjectMeta), true
}

func registeredNodeName(ctx context.Context, reader ctrlclient.Reader, node string) (string, bool) {
	var crd crdv1alpha1.Node

	err := reader.Get(ctx, types.NamespacedName{Name: Name(node)}, &crd)
	if err != nil {
		return "", false
	}

	return OriginalName(&crd.ObjectMeta), true
}

// rdSpellings and nodeSpellings answer the spellings to ask under. A failed
// lookup of the registered name only drops that one spelling: the listing
// still asks under the given and the folded ones, which is what it did before.
func rdSpellings(ctx context.Context, reader ctrlclient.Reader, rdName string) []string {
	registered, ok := registeredRDName(ctx, reader, rdName)

	return spellingSet(rdName, registered, ok)
}

func nodeSpellings(ctx context.Context, reader ctrlclient.Reader, node string) []string {
	registered, ok := registeredNodeName(ctx, reader, node)

	return spellingSet(node, registered, ok)
}

func spellingSet(given, registered string, haveRegistered bool) []string {
	out := []string{given}

	add := func(spelling string) {
		if !slices.Contains(out, spelling) {
			out = append(out, spelling)
		}
	}

	add(store.FoldName(given))

	if haveRegistered {
		add(registered)
	}

	return out
}
