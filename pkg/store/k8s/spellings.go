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
	"maps"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// Writes therefore store the CR name in spec: the definition's (and for a
// replica the node's) metadata.name. Every reader that Gets the parent by the
// spec value, or compares it with a CR name, meets the object it means, and
// the satellite derives the DRBD resource and volume names from that same
// lowercase value. The spelling the definition or node was registered under
// is kept beside it in an annotation, for what the REST surface shows.
//
// Rows stored, or adopted from LINSTOR, before this may still carry another
// spelling in spec, so a scoped listing asks under every spelling the name
// can reasonably have been stored in: the one given, the folded one, and the
// registered one.

const (
	// AnnotationLinstorRDName keeps the spelling a replica's or snapshot's
	// definition was registered under, when it differs from the CR name the
	// spec stores. Read back for display only.
	AnnotationLinstorRDName = "blockstor.io/linstor-resource-definition-name"

	// AnnotationLinstorNodeName does the same for a replica's node.
	AnnotationLinstorNodeName = "blockstor.io/linstor-node-name"
)

// SameName reports whether two spellings of a LINSTOR name, as a replica's
// spec, a snapshot's node list, a storage pool, the wire or an operator
// spell it, address the same object: whether they map to one CR name, the
// way the store addresses the object. That covers Node-1 against node-1 and
// a CR name against the spelling it was registered under. Two case variants
// of a name outside RFC 1123, such as My_Res and my_res, hash to two CR
// names and are two objects, so they are not the same name here either.
func SameName(a, b string) bool {
	return Name(a) == Name(b)
}

// storedName is what a child stores for one of its parents: the parent's CR
// name, for spec, and the spelling to show.
type storedName struct {
	crName  string
	display string
}

// annotate records the display spelling on meta under key when it differs
// from the CR name the spec carries.
func (n storedName) annotate(meta *metav1.ObjectMeta, key string) {
	if n.display == n.crName {
		return
	}

	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}

	meta.Annotations[key] = n.display
}

// storedRDName answers what a replica or snapshot of rdName stores. A
// definition that is not there (a caller that skipped the check) is stored
// under the CR name it would have and shown as given.
func storedRDName(ctx context.Context, reader ctrlclient.Reader, rdName string) storedName {
	if found, ok := lookupRD(ctx, reader, rdName); ok {
		return found
	}

	return storedName{crName: Name(rdName), display: rdName}
}

// storedNodeName answers what a replica on node stores, the same way.
// `r c pvc NODE-MIX` over a node registered as `Node-Mix` stores `node-mix`
// and shows `Node-Mix`.
func storedNodeName(ctx context.Context, reader ctrlclient.Reader, node string) storedName {
	if found, ok := lookupNode(ctx, reader, node); ok {
		return found
	}

	return storedName{crName: Name(node), display: node}
}

// shownName is the spelling to show for a parent a child stores as stored:
// the annotation when the write kept one, the spec value otherwise.
func shownName(meta *metav1.ObjectMeta, key, stored string) string {
	if v, ok := meta.Annotations[key]; ok && v != "" {
		return v
	}

	return stored
}

func lookupRD(ctx context.Context, reader ctrlclient.Reader, rdName string) (storedName, bool) {
	var crd crdv1alpha1.ResourceDefinition

	err := reader.Get(ctx, types.NamespacedName{Name: Name(rdName)}, &crd)
	if err != nil {
		return storedName{}, false
	}

	return storedName{crName: crd.Name, display: OriginalName(&crd.ObjectMeta)}, true
}

func lookupNode(ctx context.Context, reader ctrlclient.Reader, node string) (storedName, bool) {
	var crd crdv1alpha1.Node

	err := reader.Get(ctx, types.NamespacedName{Name: Name(node)}, &crd)
	if err != nil {
		return storedName{}, false
	}

	return storedName{crName: crd.Name, display: OriginalName(&crd.ObjectMeta)}, true
}

// rdSpellings and nodeSpellings answer the spellings to ask under. A failed
// lookup of the registered object only drops its spellings: the listing
// still asks under the given and the folded ones, which is what it did before.
func rdSpellings(ctx context.Context, reader ctrlclient.Reader, rdName string) []string {
	found, ok := lookupRD(ctx, reader, rdName)

	return spellingSet(rdName, found, ok)
}

func nodeSpellings(ctx context.Context, reader ctrlclient.Reader, node string) []string {
	found, ok := lookupNode(ctx, reader, node)

	return spellingSet(node, found, ok)
}

func spellingSet(given string, found storedName, haveFound bool) []string {
	out := []string{given}

	add := func(spelling string) {
		if !slices.Contains(out, spelling) {
			out = append(out, spelling)
		}
	}

	add(store.FoldName(given))
	add(Name(given))

	if haveFound {
		add(found.crName)
		add(found.display)
	}

	return out
}

// withoutNameAnnotations drops the display-spelling annotations from a wire
// annotation map: they are store-internal, and the wire carries the spelling
// itself in its name fields.
func withoutNameAnnotations(in map[string]string) map[string]string {
	_, rd := in[AnnotationLinstorRDName]
	_, node := in[AnnotationLinstorNodeName]

	if !rd && !node {
		return in
	}

	out := maps.Clone(in)
	delete(out, AnnotationLinstorRDName)
	delete(out, AnnotationLinstorNodeName)

	if len(out) == 0 {
		return nil
	}

	return out
}
