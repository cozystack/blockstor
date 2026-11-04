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

// Package planted holds the shapes the name guard must catch, each under
// variable names no vocabulary would guess, and the shapes it must leave
// alone. Lines ending in "// want" are the ones it reports.
package planted

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	crd "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// The migrate-disk shape: two names reach one comparison through a
// parameter, from two path parameters.
func moveIt(req *http.Request) bool {
	p := req.PathValue("node")

	return twoEnds(req, p)
}

func twoEnds(req *http.Request, u string) bool {
	w := req.PathValue("dst")

	return u == w // want
}

// The same, with both names known only as what the store is asked for.
func askTwice(ctx context.Context, st store.Store, q1, q2 string) bool {
	_, _ = st.Resources().Get(ctx, "pvc", q1)
	_, _ = st.Nodes().Get(ctx, q2)

	return q1 != q2 // want
}

// The node-connection filter shape: a wire field against a path parameter.
func pick(req *http.Request, rows []apiv1.Resource) int {
	zz := req.PathValue("node")
	hits := 0

	for i := range rows {
		yy := rows[i].NodeName
		if yy == zz { // want
			hits++
		}
	}

	return hits
}

// A name a helper returns, compared with another.
func spelled(req *http.Request) string { return req.PathValue("rd") }

func viaReturn(req *http.Request, snap apiv1.Snapshot) bool {
	return spelled(req) == snap.ResourceName // want
}

// A name ranged out of a list.
func ranged(req *http.Request, snap apiv1.Snapshot) bool {
	target := req.PathValue("node")

	for _, el := range snap.Nodes {
		if el == target { // want
			return true
		}
	}

	return false
}

// A map keyed by one spelling and read by another.
func indexed(req *http.Request, byX map[string]int) int {
	k9 := req.PathValue("node")

	return byX[k9] // want
}

// A search of a list of names.
func searched(req *http.Request, snap apiv1.Snapshot) bool {
	return slices.Contains(snap.Nodes, req.PathValue("node")) // want
}

// Case folded where the store hashes.
func folded(req *http.Request, r apiv1.Resource) bool {
	return strings.EqualFold(r.NodeName, req.PathValue("node")) // want
}

// A CR addressed by a raw name.
func addressed(req *http.Request) types.NamespacedName {
	return types.NamespacedName{Name: req.PathValue("rd")} // want
}

// The same through controller-runtime's alias.
func aliased(req *http.Request) client.ObjectKey {
	return client.ObjectKey{Name: req.PathValue("node")} // want
}

// A CR's own name, a key, against a raw one.
func crName(req *http.Request, obj *crd.ResourceDefinition) bool {
	return obj.Name == req.PathValue("rd") // want
}

// The shapes a second review planted.

// Two wire rows of two different listings, one field each.
func twoListings(a apiv1.Resource, b apiv1.Snapshot) bool {
	return a.Name == b.ResourceName // want
}

// The same field of two values that are not one collection.
func twoValues(a, b apiv1.Resource) bool {
	return a.NodeName == b.NodeName // want
}

// A key built inline from a raw name.
func concatKey(r apiv1.Resource, pool string, byPair map[string]int) int {
	return byPair[r.NodeName+"/"+pool] // want
}

// A key built with Sprintf.
func sprintfKey(r apiv1.Resource, byPair map[string]int) int {
	return byPair[fmt.Sprintf("%s/%s", r.NodeName, r.Name)] // want
}

type holder struct{ who string }

// A name copied through a field of a local struct.
func viaField(req *http.Request, r apiv1.Resource) bool {
	h := holder{who: req.PathValue("node")}

	return h.who == r.NodeName // want
}

// A switch on one name with a case of another.
func switched(req *http.Request, r apiv1.Resource) int {
	switch req.PathValue("node") {
	case r.NodeName: // want
		return 1
	}

	return 0
}

// A closure bound to a variable.
func viaClosure(req *http.Request, r apiv1.Resource) bool {
	eq := func(x, y string) bool {
		return x == y // want
	}

	return eq(req.PathValue("node"), r.NodeName)
}

type namer interface{ Name() string }

type pathNamer struct{ req *http.Request }

func (p pathNamer) Name() string { return p.req.PathValue("rd") }

// A name returned through an interface method.
func viaInterface(n namer, snap apiv1.Snapshot) bool {
	return n.Name() == snap.ResourceName // want
}

var _ namer = pathNamer{}

// A prefix test between two names.
func prefixed(req *http.Request, r apiv1.Resource) bool {
	return strings.HasPrefix(r.NodeName, req.PathValue("node")) // want
}

// A map literal keyed by a raw name.
func literalKey(req *http.Request) map[string]bool {
	return map[string]bool{req.PathValue("node"): true} // want
}

// More shapes.

// A name appended to a list and searched for.
func appended(req *http.Request, r apiv1.Resource) bool {
	var list []string

	list = append(list, req.PathValue("node"))

	return slices.Contains(list, r.NodeName) // want
}

// A name handed to the function a slices.*Func call runs.
func viaContainsFunc(snap apiv1.Snapshot, req *http.Request) bool {
	want := req.PathValue("node")

	return slices.ContainsFunc(snap.Nodes, func(n string) bool {
		return n == want // want
	})
}

// A helper that returns what it is given, called with a name.
func echo(s string) string { return strings.TrimSpace(s) }

func viaEcho(req *http.Request, r apiv1.Resource) bool {
	return echo(req.PathValue("node")) == r.NodeName // want
}

// A closure called in place.
func inPlace(req *http.Request, r apiv1.Resource) bool {
	return func() string { return req.PathValue("node") }() == r.NodeName // want
}

// A name joined into a key.
func joinedKey(snap apiv1.Snapshot, byNodes map[string]int) int {
	return byNodes[strings.Join(snap.Nodes, ",")] // want
}

// A name stored as a map key and ranged back out.
func viaMapKeys(req *http.Request, r apiv1.Resource) bool {
	props := map[string]string{}
	props["Paths/"+naming.Key("x")] = "a"
	props[req.PathValue("node")] = "b" // want

	for k := range props {
		head, _, _ := strings.Cut(k, "/")
		if head == r.NodeName { // want
			return true
		}
	}

	return false
}

// Shapes it leaves alone.
func fine(req *http.Request, r apiv1.Resource, dev apiv1.PhysicalDevice, byKey map[string]int) bool {
	a := req.PathValue("node")
	b := naming.Key(r.NodeName)

	return naming.Same(a, r.NodeName) ||
		naming.Key(a) == b ||
		byKey[naming.Key(a)] > 0 ||
		a == "" ||
		dev.DevicePath == a ||
		req.PathValue("key") == r.NodeName
}

// One field off two elements of one listing, by index and in a sort's
// comparator, is one source.
func oneListing(rows []apiv1.Resource) bool {
	slices.SortFunc(rows, func(x, y apiv1.Resource) int {
		if x.NodeName != y.NodeName {
			return strings.Compare(x.NodeName, y.NodeName)
		}

		return 0
	})

	return len(rows) > 1 && rows[0].NodeName == rows[1].NodeName
}

// Keys built from keys, constant keys and prefixes, a switch over
// constants, and a helper given no name return nothing raw.
func keyedShapes(req *http.Request, r apiv1.Resource, byPair map[string]int) bool {
	k := byPair[naming.Key(r.NodeName)+"/"+naming.Key(r.Name)] +
		byPair[fmt.Sprintf("%s/%s", naming.Key(r.NodeName), "pool")]

	switch req.PathValue("node") {
	case "", "all":
		return false
	}

	m := map[string]bool{"fixed": true, naming.Key(r.NodeName): true}

	return k > 0 && m["fixed"] && strings.HasPrefix(r.NodeName, "pvc-") && echo("lit") == r.NodeName
}
