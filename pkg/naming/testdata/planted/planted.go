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
