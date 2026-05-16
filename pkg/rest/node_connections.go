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

package rest

import (
	"context"
	"maps"
	"net/http"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerNodeConnections wires the upstream LINSTOR
// `/v1/node-connections{,/{src}/{dst}}` surface — Bug 101.
//
// Previously the handler returned 204 + empty body on PUT and an
// empty matrix on GET; python-linstor 1.27.1's `set_property` code
// path expects 200 + an `[]ApiCallRc` JSON envelope and crashed with
//
//	Unable to parse REST json data: Expecting value: line 1 column 1 (char 0)
//	Request-Uri: /v1/node-connections/<a>/<b>; Status: 204
//
// when the body was empty. The matrix-list and per-pair GET also
// returned an empty object so a successful "fake" PUT couldn't be
// read back — `linstor node-connection list` always showed zero
// rows.
//
// The fix is two-fold: the write surface returns the LINSTOR
// `[]ApiCallRc` envelope at 200/201, AND it persists the property
// map onto the cluster-wide `ControllerConfig.Spec.NodeConnections`
// bag so the list / get surfaces read back what the operator wrote.
// Persistence is keyed by the canonical pair-id (the lexicographically
// lower of {nodeA, nodeB}, then `::`, then the higher) so a write
// against (A, B) and a read against (B, A) hit the same record.
func (s *Server) registerNodeConnections(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/node-connections", s.handleNodeConnectionList)
	mux.HandleFunc("GET /v1/node-connections/{src}", s.handleNodeConnectionList)
	mux.HandleFunc("GET /v1/node-connections/{src}/{dst}", s.handleNodeConnectionGet)
	mux.HandleFunc("PUT /v1/node-connections/{src}/{dst}", s.handleNodeConnectionModify)
	mux.HandleFunc("POST /v1/node-connections/{src}/{dst}", s.handleNodeConnectionModify)
	mux.HandleFunc("PATCH /v1/node-connections/{src}/{dst}", s.handleNodeConnectionModify)
	mux.HandleFunc("DELETE /v1/node-connections/{src}/{dst}", s.handleNodeConnectionDelete)
}

// nodeConnectionWire is the per-pair payload upstream LINSTOR
// returns from `GET /v1/node-connections/{src}/{dst}` and embeds in
// the list response. Field names match upstream so golinstor /
// python-linstor decode without translation; the JSON encoder writes
// an empty `props` object (not `null`) when no properties are set
// because the python CLI's print loop iterates the map unconditionally.
type nodeConnectionWire struct {
	NodeA string            `json:"node_a"`
	NodeB string            `json:"node_b"`
	Props map[string]string `json:"props"`
}

// ObjRefs keys for ApiCallRc envelopes the node-connection handler
// stamps onto its success replies. Upstream LINSTOR uses these exact
// strings on the wire so audit-log greppers that already classify the
// upstream traffic catch blockstor's envelopes without an extra rule.
const (
	objRefNodeA = "NodeA"
	objRefNodeB = "NodeB"
)

// nodeConnectionPairKey returns the canonical persistence key for the
// (a, b) pair: lexicographically lower name first, then `::`, then
// the higher. The separator can't appear inside a LINSTOR node name
// (validated to `[A-Za-z0-9_-]+`), so the join is unambiguous.
func nodeConnectionPairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}

	return a + "::" + b
}

// handleNodeConnectionList serves `GET /v1/node-connections` and
// `GET /v1/node-connections/{src}` (the optional-filter form). The
// filter form is what the python CLI uses for
// `linstor node-connection list <node>`; with no path arg the CLI
// hits the bare list endpoint and renders every pair the controller
// knows about.
func (s *Server) handleNodeConnectionList(w http.ResponseWriter, r *http.Request) {
	pairs, err := s.readAllNodeConnections(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if filter := r.PathValue("src"); filter != "" {
		filtered := pairs[:0]

		for _, p := range pairs {
			if p.NodeA == filter || p.NodeB == filter {
				filtered = append(filtered, p)
			}
		}

		pairs = filtered
	}

	// Defensive non-nil: matches the rest of the REST surface
	// (`linstor-csi` rejects `null` in place of `[]`).
	if pairs == nil {
		pairs = []nodeConnectionWire{}
	}

	writeJSON(w, http.StatusOK, pairs)
}

// handleNodeConnectionGet serves `GET /v1/node-connections/{src}/{dst}`.
// A pair that has never been written returns the same shape with an
// empty `props` map — upstream LINSTOR's CtrlNodeConnectionApiCallHandler
// behaves the same way ("no properties set" is not a 404; the pair
// implicitly exists for every (src, dst) combination of registered
// nodes).
func (s *Server) handleNodeConnectionGet(w http.ResponseWriter, r *http.Request) {
	src := r.PathValue("src")
	dst := r.PathValue("dst")

	props, err := s.readNodeConnectionProps(r.Context(), src, dst)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, nodeConnectionWire{
		NodeA: src,
		NodeB: dst,
		Props: props,
	})
}

// handleNodeConnectionModify is the shared backend for PUT / POST /
// PATCH. golinstor's NodeConnection.Modify sends a
// `GenericPropsModify` envelope (override_props / delete_props /
// delete_namespaces); we merge it into the stored map with the same
// "set then delete" precedence the controller_props handler uses.
//
// Returns 200 + `[]APICallRc` envelope (NOT 204). python-linstor's
// `set_property` code path always JSON-decodes the body — a 204 with
// no content trips `json.loads("")` and surfaces as the operator-
// visible `Unable to parse REST json data` crash that Bug 101 was
// filed for.
func (s *Server) handleNodeConnectionModify(w http.ResponseWriter, r *http.Request) {
	src := r.PathValue("src")
	dst := r.PathValue("dst")

	var modify apiv1.GenericPropsModify

	// Bug 158/161: empty body or malformed JSON surfaces as 400 +
	// LINSTOR envelope (typed wire shape, no Go-side type leak),
	// and unknown top-level fields are refused at the wire boundary.
	if !decodeJSON(w, r, &modify) {
		return
	}

	if s.Client == nil {
		writeError(w, http.StatusServiceUnavailable,
			"node-connection properties require an apiserver client")

		return
	}

	// Bug 133: refuse set-property when either node doesn't exist.
	// Without this gate `linstor node-connection set-property bogus-A
	// bogus-B Sites/Site x` persisted a phantom pair entry against
	// non-existent nodes (after Bug 101 wired the persistence path).
	// Mirrors Bug 94's node-existence gate on `r c` — 404 + LINSTOR
	// envelope naming the missing node.
	if !s.refuseNodeConnectionOnUnknownNode(w, r, src) {
		return
	}

	if !s.refuseNodeConnectionOnUnknownNode(w, r, dst) {
		return
	}

	err := s.applyNodeConnectionProps(r.Context(), src, dst, &modify)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node-connection property set",
		ObjRefs: map[string]string{
			objRefNodeA: src,
			objRefNodeB: dst,
		},
	}})
}

// refuseNodeConnectionOnUnknownNode is Bug 133's node-existence
// gate. Both endpoints of a node-connection set-property must resolve
// to a registered Node CRD; without this check the handler persisted
// a phantom pair entry on `ControllerConfig.Spec.NodeConnections` and
// `linstor node-connection list` then rendered a row referencing
// nodes that don't exist. Mirrors Bug 94's gate shape — 404 + LINSTOR
// envelope naming the missing node. Returns true when the caller may
// proceed, false when the HTTP error has already been written.
func (s *Server) refuseNodeConnectionOnUnknownNode(w http.ResponseWriter, r *http.Request, name string) bool {
	if s.Store == nil {
		return true
	}

	_, err := s.Store.Nodes().Get(r.Context(), name)
	if err == nil {
		return true
	}

	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound,
			"node '"+name+"' not found: create the node first with "+
				"`linstor n c <name>` or pass a valid existing node name")

		return false
	}

	writeStoreError(w, err)

	return false
}

// handleNodeConnectionDelete drops every stored property for the
// (src, dst) pair. Idempotent on a missing record — upstream LINSTOR
// treats `node-connection drop-property` on an unset key as a no-op,
// not a 404, and the python CLI's retry-once-then-give-up loop relies
// on that.
func (s *Server) handleNodeConnectionDelete(w http.ResponseWriter, r *http.Request) {
	src := r.PathValue("src")
	dst := r.PathValue("dst")

	if s.Client == nil {
		writeError(w, http.StatusServiceUnavailable,
			"node-connection properties require an apiserver client")

		return
	}

	err := s.dropNodeConnection(r.Context(), src, dst)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node-connection deleted",
		ObjRefs: map[string]string{
			objRefNodeA: src,
			objRefNodeB: dst,
		},
	}})
}

// readNodeConnectionProps returns the stored props map for the
// canonical (lo, hi) pair-id of (src, dst). Missing CRD / missing
// pair / nil map all collapse to an empty (non-nil) map so callers
// can range over it without a nil check.
func (s *Server) readNodeConnectionProps(ctx context.Context, src, dst string) (map[string]string, error) {
	if s.Client == nil {
		return map[string]string{}, nil
	}

	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := s.Client.Get(ctx,
		client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if apierrors.IsNotFound(err) {
		return map[string]string{}, nil
	}

	if err != nil {
		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	key := nodeConnectionPairKey(src, dst)

	stored := ctrlConfig.Spec.NodeConnections[key]
	if stored == nil {
		return map[string]string{}, nil
	}

	return maps.Clone(stored), nil
}

// readAllNodeConnections returns the full matrix view. Missing CRD
// returns an empty slice (a fresh cluster has nothing wired). Result
// is sorted by (NodeA, NodeB) so the wire-side output is stable across
// reconciles.
func (s *Server) readAllNodeConnections(ctx context.Context) ([]nodeConnectionWire, error) {
	if s.Client == nil {
		return []nodeConnectionWire{}, nil
	}

	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := s.Client.Get(ctx,
		client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &ctrlConfig)
	if apierrors.IsNotFound(err) {
		return []nodeConnectionWire{}, nil
	}

	if err != nil {
		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	pairs := make([]nodeConnectionWire, 0, len(ctrlConfig.Spec.NodeConnections))

	for key, props := range ctrlConfig.Spec.NodeConnections {
		nodeA, nodeB, ok := strings.Cut(key, "::")
		if !ok {
			// Defensive: a hand-edited CRD with a malformed key
			// (no `::` separator) shouldn't break the entire
			// list response — skip the row.
			continue
		}

		clone := map[string]string{}
		if props != nil {
			clone = maps.Clone(props)
		}

		pairs = append(pairs, nodeConnectionWire{
			NodeA: nodeA,
			NodeB: nodeB,
			Props: clone,
		})
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].NodeA != pairs[j].NodeA {
			return pairs[i].NodeA < pairs[j].NodeA
		}

		return pairs[i].NodeB < pairs[j].NodeB
	})

	return pairs, nil
}

// applyNodeConnectionProps merges modify onto the persisted pair-
// id'd props map. Creates the ControllerConfig CRD on first write
// — matches the pattern controller_props.go uses for the same
// "fresh cluster, no `kubectl apply` of ControllerConfig yet"
// scenario.
//
// Empty-after-merge: when every key has been deleted, the pair
// entry is dropped from the outer map entirely so a subsequent
// list doesn't render an empty-props row.
func (s *Server) applyNodeConnectionProps(
	ctx context.Context, src, dst string, modify *apiv1.GenericPropsModify,
) error {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	key := nodeConnectionPairKey(src, dst)

	err := s.Client.Get(ctx,
		client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName},
		&ctrlConfig,
	)
	if apierrors.IsNotFound(err) {
		ctrlConfig = blockstoriov1alpha1.ControllerConfig{
			ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
			Spec: blockstoriov1alpha1.ControllerConfigSpec{
				NodeConnections: map[string]map[string]string{},
			},
		}

		mergeNodeConnectionProps(&ctrlConfig, key, modify)

		err = s.Client.Create(ctx, &ctrlConfig)
		if err != nil {
			return errors.Wrap(err, "create ControllerConfig")
		}

		return nil
	}

	if err != nil {
		return errors.Wrap(err, "get ControllerConfig")
	}

	mergeNodeConnectionProps(&ctrlConfig, key, modify)

	err = s.Client.Update(ctx, &ctrlConfig)
	if err != nil {
		return errors.Wrap(err, "update ControllerConfig")
	}

	return nil
}

// dropNodeConnection removes the entire pair entry from the outer
// map. NotFound on the CRD / unset pair both return nil — the
// operator-visible semantic of `node-connection delete` is "this
// pair has no props after this call", which a missing record
// already satisfies.
func (s *Server) dropNodeConnection(ctx context.Context, src, dst string) error {
	var ctrlConfig blockstoriov1alpha1.ControllerConfig

	err := s.Client.Get(ctx,
		client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName},
		&ctrlConfig,
	)
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return errors.Wrap(err, "get ControllerConfig")
	}

	key := nodeConnectionPairKey(src, dst)
	if _, present := ctrlConfig.Spec.NodeConnections[key]; !present {
		return nil
	}

	delete(ctrlConfig.Spec.NodeConnections, key)

	err = s.Client.Update(ctx, &ctrlConfig)
	if err != nil {
		return errors.Wrap(err, "update ControllerConfig")
	}

	return nil
}

// mergeNodeConnectionProps applies an OverrideProps / DeleteProps
// batch onto the pair entry. The map-of-maps allocation tracks the
// existing-entry / empty-after-delete cases so a freshly emptied
// pair doesn't leave a `{key: {}}` ghost on the CRD — list output
// would render it as a row with zero props, which is the same
// shape upstream LINSTOR emits for a `set-property` immediately
// followed by `drop-property`; cozystack treats "no props" as "no
// pair" for the list output so the ghost is explicitly pruned.
func mergeNodeConnectionProps(
	ctrlConfig *blockstoriov1alpha1.ControllerConfig, key string, modify *apiv1.GenericPropsModify,
) {
	if ctrlConfig.Spec.NodeConnections == nil {
		ctrlConfig.Spec.NodeConnections = map[string]map[string]string{}
	}

	props := ctrlConfig.Spec.NodeConnections[key]
	if props == nil {
		props = map[string]string{}
	}

	maps.Copy(props, modify.OverrideProps)

	for _, k := range modify.DeleteProps {
		delete(props, k)
	}

	// Namespace-prefix delete (e.g. `delete_namespaces: ["DrbdOptions/Net"]`)
	// removes every key under the given prefix. Matches upstream
	// LINSTOR's `GenericPropsModify.delete_namespaces` semantic.
	for _, ns := range modify.DeleteNamespace {
		prefix := strings.TrimSuffix(ns, "/") + "/"

		for k := range props {
			if strings.HasPrefix(k, prefix) || k == ns {
				delete(props, k)
			}
		}
	}

	if len(props) == 0 {
		delete(ctrlConfig.Spec.NodeConnections, key)

		return
	}

	ctrlConfig.Spec.NodeConnections[key] = props
}
