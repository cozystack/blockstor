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
	"encoding/json"
	"maps"
	"net"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// DefaultNetInterfaceName is the LINSTOR-canonical name for the
// per-satellite primary network interface. `linstor node create
// <name> <ip>` synthesizes a `NetInterface{Name: "default",
// Address: <ip>}` on the controller side; CLI parity audit row #2
// pins this string — the autoplacer's connection-routing path and
// `props.CurStltConnName` both default to it.
const DefaultNetInterfaceName = "default"

// resolveHostFunc is the DNS-lookup seam handleNodeCreate uses when
// the POST body omits a NetInterface address. Tests swap this for a
// deterministic stub via Server.lookupHost.
type resolveHostFunc func(ctx context.Context, host string) ([]string, error)

// defaultResolveHost wraps net.DefaultResolver.LookupHost — the
// production resolver. Hoisted into a package-level var so tests can
// override per-Server (see Server.lookupHost).
//
//nolint:gochecknoglobals // injectable test seam — see Server.lookupHost
var defaultResolveHost resolveHostFunc = func(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

// registerNodes wires the /v1/nodes endpoints on mux. It is split out of
// Server.Start so each resource group lives in its own file.
func (s *Server) registerNodes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/nodes", s.requireStore(s.handleNodesList))
	mux.HandleFunc("GET /v1/nodes/{node}", s.requireStore(s.handleNodeGet))
	mux.HandleFunc("POST /v1/nodes", s.requireStore(s.handleNodeCreate))
	mux.HandleFunc("PUT /v1/nodes/{node}", s.requireStore(s.handleNodeUpdate))
	mux.HandleFunc("DELETE /v1/nodes/{node}", s.requireStore(s.handleNodeDelete))

	// Per-interface CRUD: clusters with separate replication and
	// management networks need to add/remove NetInterfaces on a
	// running Node without a full PUT-of-the-whole-Node round-trip.
	// Maps onto Node.Spec.NetInterfaces[] inside the same Node CRD.
	mux.HandleFunc("GET /v1/nodes/{node}/net-interfaces",
		s.requireStore(s.handleNetInterfaceList))
	mux.HandleFunc("GET /v1/nodes/{node}/net-interfaces/{name}",
		s.requireStore(s.handleNetInterfaceGet))
	mux.HandleFunc("POST /v1/nodes/{node}/net-interfaces",
		s.requireStore(s.handleNetInterfaceCreate))
	mux.HandleFunc("PUT /v1/nodes/{node}/net-interfaces/{name}",
		s.requireStore(s.handleNetInterfaceUpdate))
	mux.HandleFunc("DELETE /v1/nodes/{node}/net-interfaces/{name}",
		s.requireStore(s.handleNetInterfaceDelete))
}

// handleNetInterfaceList returns the Node's NetInterfaces[] array.
// Used by golinstor's `Nodes.GetNetInterfaces(...)` and `linstor n
// interface list <node>`.
func (s *Server) handleNetInterfaceList(w http.ResponseWriter, r *http.Request) {
	nodeName := r.PathValue("node")

	node, err := s.Store.Nodes().Get(r.Context(), nodeName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if node.NetInterfaces == nil {
		writeJSON(w, http.StatusOK, []apiv1.NetInterface{})

		return
	}

	writeJSON(w, http.StatusOK, node.NetInterfaces)
}

// handleNetInterfaceGet returns a single NetInterface by name. 404
// when the named interface doesn't exist on the node — matches
// upstream LINSTOR's "name not found" semantic.
func (s *Server) handleNetInterfaceGet(w http.ResponseWriter, r *http.Request) {
	nodeName := r.PathValue("node")
	name := r.PathValue("name")

	node, err := s.Store.Nodes().Get(r.Context(), nodeName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	for i := range node.NetInterfaces {
		if node.NetInterfaces[i].Name == name {
			writeJSON(w, http.StatusOK, node.NetInterfaces[i])

			return
		}
	}

	writeError(w, http.StatusNotFound, "net-interface not found: "+name)
}

// handleNetInterfaceCreate appends a NetInterface to the Node's spec.
// Idempotent: a second create with the same name updates in place.
func (s *Server) handleNetInterfaceCreate(w http.ResponseWriter, r *http.Request) {
	mutateNetInterface(w, r, s, func(n *apiv1.Node, iface apiv1.NetInterface) error {
		for i := range n.NetInterfaces {
			if n.NetInterfaces[i].Name == iface.Name {
				n.NetInterfaces[i] = iface

				return nil
			}
		}

		n.NetInterfaces = append(n.NetInterfaces, iface)

		return nil
	})
}

// handleNetInterfaceUpdate is the per-name replace. The path's
// {name} wins over any name in the body so callers can omit it.
func (s *Server) handleNetInterfaceUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	mutateNetInterface(w, r, s, func(n *apiv1.Node, iface apiv1.NetInterface) error {
		iface.Name = name

		for i := range n.NetInterfaces {
			if n.NetInterfaces[i].Name == name {
				n.NetInterfaces[i] = iface

				return nil
			}
		}

		// Update on a missing interface is also a create — matches
		// upstream LINSTOR's PUT-creates semantic for `linstor n
		// interface modify`.
		n.NetInterfaces = append(n.NetInterfaces, iface)

		return nil
	})
}

// handleNetInterfaceDelete drops the named NetInterface. Missing →
// no-op (idempotent).
func (s *Server) handleNetInterfaceDelete(w http.ResponseWriter, r *http.Request) {
	nodeName := r.PathValue("node")
	name := r.PathValue("name")

	node, err := s.Store.Nodes().Get(r.Context(), nodeName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	out := node.NetInterfaces[:0]

	for i := range node.NetInterfaces {
		if node.NetInterfaces[i].Name == name {
			continue
		}

		out = append(out, node.NetInterfaces[i])
	}

	node.NetInterfaces = out

	err = s.Store.Nodes().Update(r.Context(), &node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "net-interface deleted: " + name,
	}})
}

// mutateNetInterface decodes a NetInterface body, runs the supplied
// mutation against the node's interface list, and persists. Used by
// both create and update so the decoder + Get + Update plumbing stays
// in one place.
func mutateNetInterface(w http.ResponseWriter, r *http.Request, s *Server, mutate func(*apiv1.Node, apiv1.NetInterface) error) {
	nodeName := r.PathValue("node")

	var iface apiv1.NetInterface

	err := json.NewDecoder(r.Body).Decode(&iface)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if iface.Name == "" && r.PathValue("name") == "" {
		writeError(w, http.StatusBadRequest, "interface name is required")

		return
	}

	node, err := s.Store.Nodes().Get(r.Context(), nodeName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = mutate(&node, iface)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	err = s.Store.Nodes().Update(r.Context(), &node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Upstream LINSTOR returns an ApiCallRc envelope for the
	// per-interface POST / PUT — golinstor decodes the response as
	// `[]ApiCallRc` and surfaces ret_code errors. Returning the full
	// Node body instead breaks its decoder ("cannot unmarshal object
	// into Go value of type []client.ApiCallRc").
	status := http.StatusOK
	if r.Method == http.MethodPost {
		status = http.StatusCreated
	}

	writeJSON(w, status, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "net-interface " + r.Method + " " + iface.Name,
	}})
}

// requireStore guards endpoints that need persistence; it returns 503 if the
// Store is nil. We can serve /v1/controller/version without a store, so this
// gate is per-handler rather than global.
func (s *Server) requireStore(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Store == nil {
			writeError(w, http.StatusServiceUnavailable, "store not configured")

			return
		}

		next(w, r)
	}
}

func (s *Server) handleNodesList(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.Store.Nodes().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Defensive non-nil: linstor-csi rejects `null` in place of the
	// empty-list envelope. Both store backends `make()` the slice,
	// but pinning here keeps the invariant local to the wire edge.
	if nodes == nil {
		nodes = []apiv1.Node{}
	}

	writeJSON(w, http.StatusOK, nodes)
}

func (s *Server) handleNodeGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	n, err := s.Store.Nodes().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, n)
}

func (s *Server) handleNodeCreate(w http.ResponseWriter, r *http.Request) {
	var n apiv1.Node

	err := json.NewDecoder(r.Body).Decode(&n)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if n.Name == "" {
		writeError(w, http.StatusBadRequest, "node name is required")

		return
	}

	// Scenario 4.W01: synthesize the default NetInterface if the
	// caller passed none. `linstor node create <name> <ip>` is the
	// canonical CLI form; the client always sends a body with at
	// least the primary address, but the CLI also allows omitting
	// the IP entirely — in which case the controller DNS-resolves
	// the name. We mirror both branches; failure of the DNS resolve
	// surfaces an actionable 400 to the operator instead of letting
	// a satellite register with an empty Address (which then breaks
	// the autoplacer's connection routing later).
	if len(n.NetInterfaces) == 0 {
		addr, dnsErr := s.resolveDefaultAddress(r.Context(), &n)
		if dnsErr != nil {
			writeError(w, http.StatusBadRequest, dnsErr.Error())

			return
		}

		n.NetInterfaces = []apiv1.NetInterface{{
			Name:    DefaultNetInterfaceName,
			Address: addr,
		}}
	}

	// Idempotent upsert (cli-parity-audit row #44): upstream LINSTOR's
	// `node create` re-issues become no-op updates, not 409s. Cozystack
	// reconcilers retry node registration on every operator restart;
	// returning 409 made the loop hot-spin until human intervention.
	// We try Create first (the common path), and fall through to Update
	// only when the store reports the name is already taken.
	err = s.Store.Nodes().Create(r.Context(), &n)
	switch {
	case err == nil:
		// fresh create — normal path
	case errors.Is(err, store.ErrAlreadyExists):
		if upErr := s.Store.Nodes().Update(r.Context(), &n); upErr != nil {
			writeStoreError(w, upErr)

			return
		}
	default:
		writeStoreError(w, err)

		return
	}

	// Upstream LINSTOR auto-provisions a `DfltDisklessStorPool`
	// per satellite at node-register time. CLI parity audit row #3.
	// Pre-existing pool of the same name is left alone (ErrAlreadyExists
	// is the no-op signal); any other error must NOT fail Node Create.
	_ = s.Store.StoragePools().Create(r.Context(), &apiv1.StoragePool{
		StoragePoolName: DfltDisklessStorPoolName,
		NodeName:        n.Name,
		ProviderKind:    apiv1.StoragePoolKindDiskless,
	})

	envelope := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node created: " + n.Name,
		ObjRefs: map[string]string{objRefNode: n.Name},
	}}

	// Append the "no active connection" warning when the satellite
	// hasn't checked in yet. cli-parity-audit row #40: upstream
	// LINSTOR's `CtrlNodeApiCallHandler.createNode` returns two
	// ApiCallRc entries on the wire — a SUCCESS for the controller-
	// side record creation, then a WARNING with the exact text
	// "No active connection to satellite '<name>'" so the operator
	// learns the daemon still needs to come up. blockstor's REST shim
	// was collapsing to a single SUCCESS entry; tooling that parses
	// `replies[].ret_code` by mask (the contract normaliser at
	// tests/contract/normalize.go, the Python CLI's print loop) then
	// missed the deployment-incomplete signal.
	//
	// We treat `ConnectionStatus == "ONLINE"` as "the operator pre-seeded
	// a connected node" — used by tests and by clusters that adopt an
	// already-running satellite. Any other value (including the empty
	// default) emits the warning. Upstream LINSTOR's wire vocabulary
	// here is {"ONLINE","OFFLINE","CONNECTING",…}; matching on ONLINE
	// is the strictest interpretation of "active connection".
	if !strings.EqualFold(n.ConnectionStatus, apiv1.NodeTypeOnline) {
		envelope = append(envelope, apiv1.APICallRc{
			RetCode: warnNoSatelliteConnection,
			Message: "No active connection to satellite '" + n.Name + "'",
			ObjRefs: map[string]string{objRefNode: n.Name},
		})
	}

	writeJSON(w, http.StatusCreated, envelope)
}

// resolveDefaultAddress synthesises the address for the default
// NetInterface when the POST /v1/nodes body omits one. Picks the
// first NetInterface address if the caller passed any (defensive —
// the caller should already have populated NetInterfaces, but this
// covers a single-IP payload with an empty name), otherwise falls
// back to DNS resolution of the node's name. Returns an actionable
// error if DNS lookup fails so the operator sees the cause rather
// than a satellite stuck with an empty Address.
func (s *Server) resolveDefaultAddress(ctx context.Context, n *apiv1.Node) (string, error) {
	addrs, err := s.lookupHost(ctx, n.Name)
	if err != nil {
		return "", errors.Wrapf(err,
			"node %q: address omitted and DNS resolution of name failed", n.Name)
	}

	if len(addrs) == 0 {
		return "", errors.Errorf(
			"node %q: address omitted and DNS resolution returned no addresses", n.Name)
	}

	return addrs[0], nil
}

// DfltDisklessStorPoolName matches upstream LINSTOR's canonical
// per-satellite diskless storage pool name. The autoplacer's
// `disklessOnRemaining` path defaults to this pool when the caller
// doesn't pin a specific pool list, and `linstor sp l` shows one
// row per registered node from this synthesised pool. CSI / piraeus
// rely on the exact string.
const DfltDisklessStorPoolName = "DfltDisklessStorPool"

func (s *Server) handleNodeUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	// golinstor's Nodes.Modify(...) sends a NodeModify body — a
	// GenericPropsModify (override_props / delete_props) wrapped
	// with optional `node_type`. Decoding into apiv1.Node would
	// silently nuke the Node's net_interfaces + type because they
	// aren't in the request. Load the existing Node, merge the
	// patch on top.
	var patch apiv1.NodeModify

	err := json.NewDecoder(r.Body).Decode(&patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	existing, err := s.Store.Nodes().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if patch.NodeType != "" {
		existing.Type = patch.NodeType
	}

	if existing.Props == nil && (len(patch.OverrideProps) > 0 || len(patch.DeleteProps) > 0) {
		existing.Props = map[string]string{}
	}

	maps.Copy(existing.Props, patch.OverrideProps)

	for _, k := range patch.DeleteProps {
		delete(existing.Props, k)
	}

	err = s.Store.Nodes().Update(r.Context(), &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node modified: " + name,
	}})
}

// handleNodeDelete drops a Node from the cluster.
//
// Idempotent on NotFound (Bug 66): Cozystack's node-evacuation
// playbook calls `linstor n d <node>` in a retry loop, so a 404 on
// the second pass crashed the python CLI's XML decoder fallback and
// surfaced as a fatal `ParseError` instead of the intended no-op.
// Folding NotFound into a 200 + warn-mask envelope keeps the retry
// loop exit-0 on the second call.
func (s *Server) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	err := s.Store.Nodes().Delete(r.Context(), name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	if err != nil {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnNodeNotFound,
			Message: "node already absent: " + name,
		}})

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node deleted: " + name,
	}})
}

// writeStoreError maps store sentinel errors to HTTP statuses so handlers
// don't repeat the same switch.
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// writeError sends the LINSTOR-shaped `[]ApiCallRc` error envelope.
// golinstor (and therefore linstor-csi) unmarshals failure responses
// into a slice — sending a `{"error": "..."}` object made every
// failed CSI call surface as `json: cannot unmarshal object into Go
// value of type client.ApiCallError` instead of the actual error
// message.
//
// retCode follows the upstream convention: high bit set means
// FATAL/error; we use 0xC000_0000 which is the masked-but-untyped
// "generic error" the controller uses when no specific code applies.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, []apiv1.APICallRc{{
		RetCode: apiCallRcError,
		Message: msg,
	}})
}

// apiCallRcError carries upstream LINSTOR's MASK_ERROR + WARN bits.
// Upstream uses 0xC000_0000_0000_0000 as a `long` literal — that's
// a negative int64 once you set both top bits. We literal-cast to
// match the wire shape golinstor expects.
const apiCallRcError int64 = -0x4000_0000_0000_0000
