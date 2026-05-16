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
	"fmt"
	"maps"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

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
	mux.HandleFunc("GET /v1/nodes/{node}/info", s.requireStore(s.handleNodeInfo))
	mux.HandleFunc("POST /v1/nodes", s.requireStore(s.handleNodeCreate))
	mux.HandleFunc("PUT /v1/nodes/{node}", s.requireStore(s.handleNodeUpdate))
	mux.HandleFunc("DELETE /v1/nodes/{node}", s.requireStore(s.handleNodeDelete))
	// Bug 142: `linstor n dp <node> <key>` returned 404 because the
	// per-key DELETE route was never registered. The controller-scope
	// analog (`DELETE /v1/controller/properties/{key...}`) already
	// works; this route mirrors the same shape — Go 1.22's `{key...}`
	// wildcard captures slash-bearing keys like `Aux/rack-id` whole.
	mux.HandleFunc("DELETE /v1/nodes/{node}/properties/{key...}",
		s.requireStore(s.handleNodePropDelete))

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

// handleNodeInfo serves `GET /v1/nodes/{node}/info`. Scenario 4.W08:
// operators run `linstor node info <node>` as the fastest "why
// didn't autoplace pick this node?" diagnostic. The response is the
// compact per-node capability table — supported + unsupported
// providers and layers — synthesised from the same source of truth
// the per-Node read path uses (SynthesizeNodeCapabilities). 404 on
// unknown node so a typo doesn't masquerade as an empty capability
// set.
func (s *Server) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	n, err := s.Store.Nodes().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, apiv1.NodeInfo{
		Name:                 n.Name,
		SupportedProviders:   append([]string(nil), n.StorageProviders...),
		SupportedLayers:      append([]string(nil), n.ResourceLayers...),
		UnsupportedProviders: n.UnsupportedProviders,
		UnsupportedLayers:    n.UnsupportedLayers,
	})
}

func (s *Server) handleNodeCreate(w http.ResponseWriter, r *http.Request) {
	var n apiv1.Node

	err := json.NewDecoder(r.Body).Decode(&n)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// Bug 97: validate at the wire boundary, before pkg/store/k8s.Name()
	// slugifies + hash-prefixes the input. Same rationale as
	// handleRDCreate; see pkg/rest/input_validation.go.
	nameErr := validateLinstorName("node", n.Name)
	if nameErr != nil {
		writeError(w, http.StatusBadRequest, nameErr.Error())

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

	// Bug 120: validate every NetInterface address at the wire
	// boundary. Upstream LINSTOR accepts either an IP literal (v4
	// or v6) or a DNS-resolvable hostname; the previous behaviour
	// accepted ANY non-empty string, so `n c bogusnode 999.999.999.999`
	// returned 201 with a satellite that could never connect.
	// validateNetInterfaceAddresses refuses an address that's neither
	// a parseable IP literal nor resolvable via DNS within 1s.
	ifaceErr := s.validateNetInterfaceAddresses(r.Context(), n.NetInterfaces)
	if ifaceErr != nil {
		writeError(w, http.StatusBadRequest, ifaceErr.Error())

		return
	}

	// Bug 132: `linstor n c <existing> <new-ip>` used to silently
	// overwrite the stored NetInterface.Address as part of the
	// idempotent-upsert path (Bug 66). Resource .res files would
	// then reference the new IP while the live satellite was still
	// bound to the old one, breaking DRBD wiring with no audit
	// trail. The refusal surfaces the mismatch as 409 + envelope —
	// matching the Bug 92 / Bug 111 refusal pattern — and the
	// `?force=true` query string preserves the deliberate-renumber
	// escape hatch.
	if !isForce(r) && s.refuseNodeIPRewrite(w, r, &n) {
		return
	}

	if !s.upsertNodeAndDiskless(w, r, &n) {
		return
	}

	writeJSON(w, http.StatusCreated, buildNodeCreateEnvelope(&n))
}

// upsertNodeAndDiskless wires the Bug-66 idempotent-upsert path
// followed by the Bug-?? `DfltDisklessStorPool` auto-provision.
// Extracted from handleNodeCreate so the parent stays under the
// funlen budget after the Bug-132 refusal gate landed.
//
// Returns true on success (caller writes the success envelope);
// false when the response has already been written with a
// store-error envelope (caller must early-return).
func (s *Server) upsertNodeAndDiskless(w http.ResponseWriter, r *http.Request, n *apiv1.Node) bool {
	// Idempotent upsert (cli-parity-audit row #44): upstream LINSTOR's
	// `node create` re-issues become no-op updates, not 409s. Cozystack
	// reconcilers retry node registration on every operator restart;
	// returning 409 made the loop hot-spin until human intervention.
	// We try Create first (the common path), and fall through to Update
	// only when the store reports the name is already taken.
	err := s.Store.Nodes().Create(r.Context(), n)
	switch {
	case err == nil:
		// fresh create — normal path
	case errors.Is(err, store.ErrAlreadyExists):
		err = s.Store.Nodes().Update(r.Context(), n)
		if err != nil {
			writeStoreError(w, err)

			return false
		}
	default:
		writeStoreError(w, err)

		return false
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

	return true
}

// refuseNodeIPRewrite implements the Bug 132 guard: a re-POST to
// `/v1/nodes` that targets an existing node whose NetInterface[]
// addresses differ from the request body refuses with 409 +
// LINSTOR envelope. Returns true when the response has been
// written (handler must early-return), false when no existing
// Node was found OR the addresses are byte-identical to the
// request (idempotent path, Bug 66 contract).
//
// Comparison is per-interface-name: a request body that re-sends
// the canonical {Name:"default", Address:"<old>"} pair against a
// stored {Name:"default", Address:"<old>"} survives as a no-op,
// while a request that flips the address (or adds an interface
// whose name matches an existing one but with a different
// address) trips the refusal. Interfaces present in the body but
// not yet stored are tolerated — that's the "add a second
// management network" workflow handleNetInterfaceCreate already
// supports.
func (s *Server) refuseNodeIPRewrite(w http.ResponseWriter, r *http.Request, requested *apiv1.Node) bool {
	existing, err := s.Store.Nodes().Get(r.Context(), requested.Name)
	if err != nil {
		// NotFound is the common path (fresh create); any other
		// error surfaces through the downstream Create/Update.
		return false
	}

	conflict := findNetInterfaceConflict(existing.NetInterfaces, requested.NetInterfaces)
	if conflict == "" {
		return false
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: "Node '" + requested.Name + "' already exists with a different network address.",
		Cause:   conflict,
		Correc: "Use `linstor node modify` (PUT /v1/nodes/<name>) to change " +
			"node properties, or pass `?force=true` to accept the " +
			"NetInterface rewrite and risk breaking existing DRBD wiring.",
		ObjRefs: map[string]string{objRefNode: requested.Name},
	}})

	return true
}

// findNetInterfaceConflict returns a human-readable description of
// the first (sorted by interface name) address mismatch between
// the stored interfaces and the request body. Returns "" when no
// stored interface shares a name with a request interface OR every
// shared name maps to a byte-identical Address. Matching is on
// Name only — Address is what we're guarding.
func findNetInterfaceConflict(stored, requested []apiv1.NetInterface) string {
	storedByName := make(map[string]string, len(stored))
	for i := range stored {
		storedByName[stored[i].Name] = stored[i].Address
	}

	// Stable iteration order so the surfaced cause is deterministic
	// across calls — operators rerun `n c` to confirm the message.
	names := make([]string, 0, len(requested))
	for i := range requested {
		names = append(names, requested[i].Name)
	}

	sort.Strings(names)

	for _, name := range names {
		storedAddr, present := storedByName[name]
		if !present {
			continue
		}

		var requestedAddr string

		for i := range requested {
			if requested[i].Name == name {
				requestedAddr = requested[i].Address

				break
			}
		}

		if storedAddr == requestedAddr {
			continue
		}

		return fmt.Sprintf(
			"NetInterface %q currently has address %q; "+
				"request body asked for %q",
			name, storedAddr, requestedAddr)
	}

	return ""
}

// buildNodeCreateEnvelope assembles the `[]ApiCallRc` reply for a
// successful node-create. Extracted from handleNodeCreate so the
// parent stays under the funlen budget after the Bug-97 validation
// gate landed.
//
// Always emits a SUCCESS entry first (the controller-side record was
// written). cli-parity-audit row #40: upstream LINSTOR's
// `CtrlNodeApiCallHandler.createNode` then appends a second WARNING
// entry — "No active connection to satellite '<name>'" — when the
// daemon hasn't checked in yet. blockstor's REST shim used to collapse
// to a single SUCCESS entry; tooling that parses `replies[].ret_code`
// by mask (tests/contract/normalize.go, the Python CLI's print loop)
// then missed the deployment-incomplete signal.
//
// `ConnectionStatus == "ONLINE"` covers the operator-pre-seeded /
// adopted-already-running-satellite case used by tests; everything
// else (including the empty default) emits the warning. Upstream's
// wire vocabulary here is {"ONLINE","OFFLINE","CONNECTING",…}; matching
// on ONLINE is the strictest interpretation of "active connection".
func buildNodeCreateEnvelope(n *apiv1.Node) []apiv1.APICallRc {
	envelope := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node created: " + n.Name,
		ObjRefs: map[string]string{objRefNode: n.Name},
	}}

	if !strings.EqualFold(n.ConnectionStatus, apiv1.NodeTypeOnline) {
		envelope = append(envelope, apiv1.APICallRc{
			RetCode: warnNoSatelliteConnection,
			Message: "No active connection to satellite '" + n.Name + "'",
			ObjRefs: map[string]string{objRefNode: n.Name},
		})
	}

	return envelope
}

// netInterfaceDNSTimeout caps the per-address DNS lookup the Bug 120
// validation falls back to when net.ParseIP rejects the literal.
// Bounded so a slow/broken resolver can't stall the node-create path
// indefinitely — upstream LINSTOR's controller also fronts the
// resolution with a short timeout. 1s is enough for any reachable
// resolver and short enough that an operator-typo for a non-existent
// hostname still surfaces a 400 within the human-perceptible budget.
const netInterfaceDNSTimeout = time.Second

// validateNetInterfaceAddresses walks every NetInterface and refuses
// the request when any Address is neither a parseable IP literal nor
// a DNS-resolvable hostname. Empty addresses are also refused — a
// satellite with no Address can never connect, and the previous
// behaviour silently persisted one (Bug 120).
//
// Two acceptance paths (mirrors upstream LINSTOR's NetInterface
// validation):
//
//  1. net.ParseIP succeeds — covers IPv4 dotted-quad, IPv4 with
//     leading zeros, IPv6 incl. link-local. Fast path, no DNS hit.
//  2. ParseIP fails BUT s.lookupHost returns at least one address
//     within netInterfaceDNSTimeout — covers DNS hostnames
//     (`worker-1.cluster.local`) that piraeus-operator commonly
//     registers via K8s DNS.
//
// On rejection the returned error carries the offending literal and
// the rule it violated, so the LINSTOR envelope's `message` is
// directly actionable for the operator.
func (s *Server) validateNetInterfaceAddresses(ctx context.Context, ifaces []apiv1.NetInterface) error {
	for i := range ifaces {
		addr := strings.TrimSpace(ifaces[i].Address)
		if addr == "" {
			return errors.Errorf(
				"net-interface %q: address is empty; use a valid IPv4 or IPv6 literal "+
					"(e.g. 10.0.0.1 or fe80::1) or a DNS-resolvable hostname",
				ifaces[i].Name)
		}

		if net.ParseIP(addr) != nil {
			continue
		}

		// DNS-fallback: upstream LINSTOR accepts hostnames here.
		// Bounded by netInterfaceDNSTimeout so a broken resolver
		// can't stall the create path.
		lookupCtx, cancel := context.WithTimeout(ctx, netInterfaceDNSTimeout)
		hosts, lookupErr := s.lookupHost(lookupCtx, addr)

		cancel()

		if lookupErr == nil && len(hosts) > 0 {
			continue
		}

		return errors.Errorf(
			"net-interface %q: address %q is not a valid IPv4 or IPv6 literal "+
				"and DNS resolution failed; use a parseable IP literal "+
				"(e.g. 10.0.0.1 or fe80::1) or a resolvable hostname",
			ifaces[i].Name, addr)
	}

	return nil
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

// handleNodePropDelete implements Bug 142: per-key DELETE for node
// properties. Mirrors the controller-property analog
// (`controller_props.go::handleControllerPropDelete`) — slash-bearing
// keys like `Aux/rack-id` round-trip intact via Go 1.22's `{key...}`
// wildcard, and a delete-of-missing folds into a 200 + warn-mask
// envelope so reconciler retry loops don't hot-spin on the second
// pass (cli-parity-audit: `linstor n dp` is idempotent on the
// upstream LINSTOR controller for the same reason).
func (s *Server) handleNodePropDelete(w http.ResponseWriter, r *http.Request) {
	nodeName := r.PathValue("node")

	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing property key")

		return
	}

	existing, err := s.Store.Nodes().Get(r.Context(), nodeName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if _, present := existing.Props[key]; !present {
		// Idempotent no-op: same surface as warnNodeNotFound for `n d`
		// — operators see a distinct warn-band entry that "already
		// absent" without an error mask.
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: maskWarn,
			Message: "node " + nodeName + " property already absent: " + key,
			ObjRefs: map[string]string{objRefNode: nodeName},
		}})

		return
	}

	delete(existing.Props, key)

	err = s.Store.Nodes().Update(r.Context(), &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node " + nodeName + " property deleted: " + key,
		ObjRefs: map[string]string{objRefNode: nodeName},
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
//
// Refuses with 409 + FAIL_IN_USE (Bug 92) when any Resource CRD
// still references the node. The previous behaviour wrote SUCCESS
// while leaving `<rd>.<node>` Resource CRDs alive: the controller
// "forgot" the satellite (`n l` dropped it), but the satellite Pod
// and its DRBD kernel state stayed up, and `n restore` returned
// `object not found` — no path back. Mirrors handleNodeEvacuate's
// in-use refusal pattern from cli-parity-audit Bug 18: the operator
// must `r d` / `n evacuate` first, or pass `?force=true` to accept
// the orphan-cascade.
func (s *Server) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	if !isForce(r) {
		refs, err := s.resourcesOnNode(r.Context(), name)
		if err != nil {
			writeStoreError(w, err)

			return
		}

		if len(refs) > 0 {
			writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
				RetCode: apiCallRcError | apiCallRcFailInUse,
				Message: "Node '" + name + "' cannot be deleted because " +
					"it still hosts resource replicas.",
				Cause: fmt.Sprintf(
					"%d resource(s) reference node '%s': %s",
					len(refs), name, strings.Join(refs, ", ")),
				Correc: "Delete the listed resources first " +
					"(`linstor r d <node> <rd>`) or evacuate the node " +
					"(`linstor n evacuate <node>`); pass `?force=true` " +
					"to delete the node anyway and orphan the replicas.",
				ObjRefs: map[string]string{
					objRefNode: name,
				},
			}})

			return
		}
	}

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

// resourcesOnNode returns the sorted list of Resource names whose
// NodeName matches the target — the wire shape upstream LINSTOR's
// `CtrlNodeApiCallHandler.delete()` builds when refusing a node-
// delete with `FAIL_IN_USE`. Sorted so the surfaced "cause" line is
// stable across calls (the Resources store has no fixed iteration
// order on the K8s backend, and operators rerun `n d` to confirm
// the refusal message after every replica drop).
func (s *Server) resourcesOnNode(ctx context.Context, node string) ([]string, error) {
	resources, err := s.Store.Resources().List(ctx)
	if err != nil {
		return nil, err
	}

	var refs []string

	for i := range resources {
		if resources[i].NodeName == node {
			refs = append(refs, resources[i].Name)
		}
	}

	sort.Strings(refs)

	return refs, nil
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
