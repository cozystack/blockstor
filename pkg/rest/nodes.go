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
	"fmt"
	"maps"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

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
//
// Bug 201: routes through Store.Nodes().PatchNetInterfaces so the
// mutation closure runs against state re-fetched after every 409,
// instead of replaying a wire snapshot captured at the REST-layer
// Get. A concurrent peer's `handleNetInterfaceDelete` cannot
// silently overwrite this addition.
func (s *Server) handleNetInterfaceCreate(w http.ResponseWriter, r *http.Request) {
	mutateNetInterface(w, r, s, func(current []apiv1.NetInterface, iface apiv1.NetInterface) ([]apiv1.NetInterface, error) {
		for i := range current {
			if current[i].Name == iface.Name {
				current[i] = iface

				return current, nil
			}
		}

		return append(current, iface), nil
	})
}

// handleNetInterfaceUpdate is the per-name replace. The path's
// {name} wins over any name in the body so callers can omit it.
//
// Bug 201: see handleNetInterfaceCreate. Same Patch routing.
func (s *Server) handleNetInterfaceUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	mutateNetInterface(w, r, s, func(current []apiv1.NetInterface, iface apiv1.NetInterface) ([]apiv1.NetInterface, error) {
		iface.Name = name

		for i := range current {
			if current[i].Name == name {
				current[i] = iface

				return current, nil
			}
		}

		// Update on a missing interface is also a create — matches
		// upstream LINSTOR's PUT-creates semantic for `linstor n
		// interface modify`.
		return append(current, iface), nil
	})
}

// handleNetInterfaceDelete drops the named NetInterface. Missing →
// no-op (idempotent).
//
// Bug 201: routes through Store.Nodes().PatchNetInterfaces — the
// delete closure re-runs against state re-fetched after every 409,
// so a sibling `handleNetInterfaceCreate` adding a different
// interface concurrently won't be silently dropped on the
// wholesale-Spec-replace path the old `Update` used.
func (s *Server) handleNetInterfaceDelete(w http.ResponseWriter, r *http.Request) {
	nodeName := r.PathValue("node")
	name := r.PathValue("name")

	err := s.Store.Nodes().PatchNetInterfaces(r.Context(), nodeName, func(current []apiv1.NetInterface) ([]apiv1.NetInterface, error) {
		out := current[:0]

		for i := range current {
			if current[i].Name == name {
				continue
			}

			out = append(out, current[i])
		}

		return out, nil
	})
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "net-interface deleted: " + name,
	}})
}

// mutateNetInterface decodes a NetInterface body and runs the
// supplied mutation against the node's NetInterface list via the
// Bug 201 Patch helper. Used by both create and update so the
// decoder + Patch plumbing stays in one place.
func mutateNetInterface(w http.ResponseWriter, r *http.Request, s *Server, mutate func([]apiv1.NetInterface, apiv1.NetInterface) ([]apiv1.NetInterface, error)) {
	nodeName := r.PathValue("node")

	var iface apiv1.NetInterface

	if !decodeJSON(w, r, &iface) {
		return
	}

	if iface.Name == "" && r.PathValue("name") == "" {
		writeError(w, http.StatusBadRequest, "interface name is required")

		return
	}

	err := s.Store.Nodes().PatchNetInterfaces(r.Context(), nodeName, func(current []apiv1.NetInterface) ([]apiv1.NetInterface, error) {
		return mutate(current, iface)
	})
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

	// Bug 183: scrub deny-listed sensitive keys (passphrase, password,
	// shared-secret, ...) from every Node's Props bag before emit.
	// Mirrors Bug 115's RD-side redaction at the REST boundary — the
	// satellite-side reads (which need the un-redacted value for the
	// LUKS-prereq gate) bypass this path.
	for i := range nodes {
		redactSensitiveProps(nodes[i].Props)
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

	// Bug 183: scrub the per-Node Props bag at the REST boundary.
	// The Get() return is a value copy so the in-place redaction is
	// local to this response — store cache stays un-redacted.
	redactSensitiveProps(n.Props)

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

	if !decodeJSON(w, r, &n) {
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

	if !decodeJSON(w, r, &patch) {
		return
	}

	// Bug 201: NodeType lives on the typed Spec.Type field (not
	// in Props), so a `node_type` toggle needs the wholesale-Spec
	// Update surface. The typical prop-only path routes through
	// PatchProps — the lost-update class lives on the prop-
	// mutation flank. node_type is rarely toggled in production
	// (linstor-csi and piraeus pin it at register time and never
	// flip it), so applying it via the wholesale Update path is
	// acceptable for now.
	//
	// We probe Get first so the 404 semantics for a missing node
	// still surface for a body that touches neither NodeType nor
	// Props/DeleteProps (`{"type":"SATELLITE"}` with no
	// node_type/override_props/delete_props is a legitimate
	// no-op-but-must-exist request shape).
	existing, err := s.Store.Nodes().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if patch.NodeType != "" && patch.NodeType != existing.Type {
		existing.Type = patch.NodeType

		err = s.Store.Nodes().Update(r.Context(), &existing)
		if err != nil {
			writeStoreError(w, err)

			return
		}
	}

	if len(patch.OverrideProps) > 0 || len(patch.DeleteProps) > 0 {
		err = s.Store.Nodes().PatchProps(r.Context(), name, func(props map[string]string) error {
			maps.Copy(props, patch.OverrideProps)

			for _, k := range patch.DeleteProps {
				delete(props, k)
			}

			return nil
		})
		if err != nil {
			writeStoreError(w, err)

			return
		}
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

	// Bug 201: routes through Store.Nodes().PatchProps. The
	// "already absent" idempotent envelope still needs a Get for
	// the warn-mask response shape, so we probe via Get first; if
	// the probe says "absent" we exit early without writing. The
	// race window between probe and Patch is benign (a concurrent
	// peer can re-add the key, and the delete then no-ops on the
	// retry — still converges).
	probe, err := s.Store.Nodes().Get(r.Context(), nodeName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if _, present := probe.Props[key]; !present {
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

	err = s.Store.Nodes().PatchProps(r.Context(), nodeName, func(props map[string]string) error {
		delete(props, key)

		return nil
	})
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
// Refuses with 409 + FAIL_IN_USE (Bug 92, Bug 179) when any
// Resource CRD OR StoragePool CRD still references the node. The
// pre-Bug-92 behaviour wrote SUCCESS while leaving `<rd>.<node>`
// Resource CRDs alive: the controller "forgot" the satellite
// (`n l` dropped it), but the satellite Pod and its DRBD kernel
// state stayed up, and `n restore` returned `object not found` —
// no path back. Bug 179 extends the refusal to StoragePools — the
// pre-Bug-179 gate ignored SPs, so `n d` of a node carrying only
// SPs (no Resources) silently orphaned the SP CRDs, and the
// autoplacer's free-space ranking then crashed on a nil-Node
// lookup the next reconcile. Mirrors handleNodeEvacuate's in-use
// refusal pattern from cli-parity-audit Bug 18: the operator
// must `r d` / `sp d` / `n evacuate` first, or pass `?force=true`
// to accept the cascade.
//
// Bug 174 (P2): wraps the pre-Delete refusal + Delete pair in the
// shared `deleteWithRollback` close so a concurrent
// `r c <node>` / `sp c <node>` that slips between the pre-walk
// and the Delete can't leak an orphan Resource / StoragePool CRD
// pointing at a deleted Node. Same shape as Bug 145 on `sp d`.
//
// `?force=true` semantics (Bug 179): cascade-delete every
// referencing Resource and StoragePool CRD before dropping the
// Node row, matching `n lost`'s cascadeOrphansForLostNode. The
// alternative — strip-and-let-the-operator-clean — would re-create
// the exact orphan-SP state Bug 179 closed, so the cascade variant
// is the only choice that keeps the post-condition "no SP / Resource
// row references a deleted Node" invariant.
func (s *Server) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")
	force := isForce(r)

	// Bug 179: `?force=true` cascade-deletes every referencing
	// Resource + StoragePool CRD before dropping the Node — same
	// shape `n lost` already enforces. Without the cascade,
	// force-delete would leave orphan SP CRDs pointing at a deleted
	// Node, which is precisely the symptom Bug 179 closed.
	if force {
		err := s.cascadeOrphansForLostNode(r.Context(), name)
		if err != nil {
			writeStoreError(w, err)

			return
		}
	}

	(&deleteWithRollback[apiv1.Node]{
		refuseIfReferenced: func() bool {
			if force {
				return false
			}

			return s.refuseNodeDeleteIfReferenced(w, r, name)
		},
		capture: func() (apiv1.Node, bool) {
			return s.captureNode(r.Context(), name)
		},
		remove: func() error {
			return s.Store.Nodes().Delete(r.Context(), name)
		},
		rolledBackIfRaced: func(captured apiv1.Node, capturedOK bool) bool {
			if force || !capturedOK {
				return false
			}

			return s.rollbackNodeDeleteIfRaced(w, r, name, &captured)
		},
		writeWarn: func() {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnNodeNotFound,
				Message: "node already absent: " + name,
			}})
		},
		writeSuccess: func() {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: maskInfo,
				Message: "node deleted: " + name,
			}})
		},
	}).run(w)
}

// refuseNodeDeleteIfReferenced runs the pre-Delete Bug 92 / Bug 179
// walk. Returns true when the HTTP error has already been written
// (the caller must stop processing) and false when the delete may
// proceed. Pulled out of handleNodeDelete so the shared Bug 174
// close (deleteWithRollback) can call it from both pre-walk and
// post-walk slots.
//
// Bug 179: walks BOTH Resources and StoragePools on the node. The
// pre-Bug-179 gate only walked Resources, so `n d` of a node
// carrying only StoragePools (no Resources) silently orphaned the
// SP CRDs — they survived pointing at a deleted Node row, and the
// autoplacer's free-space ranking then crashed on the nil-Node
// lookup. Mirrors `n lost`'s cascadeOrphansForLostNode which
// already walks both stores in lock-step.
func (s *Server) refuseNodeDeleteIfReferenced(w http.ResponseWriter, r *http.Request, name string) bool {
	resourceRefs, spRefs, err := s.referencesOnNode(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return true
	}

	if len(resourceRefs) == 0 && len(spRefs) == 0 {
		return false
	}

	writeJSON(w, http.StatusConflict, buildNodeDeleteRefusal(name, resourceRefs, spRefs))

	return true
}

// referencesOnNode bundles the two reference walks the Bug 92 /
// Bug 179 gates run in lock-step (Resources + StoragePools on the
// target node). Returning both lists in one call keeps the
// pre-walk and the post-Delete re-walk byte-identical — drift
// between the two would let a racing dependent through the gate.
func (s *Server) referencesOnNode(ctx context.Context, name string) ([]string, []string, error) {
	resourceRefs, err := s.resourcesOnNode(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	spRefs, err := s.storagePoolsOnNode(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	return resourceRefs, spRefs, nil
}

// storagePoolsOnNode returns the sorted list of StoragePool names
// hosted on the target node — the SP-side counterpart of
// resourcesOnNode used by Bug 179. Sorted so the surfaced "cause"
// line is stable across calls; the K8s backend's iteration order
// is non-deterministic, and operators rerun `n d` to confirm the
// refusal message.
//
// Bug 179: DfltDisklessStorPool — the per-satellite diskless pool
// upstreamLINSTOR (and blockstor's handleNodeCreate) auto-provisions
// at node-register time — is filtered out of the refusal list. It's
// an internal artefact of the satellite registration, not an
// operator-managed resource; surfacing it would make every `n d`
// hit the refusal even on a freshly-created idle node and the
// operator-facing fix would be "delete the auto-created pool first",
// which makes no sense. The cascade still drops it (the live
// satellite is going away with the node anyway).
func (s *Server) storagePoolsOnNode(ctx context.Context, node string) ([]string, error) {
	pools, err := s.Store.StoragePools().ListByNode(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("list storage pools on node %q: %w", node, err)
	}

	var refs []string

	for i := range pools {
		if pools[i].StoragePoolName == DfltDisklessStorPoolName {
			continue
		}

		refs = append(refs, pools[i].StoragePoolName)
	}

	sort.Strings(refs)

	return refs, nil
}

// buildNodeDeleteRefusal assembles the 409 envelope for the Bug 92
// / Bug 179 in-use refusal. Surfaces BOTH the referencing Resource
// list AND the referencing StoragePool list in a single round-trip
// so the operator sees the full cleanup workload without having to
// retry `n d` after dropping the first half. Mirrors `n lost`'s
// cause-line shape (buildNodeLostRefusalCause) which already
// concatenates the two signals.
//
// At least one of `resourceRefs` / `spRefs` is expected to be
// non-empty — callers guard the no-reference happy path. The
// envelope's Message stays object-typed ("hosts resource replicas
// and/or storage pools") so audit-log greps disambiguate
// node-delete refusals from RG-delete and SP-delete refusals which
// reuse the same FAIL_IN_USE sub-code.
func buildNodeDeleteRefusal(name string, resourceRefs, spRefs []string) []apiv1.APICallRc {
	var causeParts []string

	if len(resourceRefs) > 0 {
		causeParts = append(causeParts, fmt.Sprintf(
			"%d resource(s) reference node '%s': %s",
			len(resourceRefs), name, strings.Join(resourceRefs, ", ")))
	}

	if len(spRefs) > 0 {
		causeParts = append(causeParts, fmt.Sprintf(
			"%d storage pool(s) on node '%s': %s",
			len(spRefs), name, strings.Join(spRefs, ", ")))
	}

	correction := "Delete the listed resources first " +
		"(`linstor r d <node> <rd>`) and storage pools " +
		"(`linstor sp d <node> <pool>`), or evacuate the node " +
		"(`linstor n evacuate <node>`); pass `?force=true` to " +
		"delete the node anyway and cascade the orphans."

	return []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: "Node '" + name + "' cannot be deleted because " +
			"it still hosts resource replicas and/or storage pools.",
		Cause:  strings.Join(causeParts, "; "),
		Correc: correction,
		ObjRefs: map[string]string{
			objRefNode: name,
		},
	}}
}

// captureNode grabs a snapshot of the Node CRD so the Bug 174
// post-delete re-scan has something to restore when a racing
// `r c <node>` slipped past the pre-walk. The second return is
// false when the node no longer exists at capture time (benign
// idempotent-delete replay) — the rollback path is skipped in
// that case.
func (s *Server) captureNode(ctx context.Context, name string) (apiv1.Node, bool) {
	n, err := s.Store.Nodes().Get(ctx, name)
	if err != nil {
		return apiv1.Node{}, false
	}

	return n, true
}

// rollbackNodeDeleteIfRaced runs the Bug 174 post-Delete re-scan.
// If a Resource or StoragePool reference appeared between the
// pre-walk and the Delete, restore the captured Node and write
// the 409 envelope the pre-walk would have written. Returns true
// when the rollback fired (HTTP error already written, caller must
// stop) and false when the delete is safe to commit. Mirrors Bug
// 145's `rollbackSPDeleteIfRaced` shape; Bug 179 extends the
// re-walk to ALSO catch a racing `sp c <node>` so an SP CRD
// persisted during the TOCTOU window can't orphan into a deleted
// Node either.
func (s *Server) rollbackNodeDeleteIfRaced(w http.ResponseWriter, r *http.Request, name string, captured *apiv1.Node) bool {
	resourceRefs, spRefs, err := s.referencesOnNode(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return true
	}

	if len(resourceRefs) == 0 && len(spRefs) == 0 {
		return false
	}

	// Bug 178: best-effort restore is no longer truly best-effort.
	// On `context.Canceled` (client disconnect) or any other Create
	// failure the cluster ends up with NO Node row + a live
	// referencing Resource — and the operator gets handed a 409
	// "still hosts resource replicas" envelope while the Node they
	// were told about no longer exists. Surface a 500 envelope that
	// names the rollback failure explicitly so the operator knows
	// the deleted primary may need manual restoration.
	createErr := s.Store.Nodes().Create(r.Context(), captured)
	if createErr != nil {
		writeRollbackRestoreFailure(r.Context(), w, createErr,
			objRefNode, name, "linstor n l")

		return true
	}

	writeJSON(w, http.StatusConflict, buildNodeDeleteRefusal(name, resourceRefs, spRefs))

	return true
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
//
// Bug 162 (P0): every non-sentinel branch routes the message through
// scrubImplDetails before emitting. Without scrubbing, an etcd-side
// rejection ("etcdserver: request is too large") or an apimachinery
// status error ("controllerconfigs.blockstor.io.blockstor.io ...")
// reached the wire verbatim, leaking the persistence backend's
// identity. Bug 146 already scrubbed the inbound JSON-decode path;
// this is the matching outbound guard.
//
// Bug 164 (P1): K8s-shaped status errors (apierrors.IsConflict /
// IsAlreadyExists) bypass the local store sentinels and used to fall
// through to the default 500 branch. linstor-csi treats 5xx as fatal
// but 409 as retryable, so a single optimistic-lock collision wedged
// every CSI call against the affected RD. The IsConflict /
// IsAlreadyExists branches map to 409 with a generic message — the
// raw apimachinery string is dropped to avoid the Bug 162 leak.
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		writeError(w, http.StatusConflict, err.Error())
	case apierrors.IsConflict(err):
		// Optimistic-lock conflict. Generic message — the apimachinery
		// error embeds the GroupResource string which leaks the CRD
		// plural and API group. CSI retries on 409.
		writeError(w, http.StatusConflict,
			"conflict: store object was modified, retry the request")
	case apierrors.IsAlreadyExists(err):
		writeError(w, http.StatusConflict,
			"conflict: store object already exists")
	case apierrors.IsNotFound(err):
		// Same shape as the local sentinel — keep the wire status
		// uniform so CSI's 404-handling path fires the same way.
		writeError(w, http.StatusNotFound, "not found")
	default:
		writeError(w, http.StatusInternalServerError,
			"store error: "+scrubImplDetails(err.Error()))
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
//
// Bug 199 (P2): every message goes through `scrubImplDetails` before
// hitting the wire. Bug 162 fixed `writeStoreError`'s default branch
// to scrub etcd / apimachinery / k8s.io / `*.blockstor.io` substrings
// out of the response body, but 51 other call sites in pkg/rest/
// reach `writeError` directly with the raw `err.Error()` from the
// K8s-backed store (controller_props, snapshot_multi, autoplace,
// encryption, stats, snapshots, spawn, resource_connections,
// query_size_info, storage_pools, node_connections, node_lifecycle,
// advise, nodes, resource_groups, resource_group_extras,
// resource_definitions, rd_clone, volume_definitions, resources).
// Every one of them bypassed the scrub and leaked the persistence-
// backend identity on any 500-shaped failure.
//
// Wrapping `writeError` itself is the single-point fix: the function
// contract becomes "the LINSTOR envelope is always scrubbed of
// backend impl details," which is what every caller expects anyway.
// `scrubImplDetails` is a string-replacer with a fixed allow-list of
// backend sentinels — it is a noop on operator-friendly literal
// strings ("remote_name is required", "passphrase mismatch", ...)
// so the existing 4xx call sites pass through unchanged.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, []apiv1.APICallRc{{
		RetCode: apiCallRcError,
		Message: scrubImplDetails(msg),
	}})
}

// apiCallRcError carries upstream LINSTOR's MASK_ERROR + WARN bits.
// Upstream uses 0xC000_0000_0000_0000 as a `long` literal — that's
// a negative int64 once you set both top bits. We literal-cast to
// match the wire shape golinstor expects.
const apiCallRcError int64 = -0x4000_0000_0000_0000
