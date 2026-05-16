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
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 201 Patch helpers tune their own conflict-retry backoff
// rather than reusing `retry.DefaultRetry`. The default's 5 steps
// at 10 ms each handle low-concurrency RG/Node updates, but the
// REST surface fan-outs that Bug 201 targets — NetInterface CRUD
// from satellite controllers, RG-prop mutation under linstor-csi
// load — can fan in dozens of concurrent writers to the same CRD.
// patchRetrySteps with patchRetryBaseDuration base and
// patchRetryFactor escalation budgets enough retries for a 50-way
// burst to converge under the fake-client's all-or-nothing
// conflict model; on the real apiserver the backoff Cap caps any
// single retry below ~1 s even if the satellite reconciler is
// bombarding the object.
const (
	patchRetrySteps        = 20
	patchRetryBaseDuration = 5 * time.Millisecond
	patchRetryFactor       = 1.5
	patchRetryJitter       = 0.3
	patchRetryCap          = time.Second
)

// patchRetryBackoff returns a fresh Backoff value — wait.Backoff
// mutates `Duration`/`Steps` in place during exponential walks, so
// the loop body in `RetryOnConflict` needs a fresh copy each call.
// Keeping this a function (rather than a package-level var)
// sidesteps the gochecknoglobals lint and avoids accidental shared
// state between concurrent callers.
func patchRetryBackoff() wait.Backoff {
	return wait.Backoff{
		Steps:    patchRetrySteps,
		Duration: patchRetryBaseDuration,
		Factor:   patchRetryFactor,
		Jitter:   patchRetryJitter,
		Cap:      patchRetryCap,
	}
}

// nodes implements store.NodeStore against the Node CRD.
type nodes struct {
	c ctrlclient.Client
}

// List returns all Node CRDs as wire-shape apiv1.Node values, sorted by name.
func (n *nodes) List(ctx context.Context) ([]apiv1.Node, error) {
	var crdList crdv1alpha1.NodeList

	err := n.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list Node CRDs")
	}

	out := make([]apiv1.Node, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireNode(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// Get returns the named Node CRD as an apiv1.Node, or ErrNotFound.
func (n *nodes) Get(ctx context.Context, name string) (apiv1.Node, error) {
	var crd crdv1alpha1.Node

	err := n.c.Get(ctx, types.NamespacedName{Name: Name(name)}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.Node{}, errors.Wrapf(store.ErrNotFound, "node %q", name)
		}

		return apiv1.Node{}, errors.Wrapf(err, "get Node %q", name)
	}

	return crdToWireNode(&crd), nil
}

// Create persists a new Node CRD from an apiv1.Node value.
func (n *nodes) Create(ctx context.Context, in *apiv1.Node) error {
	if in == nil {
		return errors.New("nil Node")
	}

	crd := wireToCRDNode(in)

	err := n.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "node %q", in.Name)
		}

		return errors.Wrapf(err, "create Node %q", in.Name)
	}

	return nil
}

// Update overwrites the spec of an existing Node CRD with the value supplied.
// Status is not touched here — reconcilers own status.
func (n *nodes) Update(ctx context.Context, in *apiv1.Node) error {
	if in == nil {
		return errors.New("nil Node")
	}

	var existing crdv1alpha1.Node

	err := n.c.Get(ctx, types.NamespacedName{Name: Name(in.Name)}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "node %q", in.Name)
		}

		return errors.Wrapf(err, "get Node %q", in.Name)
	}

	existing.Spec = wireToCRDNodeSpec(in)

	err = n.c.Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "update Node %q", in.Name)
	}

	return nil
}

// SetConnectionStatus updates the Node CRD's
// `.status.connectionStatus` via the Status subresource. Survives
// subsequent Spec Update calls (which would otherwise overwrite
// nothing here, but the dedicated subresource is the kubebuilder-
// idiomatic place to land observed state).
func (n *nodes) SetConnectionStatus(ctx context.Context, name, status string) error {
	var existing crdv1alpha1.Node

	err := n.c.Get(ctx, types.NamespacedName{Name: Name(name)}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "node %q", name)
		}

		return errors.Wrapf(err, "get Node %q", name)
	}

	existing.Status.ConnectionStatus = status

	err = n.c.Status().Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "status update Node %q", name)
	}

	return nil
}

// PatchNetInterfaces runs `mutate` against the freshly-fetched current
// NetInterface list and persists the returned slice via a typed-Patch
// (JSON-merge-patch). On 409 the entire Get → mutate → Patch cycle
// re-runs against the fresh state — so a concurrent peer's
// NetInterface additions/deletions are visible to `mutate` on the
// retry, and the wholesale `existing.Spec = wireToCRDNodeSpec(in)`
// stale-snapshot lost-update (Bug 201) cannot happen.
//
// `mutate` receives the current wire-shape NetInterfaces and returns
// the desired list. Returning nil or empty wipes the field; the spec
// converter normalises both the same way as Update.
func (n *nodes) PatchNetInterfaces(ctx context.Context, name string, mutate func([]apiv1.NetInterface) ([]apiv1.NetInterface, error)) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return errors.Wrapf(retry.RetryOnConflict(patchRetryBackoff(), func() error {
		var existing crdv1alpha1.Node

		err := n.c.Get(ctx, types.NamespacedName{Name: Name(name)}, &existing)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.Wrapf(store.ErrNotFound, "node %q", name)
			}

			return errors.Wrapf(err, "get Node %q", name)
		}

		// Surface the CRD's NetInterfaces in wire shape so the
		// closure works in the same vocabulary as REST handlers.
		current := make([]apiv1.NetInterface, 0, len(existing.Spec.NetInterfaces))
		for i := range existing.Spec.NetInterfaces {
			ni := &existing.Spec.NetInterfaces[i]
			current = append(current, apiv1.NetInterface{
				Name:                    ni.Name,
				Address:                 ni.Address,
				SatellitePort:           int(ni.SatellitePort),
				SatelliteEncryptionType: ni.SatelliteEncryptionType,
			})
		}

		next, err := mutate(current)
		if err != nil {
			return err
		}

		// Build a typed-patch base: copy existing, mutate only the
		// NetInterfaces slice, send a JSON-merge-patch diff. The
		// merge-patch ensures the apiserver only sees the field we
		// intend to write — any concurrent peer edit on Props or
		// SatelliteEndpoint is preserved on the server side.
		base := existing.DeepCopy()

		existing.Spec.NetInterfaces = existing.Spec.NetInterfaces[:0]
		if cap(existing.Spec.NetInterfaces) < len(next) {
			existing.Spec.NetInterfaces = make([]crdv1alpha1.NodeNetInterface, 0, len(next))
		}

		for i := range next {
			ni := &next[i]
			existing.Spec.NetInterfaces = append(existing.Spec.NetInterfaces, crdv1alpha1.NodeNetInterface{
				Name:                    ni.Name,
				Address:                 ni.Address,
				SatellitePort:           int32(ni.SatellitePort), //nolint:gosec // upstream LINSTOR ports fit in int32
				SatelliteEncryptionType: strings.ToUpper(ni.SatelliteEncryptionType),
			})
		}

		return n.c.Patch(ctx, &existing, ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}))
	}), "patch NetInterfaces of Node %q", name)
}

// PatchNodeSpec runs `mutate` against the freshly-fetched current wire-
// shape Node and persists the result via a typed-Patch
// (JSON-merge-patch) under `RetryOnConflict` with the Bug 201 backoff.
// On 409 the cycle re-runs against fresh state — so disjoint concurrent
// edits (re-evacuate loop adding EVICTED, an operator stamping a
// different flag, etc.) converge instead of being silently dropped by
// the wholesale `Update`'s un-retried wire-snapshot replace (Bug 205).
//
// Status is not touched here (the satellite reconciler owns it via the
// Status subresource). User annotations on the Node CRD are preserved
// verbatim — the patch reaches only Spec.
//
// The closure receives the wire-shape Node so REST handlers can mutate
// Flags / Props / NetInterfaces in a single atomic patch, mirroring the
// shape the wholesale `Update` accepted.
func (n *nodes) PatchNodeSpec(ctx context.Context, name string, mutate func(*apiv1.Node) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return errors.Wrapf(retry.RetryOnConflict(patchRetryBackoff(), func() error {
		var existing crdv1alpha1.Node

		err := n.c.Get(ctx, types.NamespacedName{Name: Name(name)}, &existing)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.Wrapf(store.ErrNotFound, "node %q", name)
			}

			return errors.Wrapf(err, "get Node %q", name)
		}

		base := existing.DeepCopy()

		// Surface as wire shape so the closure runs in the same
		// vocabulary as REST handlers.
		wire := crdToWireNode(&existing)

		err = mutate(&wire)
		if err != nil {
			return err
		}

		existing.Spec = wireToCRDNodeSpec(&wire)

		return n.c.Patch(ctx, &existing, ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}))
	}), "patch Node %q", name)
}

// PatchProps runs `mutate` against the freshly-fetched current Props
// map and persists the mutated map via a typed-Patch
// (JSON-merge-patch). On 409 the cycle re-runs against fresh state.
//
// `mutate` receives the live wire-shape Props (initialised to a
// non-nil map when the CRD's was nil); in-place edits land directly.
// SatelliteEndpoint is re-derived from the post-mutate map, matching
// `wireToCRDNodeSpec`.
func (n *nodes) PatchProps(ctx context.Context, name string, mutate func(map[string]string) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return errors.Wrapf(retry.RetryOnConflict(patchRetryBackoff(), func() error {
		var existing crdv1alpha1.Node

		err := n.c.Get(ctx, types.NamespacedName{Name: Name(name)}, &existing)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.Wrapf(store.ErrNotFound, "node %q", name)
			}

			return errors.Wrapf(err, "get Node %q", name)
		}

		base := existing.DeepCopy()

		props := existing.Spec.Props
		if props == nil {
			props = map[string]string{}
		}

		err = mutate(props)
		if err != nil {
			return err
		}

		existing.Spec.Props = props
		existing.Spec.SatelliteEndpoint = props["SatelliteEndpoint"]

		return n.c.Patch(ctx, &existing, ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}))
	}), "patch Props of Node %q", name)
}

// Delete removes the named Node CRD.
func (n *nodes) Delete(ctx context.Context, name string) error {
	crd := &crdv1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: Name(name)}}

	err := n.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "node %q", name)
		}

		return errors.Wrapf(err, "delete Node %q", name)
	}

	return nil
}

// crdToWireNode flattens a Node CRD into the LINSTOR REST shape.
// Phase 10.3: re-emits typed `Spec.SatelliteEndpoint` back into the
// wire `Props["SatelliteEndpoint"]` so golinstor + the dispatcher's
// fallback path keep seeing the same shape on GET. Native
// `topology.blockstor.io/<key>` labels on the Node CRD ALSO get
// folded back into `Props["Aux/<key>"]` so the autoplacer's
// existing replicas_on_same / replicas_on_different filters keep
// working unchanged across the labels migration.
func crdToWireNode(crd *crdv1alpha1.Node) apiv1.Node {
	props := crd.Spec.Props

	if crd.Spec.SatelliteEndpoint != "" || hasTopologyLabels(crd.Labels) {
		props = maps.Clone(props)
		if props == nil {
			props = map[string]string{}
		}

		if crd.Spec.SatelliteEndpoint != "" {
			props["SatelliteEndpoint"] = crd.Spec.SatelliteEndpoint
		}

		foldTopologyLabels(props, crd.Labels)
	}

	out := apiv1.Node{
		Name:             OriginalName(&crd.ObjectMeta),
		Type:             crd.Spec.Type,
		Props:            props,
		ConnectionStatus: crd.Status.ConnectionStatus,
	}

	flags := make([]string, 0, len(crd.Spec.Flags)+len(crd.Status.Flags))
	flags = append(flags, crd.Spec.Flags...)
	flags = append(flags, crd.Status.Flags...)

	if len(flags) > 0 {
		out.Flags = flags
	}

	if len(crd.Spec.NetInterfaces) > 0 {
		out.NetInterfaces = make([]apiv1.NetInterface, 0, len(crd.Spec.NetInterfaces))
		for i := range crd.Spec.NetInterfaces {
			iface := &crd.Spec.NetInterfaces[i]
			out.NetInterfaces = append(out.NetInterfaces, apiv1.NetInterface{
				Name:                    iface.Name,
				Address:                 iface.Address,
				SatellitePort:           int(iface.SatellitePort),
				SatelliteEncryptionType: iface.SatelliteEncryptionType,
			})
		}

		// Surface upstream LINSTOR defaults on the wire when the
		// CRD didn't pin specific values. The Python CLI renders
		// `<address>:<port> (<TYPE>)` and a blank port/type leaves
		// the Addresses column empty, which the parity audit
		// (row #1) called out as the most visible wire-shape gap.
		// Phase 10.6 retired the gRPC wire (satellite ↔ controller
		// now flows through the apiserver) so 3366 is descriptive
		// metadata rather than a routable port — but the CLI uses
		// it purely for display, not dialing.
		out.NetInterfaces = apiv1.DefaultNetInterfaceFields(out.Name, out.NetInterfaces)
	}

	// Synthesise upstream-LINSTOR capability fields (UUID,
	// resource_layers, storage_providers, unsupported_*,
	// props.NodeUname, props.CurStltConnName) — these aren't
	// persisted on the CRD because they're derivable from the
	// satellite binary's own capabilities. Audit row F2 called out
	// their absence as a `linstor n l` wire-shape gap.
	apiv1.SynthesizeNodeCapabilities(&out)

	return out
}

// wireToCRDNode builds a fresh Node CRD from an apiv1.Node — used by Create.
func wireToCRDNode(in *apiv1.Node) *crdv1alpha1.Node {
	crd := &crdv1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: Name(in.Name)},
		Spec:       wireToCRDNodeSpec(in),
	}
	SetOriginalName(&crd.ObjectMeta, in.Name)

	return crd
}

// wireToCRDNodeSpec is the spec-only converter used by both Create and Update.
// SatelliteEncryptionType is uppercased on the way in: upstream LINSTOR
// accepts mixed case ("plain"/"PLAIN") but the CRD validation enum is
// uppercase-only, so we normalise here to keep wire compatibility.
//
// Phase 10.3: lifts `Props["SatelliteEndpoint"]` into the typed
// `Spec.SatelliteEndpoint` field. The dispatcher reads typed first
// (so this is the source of truth going forward); we still keep the
// original key in Props for forward-compat with any pre-migration
// reader that hasn't been updated.
func wireToCRDNodeSpec(in *apiv1.Node) crdv1alpha1.NodeSpec {
	spec := crdv1alpha1.NodeSpec{
		Type:              strings.ToUpper(in.Type),
		Props:             in.Props,
		Flags:             in.Flags,
		SatelliteEndpoint: in.Props["SatelliteEndpoint"],
	}

	if len(in.NetInterfaces) > 0 {
		spec.NetInterfaces = make([]crdv1alpha1.NodeNetInterface, 0, len(in.NetInterfaces))
		for i := range in.NetInterfaces {
			ni := &in.NetInterfaces[i]
			spec.NetInterfaces = append(spec.NetInterfaces, crdv1alpha1.NodeNetInterface{
				Name:                    ni.Name,
				Address:                 ni.Address,
				SatellitePort:           int32(ni.SatellitePort), //nolint:gosec // upstream LINSTOR ports fit in int32
				SatelliteEncryptionType: strings.ToUpper(ni.SatelliteEncryptionType),
			})
		}
	}

	return spec
}

// TopologyLabelPrefix is the native Kubernetes label namespace
// blockstor uses for topology placement keys (zone, rack, …).
// Replaces upstream LINSTOR's `Props["Aux/<key>"]` shape so that
// the autoplacer can use `client.MatchingLabels` selectors and the
// keys feed into `topologySpreadConstraints` for free. Phase 10.3.
const TopologyLabelPrefix = "topology.blockstor.io/"

// hasTopologyLabels reports whether any of the Node's metadata
// labels lives under the blockstor topology prefix. Cheap pre-
// check so we only allocate a fresh Props map when we actually
// have labels to fold in.
func hasTopologyLabels(labels map[string]string) bool {
	for k := range labels {
		if strings.HasPrefix(k, TopologyLabelPrefix) {
			return true
		}
	}

	return false
}

// foldTopologyLabels copies every `topology.blockstor.io/<key>`
// label into `props["Aux/<key>"]` so existing readers (autoplacer
// auxKey lookups, golinstor over the wire) see the topology
// information without any changes. Mutates props in place.
//
// The Aux/ Props side stays the source of truth for now — when a
// caller writes via the wire API the legacy key path is what gets
// persisted. The label-side path is purely additive (operators
// who set the native label see it surface as an Aux/ prop on
// GET); future phases will flip the source-of-truth direction
// once linstor-csi etc. learn to set labels directly.
func foldTopologyLabels(props, labels map[string]string) {
	for label, value := range labels {
		if !strings.HasPrefix(label, TopologyLabelPrefix) {
			continue
		}

		auxKey := "Aux/" + strings.TrimPrefix(label, TopologyLabelPrefix)
		// Don't clobber an explicit Props value — the operator may
		// have set both and the Props side wins (matches the
		// auxKey() lookup precedence in the placer).
		if _, exists := props[auxKey]; !exists {
			props[auxKey] = value
		}
	}
}
