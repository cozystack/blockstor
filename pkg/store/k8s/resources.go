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
	"sort"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Labels used to index Resource CRDs by their composite key.
const (
	LabelResourceDefinition = "blockstor.io/resource-definition"
)

type resources struct {
	c ctrlclient.Client
}

func resourceCRDName(rd, node string) string {
	return Name(rd + "." + node)
}

func (s *resources) List(ctx context.Context) ([]apiv1.Resource, error) {
	var crdList crdv1alpha1.ResourceList

	err := s.c.List(ctx, &crdList)
	if err != nil {
		return nil, errors.Wrap(err, "list Resource CRDs")
	}

	out := make([]apiv1.Resource, 0, len(crdList.Items))
	for i := range crdList.Items {
		out = append(out, crdToWireResource(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}

		return out[i].NodeName < out[j].NodeName
	})

	return out, nil
}

func (s *resources) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	// Fast path: label-selector via apiserver. REST-created Resources
	// always carry `LabelResourceDefinition`, so the cache filters
	// server-side. For clusters with thousands of Resources this
	// avoids a full scan on every `linstor r l -r <rd>` and every CSI
	// reconcile loop.
	var crdList crdv1alpha1.ResourceList

	err := s.c.List(ctx, &crdList,
		ctrlclient.MatchingLabels{LabelResourceDefinition: rdName})
	if err != nil {
		return nil, errors.Wrapf(err, "list Resource CRDs for RD %q", rdName)
	}

	// Correctness backstop: Resources applied via `kubectl apply`
	// (e2e fixtures, operator-authored manifests) may lack the label.
	// On an empty hit, fall back to a full scan and filter by
	// Spec.ResourceDefinitionName. If the label-selector found
	// matches, trust it — a partial-but-correct subset isn't possible
	// here since every REST writer sets the label.
	if len(crdList.Items) == 0 {
		err = s.c.List(ctx, &crdList)
		if err != nil {
			return nil, errors.Wrapf(err, "fallback list Resource CRDs for RD %q", rdName)
		}
	}

	out := make([]apiv1.Resource, 0, len(crdList.Items))

	for i := range crdList.Items {
		if crdList.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		out = append(out, crdToWireResource(&crdList.Items[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].NodeName < out[j].NodeName })

	return out, nil
}

func (s *resources) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	var crd crdv1alpha1.Resource

	err := s.c.Get(ctx, types.NamespacedName{Name: resourceCRDName(rdName, node)}, &crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return apiv1.Resource{}, errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return apiv1.Resource{}, errors.Wrapf(err, "get Resource %s/%s", rdName, node)
	}

	return crdToWireResource(&crd), nil
}

func (s *resources) Create(ctx context.Context, in *apiv1.Resource) error {
	if in == nil {
		return errors.New("nil Resource")
	}

	crd := wireToCRDResource(in)

	err := s.c.Create(ctx, crd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(store.ErrAlreadyExists, "resource %q on node %q", in.Name, in.NodeName)
		}

		return errors.Wrapf(err, "create Resource %s/%s", in.Name, in.NodeName)
	}

	return nil
}

func (s *resources) Update(ctx context.Context, in *apiv1.Resource) error {
	if in == nil {
		return errors.New("nil Resource")
	}

	var existing crdv1alpha1.Resource

	key := types.NamespacedName{Name: resourceCRDName(in.Name, in.NodeName)}

	err := s.c.Get(ctx, key, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", in.Name, in.NodeName)
		}

		return errors.Wrapf(err, "get Resource %s/%s", in.Name, in.NodeName)
	}

	existing.Spec = wireToCRDResourceSpec(in)

	// Bug 67: round-trip wire-side Annotations through metadata so the
	// REST writer's `blockstor.io/peer-changed` bump actually reaches
	// the CRD ObjectMeta — without this the bump would silently no-op
	// and the satellite's local-Resource watch would never fire.
	// nil wire means "no annotation work requested" — preserve existing
	// (mirrors RD store's `mergeUserAnnotationsInto` contract).
	if in.Annotations != nil {
		existing.Annotations = cloneAnnotations(in.Annotations)
	}

	err = s.c.Update(ctx, &existing)
	if err != nil {
		return errors.Wrapf(err, "update Resource %s/%s", in.Name, in.NodeName)
	}

	return nil
}

// PatchResourceSpec runs `mutate` against the freshly-fetched current
// wire-shape Resource and persists the mutated value via a typed-Patch
// (JSON-merge-patch) under `RetryOnConflict` with the Bug 201 backoff.
// On 409 the cycle re-runs against fresh state — so concurrent disjoint
// edits (r modify, toggle-disk, r activate, autoplace) converge instead
// of silently dropping the loser via the wholesale `Update`'s un-retried
// wire-snapshot replace (Bug 204b).
//
// Annotations follow `Update`'s contract: nil wire means "no annotation
// work requested" (preserve existing); non-nil replaces the annotation
// set atomically. The closure operates on wire-shape Annotations.
func (s *resources) PatchResourceSpec(ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return errors.Wrapf(retry.RetryOnConflict(patchRetryBackoff(), func() error {
		var existing crdv1alpha1.Resource

		key := types.NamespacedName{Name: resourceCRDName(rdName, node)}

		err := s.c.Get(ctx, key, &existing)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
			}

			return errors.Wrapf(err, "get Resource %s/%s", rdName, node)
		}

		base := existing.DeepCopy()

		wire := crdToWireResource(&existing)

		err = mutate(&wire)
		if err != nil {
			return err
		}

		existing.Spec = wireToCRDResourceSpec(&wire)

		if wire.Annotations != nil {
			existing.Annotations = cloneAnnotations(wire.Annotations)
		}

		return s.c.Patch(ctx, &existing, ctrlclient.MergeFromWithOptions(base, ctrlclient.MergeFromWithOptimisticLock{}))
	}), "patch Resource %s/%s", rdName, node)
}

func (s *resources) Delete(ctx context.Context, rdName, node string) error {
	crd := &crdv1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: resourceCRDName(rdName, node)}}

	err := s.c.Delete(ctx, crd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return errors.Wrapf(err, "delete Resource %s/%s", rdName, node)
	}

	return nil
}

// SatelliteFieldOwner is the SSA field-manager identity the
// satellite uses for its observed-state writes (DrbdState,
// per-volume DiskState/CurrentGi). The controller uses
// `ControllerFieldOwner` for its allocator outputs (DRBDPort,
// DRBDMinor, DRBDNodeID). Distinct field owners let SSA's merge
// algorithm preserve each side's writes when the two collide on
// the same Status subresource. Phase 10.2.
const (
	SatelliteFieldOwner  = "blockstor-satellite"
	ControllerFieldOwner = "blockstor-controller"
)

// SetState writes the runtime-observed state via Server-Side Apply
// on the Status subresource. Called from the satellite's events2
// observer path when role/disk state changes — these must not race
// concurrent Spec mutations (auto-diskful, resize, evacuation) nor
// the controller's allocator writes to DRBDPort/Minor/NodeID.
//
// SSA with `FieldOwner=blockstor-satellite` makes the merge
// per-field: only the fields the satellite explicitly sets in the
// apply object are claimed; the controller's allocator outputs
// (`DRBDPort`, `DRBDMinor`, `DRBDNodeID`) stay untouched even
// across racing Status writes. Phase 10.2.
//
// state.DrbdState lands on Status.DrbdState; per-volume
// DiskState/CurrentGi land on Status.Volumes[i] (the listMapKey is
// `volumeNumber`, so SSA matches up entries correctly).
func (s *resources) SetState(ctx context.Context, rdName, node string, state apiv1.ResourceState, volumes []apiv1.VolumeObservation) error {
	name := resourceCRDName(rdName, node)

	// Verify the Resource exists before applying — apiserver Apply
	// would happily create a half-formed Resource if it didn't,
	// and we'd rather surface NotFound to callers (events2
	// observer treats it as a convergence-pending case and skips).
	var existing crdv1alpha1.Resource

	err := s.c.Get(ctx, types.NamespacedName{Name: name}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return errors.Wrapf(err, "get Resource %s/%s", rdName, node)
	}

	apply := &crdv1alpha1.Resource{
		TypeMeta:   metav1.TypeMeta{Kind: "Resource", APIVersion: crdv1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: crdv1alpha1.ResourceStatus{
			InUse:     ptrBoolOrFalse(state.InUse),
			DrbdState: state.DrbdState,
			Volumes:   buildVolumeStatusForApply(volumes),
		},
	}

	// Note: client.Apply is the Patch-type-based SSA path. The
	// newer Client.Apply / SubResource.Apply methods need
	// applyconfiguration-gen output for our CRDs, which we don't
	// produce yet — sticking to the patch-type API keeps the
	// dependency tree shallow.
	err = s.c.Status().Patch(ctx, apply,
		ctrlclient.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		ctrlclient.FieldOwner(SatelliteFieldOwner),
		ctrlclient.ForceOwnership)
	if err != nil {
		return errors.Wrapf(err, "ssa apply Resource Status %s/%s", rdName, node)
	}

	return nil
}

// ClearDRBDPort drops Status.DRBDPort on the named Resource so the
// controller's allocator (resource_controller.allocateDRBDFields)
// re-picks a fresh free port on its next reconcile. The activate
// REST handler invokes this when the operator passes
// `?reallocate-port=true` to recover from a port collision via the
// deactivate + activate recipe (Bug 46).
//
// Implementation: JSON merge patch with `{"status":{"drbdPort":null}}`
// on the Status subresource. SSA can't naturally express "clear
// this single field" without re-Applying as the original field
// owner (`ControllerFieldOwner`) which would also touch DRBDNodeID
// and DRBDMinor — both of which we want to preserve. A merge patch
// is field-surgical: only `drbdPort` is removed; DRBDMinor /
// DRBDNodeID / observer-claimed Volumes survive.
func (s *resources) ClearDRBDPort(ctx context.Context, rdName, node string) error {
	name := resourceCRDName(rdName, node)

	// Verify the Resource exists first. Without this, an apiserver
	// merge-patch on a missing object returns a 404 we'd have to
	// translate downstream; surfacing ErrNotFound up-front keeps the
	// store contract uniform with the rest of the methods.
	var existing crdv1alpha1.Resource

	err := s.c.Get(ctx, types.NamespacedName{Name: name}, &existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return errors.Wrapf(err, "get Resource %s/%s", rdName, node)
	}

	// Fast-path: nothing to clear. Avoids a no-op apiserver round
	// trip for the common case where the operator hits activate
	// without `?reallocate-port=true` on a freshly-Created Resource
	// the controller hasn't allocated yet.
	if existing.Status.DRBDPort == nil {
		return nil
	}

	patch := ctrlclient.RawPatch(types.MergePatchType,
		[]byte(`{"status":{"drbdPort":null}}`))

	err = s.c.Status().Patch(ctx, &existing, patch)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
		}

		return errors.Wrapf(err, "clear Status.DRBDPort on Resource %s/%s", rdName, node)
	}

	return nil
}

// buildVolumeStatusForApply turns a slice of per-volume
// observations into the SSA-shaped Status.Volumes payload. Only
// non-empty fields land in the apply object so SSA doesn't claim
// ownership of fields the satellite didn't explicitly set.
//
// The Status.Volumes slice is a `+listType=map +listMapKey=volumeNumber`
// list, so the apiserver merges the apply against the existing
// state by volume number — a frame that only carries DiskState
// for vol 0 leaves vol 1's CurrentGi alone.
func buildVolumeStatusForApply(observations []apiv1.VolumeObservation) []crdv1alpha1.ResourceVolumeStatus {
	if len(observations) == 0 {
		return nil
	}

	out := make([]crdv1alpha1.ResourceVolumeStatus, 0, len(observations))

	for _, vol := range observations {
		out = append(out, crdv1alpha1.ResourceVolumeStatus{
			VolumeNumber: vol.VolumeNumber,
			DiskState:    vol.State.DiskState,
			CurrentGi:    vol.State.CurrentGi,
		})
	}

	return out
}

func crdToWireResource(crd *crdv1alpha1.Resource) apiv1.Resource {
	props := mergeProps(crd.Spec.Props, typedToProps(crd.Spec.DRBDOptions, crd.Spec.ExtraProps))

	// Phase 10.3 step: re-emit typed StoragePool back into Props
	// so golinstor + the dispatcher's legacy fallback see the
	// upstream-compatible shape on GET.
	if crd.Spec.StoragePool != "" {
		if props == nil {
			props = map[string]string{}
		}

		props["StorPoolName"] = crd.Spec.StoragePool
	}

	return apiv1.Resource{
		Name:     crd.Spec.ResourceDefinitionName,
		NodeName: crd.Spec.NodeName,
		Props:    props,
		Flags:    crd.Spec.Flags,
		// Resource CRD has no LayerStack — that lives on the parent RD.
		// Looking it up here turns every list/view into an N+1 query, so
		// we approximate with the default stack. The Python CLI only
		// uses this to render the Layers column on `linstor r list`;
		// blockstor's actual layer placement is driven by the RD spec
		// at apply time, not by this read-only echo.
		LayerObject: layerObjectFromCRD(crd),
		State: apiv1.ResourceState{
			InUse:             boolPtr(crd.Status.InUse),
			DrbdState:         crd.Status.DrbdState,
			ToggleDiskRetries: crd.Status.ToggleDiskRetries,
		},
		ToggleDiskCancel: crd.Spec.ToggleDiskCancel,
		Volumes: volumesWithReplicationStates(
			volumesFromStatus(crd.Status.Volumes),
			crd.Status.Connections,
		),
		UUID:        string(crd.UID),
		Annotations: cloneAnnotations(crd.Annotations),
	}
}

// volumesFromStatus projects the CRD `Status.Volumes` onto wire
// `[]Volume`. State carries DiskState + CurrentGi (Generation
// Identifier) — the latter is what the controller seeds new
// replicas with for skipping the full initial-sync. The Python CLI
// derives the per-resource rsc_state from `volumes[].state.disk_state`;
// without this projection, the rsc_state stays "Unknown" and the
// CLI suppresses the Conns column + --faulty filter.
// boolPtr returns a *bool pointing at the value. Used to satisfy the
// apiv1 tri-state contract for ResourceState.InUse: nil means "not
// observed yet"; *false means "observed Secondary"; *true means
// "observed Primary". Without the pointer, false serialises as
// absent under omitempty.
func boolPtr(v bool) *bool {
	return &v
}

// ptrBoolOrFalse is the inverse of boolPtr for the SSA-Apply path:
// the CRD's Status.InUse field is a plain bool, so nil from the
// apiv1 shape must collapse to false before write.
func ptrBoolOrFalse(p *bool) bool {
	if p == nil {
		return false
	}

	return *p
}

func volumesFromStatus(in []crdv1alpha1.ResourceVolumeStatus) []apiv1.Volume {
	if len(in) == 0 {
		return nil
	}

	// Per-resource layer stack isn't stored on the Resource CRD (it
	// lives on the parent RD). For the rsc_state gating the Python
	// CLI does, the only thing that matters is that the FIRST entry
	// is DRBD; the default stack DRBD/STORAGE covers every diskful
	// layout we currently materialise.
	layerDataList := volumeLayerDataFromStack(apiv1.DefaultLayerStack())

	out := make([]apiv1.Volume, 0, len(in))

	for i := range in {
		volStatus := &in[i]
		out = append(out, apiv1.Volume{
			VolumeNumber: volStatus.VolumeNumber,
			StoragePool:  volStatus.StoragePool,
			DevicePath:   volStatus.DevicePath,
			AllocatedKib: volStatus.AllocatedKib,
			UsableKib:    volStatus.UsableKib,
			State: apiv1.VolumeState{
				DiskState:    volStatus.DiskState,
				CurrentGi:    volStatus.CurrentGi,
				OutOfSyncKib: volStatus.OutOfSyncKib,
			},
			LayerDataList: layerDataList,
		})
	}

	return out
}

// volumesWithReplicationStates folds the resource-level per-peer
// replication state into each volume's state.replication_states
// map. The Python CLI reads exactly this shape for the Repl column
// — it cannot infer per-peer replication from
// `layer_object.drbd.connections[].replication_state`, even though
// blockstor stores the data there too.
func volumesWithReplicationStates(
	vols []apiv1.Volume,
	conns []crdv1alpha1.ResourceConnectionStatus,
) []apiv1.Volume {
	if len(vols) == 0 || len(conns) == 0 {
		return vols
	}

	states := make(map[string]apiv1.ReplicationState, len(conns))

	for i := range conns {
		c := &conns[i]
		if c.ReplicationState == "" {
			continue
		}

		states[c.PeerNodeName] = apiv1.ReplicationState{
			ReplicationState: c.ReplicationState,
		}
	}

	if len(states) == 0 {
		return vols
	}

	for i := range vols {
		vols[i].State.ReplicationStates = states
	}

	return vols
}

// volumeLayerDataFromStack mirrors the resource-level layer stack
// onto each volume's `layer_data_list`. The Python CLI's
// `volume_expects_disk_state` reads `layer_data_list[0].type == DRBD`
// to decide whether the State column should trust the observed
// `disk_state` — without this, the column always shows "Created".
func volumeLayerDataFromStack(stack []string) []apiv1.VolumeLayerData {
	if len(stack) == 0 {
		stack = apiv1.DefaultLayerStack()
	}

	out := make([]apiv1.VolumeLayerData, 0, len(stack))
	for _, kind := range stack {
		out = append(out, apiv1.VolumeLayerData{Type: kind})
	}

	return out
}

// layerObjectFromCRD wraps `layerObjectFromStack` with the CRD-side
// glue: it injects the per-replica DRBD runtime state (TCP port,
// per-peer connection map) into the top-of-stack `Drbd` field. The
// Python CLI's `--faulty` filter reads `connections[*].connected`
// to color broken peers red and to gate inclusion in the faulty
// subset; without this `r list --faulty` cannot see disconnected
// peers and silently passes them as healthy.
func layerObjectFromCRD(crd *crdv1alpha1.Resource) *apiv1.ResourceLayer {
	top := layerObjectFromStack(nil, crd.Spec.Flags)
	if top == nil {
		return nil
	}

	// Inject DRBD runtime only on the DRBD layer itself (top of the
	// default stack). If a future RG advertises a non-DRBD stack
	// (`[STORAGE]` only) this is a no-op — `Drbd` stays nil and the
	// CLI renders an empty Conns column for that resource.
	if top.Type == apiv1.LayerKindDRBD {
		top.Drbd = drbdLayerFromStatus(&crd.Status)
	}

	// Hydrate the STORAGE leaf for diskful replicas — F19 keeps the
	// leaf always emitted, but only diskful replicas have actual
	// storage_volumes derived from Status.Volumes. Diskless / tiebreaker
	// replicas keep the empty payload layerObjectFromStack stamped.
	hydrateStorageLeaf(top, crd)

	return top
}

// hydrateStorageLeaf walks the children chain looking for the STORAGE
// leaf and (for diskful replicas) attaches per-volume payload derived
// from the satellite-observed `Resource.Status.Volumes`. For diskless
// replicas the STORAGE leaf is left with the `provider_kind=DISKLESS`
// marker that layerObjectFromStack already stamped — no volumes are
// emitted because there's no backing device on this node. F19.
func hydrateStorageLeaf(top *apiv1.ResourceLayer, crd *crdv1alpha1.Resource) {
	if top == nil {
		return
	}

	diskless := false

	for _, f := range crd.Spec.Flags {
		if f == apiv1.ResourceFlagDiskless || f == apiv1.ResourceFlagTieBreaker {
			diskless = true

			break
		}
	}

	storage := findStorageLeaf(top)
	if storage == nil {
		return
	}

	if diskless {
		// layerObjectFromStack has already set provider_kind=DISKLESS
		// and left storage_volumes empty — nothing more to hydrate.
		return
	}

	if len(crd.Status.Volumes) == 0 {
		return
	}

	vols := make([]apiv1.StorageVolumeLayer, 0, len(crd.Status.Volumes))

	for i := range crd.Status.Volumes {
		vol := &crd.Status.Volumes[i]
		vols = append(vols, apiv1.StorageVolumeLayer{
			VolumeNumber:     vol.VolumeNumber,
			DevicePath:       vol.DevicePath,
			AllocatedSizeKib: vol.AllocatedKib,
			UsableSizeKib:    vol.UsableKib,
			DiskState:        vol.DiskState,
		})
	}

	if storage.Storage == nil {
		storage.Storage = &apiv1.StorageResourceLayer{}
	}

	storage.Storage.StorageVolumes = vols
}

// findStorageLeaf walks the layer_object children chain (always
// single-branch in blockstor's layouts) and returns the STORAGE entry,
// or nil if the stack carries no STORAGE layer (e.g. a future
// non-DRBD-only deployment).
func findStorageLeaf(layer *apiv1.ResourceLayer) *apiv1.ResourceLayer {
	for cursor := layer; cursor != nil; {
		if cursor.Type == apiv1.LayerKindStorage {
			return cursor
		}

		if len(cursor.Children) == 0 {
			return nil
		}

		cursor = &cursor.Children[0]
	}

	return nil
}

// drbdLayerFromStatus builds the wire-side `DrbdResourceLayer` from
// the satellite-observed CRD Status. Returns nil when no observable
// runtime state exists yet (Resource just created, satellite hasn't
// reconciled it).
func drbdLayerFromStatus(st *crdv1alpha1.ResourceStatus) *apiv1.DrbdResourceLayer {
	var out apiv1.DrbdResourceLayer

	hasAny := false

	if st.DRBDPort != nil {
		out.TCPPorts = []int32{*st.DRBDPort}
		hasAny = true
	}

	if len(st.Connections) > 0 {
		out.Connections = make(map[string]apiv1.DrbdConnection, len(st.Connections))
		for i := range st.Connections {
			c := &st.Connections[i]
			out.Connections[c.PeerNodeName] = apiv1.DrbdConnection{
				Connected:        c.Connected,
				Message:          c.Message,
				ReplicationState: c.ReplicationState,
			}
		}

		hasAny = true
	}

	// Per-volume DRBD disk-state — Python CLI's `linstor r l` State
	// column reads this exact path; without it the column shows a
	// literal "Created" regardless of the observed disk_state in
	// Status.Volumes[i].DiskState.
	if len(st.Volumes) > 0 {
		out.DrbdVolumes = make([]apiv1.DrbdVolume, 0, len(st.Volumes))
		for i := range st.Volumes {
			vol := &st.Volumes[i]
			if vol.DiskState == "" && vol.DevicePath == "" {
				continue
			}

			out.DrbdVolumes = append(out.DrbdVolumes, apiv1.DrbdVolume{
				VolumeNumber: vol.VolumeNumber,
				DiskState:    vol.DiskState,
				DevicePath:   vol.DevicePath,
			})

			hasAny = true
		}
	}

	if !hasAny {
		return nil
	}

	return &out
}

// layerObjectFromStack assembles the upstream-LINSTOR `layer_object`
// tree from a flat layer-stack slice. Returns nil when the stack is
// empty — the wire shape uses `omitempty`, but the Python CLI's
// `rsc.layer_data.layer_stack` dereferences the result
// unconditionally, so callers that need CLI compatibility should
// supply a fallback (default DRBD/STORAGE) before invoking this.
//
// DISKLESS / TIE_BREAKER resources keep the STORAGE child but the
// payload differs from a diskful replica: the layer carries
// `provider_kind=DISKLESS` and an empty `storage_volumes` list,
// matching upstream LINSTOR's wire shape (`linstor r l` Layers column
// renders `DRBD,STORAGE` for tiebreakers, not `DRBD` alone). F19.
//
// Diskful replicas synthesize one STORAGE volume per status-observed
// volume in the layer payload via layerObjectFromCRD — the bare-stack
// helper here only marks the layer kind; per-volume hydration happens
// in the CRD-aware wrapper.
func layerObjectFromStack(stack, flags []string) *apiv1.ResourceLayer {
	if len(stack) == 0 {
		stack = apiv1.DefaultLayerStack()
	}

	if len(stack) == 0 {
		return nil
	}

	diskless := false

	for _, f := range flags {
		if f == apiv1.ResourceFlagDiskless || f == apiv1.ResourceFlagTieBreaker {
			diskless = true

			break
		}
	}

	top := &apiv1.ResourceLayer{Type: stack[0]}

	cursor := top

	for _, t := range stack[1:] {
		child := apiv1.ResourceLayer{Type: t}

		if t == apiv1.LayerKindStorage && diskless {
			// F19: tiebreakers / operator-placed diskless replicas
			// still expose a STORAGE leaf so the CLI's Layers column
			// renders `DRBD,STORAGE` — the satellite simply has no
			// backing device on this node, which the payload encodes
			// via `provider_kind=DISKLESS` and an empty volume list.
			child.Storage = &apiv1.StorageResourceLayer{
				ProviderKind: apiv1.StoragePoolKindDiskless,
			}
		}

		cursor.Children = []apiv1.ResourceLayer{child}
		cursor = &cursor.Children[0]
	}

	return top
}

func wireToCRDResource(in *apiv1.Resource) *crdv1alpha1.Resource {
	return &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceCRDName(in.Name, in.NodeName),
			Labels: map[string]string{
				LabelResourceDefinition: in.Name,
				LabelNodeName:           in.NodeName,
			},
			Annotations: cloneAnnotations(in.Annotations),
		},
		Spec: wireToCRDResourceSpec(in),
	}
}

func wireToCRDResourceSpec(in *apiv1.Resource) crdv1alpha1.ResourceSpec {
	typed, extras := propsToTyped(in.Props)
	residual := stripDRBDProps(in.Props)

	return crdv1alpha1.ResourceSpec{
		ResourceDefinitionName: in.Name,
		NodeName:               in.NodeName,
		Props:                  residual,
		DRBDOptions:            typed,
		ExtraProps:             extras,
		Flags:                  in.Flags,
		// Phase 10.3 step: lift Props["StorPoolName"] into the
		// typed slot. The legacy key stays in residual Props (it's
		// non-DRBD, so stripDRBDProps left it alone) for forward-
		// compat — readers still consulting Props will see it.
		StoragePool: in.Props["StorPoolName"],
		// Bug 40: the REST cancel verb writes this through the
		// wire shape on PUT toggle-disk?cancel=true. The satellite
		// reconciler watches it and unwinds a partial conversion.
		ToggleDiskCancel: in.ToggleDiskCancel,
	}
}
