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

package satellite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// ReconcilerConfig parametrises a Reconciler.
//
// Providers maps the satellite's local storage-pool names to provisioned
// `storage.Provider` instances; an unknown pool fails the per-resource
// Apply with a surfaced error message.
//
// Adm, StateDir and NodeName drive the DRBD half: when set, Apply also
// renders the `.res` file under StateDir, runs `drbdadm create-md` on
// first activation, and `drbdadm adjust` on every reconcile.
type ReconcilerConfig struct {
	Providers map[string]storage.Provider

	// Adm is the drbdadm wrapper. nil → DRBD half is disabled (storage
	// only). Useful for unit tests of the storage path without DRBD.
	Adm *drbd.Adm

	// StateDir is where `.res` files land. Required when Adm is set.
	StateDir string

	// NodeName is this satellite's identifier; the reconciler uses it
	// to know which Peer entries describe local vs. remote.
	NodeName string

	// LocalAddress is the IP this satellite's DRBD layer should bind
	// to. Falls back into the .res file's `address` line on the local
	// `on <node>` block whenever the controller-supplied address is
	// the placeholder "0.0.0.0" (which it always is until controller
	// learns each satellite's pod IP).
	LocalAddress string

	// ShipExec runs the snapshot-ship subprocess (zfs send|recv,
	// thin-send-recv, …). Production wires storage.RealExec; tests
	// inject FakeExec to assert the command line without spinning up
	// a real ssh / zfs / thin tool.
	ShipExec storage.Exec

	// Cryptsetup is the LUKS-layer wrapper. nil → LUKS in the layer
	// stack is rejected (the satellite can't fulfil it). Production
	// wires luks.NewCryptsetup(storage.RealExec{}); tests inject
	// FakeExec.
	Cryptsetup *luks.Cryptsetup

	// CrossNodeFetcher pulls a snapshot from a peer satellite when
	// the local node doesn't host it. nil → no cross-node fallback,
	// materializeVolume falls through to a blank CreateVolume (the
	// pre-Phase-11 behaviour). The agent injects this post-manager
	// construction via SetCrossNodeFetcher because the implementation
	// needs the controller-runtime client only the manager owns.
	CrossNodeFetcher CrossNodeFetcher

	// MetadataCreatedStamper writes the `MetadataCreated=True`
	// Status Condition onto the parent Resource CRD after
	// `drbdmeta create-md` succeeds. nil → the satellite falls
	// back to the legacy on-disk `<rd>.md-created` marker for
	// firstActivation derivation (compatible with unit tests that
	// don't wire an apiserver). The agent injects this
	// post-manager construction via SetMetadataCreatedStamper —
	// the implementation needs the controller-runtime client only
	// the manager owns. Phase 11.3 Stage 1.
	MetadataCreatedStamper MetadataCreatedStamper

	// FilesystemFormattedStamper writes the
	// `FilesystemFormatted=True` Status Condition onto the parent
	// Resource CRD after `runAutoMkfs` reports every diskful
	// volume as carrying a filesystem (freshly mkfs'd or adopted
	// via blkid). nil → the satellite falls back to the legacy
	// on-disk `<rd>.mkfs.done` marker for both the
	// `needsAutoMkfsRetry` predicate and the `runAutoMkfs`
	// fast-path (compatible with unit tests that don't wire an
	// apiserver). The agent injects this post-manager construction
	// via SetFilesystemFormattedStamper — the implementation needs
	// the controller-runtime client only the manager owns. Phase
	// 11.3 Stage 2.
	FilesystemFormattedStamper FilesystemFormattedStamper

	// SkipDiskClearer releases the satellite's SSA claim on the
	// `DrbdOptions/SkipDisk` Spec.Props key when the kernel
	// re-emerges healthy (Bug 278: Talos kernel upgrade reattach).
	// nil → auto-clear path is disabled (compatible with unit tests
	// that don't wire an apiserver). The agent injects this
	// post-manager construction via SetSkipDiskClearer — the
	// implementation needs the controller-runtime client only the
	// manager owns.
	SkipDiskClearer SkipDiskClearer

	// Exec runs auxiliary shell-outs the reconciler owns directly
	// (currently: `mkfs.<type>` for the RG-driven auto-mkfs path,
	// scenario 9.W14). Production wires `storage.RealExec`; tests
	// inject `storage.FakeExec` and assert the exact command line.
	// nil disables auto-mkfs entirely — the seed path still promotes
	// and demotes via Adm, but a configured `FileSystem/Type` prop
	// becomes a no-op rather than panicking.
	Exec storage.Exec
}

// CrossNodeFetcher abstracts the "fetch a snapshot from a peer that
// hosts it locally" half of the cross-node clone path. Lives behind
// an interface so satellite.Reconciler stays free of a direct
// controller-runtime client dependency — the K8s lookup + peer-IP
// resolution + stream HTTP GET sits in pkg/satellite/controllers
// where the cached client already lives.
type CrossNodeFetcher interface {
	// Fetch opens a byte stream of (srcRD, snap, vol) from a peer
	// satellite. Returns the stream + the peer node name it came
	// from (for logging). On storage.ErrNotFound, NO peer hosts the
	// snapshot locally — the caller must decide whether to fall
	// through to a blank create or surface the error.
	Fetch(ctx context.Context, srcRD, snap string, vol int32) (io.ReadCloser, string, error)
}

// MetadataCreatedStamper abstracts the "stamp the
// `MetadataCreated=True` Status Condition on a Resource CRD" verb.
// Mirrors `CrossNodeFetcher`: the K8s SSA call lives in
// pkg/satellite/controllers (where the cached client owns the
// apiserver wire) while the satellite's apply chain stays free of
// a controller-runtime client dependency. Phase 11.3 Stage 1.
type MetadataCreatedStamper interface {
	// StampMetadataCreated SSA-patches a `MetadataCreated=True`
	// Condition onto Resource <resourceName>.Status.Conditions.
	// Idempotent — repeat calls converge on the same Condition
	// shape (LastTransitionTime moves forward on apiserver-side
	// transition only, not on every patch).
	StampMetadataCreated(ctx context.Context, resourceName string) error
}

// FilesystemFormattedStamper abstracts the "stamp the
// `FilesystemFormatted=True` Status Condition on a Resource CRD"
// verb. Mirrors `MetadataCreatedStamper`: the K8s SSA call lives
// in pkg/satellite/controllers (where the cached client owns the
// apiserver wire) while the satellite's apply chain stays free of
// a controller-runtime client dependency. Phase 11.3 Stage 2.
type FilesystemFormattedStamper interface {
	// StampFilesystemFormatted SSA-patches a
	// `FilesystemFormatted=True` Condition onto Resource
	// <resourceName>.Status.Conditions. Idempotent — repeat calls
	// converge on the same Condition shape (LastTransitionTime
	// moves forward on apiserver-side transition only, not on
	// every patch).
	StampFilesystemFormatted(ctx context.Context, resourceName string) error
}

// SkipDiskClearer abstracts the "release the satellite's SSA claim
// on Spec.Props[DrbdOptions/SkipDisk]" verb used by the Bug 278
// auto-clear path. The clearer applies a Spec.Props document under
// the same FieldOwner the observer used to stamp SkipDisk
// defensively, but without the SkipDisk key — SSA's per-key map
// merge releases that owner's claim and, when nobody else owns the
// key, the apiserver deletes it from Spec.Props. The next
// dispatcher cycle re-resolves the Spec without SkipDisk, the
// reconciler's `isSkipDiskEnabled` gate flips false, and the next
// `drbdadm adjust` re-attaches the kernel-healthy lower disk.
//
// Lives behind an interface so satellite.Reconciler stays free of
// a controller-runtime client dependency — the K8s SSA call lives
// in pkg/satellite/controllers (where the cached client owns the
// apiserver wire). Mirrors MetadataCreatedStamper /
// FilesystemFormattedStamper. Bug 278.
type SkipDiskClearer interface {
	// ClearSkipDisk SSA-applies Resource <resourceName>.Spec.Props
	// without the SkipDisk key, under the observer's own
	// FieldOwner. Idempotent — repeat calls converge on the
	// "owner releases the key" state (the apiserver no-ops the
	// second apply because the claim is already gone). NotFound
	// on the Resource CRD is silently swallowed by the
	// implementation: the convergence-pending case is the same as
	// the observer's stamp path and surfacing it here would force
	// every caller to re-implement the same silence.
	ClearSkipDisk(ctx context.Context, resourceName string) error
}

// Reconciler turns a controller-pushed DesiredResource set into local
// state. Phase-3 cut: storage provisioning + DRBD .res / drbdadm.
//
// The Reconciler also keeps an in-memory map of which storage pool each
// resource lives in (last-seen from Apply). Snapshot RPCs use it to
// dispatch to the correct provider without the controller having to
// pass the pool on every call.
type Reconciler struct {
	cfg ReconcilerConfig

	mu             sync.Mutex
	resourceToPool map[string]string
}

// NewReconciler constructs a Reconciler from cfg.
//
//nolint:gocritic // value receiver matches the public constructor convention; ReconcilerConfig is the agent's flag bundle.
func NewReconciler(cfg ReconcilerConfig) *Reconciler {
	if cfg.Providers == nil {
		// ApplyStoragePools registers providers into this map at
		// runtime; nil-init would panic on the first dynamic pool.
		cfg.Providers = map[string]storage.Provider{}
	}

	return &Reconciler{
		cfg:            cfg,
		resourceToPool: map[string]string{},
	}
}

// RegisterProvider adds (or replaces) a `storage.Provider` in the
// reconciler's pool registry under the given pool name. Phase 10.5:
// gives `ApplyStoragePools` a way to wire dynamic pools without
// restarting the satellite. Idempotent — re-registering the same
// pool overwrites the old Provider, which is what
// piraeus-operator-style "edit pool config" workflows expect.
//
// `nil` provider deregisters the pool (used for `DISKLESS` apply
// frames the controller pushes for completeness).
func (r *Reconciler) RegisterProvider(pool string, provider storage.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cfg.Providers == nil {
		r.cfg.Providers = map[string]storage.Provider{}
	}

	if provider == nil {
		delete(r.cfg.Providers, pool)

		return
	}

	r.cfg.Providers[pool] = provider
}

// SnapshotProviders returns a snapshot of the pool→provider map the
// reconciler currently holds. Used by the orphan-storage sweeper (Bug
// 43) which walks every registered provider for VolumeLister-capable
// backends. The map is copied under the same lock RegisterProvider
// takes so a concurrent registration can't tear the snapshot.
//
// Callers must treat the returned map as read-only — modifying it
// races every subsequent RegisterProvider call.
func (r *Reconciler) SnapshotProviders() map[string]storage.Provider {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[string]storage.Provider, len(r.cfg.Providers))
	for k, v := range r.cfg.Providers {
		out[k] = v
	}

	return out
}

// SetCrossNodeFetcher injects the cross-node fetcher post-construction.
// Called by the agent after the controller-runtime manager is built —
// the fetcher needs the manager's cached client to look up Snapshot +
// Node CRDs which doesn't exist at NewReconciler time. Safe to call
// before the first Apply: applyOne reads cfg.CrossNodeFetcher inside
// a single struct-field load, no extra synchronisation needed under
// "set once, then read many" semantics.
func (r *Reconciler) SetCrossNodeFetcher(f CrossNodeFetcher) {
	r.cfg.CrossNodeFetcher = f
}

// SetMetadataCreatedStamper injects the MetadataCreated stamper
// post-construction. Mirrors `SetCrossNodeFetcher`: the stamper
// needs the controller-runtime manager's cached client which doesn't
// exist at NewReconciler time. Safe to call before the first Apply.
// Phase 11.3 Stage 1.
func (r *Reconciler) SetMetadataCreatedStamper(s MetadataCreatedStamper) {
	r.cfg.MetadataCreatedStamper = s
}

// SetFilesystemFormattedStamper injects the FilesystemFormatted
// stamper post-construction. Mirrors `SetMetadataCreatedStamper`:
// the stamper needs the controller-runtime manager's cached client
// which doesn't exist at NewReconciler time. Safe to call before
// the first Apply. Phase 11.3 Stage 2.
func (r *Reconciler) SetFilesystemFormattedStamper(s FilesystemFormattedStamper) {
	r.cfg.FilesystemFormattedStamper = s
}

// SetSkipDiskClearer injects the SkipDisk clearer post-construction.
// Mirrors `SetMetadataCreatedStamper`: the clearer needs the
// controller-runtime manager's cached client which doesn't exist at
// NewReconciler time. Safe to call before the first Apply. Bug 278.
func (r *Reconciler) SetSkipDiskClearer(c SkipDiskClearer) {
	r.cfg.SkipDiskClearer = c
}

// StateDir returns the on-disk directory the reconciler uses for
// per-resource `.res` files and state markers. The OrphanSweeperRunnable
// consults it (Bug 299) to distinguish kernel-resident DRBD slots
// blockstor itself provisioned (`<StateDir>/<rsc>.res` present —
// `handleDelete` removes it only after a clean tear-down) from foreign
// slots written by a co-resident DRBD manager (e.g. an upstream
// piraeus / linstor-satellite running side-by-side on the same node).
// Without this distinction the sweeper used to issue `drbdsetup down`
// on every kernel slot that lacked a matching blockstor Resource CRD,
// which on a piraeus-coexistence stand reliably tore down LINSTOR's
// own resources between create and first-attach and surfaced as
// "Failed to adjust DRBD resource …" / "Cannot resize volume, because
// we have a non-UpToDate DRBD device" upstream.
//
// Empty string means "StateDir-based filtering disabled" — the sweeper
// then sweeps purely on CRD presence, the legacy behaviour. Tests use
// the empty default to keep assertions simple; production always
// passes the real on-disk path from cmd/satellite.
func (r *Reconciler) StateDir() string {
	return r.cfg.StateDir
}

// Apply walks res and brings local storage in line with each item.
// Each input gets a ResourceApplyResult — partial success is the norm
// (one missing pool shouldn't sink the rest of a batch).
//
// The signature returns an error too, but reserves it for context
// cancellation — per-resource failures land in the Result entries.
func (r *Reconciler) Apply(ctx context.Context, res []*intent.DesiredResource) ([]*intent.ResourceApplyResult, error) {
	results := make([]*intent.ResourceApplyResult, 0, len(res))

	for _, dr := range res {
		err := ctx.Err()
		if err != nil {
			return results, errors.Wrap(err, "apply: context cancelled")
		}

		results = append(results, r.applyOne(ctx, dr))
	}

	return results, nil
}

// CreateSnapshot dispatches to the storage provider that backs the
// resource (looked up via the resource→pool map populated by Apply).
// Returns ok=false in the response when the resource is unknown — the
// satellite never auto-creates snapshots of state it doesn't own.
//
// Terminal classification policy:
//   - providerForResource fails ⇒ Terminal=true. "Unknown resource"
//     means the parent volume never got materialised on this node;
//     a future Apply pass MIGHT change that, but the SnapshotReconciler
//     can still treat the snapshot as failed and rely on the operator
//     to delete + recreate once the parent lands. (Retrying forever on
//     an indefinitely-missing parent leaks Reconcile pressure.)
//   - provider.CreateSnapshot returns ErrTerminal (or wraps ErrNotFound)
//     ⇒ Terminal=true. Same logic.
//   - any other error ⇒ Terminal=false. Transient lvm/zfs noise.
func (r *Reconciler) CreateSnapshot(ctx context.Context, req *intent.CreateSnapshotRequest) (*intent.CreateSnapshotResponse, error) {
	provider, err := r.providerForResource(req.GetResourceName())
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &intent.CreateSnapshotResponse{Ok: false, Terminal: true, Message: err.Error()}, nil
	}

	err = provider.CreateSnapshot(ctx, storage.Snapshot{
		ResourceName: req.GetResourceName(),
		SnapshotName: req.GetSnapshotName(),
	})
	if err != nil {
		terminal := errors.Is(err, storage.ErrTerminal) || errors.Is(err, storage.ErrNotFound)
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &intent.CreateSnapshotResponse{Ok: false, Terminal: terminal, Message: err.Error()}, nil
	}

	return &intent.CreateSnapshotResponse{
		Ok: true,
		// Upstream LINSTOR's OpenAPI declares
		// `create_timestamp` as **milliseconds** since unix
		// epoch in UTC (pkg/api/openapi/types.gen.go), and the
		// python CLI's `linstor s l` "CreatedOn" column divides
		// by 1000 before formatting. UnixMilli matches; Unix
		// (seconds) would render the stamp as 1970-01-21.
		CreateTimestampUnix: time.Now().UnixMilli(),
	}, nil
}

// DeleteSnapshot mirrors CreateSnapshot. Idempotency lives at the
// provider layer (lvremove on missing LV is non-fatal there).
func (r *Reconciler) DeleteSnapshot(ctx context.Context, req *intent.DeleteSnapshotRequest) (*intent.DeleteSnapshotResponse, error) {
	provider, err := r.providerForResource(req.GetResourceName())
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &intent.DeleteSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	err = provider.DeleteSnapshot(ctx, storage.Snapshot{
		ResourceName: req.GetResourceName(),
		SnapshotName: req.GetSnapshotName(),
	})
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &intent.DeleteSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	return &intent.DeleteSnapshotResponse{Ok: true}, nil
}

// DeleteResource tears down a resource: drbdadm down (best-effort —
// the kernel handles a missing one fine), DeleteVolume on every
// requested volume_number through the named Provider, then remove
// the .res file. Idempotent on a missing resource. Per-step errors
// land in the response body so the controller can surface granular
// status without aborting the rest of the cleanup.
func (r *Reconciler) DeleteResource(ctx context.Context, req *intent.DeleteResourceRequest) (*intent.DeleteResourceResponse, error) {
	var downMsg string

	if r.cfg.Adm != nil {
		// Try `drbdadm down` first — it's the canonical teardown
		// path and exercises drbd-utils' full graceful sequence
		// (Secondary → Detach → Disconnect → Down).
		err := r.cfg.Adm.Down(ctx, req.GetName())
		if err != nil {
			// drbdadm fails with "not defined in your config (for
			// this host)" / "no resources defined!" whenever the
			// .res file in /etc/drbd.d is missing — which is the
			// state we land in when DeleteResource ran once already
			// (cleanup wiped the .res below) but the kernel slot
			// somehow survived. Fall back to `drbdsetup down`
			// (kernel-direct, no .res file needed) so the kernel
			// slot doesn't leak past CRD deletion (issue 288: the
			// leaked slot pins the resource's minor in the kernel,
			// blocking any subsequent RD re-using that minor with
			// "Device '<minor>' is configured!" on create-md).
			//
			// Best-effort either way: a "not configured" failure
			// on both is fine (kernel didn't know the resource).
			// Surface the original drbdadm error so operators can
			// see whether the fallback fired.
			downMsg = "drbdadm down: " + err.Error()

			setupErr := r.cfg.Adm.SetupDown(ctx, req.GetName())
			if setupErr != nil {
				downMsg += "; drbdsetup down: " + setupErr.Error()
			}
		}
	}

	// Tear down LUKS mappers BEFORE DeleteVolume — once the underlying
	// LV is gone, `cryptsetup luksClose` would either error out or hang
	// trying to flush the now-missing block device. Best-effort: a
	// missing mapper (delete-after-restart, never opened) is fine.
	if r.cfg.Cryptsetup != nil {
		for _, n := range req.GetVolumeNumbers() {
			_ = r.cfg.Cryptsetup.Close(ctx, luksMapperName(req.GetName(), n))
		}
	}

	if pool := req.GetStoragePool(); pool != "" {
		provider, ok := r.cfg.Providers[pool]
		if ok {
			for _, n := range req.GetVolumeNumbers() {
				err := provider.DeleteVolume(ctx, storage.Volume{
					ResourceName: req.GetName(),
					VolumeNumber: n,
				})
				if err != nil {
					//nolint:nilerr // surfaced as ok=false; gRPC error reserved for transport faults
					return &intent.DeleteResourceResponse{
						Ok:      false,
						Message: err.Error(),
					}, nil
				}
			}
		}
	}

	if r.cfg.StateDir != "" {
		// Drop the per-resource state files together. Leaving
		// `.md-created` behind would make a re-created RD with the
		// same name see firstActivation=false on its first apply,
		// skip create-md, and fail drbdadm adjust with
		// "No valid meta data found".
		for _, suffix := range []string{".res", ".md-created", ".mkfs.done"} {
			err := os.Remove(filepath.Join(r.cfg.StateDir, req.GetName()+suffix))
			if err != nil && !os.IsNotExist(err) {
				return &intent.DeleteResourceResponse{
					Ok:      false,
					Message: err.Error(),
				}, nil
			}
		}
	}

	r.forgetPool(req.GetName())

	return &intent.DeleteResourceResponse{Ok: true, Message: downMsg}, nil
}

// forgetPool drops the resource from the resource→pool map so a
// future Apply with a different pool starts clean.
func (r *Reconciler) forgetPool(resourceName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.resourceToPool, resourceName)
}

// applyOne reconciles a single DesiredResource. Diskless replicas skip
// the storage path (they're memory-backed by the DRBD stack); everything
// else routes one CreateVolume per DesiredVolume. When the DRBD half is
// enabled (cfg.Adm != nil), also renders the `.res` file and runs
// drbdadm create-md / adjust.
func (r *Reconciler) applyOne(ctx context.Context, dr *intent.DesiredResource) *intent.ResourceApplyResult {
	res := &intent.ResourceApplyResult{
		Name:     dr.GetName(),
		NodeName: dr.GetNodeName(),
		Ok:       true,
	}

	diskless := isDiskless(dr.GetFlags())

	if isInactive(dr.GetFlags()) {
		r.applyInactive(ctx, dr, res)

		return res
	}

	devices, resized, cloned, err := r.applyStorageIfDiskful(ctx, dr, diskless)
	if err != nil {
		res.Ok = false
		res.Message = err.Error()

		return res
	}

	devices, err = r.maybeLUKS(ctx, dr, diskless, devices, resized)
	if err != nil {
		res.Ok = false
		res.Message = err.Error()

		return res
	}

	// Skip DRBD when the layer_stack explicitly omits it. Empty
	// layer_stack defaults to ["DRBD","STORAGE"] so legacy clients
	// (and pre-Phase-9 dispatchers) keep getting full DRBD treatment.
	withDRBD := r.cfg.Adm != nil && needsDRBD(dr.GetLayerStack())
	if withDRBD {
		err := r.applyDRBD(ctx, dr, diskless, devices, resized, cloned)
		if err != nil {
			res.Ok = false
			res.Message = err.Error()

			return res
		}
	}

	res.Volumes = buildVolumeResults(dr, devices, diskless, withDRBD)

	return res
}

// maybeLUKS conditionally layers cryptsetup over the raw storage
// devices when the layer stack names "LUKS". Returns the (possibly
// rewritten) volume → device map for the next layer. Skips entirely
// for diskless replicas — they never open the underlying disk. When
// the storage layer just grew (resized=true), also runs cryptsetup
// resize so the mapper picks up the new size before DRBD's resize.
func (r *Reconciler) maybeLUKS(ctx context.Context, dr *intent.DesiredResource, diskless bool, devices map[int32]string, resized bool) (map[int32]string, error) {
	if diskless || !needsLUKS(dr.GetLayerStack()) {
		return devices, nil
	}

	return r.applyLUKS(ctx, dr, devices, resized)
}

// needsLUKS reports whether the satellite should layer cryptsetup
// over the storage device for this resource. Empty stack defaults to
// the no-LUKS legacy behaviour; LUKS only runs when explicitly named.
func needsLUKS(stack []string) bool {
	for _, s := range stack {
		if strings.EqualFold(s, "LUKS") {
			return true
		}
	}

	return false
}

// applyLUKS formats (first activation only) and opens every volume's
// raw device under /dev/mapper/<rd>-<volnum>, returning the new
// volNumber→DevicePath map for downstream layers (DRBD or direct
// consumer). When resized=true, also runs cryptsetup resize on each
// open mapper so the encrypted device picks up the grown LV size.
//
// Passphrase source for this slice: dr.Props["LuksPassphrase"]. The
// controller folds it in from the RD's `DrbdOptions/Encryption/passphrase`
// prop via the resolver. Empty passphrase fails the apply — explicit
// rather than silently creating an unencrypted volume.
func (r *Reconciler) applyLUKS(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, resized bool) (map[int32]string, error) {
	if r.cfg.Cryptsetup == nil {
		return nil, errors.New("LUKS in layer stack but no cryptsetup wrapper configured")
	}

	pass := dr.GetProps()["LuksPassphrase"]
	if pass == "" {
		return nil, errors.New("LUKS in layer stack but Props.LuksPassphrase empty")
	}

	out := make(map[int32]string, len(devices))
	key := []byte(pass)

	for vol, dev := range devices {
		dmName := luksMapperName(dr.GetName(), vol)

		err := r.cfg.Cryptsetup.Format(ctx, dev, key)
		if err != nil {
			return nil, errors.Wrapf(err, "luks format %s", dev)
		}

		err = r.cfg.Cryptsetup.Open(ctx, dev, dmName, key)
		if err != nil {
			// EEXIST is expected on every reconcile after the first —
			// the device is already opened. Classify via the typed
			// luks.ErrAlreadyOpen sentinel so we are immune to
			// cryptsetup output locale (Bug 215): the prior English-
			// only substring match silently failed on de_DE / fr_FR /
			// ru_RU satellites and would have triggered a luksFormat
			// retry against an already-formatted device.
			if !errors.Is(err, luks.ErrAlreadyOpen) {
				return nil, errors.Wrapf(err, "luks open %s -> %s", dev, dmName)
			}
		}

		if resized {
			err = r.cfg.Cryptsetup.Resize(ctx, dmName, key)
			if err != nil {
				return nil, errors.Wrapf(err, "luks resize %s", dmName)
			}
		}

		out[vol] = luks.DevicePath(dmName)
	}

	return out, nil
}

// luksMapperName picks the dm-crypt name for an (rd, vol) pair. The
// satellite needs a stable identifier across reconciles so a re-Open
// after restart re-uses the existing mapping when present.
func luksMapperName(rdName string, vol int32) string {
	return fmt.Sprintf("%s-%d-luks", rdName, vol)
}

// needsDRBD reports whether the satellite should render a .res and
// run drbdadm for this resource. Empty stack → default-true (legacy
// + Phase-1..8 wire compatibility); explicit stack → only run DRBD
// when it's named in the stack.
func needsDRBD(stack []string) bool {
	if len(stack) == 0 {
		return true
	}

	for _, s := range stack {
		if strings.EqualFold(s, "DRBD") {
			return true
		}
	}

	return false
}

// applyStorage walks dr.Volumes and ensures each LV/zvol/loopfile
// exists. Returns a `volNumber → DevicePath` map the DRBD half uses
// to wire the `disk` line in the .res file — this is what the
// kernel actually opens, so we never want the satellite to guess
// (`/dev/<pool>/<rd>_<vol>` only works for LVM/ZFS, not loopfile).
//
// Records the resource→pool mapping (first volume's pool) so
// subsequent snapshot RPCs can route without the controller passing
// applyInactive runs the `drbdadm down` half of the INACTIVE flag
// path. Pulled out of applyOne to keep the latter under funlen.
// Storage + .res file are intentionally untouched — activate later
// brings the kernel resource back without losing port/node-id or
// triggering a re-sync.
func (r *Reconciler) applyInactive(ctx context.Context, dr *intent.DesiredResource, res *intent.ResourceApplyResult) {
	if r.cfg.Adm == nil {
		return
	}

	err := r.cfg.Adm.Down(ctx, dr.GetName())
	if err != nil {
		res.Ok = false
		res.Message = err.Error()
	}
}

// applyStorageIfDiskful skips storage provisioning for diskless
// replicas (they have no backing disk) and routes diskful ones to
// applyStorage. Pulled out of applyOne to keep the latter under
// funlen.
//
// Bug 267 (HIGH, capacity leak): when a previously-diskful replica
// is toggled to diskless via `linstor r td <node> <rd> --diskless`,
// the REST handler flips Spec.Flags=[DISKLESS] but keeps Spec.
// StoragePool intact so the operator can toggle back. The dispatcher
// stamps the historical pool onto every DesiredVolume on the
// toggle-to-diskless path. THIS function detects that shape
// (diskless=true AND at least one Volume carries a non-empty
// StoragePool) and invokes provider.DeleteVolume to reclaim the
// backing LV / zvol — without this, the volume sits on disk forever
// counted against the pool's free-space budget; repeated
// demote-promote cycles compound the leak.
//
// Fresh DISKLESS replicas (no prior storage, every Volume's
// StoragePool empty) hit the no-op short-circuit at the top.
func (r *Reconciler) applyStorageIfDiskful(ctx context.Context, dr *intent.DesiredResource, diskless bool) (map[int32]string, bool, bool, error) {
	if diskless {
		// Bug 330 (P1, real stand): `linstor r td --diskless` returned
		// REST SUCCESS but `linstor r l` kept reporting the replica as
		// `UpToDate`. Root cause: the reconciler had no detach path —
		// the FSM dispatched plain `drbdadm adjust` against the loaded
		// slot, and drbd-utils' compare_volume does NOT cross the
		// kern->disk=<path> → conf->disk="none" boundary on its own
		// (the inverse of the Bug 319 attach direction). Without an
		// explicit `drbdadm detach`, the kernel never releases the
		// lower disk, and reclaimVolumesForDiskless below would then
		// destroy the LV out from under a still-attached DRBD slot.
		//
		// Match upstream LINSTOR's DrbdLayer.deactivateVolume sequence:
		// detach BEFORE the storage layer reclaims the backing volume.
		// detachIfStillAttached is a no-op when the kernel has already
		// dropped to Diskless on its own (idempotent re-entry on a
		// satellite restart mid-toggle).
		if err := r.detachIfStillAttached(ctx, dr); err != nil {
			return nil, false, false, err
		}

		err := r.reclaimVolumesForDiskless(ctx, dr)
		if err != nil {
			return nil, false, false, err
		}

		return map[int32]string{}, false, false, nil
	}

	return r.applyStorage(ctx, dr)
}

// detachIfStillAttached invokes `drbdadm detach --force <rd>` when
// the kernel slot is currently loaded with a non-Diskless local
// volume. This is the satellite's response to `linstor r td
// --diskless` on a previously-diskful replica (Bug 330).
//
// Probe order matters: IsLoaded → HasDisklessVolume. A slot that
// isn't loaded at all (never brought up, or torn down by an earlier
// DeleteResource) has nothing to detach; a slot already reporting
// disk:Diskless has converged to the target state and re-issuing
// detach would be a no-op shell-out but still costs a netlink
// round-trip on every reconcile pass.
//
// Probe errors fall through to a best-effort detach: a transient
// netlink hiccup shouldn't strand the toggle — the kernel will
// either accept the detach (state already matches) or surface a
// real error the caller wraps. The detach itself runs with --force
// so the kernel doesn't block on outstanding I/O references when
// the satellite has already declared the replica diskless at the
// REST layer.
//
// Why not gate inside the FSM transition table: the FSM currently
// models Spec→Phase but not the diskful→Diskless intra-Running flip
// as a distinct edge (the Bug 319 diskless→diskful flip is the
// only intra-Running transition wired today). Detach is wired here
// at the storage-layer entry point so it runs BEFORE the LV is
// reclaimed, which is the load-bearing ordering constraint. A
// future Phase will retire this in favour of a proper FSM
// ActionDetach + Phase transition.
func (r *Reconciler) detachIfStillAttached(ctx context.Context, dr *intent.DesiredResource) error {
	if r.cfg.Adm == nil {
		return nil
	}

	loaded, err := r.cfg.Adm.IsLoaded(ctx, dr.GetName())
	if err != nil || !loaded {
		// Slot absent / netlink hiccup → nothing to detach; the LV
		// reclaim path is safe to proceed without a detach.
		return nil //nolint:nilerr // probe failure is "no-op" by design
	}

	disklessVol, err := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
	if err == nil && disklessVol {
		// Kernel has already converged to Diskless (operator-driven
		// detach, prior reconcile pass, or a peer-driven event). No
		// further work — re-issuing detach is harmless but the
		// shell-out cost adds up on every reconcile of a steady-
		// state diskless replica.
		return nil
	}

	if detachErr := r.cfg.Adm.Detach(ctx, dr.GetName()); detachErr != nil {
		return errors.Wrapf(detachErr, "detach %s on diskless toggle", dr.GetName())
	}

	return nil
}

// reclaimVolumesForDiskless iterates the DesiredResource's volumes
// and calls provider.DeleteVolume on each that carries a non-empty
// StoragePool (the dispatcher's marker for a toggle-to-diskless
// transition — see applyStorageIfDiskful's godoc). Idempotent:
// the provider's DeleteVolume is a no-op on already-missing
// volumes, so a re-reconcile after a partial first pass safely
// finishes the cleanup.
//
// An unknown pool is silently skipped — the dispatcher may stamp a
// historical pool the satellite no longer has registered (e.g.
// after a pool rename). The orphan-storage sweeper backstops with
// its own scan in that edge case.
func (r *Reconciler) reclaimVolumesForDiskless(ctx context.Context, dr *intent.DesiredResource) error {
	for _, vol := range dr.GetVolumes() {
		pool := vol.GetStoragePool()
		if pool == "" {
			continue
		}

		provider, ok := r.cfg.Providers[pool]
		if !ok {
			continue
		}

		err := provider.DeleteVolume(ctx, storage.Volume{
			ResourceName: dr.GetName(),
			VolumeNumber: vol.GetVolumeNumber(),
			PoolName:     pool,
		})
		if err != nil {
			return errors.Wrapf(err,
				"reclaim volume %s/%d on diskless toggle",
				dr.GetName(), vol.GetVolumeNumber())
		}
	}

	return nil
}

// the pool.
func (r *Reconciler) applyStorage(ctx context.Context, dr *intent.DesiredResource) (map[int32]string, bool, bool, error) {
	devices := map[int32]string{}
	resized := false
	cloned := false

	for _, vol := range dr.GetVolumes() {
		provider, ok := r.cfg.Providers[vol.GetStoragePool()]
		if !ok {
			return nil, false, false, errors.Errorf("unknown storage pool %q", vol.GetStoragePool())
		}

		// Clone path: when DesiredVolume.SourceSnapshot is set (the
		// snapshot-restore-resource handler stamps it on the target
		// RD's Props, the dispatcher pipes it through), materialise
		// the volume via Provider.RestoreVolumeFromSnapshot instead
		// of CreateVolume so the new replica starts populated with
		// the snapshot's data. Idempotent: provider's clone op skips
		// when the target volume already exists.
		err := r.materializeVolume(ctx, provider, dr.GetName(), vol)
		if err != nil {
			return nil, false, false, errors.Wrapf(err, "create/restore volume %s/%d", dr.GetName(), vol.GetVolumeNumber())
		}

		if vol.GetSourceSnapshot() != "" {
			cloned = true
		}

		status, err := provider.VolumeStatus(ctx, storage.Volume{
			ResourceName: dr.GetName(),
			VolumeNumber: vol.GetVolumeNumber(),
		})
		if err != nil {
			return nil, false, false, errors.Wrapf(err, "volume status %s/%d", dr.GetName(), vol.GetVolumeNumber())
		}

		// Grow path: the controller's VolumeDefinition update set a
		// new size that's larger than what the provider has on disk.
		// Call ResizeVolume to extend the LV/zvol/file; the LUKS
		// layer (when present) and `drbdadm resize` are layered on
		// top by their own reconcile steps.
		if vol.GetSizeKib() > status.UsableKib && status.UsableKib > 0 {
			err = provider.ResizeVolume(ctx, storage.Volume{
				ResourceName: dr.GetName(),
				VolumeNumber: vol.GetVolumeNumber(),
				SizeKib:      vol.GetSizeKib(),
			})
			if err != nil {
				return nil, false, false, errors.Wrapf(err, "resize volume %s/%d to %d KiB",
					dr.GetName(), vol.GetVolumeNumber(), vol.GetSizeKib())
			}

			resized = true
		}

		devices[vol.GetVolumeNumber()] = status.DevicePath
	}

	if len(dr.GetVolumes()) > 0 {
		r.rememberPool(dr.GetName(), dr.GetVolumes()[0].GetStoragePool())
	}

	return devices, resized, cloned, nil
}

// materializeVolume picks the right provider call: clone from a
// snapshot when SourceSnapshot is set on the desired volume,
// otherwise create blank. Parses `<srcRD>:<snapName>` for the
// clone form — matches what the snapshot-restore-resource REST
// handler stamps onto the target RD's Props.
//
// Cross-node path: when SourceSnapshot is set but the snapshot
// doesn't physically exist on THIS node (autoplace landed the new
// replica on a node outside snap.Nodes), the local clone returns
// storage.ErrNotFound. With a configured CrossNodeFetcher we then
// stream the snapshot from a peer satellite that hosts it locally
// (upstream LINSTOR's `zfs send | zfs recv` shape). Without one,
// fall back to a blank CreateVolume — DRBD network resync will
// populate the data, at the cost of a known cloned-metadata vs
// fresh-metadata GI mismatch on the wire (see Phase 11 notes).
func (r *Reconciler) materializeVolume(ctx context.Context, provider storage.Provider, rdName string, vol *intent.DesiredVolume) error {
	target := storage.Volume{
		ResourceName: rdName,
		VolumeNumber: vol.GetVolumeNumber(),
		SizeKib:      vol.GetSizeKib(),
	}

	src := vol.GetSourceSnapshot()
	if src == "" {
		return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
	}

	// Cross-cluster ship guard (scenario 4.17). Upstream LINSTOR's
	// `BackupShip` payload references a remote-cluster snapshot via
	// `<remote_name>:<srcRD>:<snap>` (three colon-separated parts).
	// Cozystack's satellite knows only the local CrossNodeFetcher
	// pipeline — there is no wire shape for fetching a snapshot
	// from a different cluster's controller. Reject the 3-part form
	// up-front with an actionable error so it surfaces on the
	// resource's Status.Conditions instead of being silently mis-
	// parsed as a malformed 2-part srcRD that happens to contain
	// a colon.
	if remotePrefix, rest, hasRemote := strings.Cut(src, ":"); hasRemote {
		if _, _, hasSnap := strings.Cut(rest, ":"); hasSnap && remotePrefix != "" {
			return errors.Errorf(
				"SourceSnapshot %q references a cross-cluster remote (%q); "+
					"cluster-to-cluster ship via LINSTOR remote is not "+
					"implemented; use snapshot-restore-resource for "+
					"in-cluster ship", src, remotePrefix)
		}
	}

	srcRD, snapName, ok := strings.Cut(src, ":")
	if !ok || srcRD == "" || snapName == "" {
		return errors.Errorf("SourceSnapshot %q must be <srcRD>:<snapName>", src)
	}

	err := provider.RestoreVolumeFromSnapshot(ctx, target, storage.Snapshot{
		ResourceName: srcRD,
		SnapshotName: snapName,
		PoolName:     vol.GetStoragePool(),
	})
	if !errors.Is(err, storage.ErrNotFound) {
		return err //nolint:wrapcheck // caller wraps
	}

	// Local snapshot missing. Try the cross-node fetcher; if that
	// also doesn't pan out we fall through to a blank CreateVolume
	// so DRBD has something to resync into.
	if r.cfg.CrossNodeFetcher == nil {
		return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
	}

	return r.crossNodeClone(ctx, provider, target, srcRD, snapName, vol.GetVolumeNumber())
}

// crossNodeClone is materializeVolume's cross-node fallback branch.
// Fetches the snapshot byte stream from a peer satellite and pipes
// it into the local provider's RecvSnapshot. The provider must
// implement storage.SnapshotShipper — backends that can't ship
// (legacy file driver pre-Phase-11) fall through to a blank create
// so DRBD network resync still has somewhere to drop bytes.
func (r *Reconciler) crossNodeClone(
	ctx context.Context,
	provider storage.Provider,
	target storage.Volume,
	srcRD, snapName string,
	volNum int32,
) error {
	shipper, ok := provider.(storage.SnapshotShipper)
	if !ok {
		return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
	}

	body, peer, err := r.cfg.CrossNodeFetcher.Fetch(ctx, srcRD, snapName, volNum)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// No peer has the snapshot — DRBD resync is the last
			// resort. Returns wrong-data on receive (split-brain
			// from metadata mismatch); upstream behaviour with
			// FILE_THIN matches this for now.
			return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
		}

		return errors.Wrapf(err, "cross-node fetch %s/%s", srcRD, snapName)
	}

	defer func() { _ = body.Close() }()

	err = shipper.RecvSnapshot(ctx, target, body)
	if err != nil {
		return errors.Wrapf(err, "recv %s/%s from %s", srcRD, snapName, peer)
	}

	return nil
}

// tearDownRemovedPeers runs `drbdadm del-peer` AND `drbdmeta
// forget-peer` for every peer that was in the previous .res but
// is no longer in the new desired set.
//
// `drbdadm adjust` only adds / reconfigures peers; the kernel's
// connection slot for a dropped peer would otherwise stay alive
// in StandAlone forever. del-peer needs the peer's `on <node>`
// block still in the .res to resolve its node-id, so run it
// BEFORE overwriting the file.
//
// forget-peer clears the peer's per-peer GI / bitmap slot from
// every diskful volume's on-disk metadata block. Without it,
// DRBD-9 v09 metadata keeps the departed peer's slot occupied
// for the lifetime of the resource — after enough node-replace
// cycles the resource exhausts the MaxPeers-1 slot budget
// `drbdadm create-md --max-peers=15` carved at first activation,
// and the next replica add fails with drbdmeta running out of
// room. Errors on individual forget-peer calls are logged and
// not bubbled up: leaving a stale slot is a slow leak (recoverable
// at any point in the future), while wedging the entire reconcile
// on it would block the convergent steady-state path the dispatcher
// drives. del-peer failures still bubble — those leak a live
// kernel connection, which is a faster correctness issue.
func (r *Reconciler) tearDownRemovedPeers(ctx context.Context, dr *intent.DesiredResource, resPath string, devices map[int32]string) error {
	removed := computeRemovedPeers(resPath, dr, r.cfg.NodeName)
	if len(removed) == 0 {
		return nil
	}

	// Peer-name → node-id from the OLD .res. The desired bag may
	// no longer carry the removed peer's `peer.<name>.node-id`
	// entry (dispatcher already pruned the spec), so the .res
	// file we're about to overwrite is the only stable source.
	peerIDs := extractResFilePeerNodeIDs(resPath)

	for _, peer := range removed {
		err := r.cfg.Adm.DelPeer(ctx, dr.GetName(), peer)
		if err != nil {
			return errors.Wrapf(err, "del-peer %s from %s", peer, dr.GetName())
		}

		// forget-peer is per-volume because v09 metadata lives in
		// the per-volume block. Skip volumes without a device path
		// (DISKLESS local replica — no metadata to clean) and
		// peers without a resolvable node-id (.res malformed /
		// races a brand-new resource being torn down before its
		// peer ever rendered).
		peerID, hasID := peerIDs[peer]
		if !hasID {
			continue
		}

		for volNum, device := range devices {
			if device == "" {
				continue
			}

			// forget-peer errors are non-fatal: a stale on-disk
			// slot leaks one of the MaxPeers-1 budget entries but
			// the resource keeps serving I/O. The next reconcile
			// retries; if the leak persists, the eventual
			// create-md exhaustion surfaces a louder error than
			// any log line here could. del-peer errors still
			// bubble (above) — those leak a live kernel
			// connection, a faster correctness issue.
			_ = r.cfg.Adm.ForgetPeer(ctx, dr.GetName(), volNum, device, peerID)
		}
	}

	return nil
}

// computeRemovedPeers diffs the previously-rendered .res file against
// the new desired peer set. Returns peer node names that were present
// before but are NOT in the new layout. Empty when the .res file
// doesn't exist (first apply) or when the read fails — we'd rather
// skip the del-peer pass than wedge the reconcile.
func computeRemovedPeers(resPath string, dr *intent.DesiredResource, localNode string) []string {
	body, err := os.ReadFile(resPath)
	if err != nil {
		return nil
	}

	old := extractResFilePeers(string(body))
	if len(old) == 0 {
		return nil
	}

	want := make(map[string]struct{}, len(dr.GetPeers())+1)
	want[localNode] = struct{}{}

	for _, p := range dr.GetPeers() {
		want[p] = struct{}{}
	}

	var removed []string

	for _, p := range old {
		if _, keep := want[p]; !keep {
			removed = append(removed, p)
		}
	}

	return removed
}

// extractResFilePeers parses an `on <node> {` block list out of a
// rendered .res file. We don't need a full DRBD parser — only the
// peer node-name set, which writeOnBlock emits as `  on <name> {`.
func extractResFilePeers(body string) []string {
	var peers []string

	for line := range strings.SplitSeq(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "on ") {
			continue
		}

		rest := strings.TrimPrefix(trimmed, "on ")

		head, _, ok := strings.Cut(rest, "{")
		if !ok {
			continue
		}

		name := strings.TrimSpace(head)
		if name != "" {
			peers = append(peers, name)
		}
	}

	return peers
}

// extractResFilePeerNodeIDs parses the rendered .res file at
// resPath and returns the peer-name → DRBD-node-id map encoded
// in each `on <node> { ... node-id <N>; ... }` block. Used by
// tearDownRemovedPeers to resolve the node-id for a peer that
// was just dropped from the desired set: `drbdadm del-peer`
// reads node-id from the (still-present) `on <peer>` block, but
// `drbdmeta forget-peer` needs the raw integer, and we'd rather
// pull it from the file we're about to overwrite than guess from
// the desired bag (which the dispatcher may have already pruned).
//
// Missing file / unreadable / malformed block → empty map; the
// caller skips forget-peer for that peer rather than emit a
// bogus --node-id=0 collision against the local slot. Reads via
// os.ReadFile so a transient I/O hiccup degrades to no-op
// instead of wedging the reconcile.
// hasLateAddedVolume reports whether the desired-state Volumes[]
// includes at least one volume number that is NOT yet represented
// as a `volume <N> {` block in the OLD .res file at resPath.
//
// Bug 332: the `linstor vd c <rd> 1G` flow grows VolumeDefinitions[]
// after the RD has already passed first-activation. The dispatcher
// hands the satellite a DesiredResource with the new volume in
// Volumes[], but the on-disk .res still describes the smaller set —
// so a strict greater-than on the rendered block count is the
// late-VD signal. Returns false when the .res file is absent
// (cold-start path; the existing firstActivation gate owns
// metadata creation), when the file is unreadable (fail-safe to
// "no late vol → no extra work"), or when the desired set matches
// what's already rendered.
//
// Parser is intentionally simple: matches "volume <N> {" inside an
// `on <node> {` block. False positives across multi-host blocks are
// harmless — the helper de-duplicates by recording each volNumber
// once across the file.
func hasLateAddedVolume(resPath string, dr *intent.DesiredResource) bool {
	if dr == nil {
		return false
	}

	body, err := os.ReadFile(resPath)
	if err != nil {
		// No .res yet → cold start, existing firstActivation path
		// will create metadata for every volume via the standard
		// chain. Late-VD signal is "old file exists with fewer
		// volumes", not "no file at all".
		return false
	}

	rendered := map[int32]struct{}{}

	for line := range strings.SplitSeq(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "volume ") {
			continue
		}

		rest := strings.TrimPrefix(trimmed, "volume ")
		head, _, ok := strings.Cut(rest, "{")

		if !ok {
			continue
		}

		num, parseErr := strconv.ParseInt(strings.TrimSpace(head), 10, 32)
		if parseErr != nil {
			continue
		}

		rendered[int32(num)] = struct{}{}
	}

	for _, vol := range dr.GetVolumes() {
		if _, ok := rendered[vol.GetVolumeNumber()]; !ok {
			return true
		}
	}

	return false
}

func extractResFilePeerNodeIDs(resPath string) map[string]int32 {
	body, err := os.ReadFile(resPath)
	if err != nil {
		return nil
	}

	out := map[string]int32{}

	var currentPeer string

	for line := range strings.SplitSeq(string(body), "\n") {
		trimmed := strings.TrimSpace(line)

		// Block opener: `on <name> {`. Stash the name; the
		// matching `node-id` line follows within the block.
		if strings.HasPrefix(trimmed, "on ") {
			rest := strings.TrimPrefix(trimmed, "on ")

			head, _, ok := strings.Cut(rest, "{")
			if !ok {
				continue
			}

			currentPeer = strings.TrimSpace(head)

			continue
		}

		// node-id line shape: `node-id <N>;` (writeOnBlock emits
		// it as the second line of every on-block). Match
		// `node-id ` prefix to dodge `<peer>.node-id` style
		// option lines that might appear at the resource top
		// level.
		if currentPeer != "" && strings.HasPrefix(trimmed, "node-id ") {
			rest := strings.TrimPrefix(trimmed, "node-id ")
			rest = strings.TrimSuffix(rest, ";")
			rest = strings.TrimSpace(rest)

			id, parseErr := strconv.ParseInt(rest, 10, 32)
			if parseErr == nil {
				out[currentPeer] = int32(id)
			}

			currentPeer = ""
		}
	}

	return out
}

// renderResFile builds and writes the per-node .res file content-
// idempotently. Bug 315 invariant: skips os.WriteFile when the
// rendered body matches what's already on disk so drbdadm's config-
// file-watcher does not see a spurious mtime bump. Pure file op —
// no kernel interaction, no peer probes.
//
// Extracted from applyDRBD so the FSM dispatch path (Phase 11.2.c
// Stage 2) can call the same writer the legacy chain uses without
// forking the apply flow. devices is the volNumber → DevicePath map
// applyStorage produced; buildResFile uses it as the disk path so a
// loopfile-backed volume gets `disk /dev/loopN` rather than the
// LVM-shaped guess.
func (r *Reconciler) renderResFile(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string) error {
	_ = ctx
	body, err := buildResFile(dr, r.cfg.NodeName, r.cfg.LocalAddress, devices)
	if err != nil {
		return errors.Wrapf(err, "build .res for %s", dr.GetName())
	}
	resPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".res")
	current, _ := os.ReadFile(resPath)
	if bytes.Equal(current, []byte(body)) {
		return nil
	}
	return errors.Wrapf(os.WriteFile(resPath, []byte(body), resFilePerm), "write %s", resPath)
}

// applyDRBD renders the .res file from dr's metadata and (re)applies
// it via drbdadm. create-md runs only on first activation (we detect
// "first" by absence of the .res file before this run); diskless
// replicas skip create-md entirely.
//
// devices is the volNumber → DevicePath map applyStorage produced.
// buildResFile uses it as the disk path so a loopfile-backed volume
// gets `disk /dev/loopN` rather than the LVM-shaped guess.
func (r *Reconciler) applyDRBD(ctx context.Context, dr *intent.DesiredResource, diskless bool, devices map[int32]string, resized, cloned bool) error {
	// Bug 79: when the RD has no VolumeDefinitions yet (operator created
	// the RD and Resources before adding any VD), there is no backing
	// volume to bring DRBD up on. Returning early here keeps the
	// .md-created marker absent so a later VD-add reconcile sees
	// firstActivation=true and runs create-md against the now-present
	// backing storage. Without this guard, the empty-volume pass would
	// write the marker (runFirstActivation always writes it, even when
	// CreateMD is a no-op on zero volumes), pin firstActivation=false
	// for the lifetime of the resource, and the late VD would come up
	// with no DRBD metadata — the kernel then reports disk:Diskless
	// while Spec.Flags lacks DISKLESS, surfacing as "Unintentional
	// Diskless" in `linstor r l`.
	if len(dr.GetVolumes()) == 0 {
		return nil
	}

	// Phase 11.2.b shadow: compute the FSM phase + expected action
	// from the current Observation and log it for divergence triage.
	// READ-ONLY: the historical apply path below is unchanged, and
	// the FSM never drives a transition until Phase 11.2.c flips
	// the switch. Probe errors fall through as zero-valued fields
	// inside observeForFsm; no retries, no failures bubble up here.
	r.logFsmShadow(ctx, dr, diskless)

	resPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".res")
	mdMarkerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".md-created")

	// tearDownRemovedPeers MUST run before the FSM dispatch block:
	// it reads the OLD .res to resolve node-ids for peers that have
	// departed from the spec, and then issues del-peer / forget-peer
	// for each one. The FSM dispatch's renderResFile preamble (Phase
	// 11.2.c Stage 4 step 1) overwrites .res with the new peer set,
	// so this tear-down step must observe the pre-render state to
	// avoid leaking kernel connections and on-disk GI slots.
	err := r.tearDownRemovedPeers(ctx, dr, resPath, devices)
	if err != nil {
		return err
	}

	// Bug 332 (regression of Bug 79): MetadataCreated=True is a per-RD
	// hint but the actual drbdmeta create-md must run per-volume. When
	// `vd c` adds a new volume to an existing RD (operator-observed
	// repro on a 3-replica cluster after vol-0 reached UpToDate), the
	// per-RD Condition is True so the legacy firstActivation predicate
	// flips false — yet the new volume has no on-disk metadata, and
	// the subsequent `drbdadm adjust` would bring it up as kernel
	// disk:Diskless while Spec.Flags lacks DISKLESS (the verbatim
	// "Unintentional Diskless" surface from `linstor r l`). Mirror
	// upstream LINSTOR DrbdLayer.adjustResource: per-volume
	// hasMetaData probe, per-volume createMd for those that lack it.
	//
	// Scope: NARROW to the "late-VD added" signal — desired Volumes[]
	// count strictly exceeds the OLD .res's `volume N {` block count.
	// Without this narrowing, the steady-state path would shell out
	// `drbdadm dump-md` on every reconcile, perturbing existing tests
	// that pin "no metadata work on retry" (e.g. mid-Apply abort
	// scenarios) and adding shell cost on every converged pass.
	// Skipped on diskless replicas (no lower disk to stamp) and on
	// the diskless→diskful flip path (Bug 319 owns that re-stamp).
	if !diskless && hasLateAddedVolume(resPath, dr) &&
		!r.isDisklessToDiskfulFlip(ctx, dr, diskless) {
		err = r.ensurePerVolumeMetadata(ctx, dr, devices, diskless)
		if err != nil {
			return err
		}
	}

	// Phase 11.2.c Stage 3d: shadow-dispatch every FSM action. Each
	// helper is content-idempotent — the legacy chain below will
	// re-run the same logic later in this Apply pass and detect that
	// the state already matches. The fsmShadowAgreeCount metric tags
	// each FSM-dispatched action with `:fsm-dispatched` so production
	// dashboards can prove every gate is FSM-reachable end-to-end.
	// Stage 4 will retire the legacy chain once the metric shows
	// every transition has been FSM-dispatched in steady state for
	// a full burnin window.
	//
	// Phase 11.2.c Stage 4 step 1: the FSM dispatch path now owns
	// renderResFile (legacy unconditional call below has been
	// retired). dispatchFsmAction invokes renderResFile as a preamble
	// for every action that consumes .res (createMd, up, adjust,
	// adjustSkipDisk), and the ActionRenderRes arm continues to
	// handle the cold-start PhaseUnprovisioned case.
	{
		obs := r.observeForFsm(ctx, dr, diskless)
		phase := ObservePhase(obs)
		if next := NextTransition(phase, obs); next != nil {
			if err := r.dispatchFsmAction(ctx, dr, devices, next.Action, obs); err != nil {
				return errors.Wrapf(err, "fsm dispatch %s", next.Action)
			}
			fsmShadowAgreeCount.Add(next.Action+":fsm-dispatched", 1)
		}
	}

	// firstActivation is "did create-md succeed previously?" —
	// Phase 11.3 Stage 1 derives this from the
	// `MetadataCreated=True` Status Condition on the parent
	// Resource CRD (carried into the apply chain via
	// dr.MetadataCreated). The on-disk `.md-created` marker is a
	// belt-and-braces fallback for the migration window: if the
	// Condition is absent but the marker file is present (cluster
	// upgraded from a pre-11.3 build, Condition not yet
	// backfilled), firstActivation still flips false so we don't
	// re-run create-md on a metadata block that already exists.
	//
	// We can't gate on the .res-file existence alone: a previous
	// reconcile that wrote the .res but failed `drbdadm create-md`
	// (e.g. .res had a stale conflicting node-id from a race that
	// later got fixed) would otherwise report firstActivation=false
	// on every subsequent attempt → create-md is skipped → adjust
	// reports "No valid meta data found" forever.
	_, statErr := os.Stat(mdMarkerPath)
	firstActivation := !dr.GetMetadataCreated() && os.IsNotExist(statErr)

	// Phase 11.2.c Stage 4 step 1: legacy r.renderResFile call retired.
	// Why: the FSM shadow-dispatch above now owns renderRes — it
	// observes Phase==Unprovisioned (cold start) and dispatches
	// r.renderResFile through ActionRenderRes, and for every later
	// phase (MetadataPending / MetadataReady / Running) the dispatch
	// runs renderResFile as a preamble inside dispatchFsmAction
	// before the phase-specific action. The helper's Bug-315 content-
	// idempotent write guarantees no churn on converged state.
	// Removing the duplicate call here drops the idempotent
	// stat+compare overhead one Apply pass spent twice. Other
	// transitions (createMd, up, adjust) still run their legacy path
	// below; those legacy gates retire one-by-one in step 2-4.

	// Bug 319 (root-cause fix for Bug 303): probe BEFORE any bring-up
	// verbs whether the local kernel slot is `disk:Diskless client:yes`
	// (intentional diskless) on a Spec that has flipped to diskful
	// (`linstor r td --migrate-from`, `linstor r td --diskful`). The
	// upstream LINSTOR pattern (`DrbdLayer.createMetaData` → `drbdadm
	// adjust`) initialises metadata BEFORE every adjust and lets adjust
	// cross the diskless→diskful boundary via drbd-utils' compare_volume
	// (kern->disk=="none" + conf->disk="<path>" schedules attach_cmd).
	//
	// Match that pattern here: when the kernel reports a Diskless
	// volume and Spec is now diskful, re-enter the create-md path on
	// the now-present lower disk REGARDLESS of the .md-created marker.
	// The previous Bug 303 workaround (explicit `drbdadm attach` AFTER
	// adjust) papered over the missing create-md re-entry; removing it
	// in favour of the upstream-aligned pipeline.
	diskfulFlip := r.isDisklessToDiskfulFlip(ctx, dr, diskless)

	// Auto-promote (primary --force + auto-mkfs) and GI-seed are
	// gated on firstActivation: a Spec flag flip from diskless to
	// diskful is NOT a fresh activation — peers are already UpToDate,
	// so a primary --force here would regenerate the local Current
	// UUID out from under the cluster, and a GI-seed would corrupt
	// the in-flight handshake. Suppress firstActivation on the flip
	// so `ensureMetadata` skips GI-seed and `finishDRBDApply` skips
	// the auto-promote chain.
	effectiveFirstActivation := firstActivation && !diskfulFlip

	// Phase 11.2.c Stage 3a: fresh-replica first-activation routes
	// through the dedicated createMetadata helper so Stage 3b can
	// FSM-shadow-dispatch it (mirror of the renderResFile shadow
	// landed in Stage 2). The diskless→diskful flip case (Bug 319)
	// stays on the historical ensureMetadata(..., false) call —
	// re-stamping metadata WITHOUT the fresh-replica GI-seed, since
	// the kernel slot is already handshaken via the diskless path.
	// Behaviour identical to the previous single ensureMetadata
	// branch parameterised by effectiveFirstActivation.
	err = r.maybeStampMetadata(ctx, dr, devices, mdMarkerPath, diskless, firstActivation, diskfulFlip)
	if err != nil {
		return err
	}

	err = r.runApplyDRBDVerb(ctx, dr, effectiveFirstActivation, diskfulFlip)
	if err != nil {
		return err
	}

	return r.finishDRBDApply(ctx, dr, diskless, effectiveFirstActivation, resized, cloned)
}

// finishDRBDApply runs the post-adjust steps: pickup-time resize and
// the first-activation auto-primary seed. Extracted from applyDRBD so
// the orchestrator stays under the project's gocyclo budget.
//
// Bug 319: an earlier revision called `drbdadm attach` here for the
// diskless→diskful flip (Bug 303 workaround). That step is gone —
// `ensureMetadata` now runs create-md on the new lower disk BEFORE
// adjust, and drbd-utils' compare_volume schedules attach_cmd
// automatically when kern->disk=="none" but conf->disk points at a
// real path. Matches upstream LINSTOR's DrbdLayer pipeline.
func (r *Reconciler) finishDRBDApply(ctx context.Context, dr *intent.DesiredResource, diskless, firstActivation, resized, cloned bool) error {
	// Pickup-time resize: the storage layer was just grown, drbdadm
	// resize tells the kernel to extend the replicated device to
	// match. Adjust on its own won't do this — only resize re-reads
	// the lower disk's size. Diskless replicas don't have a lower
	// disk to resize but they still need their internal state to
	// catch up; drbdadm resize handles that case too.
	if resized {
		err := r.cfg.Adm.Resize(ctx, dr.GetName())
		if err != nil {
			return errors.Wrapf(err, "resize %s", dr.GetName())
		}
	}

	// Force-primary trigger: only when the RD-prop `auto-primary` is
	// set (controller-initiated seed for fresh replicas).
	//
	// Do NOT auto-promote on clone. Local clone (zfs clone / lvcreate
	// -s / cp --reflink) copies the source's DRBD metadata byte-for-
	// byte, so every clone replica starts with the same Current UUID.
	// Running `drbdadm primary --force` on each replica regenerates
	// the Current UUID independently per node → peers see divergent
	// UUIDs on first handshake → split-brain (StandAlone).
	autoPrimaryReplica := !diskless &&
		dr.GetDrbdOptions()["auto-primary"] == drbdBoolPropTrue
	autoPromote := firstActivation && autoPrimaryReplica
	_ = cloned

	if autoPromote {
		err := r.runAutoPromote(ctx, dr)
		if err != nil {
			return err
		}
	}

	// Bug 311: the auto-mkfs path used to live ONLY inside
	// runAutoPromote (above), wedged between `drbdadm primary --force`
	// and `drbdadm secondary`. That coupling meant any transient
	// failure in the promote/demote dance — primary --force racing the
	// initial-sync handshake, secondary racing an in-flight Open —
	// left `.mkfs.done` unwritten while `.md-created` persisted, so the
	// next reconcile saw firstActivation=false, skipped the whole
	// auto-promote branch, and mkfs never ran again. piraeus' NFS-
	// ganesha multi-volume RD (RWX PVC, two VDs, `FileSystem/Type=ext4`
	// on the RD) reproduced this every time: the resource bound but
	// `/dev/drbd/by-res/<pvc>/1` had no filesystem and ganesha's
	// `mount-recovery@<pvc>.service` failed with `fsck.ext2: Bad magic
	// number in super-block`.
	//
	// The retry path runs ONLY when firstActivation has already
	// happened (so we never double-promote a healthy fresh replica)
	// AND the `.mkfs.done` marker is still missing AND the RD asks
	// for a filesystem. It re-enters runAutoPromote which is
	// idempotent: primary --force on an already-Primary slot is a
	// kernel no-op, `runAutoMkfs` blkid-probes each device and skips
	// volumes that already carry a filesystem, and `secondary`
	// matches the regular post-mkfs demote. Once every diskful
	// volume passes the blkid probe (either freshly-mkfs'd here or
	// already populated from a previous attempt), runAutoMkfs writes
	// the marker and this branch becomes a no-op for the rest of the
	// resource's life.
	if !autoPromote && autoPrimaryReplica && r.needsAutoMkfsRetry(dr) {
		err := r.runAutoPromote(ctx, dr)
		if err != nil {
			return err
		}
	}

	return nil
}

// needsAutoMkfsRetry probes whether an auto-primary replica must
// re-enter the promote-mkfs-demote chain on a steady-state reconcile.
// Returns true only when (a) the RD asks for a filesystem
// (`FileSystem/Type` prop set), (b) the `.mkfs.done` marker is
// absent, and (c) the satellite has both an Exec wrapper and a
// StateDir wired (production always does; tests that omit them
// disable auto-mkfs entirely, matching the runAutoMkfs no-Exec branch).
//
// The marker file is the same one `runAutoMkfs` drops after every
// volume reaches a filesystem (either by mkfs or by adopting an
// existing one via blkid). Reading the marker here is a cheap
// fs.Stat — cheaper than re-running blkid on every volume just to
// decide whether we need to do anything at all.
func (r *Reconciler) needsAutoMkfsRetry(dr *intent.DesiredResource) bool {
	if strings.TrimSpace(dr.GetProps()["FileSystem/Type"]) == "" {
		return false
	}

	if r.cfg.Exec == nil || r.cfg.StateDir == "" {
		return false
	}

	// Phase 11.3 Stage 2: Condition first. When the dispatcher
	// observed `FilesystemFormatted=True` on the Resource CRD, the
	// auto-mkfs path has already finished for every diskful volume
	// — no retry needed even if the file marker happened to be
	// removed (host rebuild, operator `rm`). The per-volume blkid
	// probe inside runAutoMkfs stays as the double-mkfs safety net,
	// so a stale Condition cannot cause data loss; this read is a
	// hot-path stat-skip.
	if dr.GetFilesystemFormatted() {
		return false
	}

	// Belt-and-braces file fallback: pre-11.3-Stage-2 clusters
	// have populated `.mkfs.done` markers but no Condition stamped
	// until the next reconcile.
	markerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".mkfs.done")
	_, err := os.Stat(markerPath)

	return os.IsNotExist(err)
}

// isDisklessToDiskfulFlip probes whether the local kernel slot is
// currently `disk:Diskless client:yes` (intentional diskless) on a
// Resource whose Spec has flipped to diskful (`linstor r td
// --migrate-from`, `linstor r td --diskful`).
//
// Bug 319: this is the trigger for re-entering the create-md path
// on a flag flip even when the satellite's .md-created marker is
// already present (the previous diskless apply may not have written
// it, but it may also have been written by a prior diskful incarnation
// of the same name — we must re-stamp metadata on the newly-carved
// lower disk either way). Upstream LINSTOR's DrbdLayer always runs
// createMetaData before adjust on every reconcile pass; drb-utils'
// compare_volume then schedules attach_cmd via the
// kern->disk=="none" + conf->disk="<path>" diff. Matching that flow
// is what makes the explicit Bug 303 `drbdadm attach` unnecessary.
//
// Probe BEFORE any bring-up verbs run because adjust / CreateMD /
// etc may shift kernel state mid-flight and we'd lose the signal.
// Errors fall through to false: a netlink hiccup shouldn't strand
// the apply chain, and the next reconcile pass (driven by Status
// updates / events2) will retry the probe.
//
// Returns false when:
//   - the spec is still diskless (no boundary crossing),
//   - the kernel slot isn't loaded (the bring-up path will Up the
//     resource with the new .res, which DOES attach the disk
//     because new-resource sees a disk path),
//   - the kernel slot is loaded with no Diskless volume (already
//     diskful — re-running create-md would be a HasMD-gated no-op
//     but we skip the probe to avoid the shell-out cost).
func (r *Reconciler) isDisklessToDiskfulFlip(ctx context.Context, dr *intent.DesiredResource, diskless bool) bool {
	if diskless {
		return false
	}

	loaded, err := r.cfg.Adm.IsLoaded(ctx, dr.GetName())
	if err != nil || !loaded {
		return false
	}

	disklessVol, err := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
	if err != nil {
		return false
	}

	return disklessVol
}

// ensureMetadata is the upstream-aligned create-md entry point. It
// runs in two cases:
//
//  1. firstActivation: the resource has never had `.md-created`
//     stamped (fresh diskful replica). Behaves exactly like the
//     historical runFirstActivation — HasMD-gated CreateMD, marker
//     write, GI-seed.
//  2. diskless→diskful Spec flag flip (Bug 319): the resource was
//     previously diskless on this node, the dispatcher just dropped
//     the Diskless host marker, applyStorage carved a fresh
//     zvol/LV, and the kernel still reports `disk:Diskless
//     client:yes`. Re-enter create-md so the new lower disk has
//     valid DRBD-9 metadata; drbdadm adjust then auto-attaches via
//     drb-utils' compare_volume (kern->disk=="none" + conf->disk
//     path diff). Skip the GI-seed: it's a fresh-replica
//     optimisation, not relevant when the kernel slot is already
//     handshaken with peers via the diskless path.
//
// Idempotent on both axes: HasMD short-circuits CreateMD when the
// metadata block already exists (e.g. satellite restart between
// CreateMD and marker write), and the marker write is a one-shot
// OS truncate that doesn't churn on repeat.
func (r *Reconciler) ensureMetadata(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, mdMarkerPath string, firstActivation bool) error {
	hasMD, err := r.cfg.Adm.HasMD(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "dump-md %s", dr.GetName())
	}

	if !hasMD {
		err = r.cfg.Adm.CreateMD(ctx, dr.GetName())
		if err != nil {
			return errors.Wrapf(err, "create-md %s", dr.GetName())
		}
	}

	err = os.WriteFile(mdMarkerPath, nil, resFilePerm)
	if err != nil {
		return errors.Wrapf(err, "write %s", mdMarkerPath)
	}

	// Phase 11.3 Stage 1: stamp the `MetadataCreated=True` Status
	// Condition on the parent Resource CRD. Belt-and-braces with
	// the file marker write above: future reconciles read the
	// Condition first to derive `firstActivation`, falling back to
	// the file presence only when the Condition is absent (cluster
	// upgrade window before the satellite's startup backfill
	// pass). The stamp failure does NOT fail the apply — the file
	// marker is the transitional source of truth, so a transient
	// apiserver hiccup here just defers Condition stamping to the
	// next reconcile.
	if r.cfg.MetadataCreatedStamper != nil {
		// Why (Bug 344): the stamper SSA-patches a `Resource`
		// object whose Name is the CRD object name. Real Resource
		// CRDs are named `<rd>.<node>` (per-node sharding); passing
		// the RD-only name made the apiserver return 404 on every
		// stamp attempt, polluting ERROR logs since Phase 11.3
		// Stage 1 (#489). Best-effort tolerated (file marker is the
		// source of truth) so no functional regression, just noise.
		resourceCRDName := dr.GetName() + "." + dr.GetNodeName()
		stampErr := r.cfg.MetadataCreatedStamper.StampMetadataCreated(ctx, resourceCRDName)
		if stampErr != nil {
			log.FromContext(ctx).Error(stampErr, "stamp MetadataCreated Condition; will retry next reconcile",
				"resource", resourceCRDName)
		}
	}

	// GI-seed is fresh-replica-only: it pre-stamps the per-peer
	// bitmap slots with a peer's UpToDate GI so the initial-sync
	// handshake skips a full resync. On a diskless→diskful flip the
	// kernel slot is already handshaken with peers via the diskless
	// path — the GI-seed window has closed, and re-stamping the GI
	// would corrupt the in-flight session.
	if !firstActivation {
		return nil
	}

	err = r.seedInitialGi(ctx, dr, devices)
	if err != nil {
		return errors.Wrapf(err, "seed initial-sync GI %s", dr.GetName())
	}

	return nil
}

// ensurePerVolumeMetadata stamps DRBD-9 metadata on every diskful
// volume of `dr` that lacks it. Mirrors upstream LINSTOR's
// DrbdLayer.adjustResource (satellite/.../DrbdLayer.java L702-723):
// hasMetaData per-volume, createMd per-volume for the ones missing.
//
// Bug 332 (regression of Bug 79): the per-RD `MetadataCreated=True`
// Status Condition (Phase 11.3 Stage 1) caches "this RD has had
// create-md before". But the actual `drbdmeta create-md` is
// per-volume — when `linstor vd c <rd> 1G` adds a new volume to an
// existing RD, the Condition is True yet the new volume's lower
// disk carries no metadata. Without this helper the subsequent
// `drbdadm adjust` brings the new volume up as kernel disk:Diskless
// while Spec.Flags lacks DISKLESS (the verbatim "Unintentional
// Diskless" surface from `linstor r l`).
//
// Renders .res before probing so drbdadm dump-md / create-md can
// resolve the new volume's lower disk path. renderResFile is
// content-idempotent (Bug 315) so the redundant call when the FSM
// dispatch's renderResFile preamble runs afterwards is a stat+
// compare no-op.
//
// Per-volume scoping is the SAFETY invariant: a bare
// `drbdadm create-md --force <rd>` would walk every volume and
// wipe vol-0's existing GI + bitmap state (the W09 disk-replace
// safety guard exists for exactly this reason). We pass
// `<rd>/<volNumber>` so drbdadm targets only the missing volume.
//
// Callers MUST gate this on "the legacy firstActivation predicate
// would skip create-md" (MetadataCreated=True OR `.md-created`
// marker present). On a true first activation the existing
// createMetadata path handles every volume via a single RD-scoped
// call; running this helper in that branch duplicates work and
// races the FSM dispatch's bring-up shape.
//
// Skipped on diskless replicas (no lower disk to stamp). Errors
// bubble up — a stuck per-vol create-md must surface, not silently
// degrade to Diskless.
func (r *Reconciler) ensurePerVolumeMetadata(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, diskless bool) error {
	if diskless {
		return nil
	}

	if r.cfg.Adm == nil {
		return nil
	}

	// Render .res first — drbdadm dump-md / create-md need an
	// up-to-date .res to resolve the per-volume lower disk path.
	// Content-idempotent on converged state.
	err := r.renderResFile(ctx, dr, devices)
	if err != nil {
		return err
	}

	for _, vol := range dr.GetVolumes() {
		target := fmt.Sprintf("%s/%d", dr.GetName(), vol.GetVolumeNumber())

		hasMD, probeErr := r.cfg.Adm.HasMD(ctx, target)
		if probeErr != nil {
			return errors.Wrapf(probeErr, "dump-md %s", target)
		}

		if hasMD {
			continue
		}

		createErr := r.cfg.Adm.CreateMD(ctx, target)
		if createErr != nil {
			return errors.Wrapf(createErr, "create-md %s", target)
		}
	}

	return nil
}

// maybeStampMetadata is the create-md decision branch lifted out of
// applyDRBD so the orchestrator stays under the gocyclo budget.
// Routes the fresh-replica first-activation path through
// createMetadata (Phase 11.2.c Stage 3a) and the diskless→diskful
// flip path through ensureMetadata(..., firstActivation=false)
// (Bug 319 invariant: re-stamp metadata WITHOUT GI-seed, since the
// kernel slot is already handshaken via the diskless path).
//
// Pure dispatch — every reachable mutation is one of the two
// helpers' existing side-effects. No-op when diskless, or when
// neither firstActivation nor diskfulFlip fires.
func (r *Reconciler) maybeStampMetadata(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, mdMarkerPath string, diskless, firstActivation, diskfulFlip bool) error {
	if diskless {
		return nil
	}

	if firstActivation && !diskfulFlip {
		return r.createMetadata(ctx, dr, devices)
	}

	if diskfulFlip {
		return r.ensureMetadata(ctx, dr, devices, mdMarkerPath, false)
	}

	return nil
}

// createMetadata runs drbdadm create-md + per-peer drbdmeta set-gi
// + writes the .md-created file marker + stamps the MetadataCreated
// Condition. Idempotent re-entry: if drbdadm dump-md already shows
// metadata, skips create-md but still seeds set-gi for any peer
// slots without a matching GI line (Bug 319 invariant).
//
// Caller must have already verified firstActivation==true. The
// helper does NOT re-check the gate — moving it inside would
// change ordering vs adjust later in applyDRBD. The
// MetadataCreated Status-Condition stamp lives INSIDE this helper
// so the caller doesn't need to know about the stamper plumbing;
// any per-call .md-created marker path math is also internal.
//
// Phase 11.2.c Stage 3a: pure extract, no behaviour change. Stage 3b
// will FSM-shadow-dispatch this helper at the top of applyDRBD,
// mirror of the renderResFile shadow landed in Stage 2.
func (r *Reconciler) createMetadata(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string) error {
	mdMarkerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".md-created")

	return r.ensureMetadata(ctx, dr, devices, mdMarkerPath, true)
}

// runAutoPromote orchestrates the first-activation seed:
//
//  1. `drbdadm primary --force` — promote out of Inconsistent so the
//     kernel accepts writes.
//  2. RG-driven `mkfs.<type>` (scenario 9.W14) — runs ONLY while we
//     hold Primary; mkfs on a Secondary deadlocks on EROFS.
//  3. `drbdadm secondary` — demote so the consumer (CSI / external
//     mounter) can promote at its own discretion.
//
// Pulled out of applyDRBD so the orchestration function stays under
// the project's gocyclo budget.
func (r *Reconciler) runAutoPromote(ctx context.Context, dr *intent.DesiredResource) error {
	err := r.cfg.Adm.PrimaryForce(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "auto-primary %s", dr.GetName())
	}

	err = r.runAutoMkfs(ctx, dr)
	if err != nil {
		return errors.Wrapf(err, "auto-mkfs %s", dr.GetName())
	}

	err = r.cfg.Adm.Secondary(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "auto-secondary %s", dr.GetName())
	}

	return nil
}

// runAutoMkfs handles the RG-driven auto-mkfs path of scenario
// 9.W14. The controller folds `FileSystem/Type` (and the optional
// `FileSystem/MkfsParams`) from the RG's effective props into the
// per-RD wire Props map; the satellite consumes them here on the
// primary replica.
//
// Idempotency has two layers:
//
//  1. A per-RD `<rd>.mkfs.done` marker under StateDir (sibling to
//     `.md-created`) records the durable "we already finished mkfs
//     for every diskful volume" state. Cheap stat-only fast path.
//  2. Per-volume `blkid -o export /dev/drbd<minor>` probe (mirroring
//     upstream LINSTOR's `MkfsUtils.hasFileSystem`). When a volume
//     already carries a filesystem we skip mkfs on that volume and
//     adopt the existing fs — exactly upstream's behaviour. This
//     closes Bug 311: a previous reconcile that dropped `.md-created`
//     but failed to write `.mkfs.done` (e.g. `drbdadm primary
//     --force` raced the initial-sync handshake and returned a
//     transient error) would otherwise permanently skip mkfs on
//     subsequent passes, since firstActivation goes false. The new
//     retry gate in finishDRBDApply re-enters this function; the
//     blkid probe makes that retry safe even on a volume that was
//     partially mkfs'd before the failure.
//
// SAFETY: mkfs on a populated filesystem silently destroys data. The
// blkid probe is what protects an already-formatted volume from
// double-mkfs when the marker file is absent (manual `rm`, host
// rebuild that wipes /etc/drbd.d). DeleteResource removes the marker
// together with `.res` / `.md-created` so a re-created RD with the
// same name correctly mkfs-s again — the blkid probe sees an empty
// (freshly-carved) volume and lets mkfs run.
func (r *Reconciler) runAutoMkfs(ctx context.Context, dr *intent.DesiredResource) error {
	fsType := strings.TrimSpace(dr.GetProps()["FileSystem/Type"])
	if fsType == "" {
		return nil
	}

	if r.cfg.Exec == nil || r.cfg.StateDir == "" {
		// No exec wrapper or no state dir → can't run mkfs / can't
		// drop a marker. Skip rather than fail; production always
		// wires both. The unit test that pins the no-Exec branch
		// would otherwise need to mock half a Reconciler.
		return nil
	}

	markerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".mkfs.done")

	// Phase 11.3 Stage 2: Condition-first fast-path. When the
	// dispatcher already observed `FilesystemFormatted=True` on the
	// Resource CRD, every diskful volume of this RD has already
	// passed the auto-mkfs path (either freshly mkfs'd or adopted
	// via blkid) and we can skip the per-volume blkid round-trip
	// entirely. Belt-and-braces: a stale Condition still leaves the
	// blkid probe per-volume below as the authoritative safety net
	// against double-mkfs — but with the Condition set we never
	// reach that branch.
	if dr.GetFilesystemFormatted() {
		return nil
	}

	_, statErr := os.Stat(markerPath)
	if statErr == nil {
		// Marker present → mkfs already ran on a previous activation.
		// Re-running would wipe a populated filesystem. File-marker
		// fallback for the migration window: clusters upgraded from
		// pre-11.3 Stage 2 may have a populated marker but no
		// Condition stamped yet.
		return nil
	}

	minor, _ := strconv.Atoi(dr.GetDrbdOptions()["minor"])

	args := []string{}

	if extra := strings.TrimSpace(dr.GetProps()["FileSystem/MkfsParams"]); extra != "" {
		args = append(args, strings.Fields(extra)...)
	}

	for _, vol := range dr.GetVolumes() {
		device := fmt.Sprintf("/dev/drbd%d", minor+int(vol.GetVolumeNumber()))

		if r.deviceHasFilesystem(ctx, device) {
			// Volume already carries a filesystem. Two cases land here:
			// (a) a previous reconcile mkfs'd this volume but crashed
			// before writing the marker — adopt the fs and continue;
			// (b) the operator manually formatted the device — same
			// treatment. Matches upstream LINSTOR's MkfsUtils.
			// makeFileSystemOnMarked which short-circuits on a
			// non-empty hasFileSystem result.
			continue
		}

		cmdArgs := append(slices.Clone(args), device)

		_, err := r.cfg.Exec.Run(ctx, "mkfs."+fsType, cmdArgs...)
		if err != nil {
			return errors.Wrapf(err, "mkfs.%s %s", fsType, device)
		}
	}

	err := os.WriteFile(markerPath, nil, resFilePerm)
	if err != nil {
		return errors.Wrapf(err, "write %s", markerPath)
	}

	// Phase 11.3 Stage 2: stamp the `FilesystemFormatted=True` Status
	// Condition on the parent Resource CRD. Belt-and-braces with the
	// file marker write above: future reconciles read the Condition
	// first to short-circuit the auto-mkfs path, falling back to the
	// file presence only when the Condition is absent (cluster
	// upgraded from a pre-11.3-Stage-2 build). Stamp failure does NOT
	// fail the apply — the file marker is the transitional source of
	// truth, so a transient apiserver hiccup here just defers
	// Condition stamping to the next reconcile.
	if r.cfg.FilesystemFormattedStamper != nil {
		stampErr := r.cfg.FilesystemFormattedStamper.StampFilesystemFormatted(ctx, dr.GetName())
		if stampErr != nil {
			log.FromContext(ctx).Error(stampErr, "stamp FilesystemFormatted Condition; will retry next reconcile",
				"resource", dr.GetName())
		}
	}

	return nil
}

// deviceHasFilesystem reports whether the given DRBD device already
// carries a recognised filesystem. Wraps `blkid -o export <device>`
// the same way upstream LINSTOR's MkfsUtils.hasFileSystem does:
// presence of a `TYPE=` line in the export-format output means the
// kernel's libblkid detected a known filesystem signature. blkid's
// exit-2 (no signature found) is folded into the FakeExec /
// RealExec "non-zero exit → wrapped error" contract; we treat that
// as "no filesystem" rather than propagating the error because the
// caller's only sensible response is exactly the same: skip mkfs on
// a populated volume, run it on an empty one.
//
// A real I/O failure (device gone, kernel returned EIO) also lands
// in the error branch, but the subsequent mkfs.<type> on the same
// device would fail just as loudly with a more actionable message
// ("No such file or directory" / "Input/output error"), so the
// fall-through to mkfs preserves the failure mode operators
// already expect.
func (r *Reconciler) deviceHasFilesystem(ctx context.Context, device string) bool {
	out, err := r.cfg.Exec.Run(ctx, "blkid", "-o", "export", device)
	if err != nil {
		// Treat any blkid failure as "no recognised filesystem". The
		// most common shape is exit-code 2 (no signature) which
		// RealExec wraps into a generic error — the caller's only
		// sensible reaction is to run mkfs, which is what the
		// no-filesystem branch already does.
		return false
	}

	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "TYPE=") {
			return true
		}
	}

	return false
}

// runApplyDRBDVerb is the per-reconcile dispatch for the bring-up
// chain. First activation falls through to the SkipDisk-aware
// `drbdadm adjust` (or `adjust --skip-disk`): the .res + freshly-
// created metadata are the canonical bring-up path on master and
// existing tests pin that behaviour. Bug 319 (diskless→diskful
// flip) also routes through this arm via `diskfulFlip=true` so the
// compare_volume attach_cmd schedule has a chance to fire on a
// freshly re-stamped lower disk — even though firstActivation is
// false on the flip (metadata pre-existed from the diskless slot).
//
// The historical steady-state arm (`runBringUpOrAdjust` — kernel-
// state probe + `drbdadm up` / `drbdadm adjust` dispatch) has been
// retired in Phase 11.2.c Stage 4 step 3. The FSM shadow-dispatch
// at the top of applyDRBD now owns ActionAdjust / ActionAdjust
// SkipDisk for every non-firstActivation, non-flip pass. See the
// inline Why-comment on the steady-state branch below.
//
// Split out of applyDRBD so the orchestration function stays under
// the gocyclo budget.
func (r *Reconciler) runApplyDRBDVerb(ctx context.Context, dr *intent.DesiredResource, firstActivation, diskfulFlip bool) error {
	// Bug 319 flip stays on the legacy adjustResource call until step
	// 4 (retire createMetadata legacy) lands and the FSM owns the
	// flip transition end-to-end. The flip routes here with
	// firstActivation=false (metadata pre-existed from the diskless
	// slot) but still needs `drbdadm adjust` to fire so drb-utils'
	// compare_volume schedules attach_cmd on the freshly re-stamped
	// lower disk. The diskfulFlip=true argument suppresses the Bug
	// 280 kernel-Diskless → --skip-disk coercion inside runAdjust so
	// the attach can land.
	if firstActivation || diskfulFlip {
		return r.adjustResource(ctx, dr, diskfulFlip)
	}

	// Phase 11.2.c Stage 4 step 3: legacy runBringUpOrAdjust /
	// r.adjustResource call retired on the steady-state arm.
	//
	// Why: the FSM shadow-dispatch at the top of applyDRBD owns
	// ActionAdjust / ActionAdjustSkipDisk. The renderResFile preamble
	// inside dispatchFsmAction's adjust arm (Stage 4 step 1) ensures
	// .res is current before drbdadm adjust runs, so kernel state
	// matches the declarative spec without a second pass through this
	// site.
	//
	// drbdadm adjust is the canonical "make kernel match .res" verb —
	// naturally idempotent (Bug-287 fallback inside runAdjust still
	// re-attempts via drbdadm up on `(158) Unknown resource`). We do
	// not add any additional closed-loop recovery here; DRBD-9's own
	// resync / auto-promote logic owns post-adjust convergence, and
	// the Bug 47 / scenario 5.32 operator-down recovery loop is
	// closed by the FSM's Phase==MetadataReady → ActionUp transition
	// (retired in step 2) which observes the unloaded slot ahead of
	// any adjust attempt.
	//
	// Step 4 (retire createMetadata legacy) is the final step and
	// will also collapse the firstActivation + diskfulFlip arms
	// above into FSM dispatch once the ActionCreateMd → ActionAdjust
	// transition is wired for the flip phase.
	return nil
}

// bringUpResource runs `drbdadm up <name>` to load the kernel slot
// from the rendered .res file. Caller has already ensured .res
// exists (renderResFile) and create-md has run for diskful replicas
// (createMetadata); this helper is the third bring-up verb in the
// per-reconcile sequence, distinct from `adjustResource` which
// reconciles already-loaded kernel state.
//
// Bug 319 re-entry: if the operator flipped Spec.Flags Diskless→
// diskful but the on-disk `.md-created` marker (or
// `MetadataCreated=True` Status Condition) says metadata was
// already laid down, the kernel will refuse to up because there's
// no metadata on the LV. Detection of that flip lives at the gate
// level in applyDRBD (suppress firstActivation, force re-entry to
// createMetadata) — bringUpResource itself is ONLY the
// `drbdadm up <name>` invocation + error wrapping.
//
// The Bug-287 `(158) Unknown resource` fallback to `drbdadm up`
// inside `runAdjust` is a distinct call site and intentionally
// stays inline: it's the recovery verb in the half-torn
// kernel-slot window, not the first-load path, and its error
// wrap ("drbdadm up %s (after adjust 158 fallback)") needs to
// preserve that context.
//
// Phase 11.2.c Stage 3c: pure extract, no behaviour change. Stage 3d
// (or later) will FSM-shadow-dispatch this helper for ActionUp
// transitions at the top of applyDRBD, mirror of the renderResFile
// (Stage 2), createMetadata (Stage 3a), and adjustResource (Stage
// 3b) shadows.
func (r *Reconciler) bringUpResource(ctx context.Context, dr *intent.DesiredResource) error {
	if err := r.cfg.Adm.Up(ctx, dr.GetName()); err != nil {
		return errors.Wrapf(err, "drbdadm up %s", dr.GetName())
	}

	return nil
}

// adjustResource runs `drbdadm adjust <name>` with the right
// SkipDisk coercion: bare adjust when neither the operator prop nor
// kernel state asks for skip-disk, `--skip-disk` form when either
// signal is present. The SkipDisk arm covers both Bug 280 (kernel
// Diskless without operator prop) and operator-pinned downgrade
// (scenario 5.11).
//
// Idempotent: `drbdadm adjust` is the canonical "make kernel state
// match .res" call; safe to re-run. The caller has already ensured
// .res exists (renderResFile) and create-md has run if
// firstActivation (createMetadata). The Bug-287 fallback to
// `drbdadm up` for the `(158) Unknown resource` race lives inside
// the helper so callers don't have to know about the half-torn
// kernel-slot window.
//
// Gate computation (SkipDisk prop check + kernel-Diskless probe)
// stays inside the helper — it determines which variant to run and
// the caller must not pass that decision. `diskfulFlip` is an
// input (not an internal probe) because Bug 319 needs to suppress
// the Diskless-probe-driven SkipDisk coercion on the
// diskless→diskful transition where compare_volume must see the
// kern->disk=="none" + conf->disk path diff to schedule attach_cmd.
//
// Phase 11.2.c Stage 3b: pure extract, no behaviour change. Stage 3c
// (or later) will FSM-shadow-dispatch this helper for ActionAdjust
// and ActionAdjustSkipDisk transitions at the top of applyDRBD,
// mirror of the renderResFile (Stage 2) and createMetadata (Stage
// 3a) shadows.
func (r *Reconciler) adjustResource(ctx context.Context, dr *intent.DesiredResource, diskfulFlip bool) error {
	return r.runAdjust(ctx, dr, diskfulFlip)
}

// runAdjust dispatches to the plain `drbdadm adjust` or the
// `--skip-disk` variant based on the `DrbdOptions/SkipDisk` prop
// (scenario 5.11).
//
// Bug 280 (P1): the prop-only gate races the observer's
// SkipDisk-stamp path. When an operator runs `drbdadm detach
// --force` against the satellite shell:
//
//  1. Kernel transitions UpToDate → Diskless and emits
//     `change device disk:Diskless` on events2.
//  2. The observer's UpToDate→Diskless gate writes
//     `DrbdOptions/SkipDisk=True` onto Spec.Props.
//  3. The Diskless event also causes a Status update which fires a
//     parallel reconcile.
//
// A reconcile already in flight when the operator's command landed
// loaded `res` from the watch cache BEFORE the prop write hit the
// apiserver. Its `dr.Props` view has SkipDisk absent, the
// prop-only gate dispatches plain `drbdadm adjust`, and the disk
// re-attaches in sub-second — the operator's poll never observes
// Diskless.
//
// Probe the kernel directly via `HasDisklessVolume`: the kernel is
// the authority on the disk's current state, independent of any
// apiserver cache trail. When the kernel reports Diskless on a
// slot that's already loaded (so we're past first activation), we
// coerce the adjust onto `--skip-disk` regardless of the prop's
// cache visibility. The operator's SkipDisk-stamp is a hint that
// will catch up via the apiserver; the kernel probe closes the
// race window in the meantime.
//
// Errors from the probe fall through to the prop-only gate (the
// pre-Bug-280 behaviour) so a transient netlink hiccup doesn't
// strand the reconciler.
//
// Bug 287 / scenario 5.32 race: even when the FSM `KernelLoaded`
// observation reads as true (or when this path runs on first
// activation), the kernel slot can be torn down between the probe
// and the `drbdadm adjust` shell-out — that's the half-torn window
// right after an operator's `drbdadm down` finishes its kernel-side
// teardown. `drbdadm adjust` in that state issues
// `drbdsetup new-minor` without `new-resource` first and bails with
// `Failure: (158) Unknown resource`. Catch that exact error string,
// fall back to `drbdadm up <rsc>` (which always issues
// new-resource + new-minor + attach + connect), and let the next
// reconcile re-converge.
func (r *Reconciler) runAdjust(ctx context.Context, dr *intent.DesiredResource, diskfulFlip bool) error {
	skipDisk := isSkipDiskEnabled(dr)

	// Bug 319: on the diskless→diskful Spec flag flip we DELIBERATELY
	// want plain `drbdadm adjust` to attach the freshly create-md'd
	// lower disk via drb-utils' compare_volume (kern->disk=="none" +
	// conf->disk path diff schedules attach_cmd). Coercing
	// `--skip-disk` here — which Bug 280's kernel probe would
	// otherwise do because the kernel still reports Diskless — would
	// suppress exactly the attach we just created the metadata for.
	if !skipDisk && !diskfulFlip {
		diskless, probeErr := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
		if probeErr == nil && diskless {
			skipDisk = true
		}
	}

	// Bug 278: Talos kernel upgrade leaves SkipDisk pinned from the
	// pre-upgrade defensive stamp. Upstream LINSTOR's SkipDisk is
	// operator-only; we stamped it defensively (Phase 11.3 territory
	// — Failed→Diskless trigger in the observer) and now must
	// un-stamp when the kernel re-emerges healthy after the satellite
	// reattaches.
	//
	// Why this isn't "auto-recovery beyond upstream": SkipDisk on a
	// healthy slot is an artifact OF our defensive stamping (the
	// observer's writeSkipDiskProp under observerSkipDiskFieldOwner).
	// Removing our own stamp is symmetric with stamping it — not new
	// behavior. Operator-set SkipDisk (via `linstor r prop set ...
	// SkipDisk=True` on the controller's FieldOwner) survives the
	// SSA release because the observer's owner only ever claimed its
	// own apply, not the operator's. DRBD's own resync /
	// auto-promote logic owns post-adjust convergence; we do not
	// add a closed-loop recovery here — the clear is a one-shot SSA
	// release that lets the existing FSM transition
	// (PhaseSkipDisk→PhaseRunning on !obs.SkipDiskProp) fire on the
	// next reconcile.
	//
	// Gate: SkipDisk-from-prop AND kernel NOT in Diskless state
	// (HasDisklessVolume==false). The diskful-flip arm doesn't
	// reach this since `!skipDisk && !diskfulFlip` is the only path
	// that flips skipDisk via the kernel probe — and we explicitly
	// scope the clear to the "prop-set" origin so a freshly probed
	// kernel-Diskless does NOT trigger a clear. We also gate on
	// !diskfulFlip so the Bug 319 flip path (kernel still Diskless,
	// SkipDisk prop unset) never enters the clear path.
	if isSkipDiskEnabled(dr) && !diskfulFlip && r.cfg.SkipDiskClearer != nil {
		diskless, probeErr := r.cfg.Adm.HasDisklessVolume(ctx, dr.GetName())
		if probeErr == nil && !diskless {
			// Kernel reports the local volume as non-Diskless (UpToDate
			// / Inconsistent / Outdated — all are "backing storage
			// attached"). SkipDisk on the prop is an artifact of the
			// pre-upgrade defensive stamp; release the observer's SSA
			// claim so the next dispatcher cycle re-resolves Spec.Props
			// without SkipDisk and the next reconcile dispatches plain
			// `drbdadm adjust`.
			//
			// Best-effort: a clearer error doesn't strand the reconciler
			// (the worst case is the same as not having the clearer at
			// all — the prop stays pinned and the next pass re-tries).
			_ = r.cfg.SkipDiskClearer.ClearSkipDisk(ctx, dr.GetName())
		}
	}

	var err error

	if skipDisk {
		err = r.cfg.Adm.AdjustSkipDisk(ctx, dr.GetName())
	} else {
		err = r.cfg.Adm.Adjust(ctx, dr.GetName())
	}

	if err == nil {
		return nil
	}

	// Recover from the Bug-287 race: the kernel slot the probe just
	// saw vanished before adjust ran. `drbdadm up` is the only verb
	// that bootstraps a missing slot from a valid .res + on-disk
	// metadata; surface its error directly so the reconciler retry
	// loop can re-converge if up also fails.
	if isUnknownResourceErr(err) {
		upErr := r.cfg.Adm.Up(ctx, dr.GetName())
		if upErr != nil {
			return errors.Wrapf(upErr, "drbdadm up %s (after adjust 158 fallback)", dr.GetName())
		}

		return nil
	}

	return errors.Wrapf(err, "adjust %s", dr.GetName())
}

// isUnknownResourceErr reports whether a drbdadm error is the
// `(158) Unknown resource` failure mode — adjust saw the kernel
// slot vanish between the satellite's probe and adjust's own
// `drbdsetup new-minor` shell-out (Bug 287 / scenario 5.32 race).
// We grep the wrapped error text rather than introducing a typed
// errno because drbdadm surfaces 158 via a textual message; the
// caller's wrap chain already preserves the verbatim stderr from
// `pkg/storage/exec.go`.
//
// Bug 291 (P1): the original predicate also accepted the bare
// substring `"unknown resource"` (case-sensitive but unanchored)
// as a fallback. That substring appears verbatim in DRBD's
// `additional info from kernel: unknown resource` diagnostic — but
// also in unrelated drbdsetup errors (`drbdsetup new-path …
// unknown resource`, `drbdsetup detach … unknown resource`, even
// LINSTOR's `ApiCallRc: unknown resource <name>` when the rest
// adapter surfaces a not-found through the same wrap chain). Any
// of those false-positive matches triggers an unconditional
// `drbdadm up`, which races a partial teardown and leaves kernel
// state half-up; the next reconcile pass loops on the same
// failure mode while peers stay Connecting/StandAlone. Tightened
// to a single canonical regex anchored on the `(158)` errno + the
// `Unknown resource` verb drbdadm-9 emits (verified verbatim
// against `drbdadm adjust` on a slot-less resource).
//
// Phase 11.4.b P1: delegates to the package-level numeric exit-code
// parser (`pkg/drbd.IsErrCode`) so future call sites can switch on
// stable drbdsetup err numbers instead of duplicating ad-hoc string
// regexes. Behaviour is preserved — the previous regex anchored on
// `(158) Unknown resource` and the numeric predicate anchors on the
// `(158)` errno; existing test fixtures cover both.
func isUnknownResourceErr(err error) bool {
	return drbd.IsUnknownResourceErr(err)
}

// seedInitialGi pre-stamps each diskful volume's freshly-created
// DRBD metadata block with the GI the controller picked from an
// UpToDate peer (Phase 8.1). When SeedFromGi is empty (fresh
// cluster, no peer to seed from) the volume is skipped — DRBD will
// fall through to the full initial-sync on first connect, which is
// the acceptable cost for the first replica in a new RD.
//
// Must be called between create-md (which writes the metadata
// block this then mutates) and drbdadm adjust (which reads the
// metadata into kernel state).

func (r *Reconciler) seedInitialGi(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string) error {
	// peerNodeIDs is the deterministic peer-name → DRBD node-id map
	// the dispatcher already materialised onto DrbdOptions["peer.
	// <name>.node-id"] from each peer's controller-allocated
	// Status.DRBDNodeID. Reading from the same source the .res
	// renderer consumes guarantees both satellites stamp the same
	// per-peer bitmap slots even when their reconciles race the
	// fresh-allocation window.
	peerNodeIDs := peerNodeIDsFromOpts(dr)

	for _, vol := range dr.GetVolumes() {
		device := devices[vol.GetVolumeNumber()]
		if device == "" {
			continue
		}

		seed, ok := r.resolveSeedGi(dr.GetName(), vol)
		if !ok {
			continue
		}

		err := r.seedPerPeerGi(ctx, dr, vol, device, seed, peerNodeIDs)
		if err != nil {
			return err
		}
	}

	return nil
}

// seedPerPeerGi stamps the day0/peer GI tuple into every peer's
// bitmap slot for one (resource, volume) pair, AND into the local
// node's own current_uuid slot. DRBD 9.2+ stores current/bitmap
// UUIDs per-peer (one slot per peer node-id), and the kernel's
// `self` UUID surfaced during the GI handshake comes from the
// LOCAL node-id slot — so skipping the full initial-sync requires
// `drbdmeta set-gi --node-id <X>` to run once for EVERY node-id
// in the resource (local + all peers) with the same day0 tuple.
//
// Bug 284: when a fresh diskful replica's reconcile races the
// peer Resource's creation (sequential `linstor r create N1 RD`
// then `r create N2 RD`), `dr.GetPeers()` may be empty or only
// contain a DISKLESS tiebreaker at the moment seedInitialGi runs.
// Stamping only the peer slots leaves the local current_uuid as
// the random value `drbdadm create-md` generated. When the peer
// later joins and connects, the handshake compares its local
// (day0) current_uuid against ours (random) → `uuid_compare()=
// unrelated-data by rule=history-both` → `Unrelated data,
// aborting!` → permanent StandAlone. Stamping the local slot
// fixes the asymmetric-create case (mirrors upstream LINSTOR's
// `DrbdLayer.createMetaData` loop over `nodeId=0..NODE_ID_MAX`).
//
// Returns the first non-nil error from drbdmeta. The "requires
// --node-id" failure mode the legacy single-call form hit on DRBD
// 9.2+ is now structurally unreachable: every call carries
// `--node-id <X>`.
func (r *Reconciler) seedPerPeerGi(ctx context.Context, dr *intent.DesiredResource, vol *intent.DesiredVolume, device, seed string, peerNodeIDs map[string]int32) error {
	// Stamp the local node-id's slot FIRST so the local
	// current_uuid carries day0 even when no peers are visible yet
	// at apply time (sequential-create race, Bug 284). The peer
	// loop below adds the remaining slots; both sides converge on
	// the same day0 tuple regardless of which side's reconcile
	// runs first.
	if localID, ok := localNodeIDFromOpts(dr); ok {
		err := r.cfg.Adm.SetGi(ctx, dr.GetName(), vol.GetVolumeNumber(), device, localID, seed)
		if err != nil {
			return errors.Wrapf(err, "set-gi vol %d local (node-id %d)",
				vol.GetVolumeNumber(), localID)
		}
	}

	for _, peer := range dr.GetPeers() {
		peerID, ok := peerNodeIDs[peer]
		if !ok {
			// Controller-side allocator hasn't stamped this peer's
			// Status.DRBDNodeID yet — waitForControllerAllocation
			// SHOULD have gated apply, but be defensive: skip the
			// per-peer seed for this peer rather than emit a
			// bogus --node-id=0 that would collide with the local
			// slot. Next reconcile (driven by the peer's status
			// update event) will retry with a real id.
			continue
		}

		err := r.cfg.Adm.SetGi(ctx, dr.GetName(), vol.GetVolumeNumber(), device, peerID, seed)
		if err != nil {
			return errors.Wrapf(err, "set-gi vol %d peer %s (node-id %d)",
				vol.GetVolumeNumber(), peer, peerID)
		}
	}

	return nil
}

// localNodeIDFromOpts extracts this satellite's own DRBD node-id
// from the DesiredResource's flat DrbdOptions bag. The dispatcher
// writes `node-id` (no peer prefix) for the target replica from
// `Resource.Status.DRBDNodeID`. Returns ok=false when the entry is
// missing / malformed — callers then skip the local-slot stamp;
// DRBD falls through to a real initial-sync, slow but correct.
func localNodeIDFromOpts(dr *intent.DesiredResource) (int32, bool) {
	raw, ok := dr.GetDrbdOptions()["node-id"]
	if !ok || raw == "" {
		return 0, false
	}

	id, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false
	}

	return int32(id), true
}

// peerNodeIDsFromOpts extracts the peer-name → DRBD-node-id map
// from a DesiredResource's flat DrbdOptions bag. The keys are the
// `peer.<name>.node-id` entries dispatcher.BuildDesired's
// addPeerEntries populates from each peer's
// Status.DRBDNodeID. Bad / missing values are skipped (the caller
// then leaves that peer's bitmap slot unseeded; DRBD falls through
// to a real initial-sync on first connect with that peer — slow
// but correct).
func peerNodeIDsFromOpts(dr *intent.DesiredResource) map[string]int32 {
	opts := dr.GetDrbdOptions()
	peers := dr.GetPeers()

	out := make(map[string]int32, len(peers))

	for _, peer := range peers {
		raw, ok := opts["peer."+peer+".node-id"]
		if !ok || raw == "" {
			continue
		}

		id, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			continue
		}

		out[peer] = int32(id)
	}

	return out
}

// resolveSeedGi decides what GI to stamp on a fresh replica's
// metadata block:
//
//   - Controller-supplied SeedFromGi wins. That's the Phase 8.1
//     "copy from an existing UpToDate peer" path — the GI is the
//     real CurrentGi of the peer, so DRBD's handshake sees a true
//     match and skips initial-sync.
//
//   - Otherwise, when the backing provider is guaranteed to hand
//     back zero-initialised storage (thin LVM, thin or thick ZFS,
//     sparse file — see IsThinOrZFS), synthesise a deterministic
//     per-RD, per-volume day0 GI. Same RD name + volume number
//     produces the same GI on every node so DRBD's GI handshake
//     matches and skips initial-sync even when no peer has been
//     stamped UpToDate yet. Mirrors upstream LINSTOR's
//     `DrbdLayerUtils.skipInitSync` short-circuit (Bug 77).
//
//   - Otherwise (thick LVM, opaque file, unknown provider, DISKLESS),
//     return ok=false. DRBD then falls through to the full
//     initial-sync on first connect, which is the only safe
//     behaviour when the backing storage may carry pre-existing
//     bytes the peer doesn't have.
func (r *Reconciler) resolveSeedGi(resourceName string, vol *intent.DesiredVolume) (string, bool) {
	if seed := vol.GetSeedFromGi(); seed != "" {
		return seed, true
	}

	provider, ok := r.cfg.Providers[vol.GetStoragePool()]
	if !ok || provider == nil {
		return "", false
	}

	if !IsThinOrZFS(provider.Kind()) {
		return "", false
	}

	return day0GiFor(resourceName, vol.GetVolumeNumber()), true
}

// providerForResource resolves the provider that owns the named
// resource using the in-memory pool map. Returns an error when the
// resource isn't known or its pool isn't registered.
func (r *Reconciler) providerForResource(name string) (storage.Provider, error) {
	r.mu.Lock()
	pool, ok := r.resourceToPool[name]
	r.mu.Unlock()

	if !ok {
		return nil, errors.Errorf("resource %q not known on this satellite", name)
	}

	provider, ok := r.cfg.Providers[pool]
	if !ok {
		return nil, errors.Errorf("storage pool %q not registered", pool)
	}

	return provider, nil
}

// rememberPool records the pool that backs a resource, so subsequent
// snapshot RPCs can route to the right provider. Multi-pool resources
// are not yet a thing — we record the first volume's pool only.
func (r *Reconciler) rememberPool(resourceName, pool string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resourceToPool[resourceName] = pool
}

// buildResFile assembles a drbd.Resource from dr's flat option map.
// The proto carries DRBD config as a string→string map for now (the
// schema solidifies once the controller-side autoplacer feeds it); we
// honour the documented keys: `port`, `node-id`, `address`, `minor` for
// the local node, and `peer.<name>.{port,node-id,address}` per peer.
//
// localAddr is the satellite's own IP — when the controller-supplied
// `address` is the placeholder "0.0.0.0" we substitute localAddr so
// drbd-9 has a real interface to bind to.
//
// devices is volNumber → DevicePath; when present we use it as the
// disk path. Empty / missing → fall back to the LVM/ZFS-shaped
// `/dev/<pool>/<rd>_<vol>` guess, which is what works for those
// providers.
func buildResFile(dr *intent.DesiredResource, localNode, localAddr string, devices map[int32]string) (string, error) {
	opts := dr.GetDrbdOptions()
	port, _ := strconv.Atoi(opts["port"])
	nodeID, _ := strconv.Atoi(opts["node-id"])
	minor, _ := strconv.Atoi(opts["minor"])

	hosts := make([]drbd.Host, 0, 1+len(dr.GetPeers()))
	hosts = append(hosts, drbd.Host{
		NodeName: localNode,
		Address:  resolveAddr(opts["address"], localAddr),
		Port:     port,
		NodeID:   nodeID,
		IsLocal:  true,
		Diskless: isDiskless(dr.GetFlags()),
	})

	for _, peer := range dr.GetPeers() {
		peerPort, _ := strconv.Atoi(opts["peer."+peer+".port"])
		peerNodeID, _ := strconv.Atoi(opts["peer."+peer+".node-id"])

		hosts = append(hosts, drbd.Host{
			NodeName: peer,
			Address:  resolveAddr(opts["peer."+peer+".address"], ""),
			Port:     peerPort,
			NodeID:   peerNodeID,
			Diskless: opts["peer."+peer+".diskless"] == drbdBoolPropTrue,
		})
	}

	vols := buildResVolumes(dr, devices, minor)

	sections := splitDRBDOptions(opts)

	out, err := drbd.Build(drbd.Resource{
		Name:        dr.GetName(),
		Net:         drbd.Net{ProtocolC: true, Options: sections.Net},
		Hosts:       hosts,
		Volumes:     vols,
		Options:     sections.Resource,
		Disk:        sections.Disk,
		Handlers:    sections.Handlers,
		PeerDevice:  sections.PeerDevice,
		Connections: buildResConnections(dr),
	})
	if err != nil {
		return "", errors.Wrap(err, "drbd.Build")
	}

	return out, nil
}

// buildResConnections translates the DesiredResource's logical
// connection overrides (scenario 3.7 multi-path) into the .res
// renderer's drbd.ResourceConnection shape. Empty input returns nil
// — the renderer then falls back to the default single-host-pair
// connection block.
func buildResConnections(dr *intent.DesiredResource) []drbd.ResourceConnection {
	src := dr.GetConnections()
	if len(src) == 0 {
		return nil
	}

	out := make([]drbd.ResourceConnection, 0, len(src))

	for _, conn := range src {
		paths := make([]drbd.ResourcePath, 0, len(conn.Paths))

		for _, p := range conn.Paths {
			paths = append(paths, drbd.ResourcePath{
				Name:     p.Name,
				AddressA: p.AddressA,
				AddressB: p.AddressB,
			})
		}

		out = append(out, drbd.ResourceConnection{
			NodeA: conn.NodeA,
			NodeB: conn.NodeB,
			Paths: paths,
		})
	}

	return out
}

// buildResVolumes turns the per-RD DesiredVolumes into the
// drbd.Volume slice the .res renderer consumes. Pulled out of
// buildResFile to keep that function under the funlen budget.
//
// `minor` is the base /dev/drbd<N> minor for the resource; each
// volume offsets from it (volumeNumber 0 → minor, vol 1 → minor+1,
// …). The disk path follows applyStorage's devices map when set
// (the kernel actually opens that path); empty falls through to
// the LVM/ZFS-shaped `/dev/<pool>/<rd>_<vol5digits>` guess so
// providers that don't surface a devicePath still get a working
// .res. The meta-disk path is the scenario 6.18
// `StorPoolNameDrbdMeta` carve — see Volume.MetaPool godoc.
func buildResVolumes(dr *intent.DesiredResource, devices map[int32]string, minor int) []drbd.Volume {
	vols := make([]drbd.Volume, 0, len(dr.GetVolumes()))

	for _, vol := range dr.GetVolumes() {
		disk := devices[vol.GetVolumeNumber()]
		if disk == "" {
			disk = fmt.Sprintf("/dev/%s/%s_%05d", vol.GetStoragePool(), dr.GetName(), vol.GetVolumeNumber())
		}

		// External-metadata path (scenario 6.18). When MetaPool is set
		// we emit `meta-disk <path>;` against a sibling LV named
		// `<rd>_<vol5digits>_meta` on that pool. Path shape matches
		// the data volume's LVM/ZFS guess — `/dev/<pool>/<lv>` — so
		// the renderer doesn't need a second devices-map round trip.
		// applyStorage carves the LV (or its provider equivalent)
		// before this renders, so the file is always rendered with a
		// path that resolves on disk; create-md fails fast otherwise.
		metaDisk := ""
		if mp := vol.GetMetaPool(); mp != "" {
			metaDisk = fmt.Sprintf("/dev/%s/%s_%05d_meta", mp, dr.GetName(), vol.GetVolumeNumber())
		}

		vols = append(vols, drbd.Volume{
			Number:   int(vol.GetVolumeNumber()),
			Device:   fmt.Sprintf("/dev/drbd%d", minor+int(vol.GetVolumeNumber())),
			Disk:     disk,
			MetaDisk: metaDisk,
			Minor:    minor + int(vol.GetVolumeNumber()),
		})
	}

	return vols
}

// drbdOptionSections holds the per-section maps splitDRBDOptions
// produces. Each map corresponds to one `.res` block; the renderer
// consumes them in writeNet / writeOptions / writeNamedBlock /
// per-connection disk{}. See SectionFor for the routing decision.
type drbdOptionSections struct {
	Net        map[string]string
	Resource   map[string]string
	Disk       map[string]string
	PeerDevice map[string]string
	Handlers   map[string]string
}

// splitDRBDOptions partitions the satellite-received drbd_options bag
// into per-section maps. Per-replica wiring (port/node-id/peer.*.…)
// is dropped — those are not user-tunable knobs.
//
// Routing uses `drbd.SectionFor`, which maps each
// `DrbdOptions/<Section>/<Key>` to the right `.res` block:
//
//   - `DrbdOptions/Net/*`     → `net { }`         (Net)
//   - `DrbdOptions/Disk/*`    → `disk { }`        at resource scope
//   - `DrbdOptions/Handlers/*` → `handlers { }`   at resource scope
//   - `DrbdOptions/PeerDevice/*` → `disk { }`     inside each connection
//   - `DrbdOptions/Resource/*` (and unknown sections) → `options { }`
//     (drbd's catch-all top-level block)
//
// The renderer writes the keys verbatim with the
// `DrbdOptions/<Section>/` prefix stripped — that's the form drbdadm
// expects.
//
// Section-less keys (`DrbdOptions/<Key>` with nothing after the
// prefix beyond a single segment) are LINSTOR-controller-only props
// — e.g. `DrbdOptions/AutoEvictAllowEviction` is consumed by the
// LINSTOR controller's auto-eviction logic, NOT by DRBD. Writing
// those into the .res file makes drbdadm fail with "expected:
// cpu-mask | on-no-data-accessible | ... but got: <name>". Drop
// them on the satellite side; the convention upstream is the same.
//
// Bug 258: prior to this routing rewrite, `Disk`, `Handlers` and
// `PeerDevice` keys all collapsed onto the resource-level options{}
// map, where drbd-9 rejected them at parse time ("expected: …
// got: on-io-error") — wedging the reconciler on any
// `linstor rd sp <rd> DrbdOptions/Disk/on-io-error detach` (a common
// operator action).
func splitDRBDOptions(opts map[string]string) drbdOptionSections {
	out := drbdOptionSections{
		Net:        map[string]string{},
		Resource:   map[string]string{},
		Disk:       map[string]string{},
		PeerDevice: map[string]string{},
		Handlers:   map[string]string{},
	}

	for key, value := range opts {
		rest, ok := strings.CutPrefix(key, drbd.PropPrefix)
		if !ok {
			continue
		}

		_, rawKey, hasSection := strings.Cut(rest, "/")
		if !hasSection {
			// LINSTOR-only key (no DRBD section subpath); these
			// don't belong in the rendered .res. See doc comment.
			continue
		}

		switch drbd.SectionFor(key) {
		case drbd.SectionNet:
			out.Net[rawKey] = value
		case drbd.SectionDisk:
			out.Disk[rawKey] = value
		case drbd.SectionPeerDevice:
			out.PeerDevice[rawKey] = value
		case drbd.SectionHandlers:
			out.Handlers[rawKey] = value
		default:
			// SectionOptions — drbd's catch-all top-level block.
			// Covers `DrbdOptions/Resource/*` plus any unknown
			// section so a future upstream key still lands
			// somewhere sensible (matches SectionFor's fallback).
			out.Resource[rawKey] = value
		}
	}

	return out
}

// drbdAddrPlaceholder is what the controller stamps on a Resource
// before it learns each satellite's pod IP — `resolveAddr`
// substitutes the satellite's own IP whenever it sees this value.
const drbdAddrPlaceholder = "0.0.0.0"

// drbdBoolPropTrue mirrors dispatcher.boolPropTrue — the literal
// `true` the dispatcher stamps on flag-like drbd_options keys. We
// inline rather than re-export to keep `pkg/satellite` from
// importing `pkg/dispatcher` just for one constant.
const drbdBoolPropTrue = "true"

// skipDiskPropKey and skipDiskPropValue mirror upstream linstor's
// `ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_SKIP_DISK`
// and `ApiConsts.VAL_TRUE` constants. Scenario 5.11: the
// satellite-side observer stamps `DrbdOptions/SkipDisk=True` onto
// Resource.Spec.Props when the kernel reports `disk:Failed`; this
// reconciler reads the prop and gates `drbdadm adjust --skip-disk`
// onto its presence. Constants kept here (rather than re-exported
// from `pkg/satellite/controllers`) so the reconciler's gate
// doesn't pick up a controllers-package import cycle.
const (
	skipDiskPropKey   = "DrbdOptions/SkipDisk"
	skipDiskPropValue = "True"
)

// isSkipDiskEnabled reports whether the observer (or an operator
// via `linstor r sp <n> <r> DrbdOptions/SkipDisk True`) has marked
// this replica's lower disk as failed. The check covers both
// landing spots:
//
//   - `dr.DrbdOptions`: the dispatcher pulls every `DrbdOptions/...`
//     key out of `Spec.Props` and folds it into the per-replica
//     DrbdOptions bag before calling Apply. The production path
//     therefore reads the prop from here.
//   - `dr.Props`: the satellite reconciler unit tests build
//     DesiredResource directly without running through the
//     dispatcher's split; tests that pin the SkipDisk gate need
//     a shape that doesn't require re-implementing dispatcher
//     internals.
//
// Case-insensitive compare to mirror upstream's
// `VAL_TRUE.equalsIgnoreCase` so operators who set the prop
// manually with lower-case "true" get the same behaviour the
// observer's canonical "True" produces.
func isSkipDiskEnabled(dr *intent.DesiredResource) bool {
	if strings.EqualFold(dr.GetDrbdOptions()[skipDiskPropKey], skipDiskPropValue) {
		return true
	}

	return strings.EqualFold(dr.GetProps()[skipDiskPropKey], skipDiskPropValue)
}

// resolveAddr substitutes the satellite's own IP whenever the
// controller-supplied address is the placeholder (which it is until
// the controller starts learning each satellite's pod IP and passing
// it down). Empty fallback returns the placeholder unchanged so unit
// tests don't blow up the way a missing override would.
func resolveAddr(supplied, fallback string) string {
	if supplied == "" || supplied == drbdAddrPlaceholder {
		if fallback != "" {
			return fallback
		}
	}

	return supplied
}

// isInactive returns true when the operator has called
// `linstor r deactivate` on this replica. The reconciler keeps
// storage and the .res file intact and just drops the kernel
// resource via `drbdadm down`. Activation reverses it without
// losing port/node-id allocations.
func isInactive(flags []string) bool {
	return slices.Contains(flags, "INACTIVE")
}

// isDiskless returns true when the DRBD-layer "DISKLESS" flag is set.
// Diskless replicas live entirely in DRBD memory and have no backing
// storage, so the reconciler must skip the storage path for them.
func isDiskless(flags []string) bool {
	return slices.Contains(flags, "DISKLESS")
}

// buildVolumeResults assembles per-volume devicePath entries for
// the ResourceApplyResult, choosing the path the consumer should
// see:
//
//   - When DRBD is in the layer stack, the consumer-facing device
//     is `/dev/drbd<minor>` regardless of the lower-disk path
//     (loop/LV/zvol/dm-crypt). drbdMinor + volumeNumber follow
//     the dispatcher's per-replica allocation.
//   - When DRBD is not in the stack (LayerStack=["STORAGE"] or
//     ["LUKS","STORAGE"]), the consumer sees the raw storage /
//     dm-crypt device — that's exactly what `devices` already
//     holds after applyStorage + maybeLUKS.
//   - DISKLESS replicas have no consumer-facing device; we emit
//     no Volumes entries.
func buildVolumeResults(dr *intent.DesiredResource, devices map[int32]string, diskless, withDRBD bool) []*intent.ResourceApplyVolumeResult {
	if diskless {
		return nil
	}

	out := make([]*intent.ResourceApplyVolumeResult, 0, len(dr.GetVolumes()))

	if withDRBD {
		minor, _ := strconv.Atoi(dr.GetDrbdOptions()["minor"])

		for _, vol := range dr.GetVolumes() {
			out = append(out, &intent.ResourceApplyVolumeResult{
				VolumeNumber: vol.GetVolumeNumber(),
				DevicePath:   fmt.Sprintf("/dev/drbd%d", minor+int(vol.GetVolumeNumber())),
			})
		}

		return out
	}

	for _, vol := range dr.GetVolumes() {
		dev, ok := devices[vol.GetVolumeNumber()]
		if !ok {
			continue
		}

		out = append(out, &intent.ResourceApplyVolumeResult{
			VolumeNumber: vol.GetVolumeNumber(),
			DevicePath:   dev,
		})
	}

	return out
}

// resFilePerm is the on-disk mode for /etc/drbd.d/<name>.res. drbd is
// happy with 0o644; the file does not contain secrets the way auth-keys
// would (shared-secret is in /etc/drbd.d/global_common.conf, written
// once at install time).
const resFilePerm = 0o644
