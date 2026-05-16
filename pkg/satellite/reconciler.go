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
		err := r.cfg.Adm.Down(ctx, req.GetName())
		if err != nil {
			// Best-effort: a "not configured" error is fine here
			// (resource was already torn down on a prior pass), but we
			// still want to surface the message back to the controller
			// so it shows up in the gRPC response. Don't fail — DRBD
			// down errors shouldn't block the storage cleanup.
			downMsg = "drbdadm down: " + err.Error()
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
func (r *Reconciler) applyStorageIfDiskful(ctx context.Context, dr *intent.DesiredResource, diskless bool) (map[int32]string, bool, bool, error) {
	if diskless {
		return map[int32]string{}, false, false, nil
	}

	return r.applyStorage(ctx, dr)
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

// tearDownRemovedPeers runs `drbdadm del-peer` for every peer that
// was in the previous .res but is no longer in the new desired set.
// `drbdadm adjust` only adds / reconfigures peers; the kernel's
// connection slot for a dropped peer would otherwise stay alive in
// StandAlone forever. del-peer needs the peer's `on <node>` block
// still in the .res to resolve its node-id, so run it BEFORE
// overwriting the file.
func (r *Reconciler) tearDownRemovedPeers(ctx context.Context, dr *intent.DesiredResource, resPath string) error {
	removed := computeRemovedPeers(resPath, dr, r.cfg.NodeName)
	for _, p := range removed {
		err := r.cfg.Adm.DelPeer(ctx, dr.GetName(), p)
		if err != nil {
			return errors.Wrapf(err, "del-peer %s from %s", p, dr.GetName())
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

	resPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".res")
	mdMarkerPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".md-created")

	// firstActivation is "did create-md succeed previously?" — keyed
	// off a separate marker file written AFTER create-md returns
	// success. We can't gate on the .res-file existence: a previous
	// reconcile that wrote the .res but failed `drbdadm create-md`
	// (e.g. .res had a stale conflicting node-id from a race that
	// later got fixed) would otherwise report firstActivation=false
	// on every subsequent attempt → create-md is skipped → adjust
	// reports "No valid meta data found" forever.
	_, statErr := os.Stat(mdMarkerPath)
	firstActivation := os.IsNotExist(statErr)

	body, err := buildResFile(dr, r.cfg.NodeName, r.cfg.LocalAddress, devices)
	if err != nil {
		return errors.Wrapf(err, "build .res for %s", dr.GetName())
	}

	err = r.tearDownRemovedPeers(ctx, dr, resPath)
	if err != nil {
		return err
	}

	err = os.WriteFile(resPath, []byte(body), resFilePerm)
	if err != nil {
		return errors.Wrapf(err, "write %s", resPath)
	}

	if firstActivation && !diskless {
		err = r.runFirstActivation(ctx, dr, devices, mdMarkerPath)
		if err != nil {
			return err
		}
	}

	err = r.runApplyDRBDVerb(ctx, dr, firstActivation)
	if err != nil {
		return err
	}

	// Pickup-time resize: the storage layer was just grown, drbdadm
	// resize tells the kernel to extend the replicated device to
	// match. Adjust on its own won't do this — only resize re-reads
	// the lower disk's size. Diskless replicas don't have a lower
	// disk to resize but they still need their internal state to
	// catch up; drbdadm resize handles that case too.
	if resized {
		err = r.cfg.Adm.Resize(ctx, dr.GetName())
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
	autoPromote := firstActivation && !diskless &&
		dr.GetDrbdOptions()["auto-primary"] == drbdBoolPropTrue
	_ = cloned

	if autoPromote {
		err = r.runAutoPromote(ctx, dr)
		if err != nil {
			return err
		}
	}

	return nil
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
// primary replica's first activation only.
//
// Idempotency lives in a per-RD `<rd>.mkfs.done` marker under
// StateDir, sibling to `.md-created`. The marker is dropped AFTER
// every volume's mkfs returns success, so a partial failure leaves
// the resource in a state the next reconcile can retry. The marker
// is deleted together with `.res` / `.md-created` in DeleteResource
// so a re-created RD with the same name correctly mkfs-s again.
//
// SAFETY: mkfs on a populated filesystem silently destroys data. The
// marker file is the only thing standing between scenario 9.W14 and
// data loss on every Apply — losing the marker on a healthy resource
// (manual `rm`, satellite disk wipe) would re-run mkfs on the second
// Apply. We treat the marker as authoritative; recovery from a lost
// marker requires the operator to either delete + recreate the RD or
// touch the marker manually before the next reconcile.
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

	_, statErr := os.Stat(markerPath)
	if statErr == nil {
		// Marker present → mkfs already ran on a previous activation.
		// Re-running would wipe a populated filesystem.
		return nil
	}

	minor, _ := strconv.Atoi(dr.GetDrbdOptions()["minor"])

	args := []string{}

	if extra := strings.TrimSpace(dr.GetProps()["FileSystem/MkfsParams"]); extra != "" {
		args = append(args, strings.Fields(extra)...)
	}

	for _, vol := range dr.GetVolumes() {
		device := fmt.Sprintf("/dev/drbd%d", minor+int(vol.GetVolumeNumber()))

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

	return nil
}

// runApplyDRBDVerb is the per-reconcile dispatch between the two
// bring-up branches. First activation falls through to the SkipDisk-
// aware `drbdadm adjust` (or `adjust --skip-disk`): the .res +
// freshly-created metadata are the canonical bring-up path on master
// and existing tests pin that behaviour. The kernel-state probe +
// `drbdadm up` fallback (Bug 47 / scenario 5.32) only matters on
// steady-state passes where an operator may have torn the kernel
// slot down out-of-band — adjust on an absent slot fails with
// `(158) Unknown resource` and the resource stays down forever.
//
// Split out of applyDRBD so the orchestration function stays under
// the gocyclo budget.
func (r *Reconciler) runApplyDRBDVerb(ctx context.Context, dr *intent.DesiredResource, firstActivation bool) error {
	if firstActivation {
		return r.runAdjust(ctx, dr)
	}

	return r.runBringUpOrAdjust(ctx, dr)
}

// runBringUpOrAdjust probes the kernel for the resource's slot via
// `drbdsetup status <rsc>` and chooses the right drbdadm verb:
//
//   - kernel slot present → `drbdadm adjust` (or `adjust --skip-disk`
//     when SkipDisk is enabled, scenario 5.11).
//   - kernel slot absent  → `drbdadm up`, which performs
//     new-resource + new-path + attach + connect in one go and is
//     the only verb that bootstraps a missing slot from a valid
//     .res + on-disk metadata. `drbdadm adjust` on an absent slot
//     fails with `Failure: (158) Unknown resource` because adjust
//     only reconciles already-loaded kernel state.
//
// Why this matters (Bug 47 / scenario 5.32): an operator's
// `drbdadm down <rsc>` removes the kernel slot but leaves the
// satellite's `.md-created` marker on disk. Without this probe,
// every subsequent reconcile retries `drbdadm adjust` →
// `drbdsetup new-path` → `(158) Unknown resource` forever, and
// the resource stays down until the satellite pod restarts.
//
// IsLoaded's "genuine" error path (unexpected exec failure, not
// the resource-absent signal) is bubbled up: we'd rather surface
// a satellite-side failure than guess wrong and run the wrong
// verb against half-known kernel state.
func (r *Reconciler) runBringUpOrAdjust(ctx context.Context, dr *intent.DesiredResource) error {
	loaded, err := r.cfg.Adm.IsLoaded(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "probe kernel state for %s", dr.GetName())
	}

	if !loaded {
		err = r.cfg.Adm.Up(ctx, dr.GetName())
		if err != nil {
			return errors.Wrapf(err, "drbdadm up %s", dr.GetName())
		}

		return nil
	}

	return r.runAdjust(ctx, dr)
}

// runAdjust dispatches to the plain `drbdadm adjust` or the
// `--skip-disk` variant based on the `DrbdOptions/SkipDisk` prop
// (scenario 5.11).
func (r *Reconciler) runAdjust(ctx context.Context, dr *intent.DesiredResource) error {
	var err error

	if isSkipDiskEnabled(dr) {
		err = r.cfg.Adm.AdjustSkipDisk(ctx, dr.GetName())
	} else {
		err = r.cfg.Adm.Adjust(ctx, dr.GetName())
	}

	if err != nil {
		return errors.Wrapf(err, "adjust %s", dr.GetName())
	}

	return nil
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
// runFirstActivation runs the one-shot per-replica bring-up:
// `drbdadm create-md`, drop the md-created marker file (so the
// next reconcile sees firstActivation=false even across a
// satellite restart), then seed initial-sync GI when the
// controller has stamped a peer's UpToDate GI on the volume.
// Pulled out of applyDRBD so the orchestration function stays
// under the cyclomatic budget.
//
// SAFETY: `drbdadm create-md --force` wipes any existing metadata —
// running it on a healthy replica would drop the local
// generation-id / dirty-bitmap and effectively orphan the data
// from the cluster. Before calling create-md we ask DRBD whether
// metadata already exists (`drbdadm dump-md`). If it does, we
// adopt it: skip create-md, write the marker so subsequent
// reconciles see firstActivation=false, and continue to GI-seed
// (which is itself a no-op when SeedFromGi is empty). This makes
// the marker-file gate safe against accidental deletion / bare
// satellite restarts that lose the marker.
func (r *Reconciler) runFirstActivation(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, mdMarkerPath string) error {
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

	// GI-seed is a no-op when the controller hasn't stamped a peer's
	// CurrentGi on the volume (fresh-cluster case) and is idempotent
	// when it has; safe to run even when we adopted existing metadata.
	err = r.seedInitialGi(ctx, dr, devices)
	if err != nil {
		return errors.Wrapf(err, "seed initial-sync GI %s", dr.GetName())
	}

	return nil
}

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
// bitmap slot for one (resource, volume) pair. DRBD 9.2+ stores
// current/bitmap UUIDs per-peer (one slot per peer node-id), so
// skipping the full initial-sync requires `drbdmeta set-gi
// --node-id <peer>` to run once per peer with the same tuple. The
// peer iteration order is GetPeers() (which the dispatcher sorted
// for deterministic output, so both satellites visit slots in the
// same order — keeps test assertions stable too).
//
// Returns the first non-nil error from drbdmeta. The "requires
// --node-id" failure mode the legacy single-call form hit on DRBD
// 9.2+ is now structurally unreachable: every call carries
// `--node-id <peer>`.
func (r *Reconciler) seedPerPeerGi(ctx context.Context, dr *intent.DesiredResource, vol *intent.DesiredVolume, device, seed string, peerNodeIDs map[string]int32) error {
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

	netOpts, resOpts := splitDRBDOptions(opts)

	out, err := drbd.Build(drbd.Resource{
		Name:        dr.GetName(),
		Net:         drbd.Net{ProtocolC: true, Options: netOpts},
		Hosts:       hosts,
		Volumes:     vols,
		Options:     resOpts,
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

// splitDRBDOptions partitions the satellite-received drbd_options bag
// into per-section maps. Per-replica wiring (port/node-id/peer.*.…)
// is dropped — those are not user-tunable knobs. Anything under
// `DrbdOptions/Net/...` lands on the `net { }` block, anything else
// under `DrbdOptions/<Section>/...` lands on the resource-level
// `options { }` block (drbd's catch-all). The ConfFileBuilder writes
// the keys verbatim with the `DrbdOptions/<Section>/` prefix stripped
// — that's the form drbdadm expects.
//
// Section-less keys (`DrbdOptions/<Key>` with nothing after the
// prefix beyond a single segment) are LINSTOR-controller-only props
// — e.g. `DrbdOptions/AutoEvictAllowEviction` is consumed by the
// LINSTOR controller's auto-eviction logic, NOT by DRBD. Writing
// those into the .res file makes drbdadm fail with "expected:
// cpu-mask | on-no-data-accessible | ... but got: <name>". Drop
// them on the satellite side; the convention upstream is the same.
func splitDRBDOptions(opts map[string]string) (map[string]string, map[string]string) {
	netOpts := map[string]string{}
	resOpts := map[string]string{}

	for key, value := range opts {
		rest, ok := strings.CutPrefix(key, drbd.PropPrefix)
		if !ok {
			continue
		}

		section, rawKey, hasSection := strings.Cut(rest, "/")
		if !hasSection {
			// LINSTOR-only key (no DRBD section subpath); these
			// don't belong in the rendered .res. See doc comment.
			continue
		}

		switch strings.ToLower(section) {
		case "net":
			netOpts[rawKey] = value
		case "disk", "peerdevice", "peer-device", "handlers": //nolint:goconst,nolintlint // DRBD section name, semantic-distinct from LsblkTypeDisk
			// These sections aren't plumbed through Net struct
			// today; surface them as resource-level options so
			// drbdadm at least sees them. Full per-section
			// emission lands when ConfFileBuilder grows the
			// disk/peer-device blocks.
			resOpts[rawKey] = value
		default:
			resOpts[rawKey] = value
		}
	}

	return netOpts, resOpts
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
