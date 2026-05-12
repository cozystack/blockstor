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
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
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

// Apply walks res and brings local storage in line with each item.
// Each input gets a ResourceApplyResult — partial success is the norm
// (one missing pool shouldn't sink the rest of a batch).
//
// The signature returns an error too, but reserves it for context
// cancellation — per-resource failures land in the Result entries.
func (r *Reconciler) Apply(ctx context.Context, res []*satellitepb.DesiredResource) ([]*satellitepb.ResourceApplyResult, error) {
	results := make([]*satellitepb.ResourceApplyResult, 0, len(res))

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
func (r *Reconciler) CreateSnapshot(ctx context.Context, req *satellitepb.CreateSnapshotRequest) (*satellitepb.CreateSnapshotResponse, error) {
	provider, err := r.providerForResource(req.GetResourceName())
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &satellitepb.CreateSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	err = provider.CreateSnapshot(ctx, storage.Snapshot{
		ResourceName: req.GetResourceName(),
		SnapshotName: req.GetSnapshotName(),
	})
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &satellitepb.CreateSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	return &satellitepb.CreateSnapshotResponse{
		Ok:                  true,
		CreateTimestampUnix: time.Now().Unix(),
	}, nil
}

// DeleteSnapshot mirrors CreateSnapshot. Idempotency lives at the
// provider layer (lvremove on missing LV is non-fatal there).
func (r *Reconciler) DeleteSnapshot(ctx context.Context, req *satellitepb.DeleteSnapshotRequest) (*satellitepb.DeleteSnapshotResponse, error) {
	provider, err := r.providerForResource(req.GetResourceName())
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &satellitepb.DeleteSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	err = provider.DeleteSnapshot(ctx, storage.Snapshot{
		ResourceName: req.GetResourceName(),
		SnapshotName: req.GetSnapshotName(),
	})
	if err != nil {
		//nolint:nilerr // per-resource errors land in Ok=false; gRPC error reserved for transport faults
		return &satellitepb.DeleteSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	return &satellitepb.DeleteSnapshotResponse{Ok: true}, nil
}

// DeleteResource tears down a resource: drbdadm down (best-effort —
// the kernel handles a missing one fine), DeleteVolume on every
// requested volume_number through the named Provider, then remove
// the .res file. Idempotent on a missing resource. Per-step errors
// land in the response body so the controller can surface granular
// status without aborting the rest of the cleanup.
func (r *Reconciler) DeleteResource(ctx context.Context, req *satellitepb.DeleteResourceRequest) (*satellitepb.DeleteResourceResponse, error) {
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
					return &satellitepb.DeleteResourceResponse{
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
		for _, suffix := range []string{".res", ".md-created"} {
			err := os.Remove(filepath.Join(r.cfg.StateDir, req.GetName()+suffix))
			if err != nil && !os.IsNotExist(err) {
				return &satellitepb.DeleteResourceResponse{
					Ok:      false,
					Message: err.Error(),
				}, nil
			}
		}
	}

	r.forgetPool(req.GetName())

	return &satellitepb.DeleteResourceResponse{Ok: true, Message: downMsg}, nil
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
func (r *Reconciler) applyOne(ctx context.Context, dr *satellitepb.DesiredResource) *satellitepb.ResourceApplyResult {
	res := &satellitepb.ResourceApplyResult{
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
func (r *Reconciler) maybeLUKS(ctx context.Context, dr *satellitepb.DesiredResource, diskless bool, devices map[int32]string, resized bool) (map[int32]string, error) {
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
func (r *Reconciler) applyLUKS(ctx context.Context, dr *satellitepb.DesiredResource, devices map[int32]string, resized bool) (map[int32]string, error) {
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
			// the device is already opened. Best-effort string check
			// because cryptsetup doesn't return a structured error.
			if !strings.Contains(err.Error(), "already exists") {
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
func (r *Reconciler) applyInactive(ctx context.Context, dr *satellitepb.DesiredResource, res *satellitepb.ResourceApplyResult) {
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
func (r *Reconciler) applyStorageIfDiskful(ctx context.Context, dr *satellitepb.DesiredResource, diskless bool) (map[int32]string, bool, bool, error) {
	if diskless {
		return map[int32]string{}, false, false, nil
	}

	return r.applyStorage(ctx, dr)
}

// the pool.
func (r *Reconciler) applyStorage(ctx context.Context, dr *satellitepb.DesiredResource) (map[int32]string, bool, bool, error) {
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
		err := materializeVolume(ctx, provider, dr.GetName(), vol)
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
// Cross-node fallback: when SourceSnapshot is set but the snapshot
// doesn't physically exist on THIS node (autoplace landed the new
// replica on a node that wasn't part of the snapshot set), the
// clone returns storage.ErrNotFound. Fall back to a blank
// CreateVolume — DRBD's network resync from a peer that DID clone
// locally will populate the data. Matches upstream LINSTOR.
func materializeVolume(ctx context.Context, provider storage.Provider, rdName string, vol *satellitepb.DesiredVolume) error {
	target := storage.Volume{
		ResourceName: rdName,
		VolumeNumber: vol.GetVolumeNumber(),
		SizeKib:      vol.GetSizeKib(),
	}

	src := vol.GetSourceSnapshot()
	if src == "" {
		return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
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
	if errors.Is(err, storage.ErrNotFound) {
		// Snapshot absent on this node — let DRBD resync from a
		// peer that did clone locally.
		return provider.CreateVolume(ctx, target) //nolint:wrapcheck // caller wraps
	}

	return err //nolint:wrapcheck // caller wraps
}

// tearDownRemovedPeers runs `drbdadm del-peer` for every peer that
// was in the previous .res but is no longer in the new desired set.
// `drbdadm adjust` only adds / reconfigures peers; the kernel's
// connection slot for a dropped peer would otherwise stay alive in
// StandAlone forever. del-peer needs the peer's `on <node>` block
// still in the .res to resolve its node-id, so run it BEFORE
// overwriting the file.
func (r *Reconciler) tearDownRemovedPeers(ctx context.Context, dr *satellitepb.DesiredResource, resPath string) error {
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
func computeRemovedPeers(resPath string, dr *satellitepb.DesiredResource, localNode string) []string {
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
func (r *Reconciler) applyDRBD(ctx context.Context, dr *satellitepb.DesiredResource, diskless bool, devices map[int32]string, resized, cloned bool) error {
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

	err = r.cfg.Adm.Adjust(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "adjust %s", dr.GetName())
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
		err = r.cfg.Adm.PrimaryForce(ctx, dr.GetName())
		if err != nil {
			return errors.Wrapf(err, "auto-primary %s", dr.GetName())
		}

		err = r.cfg.Adm.Secondary(ctx, dr.GetName())
		if err != nil {
			return errors.Wrapf(err, "auto-secondary %s", dr.GetName())
		}
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
func (r *Reconciler) runFirstActivation(ctx context.Context, dr *satellitepb.DesiredResource, devices map[int32]string, mdMarkerPath string) error {
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

func (r *Reconciler) seedInitialGi(ctx context.Context, dr *satellitepb.DesiredResource, devices map[int32]string) error {
	for _, vol := range dr.GetVolumes() {
		if vol.GetSeedFromGi() == "" {
			continue
		}

		device := devices[vol.GetVolumeNumber()]
		if device == "" {
			continue
		}

		err := r.cfg.Adm.SetGi(ctx, dr.GetName(), vol.GetVolumeNumber(), device, vol.GetSeedFromGi())
		if err != nil {
			return errors.Wrapf(err, "set-gi vol %d", vol.GetVolumeNumber())
		}
	}

	return nil
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
func buildResFile(dr *satellitepb.DesiredResource, localNode, localAddr string, devices map[int32]string) (string, error) {
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

	vols := make([]drbd.Volume, 0, len(dr.GetVolumes()))
	for _, vol := range dr.GetVolumes() {
		disk := devices[vol.GetVolumeNumber()]
		if disk == "" {
			disk = fmt.Sprintf("/dev/%s/%s_%05d", vol.GetStoragePool(), dr.GetName(), vol.GetVolumeNumber())
		}

		vols = append(vols, drbd.Volume{
			Number: int(vol.GetVolumeNumber()),
			Device: fmt.Sprintf("/dev/drbd%d", minor+int(vol.GetVolumeNumber())),
			Disk:   disk,
			Minor:  minor + int(vol.GetVolumeNumber()),
		})
	}

	netOpts, resOpts := splitDRBDOptions(opts)

	out, err := drbd.Build(drbd.Resource{
		Name:    dr.GetName(),
		Net:     drbd.Net{ProtocolC: true, Options: netOpts},
		Hosts:   hosts,
		Volumes: vols,
		Options: resOpts,
	})
	if err != nil {
		return "", errors.Wrap(err, "drbd.Build")
	}

	return out, nil
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
func buildVolumeResults(dr *satellitepb.DesiredResource, devices map[int32]string, diskless, withDRBD bool) []*satellitepb.ResourceApplyVolumeResult {
	if diskless {
		return nil
	}

	out := make([]*satellitepb.ResourceApplyVolumeResult, 0, len(dr.GetVolumes()))

	if withDRBD {
		minor, _ := strconv.Atoi(dr.GetDrbdOptions()["minor"])

		for _, vol := range dr.GetVolumes() {
			out = append(out, &satellitepb.ResourceApplyVolumeResult{
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

		out = append(out, &satellitepb.ResourceApplyVolumeResult{
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
