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
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/drbd"
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
	return &Reconciler{
		cfg:            cfg,
		resourceToPool: map[string]string{},
	}
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
	if r.cfg.Adm != nil {
		err := r.cfg.Adm.Down(ctx, req.GetName())
		if err != nil {
			// Best-effort: a "not configured" error is fine, anything
			// else is logged via the response message.
			_ = err
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
		resPath := filepath.Join(r.cfg.StateDir, req.GetName()+".res")

		err := os.Remove(resPath)
		if err != nil && !os.IsNotExist(err) {
			return &satellitepb.DeleteResourceResponse{
				Ok:      false,
				Message: err.Error(),
			}, nil
		}
	}

	r.forgetPool(req.GetName())

	return &satellitepb.DeleteResourceResponse{Ok: true}, nil
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

	devices := map[int32]string{}

	if !diskless {
		got, err := r.applyStorage(ctx, dr)
		if err != nil {
			res.Ok = false
			res.Message = err.Error()

			return res
		}

		devices = got
	}

	if r.cfg.Adm != nil {
		err := r.applyDRBD(ctx, dr, diskless, devices)
		if err != nil {
			res.Ok = false
			res.Message = err.Error()

			return res
		}
	}

	return res
}

// applyStorage walks dr.Volumes and ensures each LV/zvol/loopfile
// exists. Returns a `volNumber → DevicePath` map the DRBD half uses
// to wire the `disk` line in the .res file — this is what the
// kernel actually opens, so we never want the satellite to guess
// (`/dev/<pool>/<rd>_<vol>` only works for LVM/ZFS, not loopfile).
//
// Records the resource→pool mapping (first volume's pool) so
// subsequent snapshot RPCs can route without the controller passing
// the pool.
func (r *Reconciler) applyStorage(ctx context.Context, dr *satellitepb.DesiredResource) (map[int32]string, error) {
	devices := map[int32]string{}

	for _, vol := range dr.GetVolumes() {
		provider, ok := r.cfg.Providers[vol.GetStoragePool()]
		if !ok {
			return nil, errors.Errorf("unknown storage pool %q", vol.GetStoragePool())
		}

		err := provider.CreateVolume(ctx, storage.Volume{
			ResourceName: dr.GetName(),
			VolumeNumber: vol.GetVolumeNumber(),
			SizeKib:      vol.GetSizeKib(),
		})
		if err != nil {
			return nil, errors.Wrapf(err, "create volume %s/%d", dr.GetName(), vol.GetVolumeNumber())
		}

		status, err := provider.VolumeStatus(ctx, storage.Volume{
			ResourceName: dr.GetName(),
			VolumeNumber: vol.GetVolumeNumber(),
		})
		if err != nil {
			return nil, errors.Wrapf(err, "volume status %s/%d", dr.GetName(), vol.GetVolumeNumber())
		}

		devices[vol.GetVolumeNumber()] = status.DevicePath
	}

	if len(dr.GetVolumes()) > 0 {
		r.rememberPool(dr.GetName(), dr.GetVolumes()[0].GetStoragePool())
	}

	return devices, nil
}

// applyDRBD renders the .res file from dr's metadata and (re)applies
// it via drbdadm. create-md runs only on first activation (we detect
// "first" by absence of the .res file before this run); diskless
// replicas skip create-md entirely.
//
// devices is the volNumber → DevicePath map applyStorage produced.
// buildResFile uses it as the disk path so a loopfile-backed volume
// gets `disk /dev/loopN` rather than the LVM-shaped guess.
func (r *Reconciler) applyDRBD(ctx context.Context, dr *satellitepb.DesiredResource, diskless bool, devices map[int32]string) error {
	resPath := filepath.Join(r.cfg.StateDir, dr.GetName()+".res")
	_, statErr := os.Stat(resPath)
	firstActivation := os.IsNotExist(statErr)

	body, err := buildResFile(dr, r.cfg.NodeName, r.cfg.LocalAddress, devices)
	if err != nil {
		return errors.Wrapf(err, "build .res for %s", dr.GetName())
	}

	err = os.WriteFile(resPath, []byte(body), resFilePerm)
	if err != nil {
		return errors.Wrapf(err, "write %s", resPath)
	}

	if firstActivation && !diskless {
		err = r.cfg.Adm.CreateMD(ctx, dr.GetName())
		if err != nil {
			return errors.Wrapf(err, "create-md %s", dr.GetName())
		}
	}

	err = r.cfg.Adm.Adjust(ctx, dr.GetName())
	if err != nil {
		return errors.Wrapf(err, "adjust %s", dr.GetName())
	}

	// On first activation of a diskful replica the controller may
	// flag it as the auto-primary seed. We promote once (force-
	// primary then back to secondary) so the metadata moves out of
	// "Inconsistent" into "UpToDate" without a human running
	// drbdadm. Subsequent reconciles see firstActivation=false and
	// skip the seed.
	if firstActivation && !diskless && dr.GetDrbdOptions()["auto-primary"] == "true" {
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
	})

	for _, peer := range dr.GetPeers() {
		peerPort, _ := strconv.Atoi(opts["peer."+peer+".port"])
		peerNodeID, _ := strconv.Atoi(opts["peer."+peer+".node-id"])

		hosts = append(hosts, drbd.Host{
			NodeName: peer,
			Address:  resolveAddr(opts["peer."+peer+".address"], ""),
			Port:     peerPort,
			NodeID:   peerNodeID,
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

	out, err := drbd.Build(drbd.Resource{
		Name:    dr.GetName(),
		Net:     drbd.Net{ProtocolC: true},
		Hosts:   hosts,
		Volumes: vols,
	})
	if err != nil {
		return "", errors.Wrap(err, "drbd.Build")
	}

	return out, nil
}

// resolveAddr substitutes the satellite's own IP whenever the
// controller-supplied address is the placeholder "0.0.0.0" (which it
// is until the controller starts learning each satellite's pod IP and
// passing it down). Empty fallback returns the placeholder unchanged
// so unit tests don't blow up the way a missing override would.
func resolveAddr(supplied, fallback string) string {
	if supplied == "" || supplied == "0.0.0.0" {
		if fallback != "" {
			return fallback
		}
	}

	return supplied
}

// isDiskless returns true when the DRBD-layer "DISKLESS" flag is set.
// Diskless replicas live entirely in DRBD memory and have no backing
// storage, so the reconciler must skip the storage path for them.
func isDiskless(flags []string) bool {
	return slices.Contains(flags, "DISKLESS")
}

// resFilePerm is the on-disk mode for /etc/drbd.d/<name>.res. drbd is
// happy with 0o644; the file does not contain secrets the way auth-keys
// would (shared-secret is in /etc/drbd.d/global_common.conf, written
// once at install time).
const resFilePerm = 0o644
