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
	"strings"

	"github.com/cockroachdb/errors"

	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
)

// shipExec is the storage.Exec the Reconciler uses when actually
// shipping. We expose it as a separate field on the Reconciler so the
// snapshot-ship path can use a different exec from the storage CRUD
// path (e.g. for nested ssh exec). Today we always reuse the storage
// providers' exec, but it pays for itself in tests.

// ShipSnapshot copies a snapshot from this satellite to a peer. The
// mechanism is picked by the source pool's Kind:
//   - ZFS / ZFS_THIN  → `zfs send <snap> | ssh peer zfs recv <dataset>`
//   - LVM_THIN        → `thin-send-recv` (Linbit's tool)
//   - everything else → not supported
//
// The actual data flow happens out-of-process (a single piped shell
// command); we record the chosen mechanism in the response so the
// controller can plumb stats back to the caller.
func (r *Reconciler) ShipSnapshot(ctx context.Context, req *satellitepb.ShipSnapshotRequest) (*satellitepb.ShipSnapshotResponse, error) {
	provider, err := r.providerForResource(req.GetResourceName())
	if err != nil {
		//nolint:nilerr // surfaced as ok=false; gRPC error is for transport faults
		return &satellitepb.ShipSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	mechanism := req.GetMechanism()
	if mechanism == "" {
		mechanism = pickMechanism(provider.Kind())
	}

	err = r.runShip(ctx, mechanism, req)
	if err != nil {
		//nolint:nilerr // per-RPC error path
		return &satellitepb.ShipSnapshotResponse{Ok: false, Message: err.Error()}, nil
	}

	return &satellitepb.ShipSnapshotResponse{Ok: true}, nil
}

// runShip dispatches to the right shell pipeline given the chosen
// mechanism. Errors from the exec layer surface to the caller.
func (r *Reconciler) runShip(ctx context.Context, mechanism string, req *satellitepb.ShipSnapshotRequest) error {
	exec := r.shipExec()

	switch strings.ToLower(mechanism) {
	case mechanismZFS, "zfs-send":
		return runZfsShip(ctx, exec, req)
	case mechanismThin, "lvm-thin", "thin-send-recv":
		return runThinShip(ctx, exec, req)
	default:
		return errors.Errorf("unsupported snapshot-ship mechanism %q", mechanism)
	}
}

// shipExec returns the storage.Exec the ship pipeline runs under. In
// production this is `storage.RealExec`; tests inject a `FakeExec` via
// `ReconcilerConfig.ShipExec` so they can assert command lines
// without spinning up the real zfs / ssh / thin-send-recv tools.
func (r *Reconciler) shipExec() storage.Exec {
	if r.cfg.ShipExec != nil {
		return r.cfg.ShipExec
	}

	return storage.RealExec{}
}

// runZfsShip pipes `zfs send` into `ssh <target> zfs recv`. We don't
// shell out via /bin/sh -c here — the storage.Exec abstraction runs
// one binary, so we wrap the pipe ourselves through `sh -c`.
func runZfsShip(ctx context.Context, exec storage.Exec, req *satellitepb.ShipSnapshotRequest) error {
	pipeline := fmt.Sprintf("zfs send %s | ssh %s zfs recv -F %s",
		req.GetSnapshotName(), req.GetTargetNode(), req.GetResourceName())

	_, err := exec.Run(ctx, "sh", "-c", pipeline)
	if err != nil {
		return errors.Wrap(err, "zfs send|recv")
	}

	return nil
}

// runThinShip uses Linbit's `thin-send-recv` helper, which understands
// the LVM-thin metadata block format and drives a similar pipe over
// ssh under the hood.
func runThinShip(ctx context.Context, exec storage.Exec, req *satellitepb.ShipSnapshotRequest) error {
	_, err := exec.Run(ctx, "thin-send-recv",
		"--source", req.GetResourceName()+"_"+req.GetSnapshotName()+"_00000",
		"--target", req.GetTargetNode())
	if err != nil {
		return errors.Wrap(err, "thin-send-recv")
	}

	return nil
}

// pickMechanism maps a provider Kind onto the default shipping
// mechanism string. Kinds we don't know about return "" — the caller
// surfaces that as an unsupported-mechanism error.
func pickMechanism(kind string) string {
	switch kind {
	case kindZFS, kindZFSThin:
		return mechanismZFS
	case kindLVMThin:
		return mechanismThin
	default:
		return ""
	}
}

const (
	mechanismZFS  = "zfs"
	mechanismThin = "thin"

	kindZFS     = "ZFS"
	kindZFSThin = "ZFS_THIN"
	kindLVMThin = "LVM_THIN"
)
