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

package drbd

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Adm is a thin wrapper around the `drbdadm` CLI. It exists so the
// satellite reconciler can be unit-tested without a real DRBD kernel
// module present: production injects storage.RealExec, tests inject
// storage.FakeExec and assert the exact command lines.
type Adm struct {
	exec storage.Exec
}

// NewAdm constructs an Adm with the given Exec.
func NewAdm(ex storage.Exec) *Adm {
	return &Adm{exec: ex}
}

// Up activates the resource: `drbdadm up <res>`. Idempotent on the DRBD
// side (already-up resources return 0 with a noisy warning); we don't
// try to suppress that here.
func (a *Adm) Up(ctx context.Context, resource string) error {
	return a.run(ctx, "up", resource)
}

// Down deactivates the resource: `drbdadm down <res>`. Counterpart to Up.
func (a *Adm) Down(ctx context.Context, resource string) error {
	return a.run(ctx, "down", resource)
}

// Adjust reconciles kernel state to the on-disk .res file. Called after
// the ConfFileBuilder writes a new file and we need DRBD to pick up
// changes (added/removed peers, new options).
func (a *Adm) Adjust(ctx context.Context, resource string) error {
	return a.run(ctx, "adjust", resource)
}

// CreateMD initialises on-disk metadata for the resource. We always use
// --force: a freshly-allocated LV may carry leftover signature bytes
// from its previous tenant, and DRBD bails without --force.
//
// `--max-peers` is pinned to `MaxPeers - 1` (the kernel counts the
// local node separately from peers, so a 16-replica RD needs
// `--max-peers=15`). Without this we'd inherit drbd-utils' default
// of 7, which silently caps every RD at 8 nodes total regardless of
// what the allocator says — and a later `drbdadm adjust` on the 9th
// replica would fail with a confusing "peer-id out of range" error.
//
// DANGER: `--force` overwrites whatever metadata is on the underlying
// disk. Callers MUST guarantee no valid DRBD metadata is already there
// — `--force` will happily wipe a healthy replica's GI/bitmap state,
// dropping the node's claim on its replicated data. The satellite's
// `runFirstActivation` gates the call behind a `HasMD` pre-check so
// this stays safe across satellite restarts / failed first attempts.
func (a *Adm) CreateMD(ctx context.Context, resource string) error {
	return a.run(ctx,
		"create-md",
		"--force",
		fmt.Sprintf("--max-peers=%d", MaxPeers-1),
		resource)
}

// HasMD reports whether DRBD-9 metadata already exists for the
// resource. `drbdadm dump-md <res>` exits 0 + prints a multi-line
// dump when there's a parseable metadata block on the lower disk;
// exit non-zero (with a "No valid meta data found" message) when
// there isn't. Used as the safety guard before re-running CreateMD:
// if metadata exists, the satellite must keep it (recreating with
// --force destroys the local generation identifier + dirty bitmap).
//
// Requires BOTH zero exit AND non-empty stdout to count as "present"
// — real drbdadm never returns success with no output, but a faked
// exec in unit tests can, and we'd rather err on the side of
// "missing → safe to create-md".
func (a *Adm) HasMD(ctx context.Context, resource string) (bool, error) {
	out, err := a.exec.Run(ctx, "drbdadm", "dump-md", resource)
	if err != nil {
		// `No valid meta data found` / drbdmeta "missing image" / etc.
		// all bubble up as non-zero exit. Treat as "not yet
		// initialised" — the caller's create-md will either succeed
		// (truly missing) or surface a more specific failure.
		return false, nil //nolint:nilerr // non-zero exit is the "metadata absent" signal, not a bubble-up error
	}

	return len(out) > 0, nil
}

// Primary flips the resource to Primary role so it can be opened
// read-write (mounted, exported via NBD, etc.).
func (a *Adm) Primary(ctx context.Context, resource string) error {
	return a.run(ctx, "primary", resource)
}

// PrimaryForce promotes a resource to Primary even when local disk is
// Inconsistent and no peer is UpToDate. Used as the initial-sync seed
// on a brand-new diskful replica — without --force, drbd refuses to
// promote, leaving the resource permanently "Inconsistent".
func (a *Adm) PrimaryForce(ctx context.Context, resource string) error {
	return a.run(ctx, "primary", "--force", resource)
}

// Secondary flips the resource back to Secondary role. Used after the
// consumer unmounts and before another peer takes Primary.
func (a *Adm) Secondary(ctx context.Context, resource string) error {
	return a.run(ctx, "secondary", resource)
}

// Detach drops the local lower-disk binding without bringing the
// resource down. The replica becomes Diskless on this node — peers
// stay UpToDate, the consumer keeps doing I/O via DRBD's network
// path. Used when the storage layer fails (LV evicted, zvol
// destroyed, file inode gone) and we want the kernel to stop bashing
// the dead block device. `--force` is required when the disk is
// already in a transient state.
func (a *Adm) Detach(ctx context.Context, resource string) error {
	return a.run(ctx, "detach", "--force", resource)
}

// Resize rescans the lower disk's size and tells DRBD to grow the
// replicated volume to match. The lower disk (LV / zvol / dm-crypt
// target) must already be the target size — this is a notify-only
// command. `--assume-clean` skips the resync of the new bytes since
// they were just allocated, which would otherwise serialise growing
// every replica.
func (a *Adm) Resize(ctx context.Context, resource string) error {
	return a.run(ctx, "resize", "--assume-clean", resource)
}

// SetGi pre-seeds this replica's DRBD metadata with the GI tuple of
// an existing UpToDate peer, so DRBD's GI handshake on first connect
// recognises the new replica as already-in-sync and skips the full
// initial-sync. Must be called AFTER `create-md` (which writes the
// fresh metadata block this then mutates) and BEFORE `drbdadm up`
// (which reads the metadata into kernel state).
//
// The GI tuple format DRBD's `set-gi` accepts is
// `<current>:<bitmap>:<history0>:<history1>`. We set both
// current_uuid and bitmap_uuid to peerCurrentGi so the new replica
// claims "I'm at the peer's generation; I have no dirty bits relative
// to the peer". History is zeroed — DRBD's handshake never matches
// against history when current+bitmap match, so it doesn't matter.
//
// Tested via FakeExec capture in pkg/drbd/drbdadm_test.go and
// pinned end-to-end in pkg/satellite/reconciler_drbd_test.go's
// first-activation case.
func (a *Adm) SetGi(ctx context.Context, resource string, volume int32, device, peerCurrentGi string) error {
	target := fmt.Sprintf("%s/%d", resource, volume)
	gi := fmt.Sprintf("%s:%s:0:0", peerCurrentGi, peerCurrentGi)

	_, err := a.exec.Run(ctx, "drbdmeta", "--force", target, "v09", device, "internal", "set-gi", gi)
	if err != nil {
		return errors.Wrapf(err, "drbdmeta set-gi %s", target)
	}

	return nil
}

// run is the single shell-out site so every drbdadm error gets
// uniform context (subcommand + resource) for log triage.
func (a *Adm) run(ctx context.Context, args ...string) error {
	_, err := a.exec.Run(ctx, "drbdadm", args...)
	if err != nil {
		return errors.Wrapf(err, "drbdadm %s", args[0])
	}

	return nil
}
