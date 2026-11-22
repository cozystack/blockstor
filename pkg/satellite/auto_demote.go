// SPDX-License-Identifier: Apache-2.0

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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// A replica runAutoPromote promoted with `drbdadm primary --force` stays
// Primary until something demotes it: DRBD demotes on the last close only
// a replica it promoted itself on open (auto-promote), never one promoted
// explicitly. So the satellite owes every replica it promotes a demote,
// and a demote that fails once must be tried again.
//
// It did not used to be. A host process opening the freshly formatted
// device for a moment (the probe of a new filesystem) made the one
// `drbdadm secondary` after mkfs fail with `(-12) Device is held open by
// someone`. The apply was retried, but the retry no longer ran the promote
// chain (first activation was over and mkfs had written its marker), so it
// succeeded without demoting. The replica stayed Primary with nobody using
// it: `r l` showed it InUse, and deleting the volume failed until someone
// ran `drbdsetup secondary` by hand (blockstor v0.4.0-alpha.15, issue 01
// of the upgrade test). LINSTOR does not report such a resource as applied
// either: a failed demote after mkfs fails the resource ("Failed to become
// secondary again after creating filesystem", DrbdLayer
// runPostInitializationIfNeeded in linstor-server).
//
// The debt is a state file next to `.md-created`, written before the
// promote and removed once a demote succeeds, so it survives a satellite
// restart in between. A device held open is the expected wait here, not a
// failure: right after mkfs the demote is retried a few times within the
// apply, and if the device is still held open it stays owed and is retried
// by later applies until whoever holds the device lets go. Any other
// demote error fails the apply as before.

// demotePendingSuffix names the state file recording that this satellite
// promoted a replica itself and has not yet seen it demoted.
const demotePendingSuffix = ".demote-pending"

// autoSecondaryBackoff is how long the demote right after mkfs waits
// before each retry while the new device is held open. The opener seen
// in the field (the host probing a new filesystem) lets go within a
// second; the whole wait stays well inside one apply.
func autoSecondaryBackoff() []time.Duration {
	return []time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
	}
}

// pendingDemoteRetry is how often a later apply tries a demote that is
// still owed while the device stays held open. A consumer that mounted the
// replica in the meantime may hold it for days; one failed `drbdadm
// secondary` per this interval is all it costs.
const pendingDemoteRetry = 30 * time.Second

// demoteRetry paces the retries of owed demotes per resource. Process
// memory only: after a restart the first apply retries at once.
type demoteRetry struct {
	mu      sync.Mutex
	nextAt  map[string]time.Time
	backoff []time.Duration
	every   time.Duration
	now     func() time.Time
}

func newDemoteRetry() demoteRetry {
	return demoteRetry{
		nextAt:  map[string]time.Time{},
		backoff: autoSecondaryBackoff(),
		every:   pendingDemoteRetry,
		now:     time.Now,
	}
}

// due reports whether an owed demote of name may be tried now.
func (d *demoteRetry) due(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	next, ok := d.nextAt[name]

	return !ok || !d.now().Before(next)
}

// heldOpen paces the next try of name after the device was held open.
func (d *demoteRetry) heldOpen(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextAt[name] = d.now().Add(d.every)
}

// forget drops the pacing of name.
func (d *demoteRetry) forget(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.nextAt, name)
}

// demotePendingPath is the state file of name's owed demote. Empty without
// a state dir: nothing is recorded, and only the demote right after mkfs
// is tried, as before.
func (r *Reconciler) demotePendingPath(name string) string {
	if r.cfg.StateDir == "" {
		return ""
	}

	return filepath.Join(r.cfg.StateDir, name+demotePendingSuffix)
}

// owePendingDemote records that name is about to be promoted by this
// satellite and owes a demote.
func (r *Reconciler) owePendingDemote(name string) error {
	path := r.demotePendingPath(name)
	if path == "" {
		return nil
	}

	err := os.WriteFile(path, nil, resFilePerm)
	if err != nil {
		return errors.Wrapf(err, "write %s", path)
	}

	return nil
}

// demoteOwed reports whether name still owes a demote.
func (r *Reconciler) demoteOwed(name string) bool {
	path := r.demotePendingPath(name)
	if path == "" {
		return false
	}

	_, err := os.Stat(path)

	return err == nil
}

// settlePendingDemote drops name's debt once a demote succeeded.
func (r *Reconciler) settlePendingDemote(name string) error {
	r.demotes.forget(name)

	path := r.demotePendingPath(name)
	if path == "" {
		return nil
	}

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove %s", path)
	}

	return nil
}

// autoSecondary is the demote right after runAutoPromote's promote and
// mkfs. While the device is held open it waits and tries again a few
// times; a device still held open after that leaves the demote owed to a
// later apply and does not fail this one.
func (r *Reconciler) autoSecondary(ctx context.Context, name string) error {
	err := r.cfg.Adm.Secondary(ctx, name)

	for _, wait := range r.demotes.backoff {
		if !drbd.IsDeviceHeldOpenErr(err) {
			break
		}

		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "auto-secondary %s", name)
		case <-time.After(wait):
		}

		err = r.cfg.Adm.Secondary(ctx, name)
	}

	switch {
	case err == nil:
		return r.settlePendingDemote(name)
	case drbd.IsDeviceHeldOpenErr(err) && r.demotePendingPath(name) != "":
		r.demotes.heldOpen(name)

		log.FromContext(ctx).Info("auto-secondary: the device is still held open; the demote is owed and retried by a later apply",
			"resource", name, "retryAfter", r.demotes.every.String(), "err", err.Error())

		return nil
	default:
		return errors.Wrapf(err, "auto-secondary %s", name)
	}
}

// retryPendingDemote tries the demote name still owes from an earlier
// auto-promote, at most once per pendingDemoteRetry while the device stays
// held open.
func (r *Reconciler) retryPendingDemote(ctx context.Context, name string) error {
	if r.cfg.Adm == nil || !r.demoteOwed(name) || !r.demotes.due(name) {
		return nil
	}

	err := r.cfg.Adm.Secondary(ctx, name)

	switch {
	case err == nil:
		log.FromContext(ctx).Info("auto-secondary: the demote owed since the auto-promote went through",
			"resource", name)

		return r.settlePendingDemote(name)
	case drbd.IsDeviceHeldOpenErr(err):
		r.demotes.heldOpen(name)

		log.FromContext(ctx).V(1).Info("auto-secondary: the device is still held open; the demote stays owed",
			"resource", name, "retryAfter", r.demotes.every.String())

		return nil
	default:
		return errors.Wrapf(err, "auto-secondary %s", name)
	}
}
