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

package drbd

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"

	"github.com/cockroachdb/errors"
)

// QuorumRecheck is what RecheckQuorumAfterVolumeRemoval did.
type QuorumRecheck int

const (
	// QuorumRecheckNotNeeded: no stale quorum verdict can be left, or one
	// that is left is DRBD's to keep (a volume without quorum).
	QuorumRecheckNotNeeded QuorumRecheck = iota
	// QuorumRecheckRan: DRBD recomputed quorum from the volumes that are
	// left.
	QuorumRecheckRan
)

// The two on-no-quorum policies. drbdsetup show leaves the default,
// suspend-io, out.
const (
	suspendIO = "suspend-io"
	ioError   = "io-error"
)

// RemoveMinorsKeepingQuorum takes the given minors out of the resource, as
// RemoveMinor does each, without suspending the volumes that stay.
//
// del-minor turns replication to each peer off before it unregisters the
// device, so the volume going away ends with no peer and no quorum, and
// DRBD computes the resource's quorum suspension from every device it still
// has. With on-no-quorum suspend-io that suspends the whole resource; the
// device is then unregistered without a state change, so nothing lifts the
// suspension until some later state change recomputes it, and on the
// replica that removes the volume last none comes.
//
// So on a Secondary that is not suspended, whose remaining volumes all have
// quorum and none is open, on-no-quorum is set to io-error for the removal
// and back to suspend-io after it. io-error never suspends, and with no
// opener on the remaining volumes there is no request to fail; quorum is
// still enforced for a promotion. Setting it back runs a state change that
// recomputes the verdict from the volumes that are left. Anywhere else the
// minors are removed as they are, and RecheckQuorumAfterVolumeRemoval
// recomputes afterwards. The policy is restored even when a removal fails;
// should that fail too, the next `drbdadm adjust` sets it from the .res.
//
// Nothing locks out an opener while this runs, and an open on a remaining
// volume can auto-promote the resource. A Primary under io-error would see
// EIO where suspend-io holds its requests, so the role and the openers are
// read again after each detach, right before del-minor turns replication
// off, and after each del-minor that another one follows. Once either
// shows up, suspend-io is set back before anything else runs, and the
// removal goes on as it does anywhere else. A promotion that lands inside
// a del-minor itself, or between the last one and setting suspend-io back,
// is not caught: that window is two drbdsetup calls wide and is accepted.
func (a *Adm) RemoveMinorsKeepingQuorum(ctx context.Context, resource string, minors []int32) error {
	relax, err := a.canRelaxQuorumPolicy(ctx, resource, minors)
	if err != nil {
		return err
	}

	if relax {
		err = a.setOnNoQuorum(ctx, resource, ioError)
		if err != nil {
			return err
		}
	}

	// keepRelaxed sets suspend-io back as soon as the resource is Primary
	// or a remaining volume is open.
	keepRelaxed := func(ctx context.Context) error {
		if !relax {
			return nil
		}

		ok, err := a.stillRelaxable(ctx, resource, minors)
		if err != nil || ok {
			return err
		}

		relax = false

		return a.restoreOnNoQuorum(ctx, resource)
	}

	var removeErr error

	for i, minor := range minors {
		removeErr = a.removeMinor(ctx, minor, keepRelaxed)
		if removeErr == nil && i < len(minors)-1 {
			removeErr = keepRelaxed(ctx)
		}

		if removeErr != nil {
			break
		}
	}

	if relax {
		err = a.restoreOnNoQuorum(ctx, resource)
		if err != nil {
			return errors.CombineErrors(removeErr, err) //nolint:wrapcheck // both are wrapped
		}
	}

	return removeErr
}

// restoreOnNoQuorum sets the resource's on-no-quorum back to suspend-io.
func (a *Adm) restoreOnNoQuorum(ctx context.Context, resource string) error {
	err := a.setOnNoQuorum(ctx, resource, suspendIO)
	if err != nil {
		return errors.Wrapf(err, "restore on-no-quorum of %s", resource)
	}

	return nil
}

// stillRelaxable reports whether the resource is still Secondary with no
// opener on a volume that stays, so io-error may stay in force.
func (a *Adm) stillRelaxable(ctx context.Context, resource string, minors []int32) (bool, error) {
	res, err := a.quorumStatus(ctx, resource)
	if err != nil || res == nil || Role(res.Role).IsPrimary() {
		return false, err
	}

	for _, dev := range res.Devices {
		if dev.Open && !slices.Contains(minors, dev.Minor) {
			return false, nil
		}
	}

	return true, nil
}

// canRelaxQuorumPolicy reports whether on-no-quorum may be io-error while
// the minors are removed: quorum suspends the resource, it is Secondary and
// not suspended, and every volume that stays has quorum and no opener.
func (a *Adm) canRelaxQuorumPolicy(ctx context.Context, resource string, minors []int32) (bool, error) {
	res, err := a.quorumStatus(ctx, resource)
	if err != nil || res == nil || Role(res.Role).IsPrimary() || suspended(res) || !remainingVolumesHoldQuorum(res, minors) {
		return false, err
	}

	show, err := a.quorumConfig(ctx, resource)
	if err != nil || show == nil {
		return false, err
	}

	return quorumEnabled(show) && onNoQuorum(show) == suspendIO, nil
}

// suspended reports whether the kernel holds the resource's I/O for any
// reason.
func suspended(res *drbdsetupStatusResource) bool {
	return (res.SuspendedUser != nil && *res.SuspendedUser) || res.SuspendedNoData || res.SuspendedFencing || res.SuspendedQuorum
}

// remainingVolumesHoldQuorum reports whether the resource keeps a volume
// besides the given minors, and every such volume has quorum and no opener.
func remainingVolumesHoldQuorum(res *drbdsetupStatusResource, minors []int32) bool {
	remaining := 0

	for _, dev := range res.Devices {
		if slices.Contains(minors, dev.Minor) {
			continue
		}

		if dev.Open || dev.Quorum == nil || !*dev.Quorum {
			return false
		}

		remaining++
	}

	return remaining > 0
}

// RecheckQuorumAfterVolumeRemoval has DRBD recompute the resource's quorum
// verdict after a volume was taken out of it the way `drbdadm adjust` and
// LINSTOR take it out, so the volumes that stay are not held by the one
// that went (see RemoveMinorsKeepingQuorum). With on-no-quorum suspend-io
// the resource stays suspended on quorum. With io-error it keeps a cached
// "not every device has quorum", and every request on the remaining volumes
// of a Primary completes with EIO while the status shows nothing suspended
// and quorum on every volume.
//
// It runs only when every remaining volume has quorum, so a resource that
// has really lost it stays as DRBD put it. It switches on-no-quorum to the
// other policy and back: a change of that option is the one admin command
// that runs a forced state change, which recomputes both from the volumes
// that are left and lifts no other suspension. resume-io is not used: it
// also lifts a snapshot's, a no-data and a fencing suspension, and on a
// resource that is not suspended it changes nothing, so DRBD skips the
// state change and the io-error verdict stays stale.
//
// For the two calls' duration the resource runs under the other policy. A
// Primary held on quorum thus runs under io-error for that moment, and a
// quorum it really loses inside it fails its requests with EIO instead of
// holding them. Switching quorum, say from majority to all and back,
// would force the same recompute without passing through io-error, but it
// is not used: under all, a resource with one peer down and not yet
// outdated loses quorum for certain rather than by a race, which suspends
// it and runs the quorum-lost handler of a Primary, and a quorum only the
// diskless tiebreaker held need not come back after it, as the tiebreaker
// keeps a quorum the volume has and never grants one it lost.
func (a *Adm) RecheckQuorumAfterVolumeRemoval(ctx context.Context, resource string) (QuorumRecheck, error) {
	res, err := a.quorumStatus(ctx, resource)
	if err != nil || res == nil || !everyVolumeHasQuorum(res) {
		return QuorumRecheckNotNeeded, err
	}

	show, err := a.quorumConfig(ctx, resource)
	if err != nil || show == nil || !quorumEnabled(show) {
		return QuorumRecheckNotNeeded, err
	}

	policy := onNoQuorum(show)
	if policy == suspendIO && !res.SuspendedQuorum {
		return QuorumRecheckNotNeeded, nil
	}

	other := suspendIO
	if policy == suspendIO {
		other = ioError
	}

	err = a.setOnNoQuorum(ctx, resource, other)
	if err != nil {
		return QuorumRecheckNotNeeded, err
	}

	err = a.setOnNoQuorum(ctx, resource, policy)
	if err != nil {
		return QuorumRecheckNotNeeded, errors.Wrapf(err, "restore on-no-quorum of %s", resource)
	}

	return QuorumRecheckRan, nil
}

// setOnNoQuorum changes the resource's on-no-quorum policy alone; the
// other resource options keep their values.
func (a *Adm) setOnNoQuorum(ctx context.Context, resource, policy string) error {
	_, err := a.exec.Run(ctx, "drbdsetup", "resource-options", resource, "--on-no-quorum="+policy)
	if err != nil {
		return errors.Wrapf(err, "drbdsetup resource-options %s --on-no-quorum=%s", resource, policy)
	}

	return nil
}

// everyVolumeHasQuorum reports whether the resource has volumes, each of
// them with quorum.
func everyVolumeHasQuorum(res *drbdsetupStatusResource) bool {
	if len(res.Devices) == 0 {
		return false
	}

	for _, dev := range res.Devices {
		if dev.Quorum == nil || !*dev.Quorum {
			return false
		}
	}

	return true
}

// quorumEnabled reports whether the resource enforces quorum.
func quorumEnabled(show *drbdsetupShowResource) bool {
	return show.Options.Quorum != "" && show.Options.Quorum != "off"
}

// onNoQuorum is the resource's on-no-quorum policy.
func onNoQuorum(show *drbdsetupShowResource) string {
	if show.Options.OnNoQuorum == "" {
		return suspendIO
	}

	return show.Options.OnNoQuorum
}

// quorumStatus reads `drbdsetup status <res> --json`; nil when the kernel
// does not have the resource.
func (a *Adm) quorumStatus(ctx context.Context, resource string) (*drbdsetupStatusResource, error) {
	out, err := a.status(ctx, resource, "--json")
	if err != nil {
		if isResourceNotLoadedErr(err, out) {
			return nil, nil //nolint:nilnil // a resource the kernel does not have has nothing to recheck
		}

		return nil, errors.Wrapf(err, "drbdsetup status %s --json", resource)
	}

	if len(bytes.TrimSpace(out)) == 0 {
		return nil, nil //nolint:nilnil // as above
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil {
		return nil, errors.Wrapf(err, "parse drbdsetup status %s --json", resource)
	}

	if len(status) == 0 {
		return nil, nil //nolint:nilnil // as above
	}

	return &status[0], nil
}

// quorumConfig reads `drbdsetup show <res> --json`; nil when the kernel
// does not have the resource.
func (a *Adm) quorumConfig(ctx context.Context, resource string) (*drbdsetupShowResource, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "show", resource, "--json")
	if err != nil {
		if isResourceNotLoadedErr(err, out) {
			return nil, nil //nolint:nilnil // a resource the kernel does not have has nothing to recheck
		}

		return nil, errors.Wrapf(err, "drbdsetup show %s --json", resource)
	}

	if len(bytes.TrimSpace(out)) == 0 {
		return nil, nil //nolint:nilnil // as above
	}

	var show []drbdsetupShowResource

	err = json.Unmarshal(out, &show)
	if err != nil {
		return nil, errors.Wrapf(err, "parse drbdsetup show %s --json", resource)
	}

	if len(show) == 0 {
		return nil, nil //nolint:nilnil // as above
	}

	return &show[0], nil
}
