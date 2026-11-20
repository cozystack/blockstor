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
	"strconv"

	"github.com/cockroachdb/errors"
)

// QuorumRecheck is what RecheckQuorumAfterVolumeRemoval did.
type QuorumRecheck int

const (
	// QuorumRecheckNotNeeded: no stale quorum verdict can be left, or one
	// that is left is DRBD's to keep (a volume without quorum, a
	// suspension with another reason).
	QuorumRecheckNotNeeded QuorumRecheck = iota
	// QuorumRecheckRan: resume-io ran and DRBD recomputed quorum from
	// the volumes that are left.
	QuorumRecheckRan
	// QuorumRecheckUnsafe: a stale verdict may be left, and the resource
	// fences peers or suspends on no data, where resume-io could lift a
	// suspension that lands between the status read and the resume.
	// Nothing ran, as in LINSTOR.
	QuorumRecheckUnsafe
)

// suspendIO is the on-no-quorum and on-no-data-accessible value that
// suspends I/O instead of failing it.
const suspendIO = "suspend-io"

// RecheckQuorumAfterVolumeRemoval has DRBD recompute the resource's quorum
// verdict after a volume was taken out of it, so the volumes that stay are
// not held by the one that went.
//
// DRBD computes that verdict from every device of the resource, and only
// when a state change runs. del-minor turns replication to each peer off
// before it unregisters the device, so the volume going away ends with no
// peer and no quorum, then the device is unregistered and nothing
// recomputes. A peer's later state change does; the replica that removes
// the volume last never sees one. With on-no-quorum suspend-io its remaining
// volumes stay suspended on quorum. With io-error the resource keeps its
// cached "not every device has quorum", and every request on the remaining
// volumes of a Primary completes with EIO while the status shows nothing
// suspended and quorum on every volume.
//
// resume-io runs a state change, which recomputes both; on a resource that
// is not suspended DRBD treats it as nothing to do and still recomputes. It
// runs only when every remaining volume has quorum and nothing but quorum
// holds the resource suspended, since resume-io also lifts a snapshot's
// suspend-io, a no-data or a fencing suspension. The kernel can set the last
// two between the status read and the resume, and no drbdsetup command
// closes that window, so a resource that fences peers or suspends on no
// data is left as it is.
func (a *Adm) RecheckQuorumAfterVolumeRemoval(ctx context.Context, resource string) (QuorumRecheck, error) {
	res, err := a.quorumStatus(ctx, resource)
	if err != nil || res == nil || !everyVolumeHasQuorumAlone(res) {
		return QuorumRecheckNotNeeded, err
	}

	show, err := a.quorumConfig(ctx, resource)
	if err != nil || show == nil {
		return QuorumRecheckNotNeeded, err
	}

	verdict := quorumRecheckFor(res, show)
	if verdict != QuorumRecheckRan {
		return verdict, nil
	}

	m := strconv.Itoa(int(res.Devices[0].Minor))

	_, err = a.exec.Run(ctx, "drbdsetup", "resume-io", m)
	if err != nil {
		return QuorumRecheckNotNeeded, errors.Wrapf(err, "drbdsetup resume-io %s", m)
	}

	return QuorumRecheckRan, nil
}

// everyVolumeHasQuorumAlone reports whether the resource has volumes, each
// of them with quorum, and nothing but quorum holds it suspended.
func everyVolumeHasQuorumAlone(res *drbdsetupStatusResource) bool {
	if res.SuspendedUser || res.SuspendedNoData || res.SuspendedFencing || len(res.Devices) == 0 {
		return false
	}

	for _, dev := range res.Devices {
		if dev.Quorum == nil || !*dev.Quorum {
			return false
		}
	}

	return true
}

// quorumRecheckFor decides, from the resource's configuration, whether a
// stale quorum verdict can be left and whether resume-io may run for it.
func quorumRecheckFor(res *drbdsetupStatusResource, show *drbdsetupShowResource) QuorumRecheck {
	quorumOn := show.Options.Quorum != "" && show.Options.Quorum != "off"
	if !res.SuspendedQuorum && (!quorumOn || show.Options.OnNoQuorum == suspendIO) {
		return QuorumRecheckNotNeeded
	}

	if show.Options.OnNoData == suspendIO {
		return QuorumRecheckUnsafe
	}

	for _, c := range show.Connections {
		if c.Net.Fencing != "" && c.Net.Fencing != "dont-care" {
			return QuorumRecheckUnsafe
		}
	}

	return QuorumRecheckRan
}

// quorumStatus reads `drbdsetup status <res> --json`; nil when the kernel
// does not have the resource.
func (a *Adm) quorumStatus(ctx context.Context, resource string) (*drbdsetupStatusResource, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", resource, "--json")
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
