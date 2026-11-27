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
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// ErrSuspendStateUnknown is returned by SuspendState when the kernel
// answered but its answer could not be read: an empty or malformed
// `drbdsetup status --json`, or one without the suspended-user key.
// Callers that must not claim a state they have not seen treat it as
// "unknown", never as "not suspended".
var ErrSuspendStateUnknown = errors.New("drbd suspend state unknown")

// SuspendState is the local kernel's view of one resource as far as a
// snapshot's suspend window cares: is it loaded, who is Primary, is I/O
// suspended by a user request, and which minor backs each volume.
type SuspendState struct {
	// Loaded is false when the kernel holds no slot for the resource.
	// An unloaded resource has no I/O to suspend.
	Loaded bool
	// Role is the local role, Primary or Secondary.
	Role string
	// SuspendedUser is the kernel's `suspended-user` flag, the one
	// `drbdsetup suspend-io` sets and `resume-io` clears. LINSTOR reads
	// the same flag to decide whether a resume is still owed
	// (DrbdLayer.updateSuspendState).
	SuspendedUser bool
	// Minors maps volume number to its kernel minor (/dev/drbd<minor>).
	Minors map[int32]int32
}

// SuspendState reads `drbdsetup status <resource> --json`. The query is
// a netlink read that does not touch the data path, so it answers while
// the resource is suspended.
func (a *Adm) SuspendState(ctx context.Context, resource string) (SuspendState, error) {
	out, err := a.status(ctx, resource, "--json")
	if err != nil {
		if notLoadedErr(string(out), err) {
			return SuspendState{}, nil
		}

		return SuspendState{}, errors.Wrapf(err, "drbdsetup status %s", resource)
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil || len(status) == 0 || status[0].SuspendedUser == nil {
		return SuspendState{}, errors.Wrapf(ErrSuspendStateUnknown, "drbdsetup status %s", resource)
	}

	state := SuspendState{
		Loaded:        true,
		Role:          status[0].Role,
		SuspendedUser: *status[0].SuspendedUser,
		Minors:        make(map[int32]int32, len(status[0].Devices)),
	}

	for _, dev := range status[0].Devices {
		state.Minors[dev.VolumeNumber] = dev.Minor
	}

	return state, nil
}

// SuspendStates reads `drbdsetup status --json` for every resource the
// kernel holds, in one call, keyed by resource name. A resource whose
// entry lacks the suspended-user key is left out rather than reported
// as not suspended. A kernel with no resources reports none.
func (a *Adm) SuspendStates(ctx context.Context) (map[string]SuspendState, error) {
	out, err := a.status(ctx, "--json")
	if err != nil {
		if notLoadedErr(string(out), err) {
			return map[string]SuspendState{}, nil
		}

		return nil, errors.Wrap(err, "drbdsetup status")
	}

	var status drbdsetupStatusRoot

	err = json.Unmarshal(out, &status)
	if err != nil {
		return nil, errors.Wrapf(ErrSuspendStateUnknown, "drbdsetup status: %v", err)
	}

	states := make(map[string]SuspendState, len(status))

	for _, res := range status {
		if res.Name == "" || res.SuspendedUser == nil {
			continue
		}

		state := SuspendState{
			Loaded:        true,
			Role:          res.Role,
			SuspendedUser: *res.SuspendedUser,
			Minors:        make(map[int32]int32, len(res.Devices)),
		}

		for _, dev := range res.Devices {
			state.Minors[dev.VolumeNumber] = dev.Minor
		}

		states[res.Name] = state
	}

	return states, nil
}

// ResumeIOMinor runs `drbdsetup resume-io <minor>`. It needs no .res
// file, which is what a restarted satellite has to work with: the
// startup wipe removes the .res of every resource it does not consider
// healthy. LINSTOR falls back the same way when the .res is gone
// (DrbdLayer.resumeIo -> DrbdAdm.resumeIoOnMinorNr).
func (a *Adm) ResumeIOMinor(ctx context.Context, minor int32) error {
	_, err := a.exec.Run(ctx, "drbdsetup", "resume-io", strconv.Itoa(int(minor)))
	if err != nil {
		return errors.Wrapf(err, "drbdsetup resume-io %d", minor)
	}

	return nil
}

// notLoadedErr reports whether a failed `drbdsetup status <res>` means
// the kernel has no such resource rather than a probe failure.
func notLoadedErr(out string, err error) bool {
	if IsUnknownResourceErr(err) {
		return true
	}

	for _, text := range []string{out, err.Error()} {
		if strings.Contains(text, "No such resource") ||
			strings.Contains(text, "No currently configured DRBD") {
			return true
		}
	}

	return false
}
