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

package controllers

// WorkaroundHints is what the observer's events2 cache shows of the shapes
// the DRBD workarounds act on. A pass reads `drbdsetup status` only when
// one of them may be there, so a node with every replica Secondary and
// UpToDate costs the workarounds no command at all.
type WorkaroundHints struct {
	// Primary: some resource is Primary on this node.
	Primary bool
	// Inconsistent: some local volume is Inconsistent.
	Inconsistent bool
	// ForcedIOFailures: some resource reports force-io-failures:yes.
	ForcedIOFailures bool
}

// WorkaroundHints reads the hints off the observer's caches. A resource
// whose kernel slot was destroyed gives none.
func (o *ObserverRunnable) WorkaroundHints() WorkaroundHints {
	var hints WorkaroundHints

	unloaded := map[string]bool{}

	o.resMu.Lock()

	for name, res := range o.resCache {
		if res.Unloaded {
			unloaded[name] = true

			continue
		}

		hints.Primary = hints.Primary || res.Role == drbdRolePrimary
		hints.ForcedIOFailures = hints.ForcedIOFailures || res.ForceIOFailures
	}

	o.resMu.Unlock()

	o.volMu.Lock()
	defer o.volMu.Unlock()

	for name, volumes := range o.volCache {
		if unloaded[name] {
			continue
		}

		for _, vol := range volumes {
			if vol.DiskState == drbdDiskStateInconsistent {
				hints.Inconsistent = true
			}
		}
	}

	return hints
}

// drbdDiskStateInconsistent is the DRBD-9 disk_state of a volume that
// holds a partial copy, as a resync target does.
const drbdDiskStateInconsistent = "Inconsistent"
