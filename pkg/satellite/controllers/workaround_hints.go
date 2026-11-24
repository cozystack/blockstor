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
// one of them may be there, so a node whose replicas are all Secondary
// and UpToDate, or Primary and in use, costs the workarounds no command
// at all.
type WorkaroundHints struct {
	// IdlePrimary: some resource is Primary on this node with no volume
	// open. events2 device frames carry `open:yes|no|unknown`; a volume
	// whose open state the kernel does not report counts as open.
	IdlePrimary bool
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

	var primaries []string

	o.resMu.Lock()

	for name, res := range o.resCache {
		if res.Unloaded {
			unloaded[name] = true

			continue
		}

		if res.Role == drbdRolePrimary {
			primaries = append(primaries, name)
		}

		hints.ForcedIOFailures = hints.ForcedIOFailures || res.ForceIOFailures
	}

	o.resMu.Unlock()

	o.volMu.Lock()
	defer o.volMu.Unlock()

	for _, name := range primaries {
		if !anyVolumeMaybeOpen(o.volCache[name]) {
			hints.IdlePrimary = true
		}
	}

	for name, volumes := range o.volCache {
		if unloaded[name] {
			continue
		}

		for num := range volumes {
			if volumes[num].DiskState == drbdDiskStateInconsistent {
				hints.Inconsistent = true
			}
		}
	}

	return hints
}

// OpenUnknown reports whether the kernel does not report whether some
// volume of the resource name is open (events2 `open:unknown`, a DRBD
// module too old to say). `drbdsetup status --json` prints such a volume
// as not open, so the checks that act only on an unused replica leave
// this one alone.
func (o *ObserverRunnable) OpenUnknown(name string) bool {
	o.volMu.Lock()
	defer o.volMu.Unlock()

	volumes := o.volCache[name]
	for num := range volumes {
		if volumes[num].Open == drbdOpenUnknown {
			return true
		}
	}

	return false
}

// anyVolumeMaybeOpen reports whether some volume is open or its open
// state is unknown.
func anyVolumeMaybeOpen(volumes map[int32]volumeObservation) bool {
	for num := range volumes {
		if open := volumes[num].Open; open == drbdFlagYes || open == drbdOpenUnknown {
			return true
		}
	}

	return false
}

// drbdOpenUnknown is events2's `open:` of a volume whose open state the
// kernel does not report.
const drbdOpenUnknown = "unknown"

// drbdDiskStateInconsistent is the DRBD-9 disk_state of a volume that
// holds a partial copy, as a resync target does.
const drbdDiskStateInconsistent = "Inconsistent"
