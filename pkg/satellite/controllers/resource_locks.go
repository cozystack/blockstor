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

import "sync"

// ResourceLocks serialises the satellite's work on one DRBD resource
// between the resource reconciler's apply pass and the startup
// MetadataCreated backfill. Both run drbdmeta against the same lower
// disks, and the backfill reads a lower disk it cannot open
// exclusively as one carrying metadata. Run beside a pass that is
// about to create that metadata, a busy disk would stamp the
// Condition on an empty volume, and the stamp turns create-md off for
// good. LINSTOR's device manager never probes a resource while it is
// adjusting it, for the same reason.
//
// controller-runtime already keeps two passes of one Resource apart;
// this lock only adds the backfill to that. A nil *ResourceLocks
// locks nothing, so reconcilers built without one keep working.
type ResourceLocks struct {
	mu   sync.Mutex
	held map[string]*resourceLock
}

type resourceLock struct {
	mu   sync.Mutex
	refs int
}

// Lock blocks until name is free and returns the function that frees
// it again. An entry lives only while someone holds or waits for it.
func (l *ResourceLocks) Lock(name string) func() {
	if l == nil {
		return func() {}
	}

	l.mu.Lock()

	if l.held == nil {
		l.held = map[string]*resourceLock{}
	}

	entry := l.held[name]
	if entry == nil {
		entry = &resourceLock{}
		l.held[name] = entry
	}

	entry.refs++
	l.mu.Unlock()

	entry.mu.Lock()

	return func() {
		entry.mu.Unlock()

		l.mu.Lock()
		defer l.mu.Unlock()

		entry.refs--
		if entry.refs == 0 {
			delete(l.held, name)
		}
	}
}
