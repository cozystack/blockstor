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

import "sync"

// inFlight counts the applies and deletes running per resource. The DRBD
// workarounds run outside the apply lane and stay off a resource while
// the apply is changing it: an auto-promote is Primary with nothing open
// until its mkfs opens the device, and that is not an idle Primary.
type inFlight struct {
	mu      sync.Mutex
	running map[string]int
}

// enter marks name busy until the returned func runs.
func (f *inFlight) enter(name string) func() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.running == nil {
		f.running = map[string]int{}
	}

	f.running[name]++

	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()

		f.running[name]--
		if f.running[name] <= 0 {
			delete(f.running, name)
		}
	}
}

// Busy reports whether an apply or a delete of the resource name is
// running on this satellite right now.
func (r *Reconciler) Busy(name string) bool {
	r.inflight.mu.Lock()
	defer r.inflight.mu.Unlock()

	return r.inflight.running[name] > 0
}
