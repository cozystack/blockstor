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

// inFlight counts the applies and deletes running per resource and
// the resources a DRBD workaround holds. The workarounds run outside
// the apply lane and stay off a resource while the apply is changing it:
// an auto-promote is Primary with nothing open until its mkfs opens the
// device, and that is not an idle Primary. The other way round, an apply
// or delete waits for a workaround that holds the resource: one that
// started between a workaround's `drbdadm down` and `up` would find no
// kernel slot and run its own up at the same moment.
type inFlight struct {
	mu      sync.Mutex
	free    *sync.Cond
	running map[string]int
	held    map[string]bool
}

// init readies the maps and the condition; called with mu held.
func (f *inFlight) init() {
	if f.running == nil {
		f.running = map[string]int{}
		f.held = map[string]bool{}
		f.free = sync.NewCond(&f.mu)
	}
}

// enter marks name busy until the returned func runs, first waiting for
// a workaround that holds name to let go. A workaround holds a resource
// for a couple of drbdadm calls at most.
func (f *inFlight) enter(name string) func() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.init()

	for f.held[name] {
		f.free.Wait()
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

// hold marks name held by a workaround until the returned func runs, so
// no apply or delete of it starts meanwhile. ok is false, and nothing is
// held, while an apply or delete of name runs or another hold has it.
func (f *inFlight) hold(name string) (func(), bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.init()

	if f.running[name] > 0 || f.held[name] {
		return nil, false
	}

	f.held[name] = true

	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()

		delete(f.held, name)
		f.free.Broadcast()
	}, true
}

// Busy reports whether an apply or a delete of the resource name is
// running on this satellite right now.
func (r *Reconciler) Busy(name string) bool {
	r.inflight.mu.Lock()
	defer r.inflight.mu.Unlock()

	return r.inflight.running[name] > 0
}

// Hold claims the resource name for a DRBD workaround: until the returned
// func runs, no apply or delete of name starts. It reports false while one is
// running, and then the workaround leaves name alone.
func (r *Reconciler) Hold(name string) (func(), bool) {
	return r.inflight.hold(name)
}
