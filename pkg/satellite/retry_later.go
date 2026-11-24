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

import "time"

// bestEffortRetry is how soon a pass whose best-effort step failed asks
// to be run again, the same wait as a failed apply.
const bestEffortRetry = 5 * time.Second

// minRetry is the shortest wait TakeRetry hands out, so a retry that is
// already due still goes through the queue rather than spinning.
const minRetry = 100 * time.Millisecond

// TakeRetry returns how soon the passes of name since the last call
// asked for another, and forgets the ask.
func (r *Reconciler) TakeRetry(name string) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	due, ok := r.retryAt[name]
	if !ok {
		return 0, false
	}

	delete(r.retryAt, name)

	return max(time.Until(due), minRetry), true
}

// retryLater asks for another pass of name after d. Of several asks the
// earliest wins.
func (r *Reconciler) retryLater(name string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.retryLaterLocked(name, d)
}

// retryLaterLocked is retryLater for a caller that holds r.mu.
func (r *Reconciler) retryLaterLocked(name string, d time.Duration) {
	if r.retryAt == nil {
		r.retryAt = map[string]time.Time{}
	}

	due := time.Now().Add(d)
	if cur, ok := r.retryAt[name]; !ok || due.Before(cur) {
		r.retryAt[name] = due
	}
}
