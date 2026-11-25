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

import (
	"sync"
	"time"
)

// applyFailureMaxRequeue caps the back-off between the retries of an
// apply that keeps failing. A retry costs a full pass, so one that
// cannot succeed is not repeated every few seconds for as long as it
// fails; the cap keeps a conflict that clears up by itself, a port
// another resource releases, from waiting long for the next try.
const applyFailureMaxRequeue = 5 * time.Minute

// applyBackoff doubles the delay before each retry of a replica whose
// apply keeps failing, from applyFailureRequeue up to
// applyFailureMaxRequeue, the way LINSTOR's controller spaces the
// retries of a failed resource (RetryResourcesTask). Only the retry
// that comes due counts: a change to the Resource or its definition
// still reconciles at once, and one that fails before the retry is due
// keeps the schedule rather than pushing it out, as LINSTOR counts its
// retries and not every failed update. The zero value is ready to use.
type applyBackoff struct {
	mu      sync.Mutex
	pending map[string]applyRetry

	// now is the clock, time.Now when nil. Tests set it.
	now func() time.Time
}

// applyRetry is the schedule of one replica whose apply keeps failing.
type applyRetry struct {
	// failures counts every failed apply, the retries that came due and
	// the reconciles a watch event triggered in between.
	failures int
	// retries counts the retries that came due and failed.
	retries int
	// due is when the next retry is.
	due time.Time
}

// next records one more failed apply of name and returns the delay
// before its retry.
func (b *applyBackoff) next(name string) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	clock := b.now
	if clock == nil {
		clock = time.Now
	}

	now := clock()

	if b.pending == nil {
		b.pending = map[string]applyRetry{}
	}

	retry, failing := b.pending[name]
	retry.failures++

	if failing && now.Before(retry.due) {
		b.pending[name] = retry

		return retry.due.Sub(now)
	}

	if failing {
		retry.retries++
	}

	delay := applyFailureRequeue
	for range retry.retries {
		delay *= 2
		if delay >= applyFailureMaxRequeue {
			delay = applyFailureMaxRequeue

			break
		}
	}

	retry.due = now.Add(delay)
	b.pending[name] = retry

	return delay
}

// reset forgets the failures of name after an apply that went through,
// or after the Resource is gone, and returns how many applies of name
// had failed since the last one that went through: zero when name was
// not failing, so the caller reports a recovery only once.
func (b *applyBackoff) reset(name string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	failures := b.pending[name].failures
	delete(b.pending, name)

	return failures
}
