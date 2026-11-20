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
// retries of a failed resource (RetryResourcesTask). A change to the
// Resource or its definition still reconciles at once. The zero value
// is ready to use.
type applyBackoff struct {
	mu       sync.Mutex
	failures map[string]int
}

// next records one more failed apply of name and returns the delay
// before its retry.
func (b *applyBackoff) next(name string) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.failures == nil {
		b.failures = map[string]int{}
	}

	delay := applyFailureRequeue
	for range b.failures[name] {
		delay *= 2
		if delay >= applyFailureMaxRequeue {
			delay = applyFailureMaxRequeue

			break
		}
	}

	b.failures[name]++

	return delay
}

// reset forgets the failures of name after an apply that went through,
// or after the Resource is gone.
func (b *applyBackoff) reset(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.failures, name)
}
