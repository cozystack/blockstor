//go:build integration

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

package harness

import (
	"fmt"
	"runtime/debug"
	"sync"
	"testing"
)

// RunParallel spawns n goroutines, calls body(i) on each, waits
// for all to finish, and propagates any panic back to t.Fatal.
// The goroutine-storm shape exists for Group L
// (`concurrent_test.go`) where we exercise reconcile races against
// the apiserver.
//
// Panics are captured per-goroutine and reported under a stable
// banner; the first panic short-circuits the test, the rest are
// logged via t.Log so the operator can see the full picture.
func RunParallel(t *testing.T, n int, body func(i int)) {
	t.Helper()

	if n <= 0 {
		t.Fatalf("RunParallel: n must be positive, got %d", n)
	}

	var wg sync.WaitGroup

	panics := make([]string, n)
	wg.Add(n)

	for i := range n {
		go func(idx int) {
			defer wg.Done()
			defer func() {
				rec := recover()
				if rec != nil {
					panics[idx] = fmt.Sprintf("goroutine %d panic: %v\n%s",
						idx, rec, debug.Stack())
				}
			}()

			body(idx)
		}(i)
	}

	wg.Wait()

	first := ""

	for i := range panics {
		if panics[i] == "" {
			continue
		}

		if first == "" {
			first = panics[i]
		} else {
			t.Log(panics[i])
		}
	}

	if first != "" {
		t.Fatal(first)
	}
}
