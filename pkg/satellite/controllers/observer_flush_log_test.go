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
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The pass after a dump logs how many replicas it wrote and how long
// the node's state took. A pass that shutdown cut short wrote fewer
// than it names, so it must not log that line.
func TestObserverLogsNoDumpPassThatShutdownCutShort(t *testing.T) {
	var buf bytes.Buffer

	ctx, cancel := context.WithCancel(log.IntoContext(t.Context(), captureLogger(&buf)))
	cancel()

	o := &ObserverRunnable{
		pending:    map[string]pendingWrite{"r0": {}, "r1": {}},
		releasedAt: time.Now(),
	}

	o.flush(ctx, nil)

	if strings.Contains(buf.String(), "written after the events2 dump") {
		t.Fatalf("a pass cut short logged it had written the dump:\n%s", buf.String())
	}
}
