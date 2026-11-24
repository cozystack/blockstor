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
	"context"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// handleObservation is one frame end to end: the reader's ingest, then
// a writer pass. Start runs the two on separate goroutines; tests that
// assert on a single frame's outcome run them in turn.
func (o *ObserverRunnable) handleObservation(ctx context.Context, adm *drbd.Adm, ev *observation) {
	o.ingest(ctx, ev)
	o.flush(ctx, adm)
}
