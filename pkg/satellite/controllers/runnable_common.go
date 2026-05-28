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
	"time"

	"github.com/go-logr/logr"
)

// runPeriodicTick is the shared serve loop used by every
// fire-and-forget periodic runnable in the satellite-controllers
// package (DiscoveredStorageRunnable, OrphanSweeperRunnable, …).
//
// Shape: immediate first-tick on entry — controller-runtime has
// already waited for cache sync before calling Start, so we can
// safely fire the initial pass without warm-up logic. After that
// a time.Ticker drives the loop at `period`. Ticker errors are
// logged through `logger` under `tickErrLabel` so the audit grep
// for the per-runnable error label stays stable across loops.
//
// Centralised here so dupl doesn't flag the two byte-identical
// loops that previously lived inline in DiscoveredStorageRunnable
// and OrphanSweeperRunnable.
func runPeriodicTick(
	ctx context.Context,
	period time.Duration,
	logger logr.Logger,
	tick func(context.Context, logr.Logger) error,
	initialErrLabel, tickErrLabel string,
) error {
	err := tick(ctx, logger)
	if err != nil {
		logger.Error(err, initialErrLabel)
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err = tick(ctx, logger)
			if err != nil {
				logger.Error(err, tickErrLabel)
			}
		}
	}
}
