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

	"github.com/go-logr/logr"
)

// ScanOnceForTest is the test-only entry point that drives one
// PhysicalDevice discovery cycle without starting the ticker
// goroutine. Mirrors the storage-sweeper's `sweepOnce(ctx, logger)`
// pattern — kept exported via this file (only compiled into the
// _test binary) so the test package can pin per-tick behaviour
// without exporting an API for production callers.
func ScanOnceForTest(ctx context.Context, p *PhysicalDeviceDiscoveryRunnable, logger logr.Logger) error {
	return p.scanOnce(ctx, logger)
}

// DiscoveryTickForTest is the test-only entry point that drives one
// DiscoveredStorage tick without starting the ticker goroutine.
// Same pattern as ScanOnceForTest — kept exported in the _test
// binary only so production callers can't accidentally bypass the
// loop's error-logging path.
func DiscoveryTickForTest(ctx context.Context, d *DiscoveredStorageRunnable, logger logr.Logger) error {
	return d.tick(ctx, logger)
}
