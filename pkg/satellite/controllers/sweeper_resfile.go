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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
)

// resFileSuffix is the on-disk suffix the satellite reconciler renders
// per-resource DRBD config into (`<StateDir>/<rd>.res`). Production
// points StateDir at the host-shared /etc/drbd.d/ (Bug 310), which
// `/etc/drbd.conf` includes via `*.res` — so EVERY rendered file is
// parsed by `drbdadm adjust`, even for an unrelated resource.
const resFileSuffix = ".res"

// orphanResForRemoval selects, from the .res files found in StateDir,
// the set whose RD this node no longer owns and that are safe to
// garbage-collect. It is the pure decision core of the .res GC, split
// out from the filesystem + drbdsetup side effects so the selection
// rules can be unit-tested against a canned desired set.
//
// Why this exists (the bug): the satellite renders every owned RD's
// .res into the host-shared /etc/drbd.d/. When a Resource is deleted on
// this node — or a prior test/satellite crash left a file behind —
// the `<rd>.res` can linger. `drbdadm adjust <other-rd>` parses the
// WHOLE directory and a device-minor collision between the stale file
// and a freshly-created RD that reuses that minor returns exit 10
// ("conflicting use of device-minor ...: first used here: exit status
// 10"), making zero kernel changes. The affected peer then stays
// `Connecting` forever. Mirrors upstream LINSTOR DrbdLayer, which
// regenerates the full desired .res set and drops any file not in it
// before running adjust.
//
// Selection rules (a file is removed only when ALL hold):
//   - The directory entry is a regular `*.res` file (never a marker
//     like `.owned`/`.md-created`/`.mkfs.done`, never global_common.conf,
//     never a sub-directory).
//   - Its RD name (basename minus `.res`) is NOT in `owned` — the set of
//     RD names this node currently hosts a Resource CRD for.
//   - The RD is past the create/delete grace window (Bug 291 race: the
//     controller writes the RD before the per-node Resource CRDs, and a
//     delete drops the cached Resource before DeleteResource finishes;
//     a sweep firing inside that window would reap a file the reconciler
//     is about to (re)write). `grace <= 0` disables the window. An RD
//     name absent from `rdAges` is treated as "no grace applies" (the
//     genuine force-strip / crashed-test leftover this GC targets).
//
// `entries` is the result of os.ReadDir(stateDir); passing it in (rather
// than reading inside) keeps this function side-effect free and trivially
// testable. Returns RD names (not file paths) so the caller can correlate
// against the kernel slot list.
func orphanResForRemoval(
	entries []os.DirEntry,
	owned map[string]struct{},
	rdAges map[string]time.Time,
	now time.Time,
	grace time.Duration,
) []string {
	var out []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, resFileSuffix) {
			// Leaves global_common.conf, *.owned, *.md-created,
			// *.mkfs.done and any operator-supplied file untouched.
			continue
		}

		rd := strings.TrimSuffix(name, resFileSuffix)
		if rd == "" {
			continue
		}

		if _, ok := owned[rd]; ok {
			// Live resource this node hosts — never GC its config.
			continue
		}

		if grace > 0 {
			if anchor, ok := rdAges[rd]; ok && now.Sub(anchor) < grace {
				// RD was created/deleted inside the grace window — a
				// create-fanout or delete-in-progress race, not a
				// genuine orphan. Defer to a later cycle.
				continue
			}
		}

		out = append(out, rd)
	}

	return out
}

// sweepOrphanResFiles garbage-collects stale `<rd>.res` files from the
// host-shared StateDir whose RD this node no longer owns. For each
// removal candidate it first tears down the matching kernel slot (when
// `drbdadm adjust` had loaded it from the stale file) so removing the
// file can't leave a dangling minor, THEN unlinks the `.res`.
//
// Idempotent and safe under concurrent reconciles: a missing file is a
// no-op (os.Remove on ENOENT is ignored), and a file the reconciler
// re-renders between the ReadDir and the Remove simply reappears on the
// next render. Only `*.res` files are ever removed — `.owned` (Bug 432
// ownership marker) and live resources are left strictly alone by
// orphanResForRemoval's selection rules.
//
// The kernel-slot probe is per-candidate via Adm.IsLoaded so it reads
// the state AFTER sweepOnce's tearDownOrphans has run — a slot that
// path already downed reads not-loaded here, avoiding a redundant
// `drbdsetup down`.
//
// No-ops when StateDir is unset (unit-test fixtures that wire no
// on-disk state, and the legacy "filter disabled" mode).
func (s *OrphanSweeperRunnable) sweepOrphanResFiles(
	ctx context.Context,
	logger logr.Logger,
	owned map[string]struct{},
	rdAges map[string]time.Time,
) {
	if s.StateDir == "" {
		return
	}

	entries, err := os.ReadDir(s.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Dir not created yet — the first reconcile makes it.
			return
		}

		logger.Error(err, "read StateDir for orphan .res GC", "dir", s.StateDir)

		return
	}

	grace := s.RDGrace
	if grace == 0 {
		grace = SweeperRDGrace
	}

	clock := s.now
	if clock == nil {
		clock = time.Now
	}

	now := clock()

	stale := orphanResForRemoval(entries, owned, rdAges, now, grace)
	if len(stale) == 0 {
		return
	}

	for _, rd := range stale {
		s.removeOrphanResFile(ctx, logger, rd)
	}
}

// removeOrphanResFile tears down the kernel slot (when loaded) for one
// orphan RD and unlinks its `.res`. Per-RD failures are logged and
// skipped so one wedged slot can't stall GC of the rest — the next
// sweep cycle retries.
func (s *OrphanSweeperRunnable) removeOrphanResFile(
	ctx context.Context,
	logger logr.Logger,
	rd string,
) {
	// Probe kernel state at removal time (after sweepOnce's
	// tearDownOrphans). IsLoaded folds any non-zero drbdsetup exit into
	// "not loaded", so a transient probe hiccup biases toward "leave the
	// slot, just remove the file" — acceptable: the next reconcile/sweep
	// re-converges, and the goal is breaking the .res-collision.
	loaded, _ := s.isSlotLoaded(ctx, rd)
	if loaded {
		// Kernel still holds the slot the stale file described. Tear it
		// down (kernel-direct `drbdsetup down`, bounded by
		// SweeperSetupDownTimeout) BEFORE removing the file so we don't
		// orphan a minor in the kernel. If the down fails, keep the file
		// so the next cycle can retry — removing it now would strand the
		// kernel slot with no config to recover it from.
		downErr := s.callSetupDown(ctx, rd)
		if downErr != nil {
			logger.Error(downErr, "drbdsetup down before orphan .res removal; keeping file for retry",
				"resource", rd)

			return
		}

		logger.Info("tore down kernel slot for orphan .res", "resource", rd)
	}

	path := filepath.Join(s.StateDir, rd+resFileSuffix)

	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error(err, "remove orphan .res file", "path", path)

		return
	}

	logger.Info("removed orphan .res file", "path", path)
}

// isSlotLoaded reports whether the kernel currently holds a DRBD slot
// for rd. Defaults to Adm.IsLoaded; the isLoadedFn test hook lets unit
// tests simulate kernel state (and the post-teardown transition) without
// a real drbdsetup. Production callers must leave isLoadedFn nil.
func (s *OrphanSweeperRunnable) isSlotLoaded(ctx context.Context, rd string) (bool, error) {
	if s.isLoadedFn != nil {
		return s.isLoadedFn(ctx, rd)
	}

	loaded, err := s.Adm.IsLoaded(ctx, rd)

	return loaded, errors.Wrap(err, "probe kernel slot")
}
