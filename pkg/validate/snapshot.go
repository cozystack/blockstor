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

package validate

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrRestoreNodeLacksSnapshot refuses a restore onto a node that does not
// hold the snapshot being restored.
var ErrRestoreNodeLacksSnapshot = errors.New("node does not hold this snapshot")

// RestoreNodesHoldSnapshot checks that every node an operator named for a
// restore actually carries the snapshot.
//
// A snapshot exists on the nodes that were holding the source when it was
// taken, and nowhere else. Restoring onto a node outside that set produces a
// replica with no data behind it: the definition and the resource are created,
// the satellite finds nothing to receive, and the result reports success while
// the volume is empty. The restore has to fail before it writes anything.
//
// Two carve-outs, both matching the REST path:
//   - No named nodes: the caller falls back to the snapshot's own node list,
//     which trivially holds it.
//   - A snapshot that records no nodes at all: nothing to check against, so
//     the caller's own emptiness guard is the one that applies.
func RestoreNodesHoldSnapshot(named, snapshotNodes []string) error {
	missing := RestoreNodesMissingSnapshot(named, snapshotNodes)
	if len(missing) == 0 {
		return nil
	}

	return fmt.Errorf("%w: %s (snapshot is on %s)",
		ErrRestoreNodeLacksSnapshot,
		strings.Join(missing, ", "),
		strings.Join(snapshotNodes, ", "))
}

// RestoreNodesMissingSnapshot lists the named nodes that do not hold the
// snapshot, sorted, with duplicates and empty entries dropped. Empty means
// the restore may proceed.
//
// Separate from the error so a caller that shapes its own refusal — the REST
// door builds a typed envelope naming each node — works off the same decision
// rather than a second implementation of it.
func RestoreNodesMissingSnapshot(named, snapshotNodes []string) []string {
	if len(named) == 0 || len(snapshotNodes) == 0 {
		return nil
	}

	holds := make(map[string]struct{}, len(snapshotNodes))
	for _, n := range snapshotNodes {
		holds[n] = struct{}{}
	}

	seen := make(map[string]struct{}, len(named))
	missing := make([]string, 0, len(named))

	for _, n := range named {
		if n == "" {
			continue
		}

		if _, dup := seen[n]; dup {
			continue
		}

		seen[n] = struct{}{}

		if _, ok := holds[n]; !ok {
			missing = append(missing, n)
		}
	}

	sort.Strings(missing)

	return missing
}
