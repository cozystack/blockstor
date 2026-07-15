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

package linstormigrate

// LINSTOR stores object flags in its database as integer bitmasks; the
// REST API (and blockstor) speak flag STRINGS. The bit values below were
// calibrated EMPIRICALLY by cross-referencing production database dumps
// against the same cluster's `linstor --machine-readable r l` / `s l`
// output (observed bit sets ↔ reported flag strings), NOT by reading the
// LINSTOR source:
//
//   - resource_flags 388 = 0b110000100 on a replica the REST API reports
//     as DISKLESS,DRBD_DISKLESS,TIE_BREAKER;
//   - resource_flags 268 = 0b100001100 on plain diskless replicas
//     (DISKLESS,DRBD_DISKLESS without TIE_BREAKER) — isolating bit 128
//     as TIE_BREAKER and bit 256 as DRBD_DISKLESS;
//   - a snapshot definition with resource_flags=2 that `linstor s l`
//     reports as FAILED_DEPLOYMENT, and the complementary population of
//     flags=1 (successfully deployed snapshots) pinning SUCCESSFUL=1.
//
// Bits observed in production dumps that map to no REST-visible flag
// (e.g. 128 on a DISKFUL replica after an auto-diskful toggle, 8 on
// diskless replicas, 64/2048/262144) are runtime bookkeeping — the
// converter reports them and deliberately does NOT emit flag strings
// for them.
const (
	// resourceFlagDelete marks an object under deletion (bit 1<<1,
	// shared by resources, resource definitions, volumes and snapshot
	// definitions in LINSTOR's schema; for snapshot definitions the
	// same bit doubles as FAILED_DEPLOYMENT — see snapshot handling).
	resourceFlagDelete int64 = 1 << 1

	// resourceFlagDiskless is the generic diskless bit (1<<2).
	resourceFlagDiskless int64 = 1 << 2

	// resourceFlagTieBreaker marks an auto-placed quorum witness
	// (1<<7). Only meaningful on a replica that is also diskless —
	// production dumps show stale TIE_BREAKER bits left behind on
	// replicas that auto-diskful later converted to diskful, which
	// the REST API filters out.
	resourceFlagTieBreaker int64 = 1 << 7

	// resourceFlagDrbdDiskless (1<<8) is the DRBD-layer diskless bit
	// that accompanies resourceFlagDiskless on DRBD-backed replicas.
	resourceFlagDrbdDiskless int64 = 1 << 8

	// snapDfnFlagSuccessful (1<<0) marks a snapshot definition whose
	// deployment completed on every node.
	snapDfnFlagSuccessful int64 = 1 << 0

	// snapDfnFlagFailedDeployment (1<<1) marks a snapshot definition
	// whose take failed — calibrated against a live `linstor s l`
	// showing FAILED_DEPLOYMENT for a dump row with resource_flags=2.
	snapDfnFlagFailedDeployment int64 = 1 << 1
)

// blockstor flag string vocabulary (matches pkg/rest/flags_validation.go
// and upstream golinstor apiconsts).
const (
	flagStrDiskless     = "DISKLESS"
	flagStrDrbdDiskless = "DRBD_DISKLESS"
	flagStrTieBreaker   = "TIE_BREAKER"
)

// decodeResourceFlags translates a RESOURCES-row bitmask into blockstor
// flag strings. The TIE_BREAKER bit is honoured only when the DISKLESS
// bit is also set: production dumps show stale TIE_BREAKER bits left on
// replicas that auto-diskful converted to diskful (which the REST API
// filters out the same way). Note LINSTOR writes no LAYER_STORAGE_VOLUMES
// row for diskless replicas, so the replica's own flag bits — not the
// storage layer — are the authoritative diskless signal.
// The second return carries the bits the converter dropped (recognised
// but unrepresentable, or unknown) for the migration report; 0 when
// everything mapped.
func decodeResourceFlags(flags int64) ([]string, int64) {
	var out []string

	rest := flags
	diskless := flags&resourceFlagDiskless != 0

	if diskless {
		out = append(out, flagStrDiskless)
		rest &^= resourceFlagDiskless
	}

	if flags&resourceFlagDrbdDiskless != 0 {
		out = append(out, flagStrDrbdDiskless)
		rest &^= resourceFlagDrbdDiskless
	}

	if flags&resourceFlagTieBreaker != 0 && diskless {
		out = append(out, flagStrTieBreaker)
		rest &^= resourceFlagTieBreaker
	}

	return out, rest
}
