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

package drbd

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// ParseRange decodes a "min-max" prop value (decimal). Used by the
// controller to read per-node TCP-port and minor ranges off the Node
// CRD's prop bag, matching upstream LINSTOR's `linstor n set-property
// node DrbdOptions/TcpPortRange "7000-7999"` UX.
func ParseRange(s string) (int32, int32, error) {
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return 0, 0, errors.Errorf("range %q must be \"min-max\"", s)
	}

	low, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "parse low of %q", s)
	}

	high, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 32)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "parse high of %q", s)
	}

	if low > high {
		return 0, 0, errors.Errorf("range %q: low %d > high %d", s, low, high)
	}

	return int32(low), int32(high), nil
}

// DefaultPortRange mirrors the upstream LINSTOR TcpPortPool default.
// Operators can override via controller config; the allocator only
// hands out ports inside [min, max].
//
// DefaultMinorMin is intentionally shifted away from the upstream
// LINSTOR default of 1000 to 20000 so blockstor's per-RD minor
// allocator and any coexisting LINSTOR allocator on the same host
// occupy disjoint device-minor windows. Even when both stacks render
// .res files into separate state directories, a shared minor would
// cause /dev/drbd<N> kernel-object collisions the moment a manual
// drbdadm invocation or a leaked .res reaches the wrong scope. The
// unconditional shift also keeps us clear of any per-node operator
// drbdadm commands stamped at minor 1000 in production deployments.
const (
	DefaultPortMin = 7000
	DefaultPortMax = 7999

	DefaultMinorMin = 20000
	DefaultMinorMax = 65535
)

// ErrPortPoolExhausted is returned when the requested range is fully
// allocated. Callers surface this as 503/conflict so the operator
// either widens the pool or evicts stale allocations.
var (
	ErrPortPoolExhausted  = errors.New("DRBD port pool exhausted")
	ErrMinorPoolExhausted = errors.New("DRBD minor pool exhausted")
)

// LowestFreePort returns the smallest port in [min, max] not present
// in taken. Deterministic — two callers seeing the same taken set
// produce the same answer, so racing allocators converge instead of
// diverging. Returns ErrPortPoolExhausted when the range is full.
//
// The taken slice may include values outside [min, max]; they're
// ignored. This makes it safe to feed in the result of an unfiltered
// scan over Resource.Status.DRBDPort across the whole cluster.
func LowestFreePort(taken []int32, low, high int32) (int32, error) {
	used := make(map[int32]bool, len(taken))
	for _, p := range taken {
		if p >= low && p <= high {
			used[p] = true
		}
	}

	for i := low; i <= high; i++ {
		if !used[i] {
			return i, nil
		}
	}

	return 0, ErrPortPoolExhausted
}

// LowestFreeMinor mirrors LowestFreePort for /dev/drbd<N>. The minor
// range is independent of the port range — DRBD uses the local
// /dev/drbd<N> device path while the port appears on the wire.
func LowestFreeMinor(taken []int32, low, high int32) (int32, error) {
	used := make(map[int32]bool, len(taken))
	for _, m := range taken {
		if m >= low && m <= high {
			used[m] = true
		}
	}

	for i := low; i <= high; i++ {
		if !used[i] {
			return i, nil
		}
	}

	return 0, ErrMinorPoolExhausted
}
