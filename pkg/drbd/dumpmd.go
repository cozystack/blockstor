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

package drbd

import (
	"bufio"
	"bytes"
	"context"
	"strings"

	"github.com/cockroachdb/errors"
)

// MetadataUUIDs runs `drbdadm dump-md <target>` and returns every
// generation UUID the metadata carries: the current UUID, the bitmap UUID of
// every peer slot and the history UUIDs. `get-gi` shows only one peer slot,
// and when a Primary loses a peer the old current UUID moves into that
// peer's bitmap slot, so a check against get-gi alone misses it.
func (a *Adm) MetadataUUIDs(ctx context.Context, target string) ([]string, error) {
	out, err := a.exec.Run(ctx, "drbdadm", "dump-md", target)
	if err != nil {
		return nil, errors.Wrapf(err, "drbdadm dump-md %s", target)
	}

	return ParseMetadataUUIDs(out), nil
}

// dump-md prints the bitmap as long lines; the scanner buffer starts small
// and may grow to dumpLineMax.
const (
	dumpLineBuffer = 64 << 10
	dumpLineMax    = 16 << 20
)

// ParseMetadataUUIDs extracts the generation UUIDs from a dump-md listing,
// upper-cased and without the 0x prefix; zero UUIDs are left out.
func ParseMetadataUUIDs(dump []byte) []string {
	var out []string

	scanner := bufio.NewScanner(bytes.NewReader(dump))
	scanner.Buffer(make([]byte, 0, dumpLineBuffer), dumpLineMax)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		for _, key := range []string{"current-uuid", "bitmap-uuid", "history-uuids"} {
			rest, found := strings.CutPrefix(line, key)
			if !found {
				continue
			}

			rest, _, _ = strings.Cut(rest, ";")

			for field := range strings.FieldsSeq(rest) {
				uuid := strings.ToUpper(strings.TrimPrefix(strings.TrimPrefix(field, "0x"), "0X"))
				if strings.Trim(uuid, "0") != "" {
					out = append(out, uuid)
				}
			}
		}
	}

	return out
}

// SameGeneration compares two generation UUIDs the way DRBD does: the
// lowest bit marks a UUID issued while Primary and is cleared when the UUID
// moves into history, so it takes no part in the identity.
func SameGeneration(a, b string) bool {
	return strings.EqualFold(clearPrimaryBit(a), clearPrimaryBit(b))
}

func clearPrimaryBit(uuid string) string {
	uuid = strings.ToUpper(strings.TrimPrefix(strings.TrimPrefix(uuid, "0x"), "0X"))
	if uuid == "" {
		return uuid
	}

	last := uuid[len(uuid)-1]

	const hex = "0123456789ABCDEF"

	idx := strings.IndexByte(hex, last)
	if idx < 0 {
		return uuid
	}

	return uuid[:len(uuid)-1] + string(hex[idx&^1])
}
