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

	uuids, err := ParseMetadataUUIDs(out)
	if err != nil {
		return nil, errors.Wrapf(err, "drbdadm dump-md %s", target)
	}

	return uuids, nil
}

// The dump carries the whole bitmap, one line per run or per few words, so
// the lines are short; the buffer still grows to dumpLineMax for a dump whose
// format changes. The output as a whole is held in memory: for a large,
// heavily dirty volume that is tens of megabytes, once per check.
const (
	dumpLineBuffer = 64 << 10
	dumpLineMax    = 16 << 20
)

// ParseMetadataUUIDs extracts the generation UUIDs from a dump-md listing,
// upper-cased and without the 0x prefix; zero UUIDs are left out.
//
// drbdmeta prints them as statements and blocks (print_dump_gi in
// drbd-utils' drbdmeta.c). In the v09 format the current UUID and each peer
// slot's bitmap UUID are single statements, and the history is a block of
// several UUIDs to a line:
//
//	current-uuid 0x9F00AA11BB22CC33;
//	peer[1] {
//	    bitmap-uuid 0x6E3A5D0B2C1F4A96;
//	}
//	history-uuids {
//	        0x1234567890ABCDEF; 0x0000000000000000; ...
//	}
//
// The v08 format has a single block of four UUIDs, current, bitmap and two
// of history, then the flags. Nothing else is read: the bitmap words and the
// device UUID are not generation identifiers.
//
// A dump that names no current UUID, a v09 `current-uuid` statement or a v08
// `uuid` block, is not one this parser can read: empty output, a keyword
// drbdmeta renamed, or a line the scanner gave up on. That is an error, never
// an empty answer, since the caller would read an empty answer as "this
// superblock holds none of our generations" and re-initialise it.
func ParseMetadataUUIDs(dump []byte) ([]string, error) {
	var (
		out     []string
		block   bool
		current bool
	)

	scanner := bufio.NewScanner(bytes.NewReader(dump))
	scanner.Buffer(make([]byte, 0, dumpLineBuffer), dumpLineMax)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if block {
			body, _, closed := strings.Cut(line, "}")
			out = appendUUIDStatements(out, body)
			block = !closed

			continue
		}

		key, rest, _ := strings.Cut(line, " ")

		switch key {
		case dumpKeyCurrent, "bitmap-uuid":
			current = current || key == dumpKeyCurrent
			out = appendUUIDStatements(out, rest)
		case "history-uuids", dumpKeyV08:
			current = current || key == dumpKeyV08

			body, opens := strings.CutPrefix(strings.TrimSpace(rest), "{")
			if !opens {
				out = appendUUIDStatements(out, rest)

				continue
			}

			body, _, closed := strings.Cut(body, "}")
			out = appendUUIDStatements(out, body)
			block = !closed
		}
	}

	err := scanner.Err()
	if err != nil {
		return nil, errors.Wrap(err, "read the dump-md output")
	}

	if !current {
		return nil, ErrUnreadableMetadataDump
	}

	return out, nil
}

// The statements that name a current UUID: v09's own one, and v08's block
// that leads with it.
const (
	dumpKeyCurrent = "current-uuid"
	dumpKeyV08     = "uuid"
)

// ErrUnreadableMetadataDump reports dump-md output that names no current
// UUID, which this parser therefore cannot vouch for.
var ErrUnreadableMetadataDump = errors.New("dump-md printed no current UUID this parser recognises")

// appendUUIDStatements adds every statement of text that is a bare non-zero
// hex UUID, such as `0x6E3A5D0B2C1F4A96;`. A keyed statement, such as v08's
// `flags 0x00000011;` inside the uuid block, is not one.
func appendUUIDStatements(out []string, text string) []string {
	for statement := range strings.SplitSeq(text, ";") {
		digits, ok := strings.CutPrefix(strings.TrimSpace(statement), "0x")
		if !ok || !isHex(digits) || strings.Trim(digits, "0") == "" {
			continue
		}

		out = append(out, strings.ToUpper(digits))
	}

	return out
}

func isHex(digits string) bool {
	if digits == "" {
		return false
	}

	for _, c := range strings.ToLower(digits) {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}

	return true
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
