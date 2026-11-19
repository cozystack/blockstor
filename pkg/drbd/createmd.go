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
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// CreateMDWithMaxPeers is CreateMD with the peer-slot count given.
//
// The size of internal metadata grows with its peer slots, and it sits at
// the end of the lower disk, so the slot count decides how much of the disk
// is left for data. A lower disk that already holds data laid out for N
// slots has room for metadata of N slots; created with more, the data area
// shrinks under the filesystem and drbdmeta refuses.
func (a *Adm) CreateMDWithMaxPeers(ctx context.Context, resource string, maxPeers int) error {
	return a.run(ctx,
		"create-md",
		"--force",
		fmt.Sprintf("--max-peers=%d", maxPeers),
		resource)
}

// MetadataMaxPeers reads the peer-slot count of the superblock on the
// lower disk of target, a `<rd>/<volume>` drbdadm argument.
//
// It reads a superblock whose activity log was not applied, too: that is
// the state a snapshot of an attached volume carries into its clones, and
// dump-md refuses it without --force. The count is in the header, which
// the unapplied log does not affect.
func (a *Adm) MetadataMaxPeers(ctx context.Context, target string) (int, error) {
	out, err := a.exec.Run(ctx, "drbdadm", "dump-md", "--force", target)
	if err != nil {
		return 0, errors.Wrapf(err, "drbdadm dump-md --force %s", target)
	}

	peers, err := ParseMetadataMaxPeers(out)
	if err != nil {
		return 0, errors.Wrapf(err, "drbdadm dump-md --force %s", target)
	}

	return peers, nil
}

// ErrNoMaxPeers reports a dump-md listing with no readable max-peers
// statement: a v08 superblock, which has no peer slots to speak of, or a
// listing in a shape this parser does not know.
var ErrNoMaxPeers = errors.New("the metadata dump names no peer-slot count")

// ParseMetadataMaxPeers returns the value of the `max-peers N;` statement
// of a v09 dump-md listing. drbdmeta prints it in the header, before the
// generation identifiers and the bitmap.
func ParseMetadataMaxPeers(dump []byte) (int, error) {
	scanner := bufio.NewScanner(bytes.NewReader(dump))
	scanner.Buffer(make([]byte, 0, dumpLineBuffer), dumpLineMax)

	for scanner.Scan() {
		value, ok := strings.CutPrefix(strings.TrimSpace(scanner.Text()), "max-peers ")
		if !ok {
			continue
		}

		peers, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(value, ";")))
		if err != nil || peers < 1 || peers > MaxPeers-1 {
			return 0, errors.Wrapf(ErrNoMaxPeers, "max-peers %q", value)
		}

		return peers, nil
	}

	err := scanner.Err()
	if err != nil {
		return 0, errors.Wrap(err, "read the dump-md output")
	}

	return 0, ErrNoMaxPeers
}
