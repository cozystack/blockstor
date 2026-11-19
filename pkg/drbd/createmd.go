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

// ErrCreateMDRefused reports that drbdmeta refused to write metadata
// because it would land on data: the data area left by the new metadata
// is smaller than the filesystem already on the lower disk, or the
// metadata would overwrite it. drbdmeta refuses this the same way on
// every attempt, and `--force` does not override it.
var ErrCreateMDRefused = errors.New("drbdmeta refused to create metadata over the data on the lower disk")

// drbdmetaRefused is the line drbdmeta prints on stdout before it exits
// 40 out of check_for_existing_data.
const drbdmetaRefused = "Operation refused."

// CreateMDWithMaxPeers is CreateMD with the peer-slot count given.
//
// The size of internal metadata grows with its peer slots, and it sits at
// the end of the lower disk, so the slot count decides how much of the disk
// is left for data. A lower disk that already holds data laid out for N
// slots has room for metadata of N slots; created with more, the data area
// shrinks under the filesystem and drbdmeta refuses (ErrCreateMDRefused).
//
// drbdmeta explains a refusal on stdout and exits with a bare status, so
// the explanation is carried in the error: without it the log shows only
// "exit code 40" and the reason is lost.
func (a *Adm) CreateMDWithMaxPeers(ctx context.Context, resource string, maxPeers int) error {
	out, err := a.exec.Run(ctx,
		"drbdadm",
		"create-md",
		"--force",
		fmt.Sprintf("--max-peers=%d", maxPeers),
		resource)
	if err == nil {
		return nil
	}

	reason, refused := createMDRefusal(out)
	if !refused {
		return errors.Wrap(err, "drbdadm create-md")
	}

	return errors.Wrapf(ErrCreateMDRefused, "drbdadm create-md: %s (%v)", reason, err)
}

// createMDRefusal extracts drbdmeta's explanation of a refusal: the text
// from the line naming what it found on the disk up to "Operation
// refused.", on one line.
func createMDRefusal(stdout []byte) (string, bool) {
	text := string(stdout)

	end := strings.Index(text, drbdmetaRefused)
	if end < 0 {
		return "", false
	}

	text = text[:end+len(drbdmetaRefused)]

	if start := strings.LastIndex(text, "\nFound "); start >= 0 {
		text = text[start+1:]
	}

	return strings.Join(strings.Fields(text), " "), true
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
