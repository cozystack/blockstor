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

// CreateMDWithMaxPeers initialises metadata with the peer-slot count
// given and the default activity-log layout.
func (a *Adm) CreateMDWithMaxPeers(ctx context.Context, resource string, maxPeers int) error {
	return a.CreateMDWithGeometry(ctx, resource, GeometryForPeerSlots(maxPeers))
}

// CreateMDWithGeometry initialises on-disk metadata for the resource in
// the layout given.
//
// `--force` is always passed: a freshly allocated volume may carry
// leftover signature bytes from its previous tenant, and drbdmeta bails
// without it. It also overwrites whatever metadata is there, so a caller
// must know no valid metadata is: the satellite gates the call behind a
// HasMD probe.
//
// The size of internal metadata grows with its peer slots and its activity
// log, and it sits at the end of the lower disk, so the layout decides how
// much of the disk is left for data. A lower disk that already holds data
// laid out for one layout has room for metadata of that layout; created
// larger, the data area shrinks under the filesystem. drbdmeta refuses that
// for a filesystem it knows (ErrCreateMDRefused), and for content it does
// not know, `--force` answers its question with yes and the end of the data
// is overwritten. So a caller that re-initialises a superblock passes the
// layout that superblock had.
//
// drbdmeta explains a refusal on stdout and exits with a bare status, so
// the explanation is carried in the error: without it the log shows only
// "exit code 40" and the reason is lost.
func (a *Adm) CreateMDWithGeometry(ctx context.Context, resource string, geometry MetadataGeometry) error {
	args := []string{"create-md", "--force", fmt.Sprintf("--max-peers=%d", geometry.MaxPeers)}

	// The default layout is left to drbdmeta, which keeps the command the
	// same as before for every volume that never had another one.
	if !geometry.defaultActivityLog() {
		args = append(args,
			fmt.Sprintf("--al-stripes=%d", geometry.ALStripes),
			fmt.Sprintf("--al-stripe-size-kB=%d", geometry.ALStripeSize4k*alStripeUnitKiB))
	}

	out, err := a.exec.Run(ctx, "drbdadm", append(args, resource)...)
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

// MetadataGeometry is the layout of a v09 superblock that decides the size
// of its metadata, and the node id it was written for.
type MetadataGeometry struct {
	// MaxPeers is the number of peer slots, 1 to MetadataMaxPeerSlots.
	MaxPeers int
	// ALStripes and ALStripeSize4k are the activity-log layout, the
	// latter in 4 KiB units as drbdmeta prints it.
	ALStripes      int
	ALStripeSize4k int
	// NodeID is the node id the superblock carries, -1 for metadata no
	// kernel attached yet.
	NodeID int
}

// MetadataMaxPeerSlots is the largest peer-slot count a v09 superblock can
// carry (DRBD_PEERS_MAX - 1), and the most LINSTOR lets a definition have.
const MetadataMaxPeerSlots = 31

// The default activity-log layout drbdmeta uses without --al-stripes and
// --al-stripe-size-kB: one stripe of 32 KiB.
const (
	defaultALStripes      = 1
	defaultALStripeSize4k = 8
	alStripeUnitKiB       = 4
)

// GeometryForPeerSlots is the layout blockstor creates metadata with on a
// lower disk of its own: the replica's peer slots and the default
// activity log, as LINSTOR creates it (DrbdLayer.createMetaData).
func GeometryForPeerSlots(peerSlots int) MetadataGeometry {
	return MetadataGeometry{
		MaxPeers:       peerSlots,
		ALStripes:      defaultALStripes,
		ALStripeSize4k: defaultALStripeSize4k,
		NodeID:         -1,
	}
}

// ClampedTo returns g with no more than peerSlots peer slots, the count
// the replica creates metadata with. Fewer slots make smaller metadata,
// so a lower disk laid out for more still holds it; the data area grows
// by the difference.
func (g MetadataGeometry) ClampedTo(peerSlots int) MetadataGeometry {
	g.MaxPeers = min(g.MaxPeers, peerSlots)

	return g
}

func (g MetadataGeometry) defaultActivityLog() bool {
	return g.ALStripes == defaultALStripes && g.ALStripeSize4k == defaultALStripeSize4k
}

// MetadataGeometry reads the layout of the superblock on the lower disk of
// target, a `<rd>/<volume>` drbdadm argument.
//
// It reads a superblock whose activity log was not applied, too: that is
// the state a snapshot of an attached volume carries into its clones, and
// dump-md refuses it without --force. The layout is in the header, which
// the unapplied log does not affect.
func (a *Adm) MetadataGeometry(ctx context.Context, target string) (MetadataGeometry, error) {
	out, err := a.exec.Run(ctx, "drbdadm", "dump-md", "--force", target)
	if err != nil {
		return MetadataGeometry{}, errors.Wrapf(err, "drbdadm dump-md --force %s", target)
	}

	geometry, err := ParseMetadataGeometry(out)
	if err != nil {
		return MetadataGeometry{}, errors.Wrapf(err, "drbdadm dump-md --force %s", target)
	}

	return geometry, nil
}

// ErrNoGeometry reports a dump-md listing that does not state the layout of
// its superblock: a v08 superblock, which has no peer slots to speak of, or
// a listing in a shape this parser does not know.
var ErrNoGeometry = errors.New("the metadata dump does not state its layout")

// geometryFields are the header statements ParseMetadataGeometry needs,
// with the range each must be in.
//
//nolint:gochecknoglobals // a fixed vocabulary, matched in one place
var geometryFields = map[string][2]int{
	"max-peers":         {1, MetadataMaxPeerSlots},
	"al-stripes":        {1, 1 << 20},
	"al-stripe-size-4k": {1, 1 << 20},
	"node-id":           {-1, MetadataMaxPeerSlots},
}

// ParseMetadataGeometry returns the layout a v09 dump-md listing states.
// Every statement it needs has to be there: a layout guessed is a layout
// that can be wrong, and the caller sizes metadata over data with it.
func ParseMetadataGeometry(dump []byte) (MetadataGeometry, error) {
	scanner := bufio.NewScanner(bytes.NewReader(dump))
	scanner.Buffer(make([]byte, 0, dumpLineBuffer), dumpLineMax)

	values := map[string]int{}

	for scanner.Scan() && len(values) < len(geometryFields) {
		key, value, ok := strings.Cut(strings.TrimSpace(scanner.Text()), " ")
		bounds, known := geometryFields[key]

		if !ok || !known {
			continue
		}

		if _, seen := values[key]; seen {
			continue
		}

		n, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(value, ";")))
		if err != nil || n < bounds[0] || n > bounds[1] {
			return MetadataGeometry{}, errors.Wrapf(ErrNoGeometry, "%s %q", key, value)
		}

		values[key] = n
	}

	err := scanner.Err()
	if err != nil {
		return MetadataGeometry{}, errors.Wrap(err, "read the dump-md output")
	}

	for key := range geometryFields {
		if _, ok := values[key]; !ok {
			return MetadataGeometry{}, errors.Wrapf(ErrNoGeometry, "no %s statement", key)
		}
	}

	return MetadataGeometry{
		MaxPeers:       values["max-peers"],
		ALStripes:      values["al-stripes"],
		ALStripeSize4k: values["al-stripe-size-4k"],
		NodeID:         values["node-id"],
	}, nil
}
