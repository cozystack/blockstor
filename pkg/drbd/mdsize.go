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

// Internal metadata sits at the end of the lower disk, so a volume whose
// DRBD device must expose N KiB needs a lower disk of N KiB plus the
// metadata. The layout, as the DRBD 9 kernel and drbdmeta lay it out:
//
//   - a 4 KiB superblock;
//   - the activity log, its stripes times its stripe size;
//   - one bitmap per peer slot, one bit per 4 KiB of the lower disk,
//     each peer's bitmap a whole number of 8-byte words;
//   - the whole bitmap area in 4 KiB blocks.
//
// The data area is the lower disk in 4 KiB blocks, less the metadata.
// LINSTOR sizes the lower disks it creates the same way, so a lower disk
// sized here holds exactly the device a LINSTOR peer with the same peer
// slots exposes.
const (
	mdBlockKiB        = 4
	mdSuperblockKiB   = 4
	bitmapBitCoverKiB = 4
	bitmapByteCover   = 8 * bitmapBitCoverKiB
	bitmapWordBytes   = 8
	bytesPerKiB       = 1024
	// minGrossKiB is the smallest lower disk DRBD attaches.
	minGrossKiB = 68
)

// InternalMetadataGrossKiB returns the smallest lower disk, in KiB, whose
// data area under internal metadata of the given layout is at least
// netKiB. It is the size to create a lower disk with; the storage
// provider rounds it up to its own allocation unit.
//
// The bitmap covers the whole lower disk, metadata included, so its size
// depends on the answer. It is grown until it covers the disk it is part
// of, which takes two or three rounds.
func InternalMetadataGrossKiB(netKiB int64, geometry MetadataGeometry) int64 {
	data := alignUp(max(netKiB, 0), mdBlockKiB)
	fixed := mdSuperblockKiB + activityLogKiB(geometry)

	var bitmap int64

	for {
		gross := data + fixed + bitmap

		need := bitmapKiB(gross, geometry.MaxPeers)
		if need <= bitmap {
			return max(gross, minGrossKiB)
		}

		bitmap = need
	}
}

// InternalMetadataNetKiB returns the data area, in KiB, that internal
// metadata of the given layout leaves on a lower disk of grossKiB: the
// size of the DRBD device the kernel exposes once that disk is attached.
// Zero when the disk cannot hold the metadata.
func InternalMetadataNetKiB(grossKiB int64, geometry MetadataGeometry) int64 {
	usable := grossKiB / mdBlockKiB * mdBlockKiB
	metadata := mdSuperblockKiB + activityLogKiB(geometry) + bitmapKiB(grossKiB, geometry.MaxPeers)

	return max(usable-metadata, 0)
}

// bitmapKiB is the size of the bitmap area covering a lower disk of
// grossKiB for the given number of peer slots.
func bitmapKiB(grossKiB int64, peers int) int64 {
	perPeer := alignUp(ceilDiv(alignUp(grossKiB, bitmapBitCoverKiB), bitmapByteCover), bitmapWordBytes)
	total := perPeer * int64(max(peers, 1))

	return alignUp(ceilDiv(total, bytesPerKiB), mdBlockKiB)
}

// activityLogKiB is the size of the activity log.
func activityLogKiB(geometry MetadataGeometry) int64 {
	stripes := geometry.ALStripes
	if stripes < 1 {
		stripes = defaultALStripes
	}

	stripeSize4k := geometry.ALStripeSize4k
	if stripeSize4k < 1 {
		stripeSize4k = defaultALStripeSize4k
	}

	return int64(stripes) * int64(stripeSize4k) * alStripeUnitKiB
}

func ceilDiv(value, divisor int64) int64 {
	return (value + divisor - 1) / divisor
}

func alignUp(value, alignment int64) int64 {
	return ceilDiv(value, alignment) * alignment
}
