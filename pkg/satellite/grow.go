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

package satellite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Sizes a grow reserves under the definition size, in KiB, as LINSTOR's
// layer size calculators do (DrbdLayerSizeCalculator with
// MetaData.getGrossSize, LuksLayerSizeCalculator).
const (
	// drbdMetaSuperblockKib is DRBD's 4 KiB metadata superblock.
	drbdMetaSuperblockKib = 4
	// drbdMetaActivityLogKib is the default activity log: one stripe of
	// 32 KiB.
	drbdMetaActivityLogKib = 32
	// luks2HeaderKib is the LUKS2 header cryptsetup reserves by default.
	luks2HeaderKib = 16 * 1024
	// drbdMetaAlignKib is the alignment of DRBD's on-disk bitmap.
	drbdMetaAlignKib = 4
	// kibPerBitmapBit is the data one bit of DRBD's bitmap tracks.
	kibPerBitmapBit = 4
	// kibPerMib is the unit the providers resize in.
	kibPerMib   = 1024
	bitsPerByte = 8
	bytesPerKib = 1024
)

// sysClassBlock is where sysfs lists every block device by kernel name.
const sysClassBlock = "/sys/class/block/"

// resizeRetryLimit bounds how many passes in a row may re-run the resize
// of a grow, for one target size, when the resize succeeds and the device
// still does not grow. That is a device held back by a peer whose
// backing has not grown yet (DRBD grows to the smallest backing of its
// peers, and that peer's own resize grows it everywhere), and each such
// pass keeps the late-add self-heals off (maybePromoteSelfHeals).
const resizeRetryLimit = 3

func ceilDiv(n, d int64) int64 { return (n + d - 1) / d }

func alignUp(n, to int64) int64 { return ceilDiv(n, to) * to }

// drbdInternalMetaKib is an upper bound on the internal metadata DRBD
// keeps at the end of a backing device of backingKib for the given peer
// slots: the superblock, the activity log, and a bitmap of one bit per
// 4 KiB per peer slot, in 64-bit words, rounded to 4 KiB.
func drbdInternalMetaKib(backingKib int64, peerSlots int) int64 {
	bits := alignUp(ceilDiv(backingKib, kibPerBitmapBit), 64)
	bitmapKib := alignUp(ceilDiv(bits/bitsPerByte*int64(peerSlots), bytesPerKib), drbdMetaAlignKib)

	return drbdMetaSuperblockKib + drbdMetaActivityLogKib + bitmapKib
}

// drbdLeastMetaKib is the least internal metadata DRBD can keep at the
// end of a backing device of backingKib: the superblock, the activity
// log and a bitmap for a single peer slot.
func drbdLeastMetaKib(backingKib int64) int64 {
	bits := alignUp(ceilDiv(backingKib, kibPerBitmapBit), 64)

	return drbdMetaSuperblockKib + drbdMetaActivityLogKib + alignUp(ceilDiv(bits/bitsPerByte, bytesPerKib), drbdMetaAlignKib)
}

// backingTooSmallForDevice reports whether vol's backing of usableKib,
// of which a LUKS header takes headerKib from the front, cannot hold the
// DRBD device the replicas report even with the least metadata DRBD
// keeps on it, so the kernel refuses to attach it ("cannot attach
// smaller disk"). Such a backing was never attached at that device
// size: a replica attached to it would have kept the device within it.
func backingTooSmallForDevice(vol *intent.DesiredVolume, usableKib, headerKib int64) bool {
	device := vol.GetDeviceSizeKib()
	if usableKib <= 0 || device <= 0 || vol.GetMetaPool() != "" {
		return false
	}

	data := usableKib - headerKib

	return data-drbdLeastMetaKib(data) < device
}

// disklessProbe asks the kernel, at most once per pass and only when a
// size check calls for it, which volumes of a resource it holds Diskless
// (drbd.Adm.DisklessVolumes): every volume of a resource is answered by
// the same `drbdsetup status`.
type disklessProbe struct {
	adm      *drbd.Adm
	resource string
	volumes  map[int32]struct{}
	asked    bool
}

func newDisklessProbe(adm *drbd.Adm, resource string) *disklessProbe {
	return &disklessProbe{adm: adm, resource: resource}
}

// holds reports whether the kernel holds volume vn Diskless.
func (p *disklessProbe) holds(ctx context.Context, vn int32) bool {
	if p == nil || p.adm == nil {
		return false
	}

	if !p.asked {
		p.volumes = p.adm.DisklessVolumes(ctx, p.resource)
		p.asked = true
	}

	_, diskless := p.volumes[vn]

	return diskless
}

// drbdGrossKib is the backing size a DRBD volume with internal metadata
// for peerSlots needs to offer netKib: the bitmap covers the whole
// backing device, so the smallest size whose metadata leaves netKib for
// data.
func drbdGrossKib(netKib int64, peerSlots int) int64 {
	gross := netKib + drbdInternalMetaKib(netKib, peerSlots)
	for gross-drbdInternalMetaKib(gross, peerSlots) < netKib {
		gross = netKib + drbdInternalMetaKib(gross, peerSlots)
	}

	return gross
}

// growBackingKib is the backing size a grow of vol has to reach for the
// device the consumer sees to have the definition size: the size plus
// DRBD's internal metadata when DRBD keeps it on this device, plus the
// LUKS header when LUKS sits under it. The metadata is sized for the
// replica's peer slots but never for fewer than 15 (grownPeerSlots). LINSTOR sizes the backing the
// same way, so an adopted volume's backing already carries both. Under
// DRBD the data area is the definition or the largest device any
// replica reports, whichever is larger, as for a new lower disk
// (lowerDiskKib): a volume adopted from LINSTOR exposes a few KiB more
// than its definition, and a backing grown short of that could not be
// attached.
func growBackingKib(dr *intent.DesiredResource, vol *intent.DesiredVolume, withDRBD bool) int64 {
	size := vol.GetSizeKib()

	if withDRBD && vol.GetMetaPool() == "" {
		size = drbdGrossKib(max(size, vol.GetDeviceSizeKib()), grownPeerSlots(dr))
	}

	if needsLUKS(dr.GetLayerStack()) {
		size += luks2HeaderKib
	}

	// The LVM and ZFS providers resize in whole MiB, rounding down, so
	// an unaligned size would lose the margin added above.
	return alignUp(size, kibPerMib)
}

// grownPeerSlots is the peer-slot count a grow sizes the metadata of an
// existing device for. The superblock can hold more slots than the
// replica records: a satellite that predates the record creates 15
// whatever the controller stamped, as during a rolling upgrade. Sized
// for the record alone, the grow would leave the device short of the
// definition by the difference; sized for 15 when the record is lower,
// it only oversizes the bitmap, by 32 KiB per GiB for each slot too
// many.
func grownPeerSlots(dr *intent.DesiredResource) int {
	return max(drbd.EffectivePeerSlots(dr.GetPeerSlots()), drbd.LegacyPeerSlots)
}

// growKey builds the key of the per-volume grow tables.
func growKey(rdName string, vn int32) string {
	return rdName + "/" + strconv.FormatInt(int64(vn), 10)
}

// growStep is what applyStorage has to do about a grow of one volume
// on this pass.
type growStep struct {
	// pending: the replica has not finished a grow to the definition
	// size, so the device is measured before the size is reported.
	pending bool
	// backingKib, when above zero, is the size to grow the backing to.
	backingKib int64
	// retryResize: the backing needs no growing, but the layers above
	// have not caught up, so they are resized again.
	retryResize bool
	// beforeAttach: the backing grows so the kernel can attach it to
	// a device that is already larger (planAttachGrow); the device keeps
	// its size, so nothing above is resized.
	beforeAttach bool
}

// planAttachGrow decides a grow of the backing of a replica the kernel
// holds without its disk, when that backing cannot hold the device the
// replicas report. An older satellite created such backings at the
// definition's size for volumes adopted from LINSTOR, whose device is a
// few KiB larger than the definition, and the kernel refused each attach
// with "cannot attach smaller disk". The definition never changed, so
// planGrow sees no grow, and the attach would be retried and refused
// for good.
//
// The backing grows as a grow of the definition would grow it
// (growBackingKib), so the next attach succeeds and the replica syncs
// from its peers. It grows only while the kernel positively reports the
// volume Diskless: a backing it cannot attach holds no data the device
// serves, and a replica the kernel does not hold yet, as after a reboot,
// is left alone. An attached backing is never grown here, so volumes
// whose backings are exactly the definition keep them. The kernel is
// asked only once the sizes call for it, so a backing that holds the
// device costs no exec. Under LUKS the mapper grows after it
// (luksMapperTooSmallForDevice).
func (r *Reconciler) planAttachGrow(
	ctx context.Context, dr *intent.DesiredResource, vol *intent.DesiredVolume, status storage.VolumeStatus, kernel *disklessProbe,
) growStep {
	usableKib := status.UsableKib

	header := int64(0)
	if needsLUKS(dr.GetLayerStack()) {
		header = luks2HeaderKib
	}

	if !backingTooSmallForDevice(vol, usableKib, header) {
		return growStep{}
	}

	// A LUKS1 header, or one some LUKS2 builds wrote, is smaller than
	// the 16 MiB a new one takes; read the one on the backing rather
	// than grow a backing that holds the device.
	if header > 0 && r.cfg.Cryptsetup != nil {
		actual, ok := r.cfg.Cryptsetup.DataOffsetKiB(ctx, status.DevicePath)
		if ok && !backingTooSmallForDevice(vol, usableKib, actual) {
			return growStep{}
		}
	}

	if !kernel.holds(ctx, vol.GetVolumeNumber()) {
		return growStep{}
	}

	target := growBackingKib(dr, vol, true)
	if target <= usableKib {
		return growStep{}
	}

	log.FromContext(ctx).Info("growing a backing too small for the device before attaching it",
		"resource", dr.GetName(), "volume", vol.GetVolumeNumber(),
		"backingKib", usableKib, "deviceSizeKib", vol.GetDeviceSizeKib(), "targetKib", target)

	return growStep{backingKib: target, beforeAttach: true}
}

// luksMapperTooSmallForDevice reports whether the open LUKS mapper of
// vol, of mapperKib, cannot hold the DRBD device the replicas report
// while the kernel holds the volume Diskless. Its backing grew before
// the attach (planAttachGrow) on this pass or an earlier one, and the
// mapper, which does not follow its backing on its own, has to grow too
// for the attach to succeed. As for the backing, the device keeps its
// size: the grow is not one `drbdadm resize` should follow, and that
// resize would fail while the replica syncs and later grow a volume
// nobody resized.
func (r *Reconciler) luksMapperTooSmallForDevice(
	ctx context.Context, dr *intent.DesiredResource, vol *intent.DesiredVolume, mapperKib int64, kernel *disklessProbe,
) bool {
	if r.cfg.Adm == nil || !needsDRBD(dr.GetLayerStack()) {
		return false
	}

	return backingTooSmallForDevice(vol, mapperKib, 0) && kernel.holds(ctx, vol.GetVolumeNumber())
}

// planGrow decides the grow step for vol, whose backing has usableKib.
//
// A grow is pending when the definition is larger than the backing, or
// than the size the replica last reported applied. The backing is then
// grown to what the layers above need (growBackingKib): growing it only
// to the definition size left a DRBD device short of the definition by
// its metadata, and an adopted LINSTOR volume, whose backing already
// carries the metadata, was not grown at all when the new size fitted
// in the old backing. When the backing is large enough, the layers are
// resized again (an earlier pass grew the backing but its resize
// failed), unless resizeRetryLimit passes already did so in vain.
//
// Only a change of the definition grows the device, as LINSTOR resizes
// a DRBD volume only while its definition is being resized. A device
// smaller than the definition with no such change is the volume's size:
// a volume created before backings were sized for metadata has backings
// of exactly the definition, so its device is short of the definition
// by the metadata. A replica added to it later gets a backing sized for
// metadata, larger than the definition, and resizing the device from
// that replica would fail while it resyncs and, once enough replicas
// were replaced, grow the volume nobody resized.
//
// The change is read from the size the replica last applied, so a
// replica that has never reported one cannot see a grow that stays
// inside its backing: that is a volume adopted from LINSTOR, or a
// replica created before the size was reported, whose definition grows
// by less than the room its backing keeps for metadata before its
// satellite's first pass. Its sizes are those of a new replica of a
// volume created at the definition's size, which must not resize; only
// a resize signal from the controller, as LINSTOR's RESIZE flags are,
// would tell the two apart.
func (r *Reconciler) planGrow(dr *intent.DesiredResource, vol *intent.DesiredVolume, usableKib int64, withDRBD bool) growStep {
	applied := vol.GetAppliedSizeKib()
	lagging := applied > 0 && vol.GetSizeKib() > applied

	// A provider that cannot tell the backing size (usableKib 0) gives
	// no grow signal of its own; the replica's report still does.
	backingShort := usableKib > 0 && vol.GetSizeKib() > usableKib
	if !backingShort && !lagging {
		r.forgetGrow(dr.GetName(), vol.GetVolumeNumber())

		return growStep{}
	}

	step := growStep{pending: true}

	if usableKib <= 0 {
		// Nothing to grow the backing from; the device is measured
		// before the size is reported.
		return step
	}

	if target := growBackingKib(dr, vol, withDRBD); target > usableKib {
		step.backingKib = target

		return step
	}

	step.retryResize = lagging && r.resizeRetriesLeft(dr.GetName(), vol)

	return step
}

// resizeRetry counts the passes that re-ran the resize for one size and
// found the device still short of it.
type resizeRetry struct {
	sizeKib int64
	count   int
}

// resizeRetriesLeft reports whether a pass may re-run the resize of vol.
func (r *Reconciler) resizeRetriesLeft(rdName string, vol *intent.DesiredVolume) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	retry := r.resizeRetries[growKey(rdName, vol.GetVolumeNumber())]

	return retry.sizeKib != vol.GetSizeKib() || retry.count < resizeRetryLimit
}

// noteRetryInVain counts a pass whose resize succeeded and left the
// device short of vol's size.
func (r *Reconciler) noteRetryInVain(ctx context.Context, rdName string, vol *intent.DesiredVolume) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := growKey(rdName, vol.GetVolumeNumber())

	retry := r.resizeRetries[key]
	if retry.sizeKib != vol.GetSizeKib() {
		retry = resizeRetry{sizeKib: vol.GetSizeKib()}
	}

	retry.count++
	r.resizeRetries[key] = retry

	if retry.count == resizeRetryLimit {
		log.FromContext(ctx).Info("the device did not grow after resizing it again; waiting for a peer's resize to grow it",
			"resource", rdName, "volume", vol.GetVolumeNumber(), "sizeKib", vol.GetSizeKib())
	}
}

// forgetGrow drops the retry count of a volume with no grow pending.
func (r *Reconciler) forgetGrow(rdName string, vn int32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.resizeRetries, growKey(rdName, vn))
}

// drbdDevicePath is the DRBD device of vol.
func drbdDevicePath(dr *intent.DesiredResource, vol *intent.DesiredVolume) string {
	minor, _ := strconv.Atoi(dr.GetDrbdOptions()["minor"])

	return fmt.Sprintf("/dev/drbd%d", volMinorOrBase(vol, minor))
}

// deviceSizeKib reads the size of a block device.
func (r *Reconciler) deviceSizeKib(ctx context.Context, device string) (int64, error) {
	if r.cfg.DeviceSizeKib != nil {
		return r.cfg.DeviceSizeKib(ctx, device)
	}

	return sysfsDeviceSizeKib(device)
}

// sysfsDeviceSizeKib reads a block device's size from sysfs. Opening the
// device is not an option for DRBD: a Secondary whose data is not
// UpToDate, as during the resync a grow starts, refuses the open.
func sysfsDeviceSizeKib(device string) (int64, error) {
	resolved, err := filepath.EvalSymlinks(device)
	if err != nil {
		return 0, errors.Wrapf(err, "resolve %s", device)
	}

	raw, err := os.ReadFile(sysClassBlock + filepath.Base(resolved) + "/size")
	if err != nil {
		return 0, errors.Wrapf(err, "read the size of %s", device)
	}

	sectors, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parse the size of %s", device)
	}

	const sectorsPerKib = 2

	return sectors / sectorsPerKib, nil
}

// appliedSizeKib is the size a pass reports applied for vol, whose
// consumer device is at device. Without a grow pending it is the
// definition size. With one, it is the definition size only once the
// device has grown to it; until then the last reported size stays, so
// the grow wait and this replica's retry both carry on.
func (r *Reconciler) appliedSizeKib(
	ctx context.Context, dr *intent.DesiredResource, vol *intent.DesiredVolume, device string, step growStep,
) (int64, bool) {
	if !step.pending {
		return vol.GetSizeKib(), false
	}

	deviceKib, err := r.deviceSizeKib(ctx, device)
	if err == nil && deviceKib >= vol.GetSizeKib() {
		r.forgetGrow(dr.GetName(), vol.GetVolumeNumber())

		return vol.GetSizeKib(), false
	}

	if step.retryResize {
		r.noteRetryInVain(ctx, dr.GetName(), vol)
	}

	return vol.GetAppliedSizeKib(), true
}
