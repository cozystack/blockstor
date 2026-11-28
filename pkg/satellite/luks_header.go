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

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/luks"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// luksMapperMarginBytes is how far past its header a mapper may lag the
// device under it before it counts as behind (luksMapperBehind).
const luksMapperMarginBytes int64 = 16 << 20

// luksHeader is what the LUKS header on a backing device takes from its
// front, as read off that device.
type luksHeader struct {
	device string
	kib    int64
}

// luksHeaderKib is what the LUKS header on device, the backing of volume
// vn of dr, takes from its front: where its data starts. It is 0 for a
// resource without a LUKS layer.
//
// The size is read off the header, never assumed. cryptsetup luksFormat,
// called as LINSTOR calls it, without --offset or a keyslots size, puts
// the data of a LUKS2 header at 16 MiB only when the device reports no
// I/O size it has to be aligned to; it rounds the header up to that size
// otherwise, which gave 32 MiB on zvols. LUKS1 headers take 2 MiB and
// some LUKS2 builds wrote 4 MiB, and a volume adopted from LINSTOR or
// restored from a snapshot carries whichever header it was formatted
// with.
//
// The header is read once per process and device, since it moves only
// when the device is formatted again (noteLUKSFormatted). A device whose
// header cannot be read, as one not formatted yet, counts as holding the
// 16 MiB a new header takes by default, and is read again next time.
func (r *Reconciler) luksHeaderKib(ctx context.Context, dr *intent.DesiredResource, vn int32, device string) int64 {
	if !needsLUKS(dr.GetLayerStack()) {
		return 0
	}

	key := growKey(dr.GetName(), vn)

	r.mu.Lock()
	known, ok := r.luksHeaders[key]
	r.mu.Unlock()

	if ok && known.device == device {
		return known.kib
	}

	if r.cfg.Cryptsetup == nil || device == "" {
		return luks.HeaderKiB
	}

	kib, ok := r.cfg.Cryptsetup.DataOffsetKiB(ctx, device)
	if !ok {
		return luks.HeaderKiB
	}

	r.mu.Lock()
	r.luksHeaders[key] = luksHeader{device: device, kib: kib}
	r.mu.Unlock()

	return kib
}

// noteLUKSFormatted records that this process just wrote a LUKS header on
// the backing of volume vn of rdName: the header read before, if any, is
// stale, and the backing may have to grow to hold the new one before it
// is opened (fitBackingToLUKSHeader).
func (r *Reconciler) noteLUKSFormatted(rdName string, vn int32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := growKey(rdName, vn)
	delete(r.luksHeaders, key)
	r.luksUnfitted[key] = struct{}{}
}

// fitBackingToLUKSHeader grows the backing of vol, which this process
// formatted and has not opened yet, by what its LUKS header takes beyond
// the 16 MiB the backing was created for (lowerDiskKib). Without that, a
// header cryptsetup aligned to 32 MiB left every new LUKS volume 16 MiB
// short of its definition, and DRBD created its metadata on the short
// mapper.
//
// The backing holds nothing but the header yet: Format writes one only
// on a device that carries no signature, and the mapper, which holds
// DRBD's metadata and the data, opens only after this. So growing it
// changes no data, and it only ever grows. A backing whose grow fails
// stays listed and is retried before the next open; one this process did
// not format is left alone.
func (r *Reconciler) fitBackingToLUKSHeader(
	ctx context.Context, dr *intent.DesiredResource, vol *intent.DesiredVolume, dev string,
) error {
	key := growKey(dr.GetName(), vol.GetVolumeNumber())

	r.mu.Lock()
	_, unfitted := r.luksUnfitted[key]
	r.mu.Unlock()

	if !unfitted {
		return nil
	}

	header := r.luksHeaderKib(ctx, dr, vol.GetVolumeNumber(), dev)
	if header > luks.HeaderKiB {
		err := r.growBackingForHeader(ctx, dr, vol, header)
		if err != nil {
			return err
		}
	}

	r.mu.Lock()
	delete(r.luksUnfitted, key)
	r.mu.Unlock()

	return nil
}

// growBackingForHeader grows the backing of vol to the lower disk a LUKS
// header of headerKib needs, when it is smaller.
func (r *Reconciler) growBackingForHeader(
	ctx context.Context, dr *intent.DesiredResource, vol *intent.DesiredVolume, headerKib int64,
) error {
	provider, ok := r.provider(vol.GetStoragePool())
	if !ok {
		return errors.Errorf("unknown storage pool %q", vol.GetStoragePool())
	}

	target := storage.Volume{ResourceName: dr.GetName(), VolumeNumber: vol.GetVolumeNumber()}

	status, err := provider.VolumeStatus(ctx, target)
	if err != nil {
		return errors.Wrapf(err, "volume status %s/%d", dr.GetName(), vol.GetVolumeNumber())
	}

	need := lowerDiskKibUnder(dr, vol, headerKib)
	if status.UsableKib <= 0 || need <= status.UsableKib {
		return nil
	}

	log.FromContext(ctx).Info("growing a new LUKS backing to hold the header cryptsetup wrote",
		"resource", dr.GetName(), "volume", vol.GetVolumeNumber(),
		"headerKib", headerKib, "backingKib", status.UsableKib, "targetKib", need)

	target.SizeKib = need

	err = provider.ResizeVolume(ctx, target)
	if err != nil {
		return errors.Wrapf(err, "resize volume %s/%d to %d KiB for its LUKS header",
			dr.GetName(), vol.GetVolumeNumber(), need)
	}

	return nil
}

// luksMapperLagsHeader reports whether a mapper of mapperSize bytes lags
// the devSize bytes under it by more than a LUKS header of headerKib and
// the margin luksMapperBehind keeps. For a header up to the 16 MiB one
// that is luksMapperBehind itself; past it, a mapper opened over a 32 MiB
// header is not taken for one its device outgrew, which would resize it
// and DRBD over it on every pass.
func luksMapperLagsHeader(devSize, mapperSize, headerKib int64) bool {
	header := max(headerKib, luks.HeaderKiB) * bytesPerKib

	return devSize > mapperSize+header+luksMapperMarginBytes
}
