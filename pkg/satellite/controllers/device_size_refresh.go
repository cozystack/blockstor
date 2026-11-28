// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// sysBlockDir is where the kernel lists block devices with their size.
const sysBlockDir = "/sys/class/block/"

// refreshDeviceSizes reads the size of every DRBD device the cache
// knows the minor of and keeps it as the volume's device size.
//
// The size frames carry is not enough. DRBD prints `size:` with the
// statistics of a device frame, and an online resize does not reliably
// bring one: after a grow, every replica of a plain volume kept the
// size from before it for minutes, as long as nothing else happened to
// the volume. LINSTOR reports the size it computes for the volume from
// the definition and the backing disk, which a resize moves at once.
//
// A device sysfs does not list, or lists at zero (neither attached nor
// connected), keeps what the frames said.
func (o *ObserverRunnable) refreshDeviceSizes() {
	type sample struct {
		name   string
		volume int32
		minor  string
		cached int64
	}

	var samples []sample

	o.volMu.Lock()
	for name, cache := range o.volCache {
		for volNum, vol := range cache {
			if vol.Minor != "" && !vol.Removed {
				samples = append(samples, sample{name: name, volume: volNum, minor: vol.Minor, cached: vol.DeviceSizeKib})
			}
		}
	}
	o.volMu.Unlock()

	read := o.readDeviceSize
	if read == nil {
		read = sysfsDrbdSizeKib
	}

	for _, s := range samples {
		size, err := read(s.minor)
		if err != nil || size <= 0 || size == s.cached {
			continue
		}

		o.volMu.Lock()
		// A frame that landed while sysfs was read is newer than the
		// read: leave its size.
		if vol, ok := o.volCache[s.name][s.volume]; ok && vol.DeviceSizeKib == s.cached && vol.Minor == s.minor {
			vol.DeviceSizeKib = size
			o.volCache[s.name][s.volume] = vol
		}
		o.volMu.Unlock()
	}
}

// sysfsDrbdSizeKib reads the size of DRBD minor from sysfs, in KiB.
func sysfsDrbdSizeKib(minor string) (int64, error) {
	if _, err := strconv.ParseUint(minor, 10, 32); err != nil {
		return 0, errors.Wrapf(err, "parse DRBD minor %q", minor)
	}

	raw, err := os.ReadFile(sysBlockDir + "drbd" + minor + "/size")
	if err != nil {
		return 0, errors.Wrapf(err, "read the size of drbd%s", minor)
	}

	sectors, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parse the size of drbd%s", minor)
	}

	const sectorsPerKib = 2

	return sectors / sectorsPerKib, nil
}
