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
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// LsblkTypeDisk is the lsblk `TYPE` value for a real block device
// (as opposed to "part", "lvm", "loop", etc). Compared against on
// the discovery filter path. Phase 10.7.
const LsblkTypeDisk = "disk"

// LsblkDevice is one row of `lsblk -P` output, parsed and typed.
// Mirrors the subset of fields the PhysicalDevice discovery loop
// consumes — name, kernel name, size in bytes, filesystem type,
// device type, mountpoint, WWN, model, rotational, transport.
//
// Phase 10.7 step (lsblk filter parity with upstream LINSTOR's
// LsBlkUtils.java).
type LsblkDevice struct {
	Name       string
	KName      string
	SizeBytes  int64
	FSType     string
	Type       string
	Mountpoint string
	WWN        string
	Model      string
	Serial     string
	Rotational bool
	Transport  string
}

// IsFreeBlockDevice reports whether this device is a candidate for
// PhysicalDevice publication: real disk (`TYPE=disk`), no
// filesystem signature, no current mountpoint. Partitions and
// loopback devices are filtered out by `Type != "disk"`. A
// non-empty FSType means another tenant (LVM PV, ZFS, ext4, …)
// already claimed the device — don't surface it as free.
//
// Note: this is the lsblk-only first pass. The discovery loop
// then runs `pvs --noheadings`, `zpool list -PHv`, and
// `drbdmeta show-md` cross-checks before publishing — lsblk on
// its own can miss already-attached LVM PVs that haven't been
// fully formatted with a kernel-recognised signature.
func (d *LsblkDevice) IsFreeBlockDevice() bool {
	if d.Type != LsblkTypeDisk {
		return false
	}

	if d.FSType != "" {
		return false
	}

	if d.Mountpoint != "" {
		return false
	}

	return true
}

// Lsblk runs `lsblk -Pb -o NAME,KNAME,SIZE,FSTYPE,TYPE,MOUNTPOINT,
// WWN,MODEL,SERIAL,ROTA,TRAN` and parses the output into a slice
// of `LsblkDevice`. The `-P` flag emits key=value pairs; `-b`
// switches SIZE to raw bytes so callers don't have to parse
// human-readable suffixes. Phase 10.7.
//
// Returns the unfiltered list — callers run `IsFreeBlockDevice` on
// each row + the cross-checks (pvs, zpool, drbdmeta) before
// publishing.
func Lsblk(ctx context.Context, exec storage.Exec) ([]LsblkDevice, error) {
	out, err := exec.Run(ctx, "lsblk",
		"-Pb",
		"-o", "NAME,KNAME,SIZE,FSTYPE,TYPE,MOUNTPOINT,WWN,MODEL,SERIAL,ROTA,TRAN")
	if err != nil {
		return nil, errors.Wrap(err, "lsblk")
	}

	return parseLsblkPairs(string(out)), nil
}

// parseLsblkPairs turns lsblk's `KEY="value" KEY="value"...`
// per-line shape into typed structs. Exposed for unit tests so
// they don't need a real lsblk binary.
func parseLsblkPairs(raw string) []LsblkDevice {
	var devices []LsblkDevice

	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := parseLsblkLine(line)

		dev := LsblkDevice{
			Name:       fields["NAME"],
			KName:      fields["KNAME"],
			FSType:     fields["FSTYPE"],
			Type:       fields["TYPE"],
			Mountpoint: fields["MOUNTPOINT"],
			WWN:        fields["WWN"],
			Model:      strings.TrimSpace(fields["MODEL"]),
			Serial:     strings.TrimSpace(fields["SERIAL"]),
			Transport:  fields["TRAN"],
		}

		size, err := strconv.ParseInt(fields["SIZE"], 10, 64)
		if err == nil {
			dev.SizeBytes = size
		}

		if rota := fields["ROTA"]; rota == "1" {
			dev.Rotational = true
		}

		devices = append(devices, dev)
	}

	return devices
}

// parseLsblkLine splits a single lsblk -P row into its key/value
// pairs. The output format is `KEY="value" KEY="value" ...` —
// quotes are mandatory, values themselves may contain spaces
// (model strings: `MODEL="Samsung SSD 980 PRO"`). We hand-roll the
// parser rather than using a library because the format is
// trivially regular and the dependency surface stays small.
func parseLsblkLine(line string) map[string]string {
	out := map[string]string{}

	i := 0

	for i < len(line) {
		// Skip leading whitespace.
		for i < len(line) && line[i] == ' ' {
			i++
		}

		if i >= len(line) {
			break
		}

		// Read key up to '='.
		keyStart := i
		for i < len(line) && line[i] != '=' {
			i++
		}

		if i >= len(line) {
			break
		}

		key := line[keyStart:i]
		i++ // skip '='

		if i >= len(line) || line[i] != '"' {
			break
		}

		i++ // skip opening '"'

		// Read value up to closing '"'. Backslash-escapes are
		// not used in lsblk output — values stop at the next ".
		valStart := i
		for i < len(line) && line[i] != '"' {
			i++
		}

		out[key] = line[valStart:i]

		if i < len(line) {
			i++ // skip closing '"'
		}
	}

	return out
}
