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

package lvm

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// State strings shared between Thin / Thick. Match the upstream LINSTOR
// values that REST clients display via `linstor v list`.
const (
	stateProvisioned    = "PROVISIONED"
	stateNotProvisioned = "NOT_PROVISIONED"
)

// volumeStatusViaLVS runs `lvs <vg>/<lv>` and parses the resulting
// (path, size) pair into a storage.VolumeStatus. Empty output → the
// LV doesn't exist; surface NOT_PROVISIONED so the satellite reconciler's
// next pass triggers a CreateVolume.
func volumeStatusViaLVS(ctx context.Context, exec storage.Exec, vgLV string) (storage.VolumeStatus, error) {
	out, err := exec.Run(ctx, "lvs",
		"--noheadings",
		"--separator", "|",
		"-o", "lv_path,lv_size",
		"--units", "k",
		"--nosuffix",
		vgLV)
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrap(err, "lvs")
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return storage.VolumeStatus{State: stateNotProvisioned}, nil
	}

	parts := strings.SplitN(line, "|", lvsCols)
	if len(parts) != lvsCols {
		return storage.VolumeStatus{}, errors.Errorf("lvs: unexpected line %q", line)
	}

	sizeKib, err := parseFloatToInt64(parts[1])
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrap(err, "parse lv_size")
	}

	return storage.VolumeStatus{
		DevicePath:   strings.TrimSpace(parts[0]),
		AllocatedKib: sizeKib,
		UsableKib:    sizeKib,
		State:        stateProvisioned,
	}, nil
}
