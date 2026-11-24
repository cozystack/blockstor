//go:build !windows

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
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

const sIFBLK = syscall.S_IFBLK

func mknod(path string, mode uint32, dev uint64) error {
	return errors.WithStack(syscall.Mknod(path, mode, int(dev))) //nolint:gosec // dev encodes major=147 + minor<=65535; int conversion never truncates
}

func statRdev(info os.FileInfo) (uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}

	return uint64(stat.Rdev), true //nolint:unconvert // Rdev type varies by OS and arch
}
