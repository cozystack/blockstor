//go:build windows

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

	"github.com/cockroachdb/errors"
)

// The blockstor CLI is built for Windows and links this package, but
// DRBD device nodes exist only on a Linux satellite.
const sIFBLK = 0

func mknod(string, uint32, uint64) error {
	return errors.New("drbd: device nodes are not supported on windows")
}

func statRdev(os.FileInfo) (uint64, bool) {
	return 0, false
}
