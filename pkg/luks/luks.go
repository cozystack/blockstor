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

// Package luks wraps the `cryptsetup` CLI so the satellite can layer
// LUKS encryption between a storage provider's raw block device and
// DRBD's lower disk. Tests substitute storage.FakeExec to assert
// command lines without root or a real cryptsetup install.
package luks

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Cryptsetup is a thin wrapper around the `cryptsetup` binary.
type Cryptsetup struct {
	exec storage.Exec
}

// NewCryptsetup constructs a wrapper with the given Exec.
func NewCryptsetup(ex storage.Exec) *Cryptsetup {
	return &Cryptsetup{exec: ex}
}

// Format runs `cryptsetup luksFormat` against device with the given
// keyfile contents. Idempotent: if the device already carries a LUKS
// header, we no-op.
func (c *Cryptsetup) Format(ctx context.Context, device string, key []byte) error {
	err := c.runProbe(ctx, "isLuks", device)
	if err == nil {
		return nil
	}

	err = c.runWithKey(ctx, key, "luksFormat", "--batch-mode", device, "--key-file", "-")
	if err != nil {
		return errors.Wrapf(err, "luksFormat %s", device)
	}

	return nil
}

// Open unlocks device under the dm-crypt name `dmName`. The opened
// device shows up at /dev/mapper/<dmName>.
func (c *Cryptsetup) Open(ctx context.Context, device, dmName string, key []byte) error {
	err := c.runWithKey(ctx, key, "luksOpen", device, dmName, "--key-file", "-")
	if err != nil {
		return errors.Wrapf(err, "luksOpen %s -> %s", device, dmName)
	}

	return nil
}

// Resize tells cryptsetup the underlying device has grown — without
// it the dm-crypt target keeps the original size and `drbdadm resize`
// only sees the LUKS-mapped portion. Idempotent: a no-op when the
// device size already matches the dm target.
func (c *Cryptsetup) Resize(ctx context.Context, dmName string, key []byte) error {
	err := c.runWithKey(ctx, key, "resize", dmName, "--key-file", "-")
	if err != nil {
		return errors.Wrapf(err, "luksResize %s", dmName)
	}

	return nil
}

// Close removes the dm-crypt mapping. Counterpart to Open.
func (c *Cryptsetup) Close(ctx context.Context, dmName string) error {
	_, err := c.exec.Run(ctx, "cryptsetup", "luksClose", dmName)
	if err != nil {
		return errors.Wrapf(err, "luksClose %s", dmName)
	}

	return nil
}

// runProbe runs a status-check subcommand whose exit code is what
// matters (cryptsetup isLuks: 0 yes, !=0 no).
func (c *Cryptsetup) runProbe(ctx context.Context, args ...string) error {
	_, err := c.exec.Run(ctx, "cryptsetup", args...)
	if err != nil {
		return errors.Wrap(err, "cryptsetup probe")
	}

	return nil
}

// runWithKey executes cryptsetup with the passphrase delivered on
// stdin via `--key-file -`. We invoke the binary directly through
// storage.Exec.RunWithStdin — no `sh -c`, no `printf`, no shell
// metacharacter parsing — because the previous shell-pipeline form
// (Bug 175, P1 SECURITY) used fmt.Sprintf("%q", key) which does NOT
// escape shell command substitution (`$(...)`, backticks) or
// statement separators. A passphrase set through unauthenticated
// REST PATCH on a resource-definition therefore got evaluated as
// root on every satellite node.
//
// Mirrors the stdin pattern already in pkg/storage/zfs/zfs.go and
// pkg/storage/lvm/lvm_thin.go: pass an io.Reader, never a shell
// string. Argv carries only fixed cryptsetup flags and operator-
// owned device/dm names.
func (c *Cryptsetup) runWithKey(ctx context.Context, key []byte, args ...string) error {
	_, err := c.exec.RunWithStdin(ctx,
		strings.NewReader(string(key)),
		"cryptsetup", args...)
	if err != nil {
		return errors.Wrap(err, "cryptsetup")
	}

	return nil
}

// DevicePath returns /dev/mapper/<name> — the post-Open device path
// callers should hand to DRBD as the lower disk.
func DevicePath(dmName string) string {
	return "/dev/mapper/" + dmName
}
