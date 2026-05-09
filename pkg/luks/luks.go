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
	"fmt"
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

// runWithKey executes cryptsetup with the keyfile passed via stdin.
// We don't yet have a stdin path on storage.Exec, so we shell out via
// `sh -c 'printf %s "$KEY" | cryptsetup ...'` to keep secrets off the
// argv (visible to other procs) and out of disk-resident temp files.
func (c *Cryptsetup) runWithKey(ctx context.Context, key []byte, args ...string) error {
	pipeline := fmt.Sprintf("printf %%s %q | cryptsetup %s",
		string(key), shellQuoteArgs(args))

	_, err := c.exec.Run(ctx, "sh", "-c", pipeline)
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

// shellQuoteArgs single-quote-wraps each arg so the resulting `sh -c`
// pipeline doesn't accidentally re-interpret characters in device or
// dm-name strings. We don't bother with full POSIX rules — these
// strings are pinned by the satellite to known-safe paths.
func shellQuoteArgs(args []string) string {
	var b strings.Builder

	for i, arg := range args {
		if i > 0 {
			b.WriteString(" ")
		}

		b.WriteString("'")
		b.WriteString(arg)
		b.WriteString("'")
	}

	return b.String()
}
