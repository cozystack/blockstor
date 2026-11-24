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

// Package luks wraps the `cryptsetup` CLI so the satellite can layer
// LUKS encryption between a storage provider's raw block device and
// DRBD's lower disk. Tests substitute storage.FakeExec to assert
// command lines without root or a real cryptsetup install.
package luks

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Cryptsetup is a thin wrapper around the `cryptsetup` binary.
type Cryptsetup struct {
	exec          storage.Exec
	isBlockDevice func(path string) (bool, error)
}

// Option configures a Cryptsetup.
type Option func(*Cryptsetup)

// WithBlockDeviceCheck replaces the check Format makes that a device
// blkid found empty is a block device. Tests whose devices exist only
// in a fake Exec use it.
func WithBlockDeviceCheck(check func(path string) (bool, error)) Option {
	return func(c *Cryptsetup) { c.isBlockDevice = check }
}

// NewCryptsetup constructs a wrapper with the given Exec.
func NewCryptsetup(ex storage.Exec, opts ...Option) *Cryptsetup {
	c := &Cryptsetup{exec: ex, isBlockDevice: statBlockDevice}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

// statBlockDevice reports whether path names a block device.
func statBlockDevice(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, errors.Wrapf(err, "stat %s", path)
	}

	mode := info.Mode()

	return mode&os.ModeDevice != 0 && mode&os.ModeCharDevice == 0, nil
}

// HeaderKiB is what a LUKS2 header Format writes takes from the front of
// the device: cryptsetup's default LUKS2 data offset, 16 MiB. The opened
// device is the lower one less this. LINSTOR assumes the same size for
// LUKS2 when it sizes the volume under a LUKS layer.
const HeaderKiB = 16 * 1024

// Format runs `cryptsetup luksFormat` against device with the given
// keyfile contents. Idempotent: if the device already carries a LUKS
// header, we no-op.
//
// A device that is not LUKS but carries any other signature is refused
// with ErrDeviceHoldsData rather than formatted: luksFormat over it
// destroys what is there. See probeSignatures.
func (c *Cryptsetup) Format(ctx context.Context, device string, key []byte) error {
	err := c.runProbe(ctx, "isLuks", device)
	if err == nil {
		return nil
	}

	signatures, err := c.probeSignatures(ctx, device)
	if err != nil {
		return err
	}

	if len(signatures) > 0 {
		return errors.Wrapf(ErrDeviceHoldsData,
			"%s carries %s; refusing luksFormat. Only if nothing was ever written to this volume and the signature is left over from an earlier one on the same extents, clear it with wipefs; DRBD metadata on a volume already in use is its own",
			device, strings.Join(signatures, ", "))
	}

	// LUKS2 is named rather than left to the host's default, which is
	// LUKS1 on older distributions. The key is a 256-bit random password, so the header's own KDF
	// protects nothing a guess could reach; cryptsetup's default Argon2id
	// would cost seconds and up to a GiB per open, inside the satellite's
	// cgroup, for every volume after a node reboot. PBKDF2 at its minimum
	// keeps opening cheap.
	err = c.runWithKey(ctx, key, "luksFormat", "--batch-mode", "--type", "luks2",
		"--pbkdf", "pbkdf2", "--pbkdf-force-iterations", "1000", device, "--key-file", "-")
	if err != nil {
		return errors.Wrapf(err, "luksFormat %s", device)
	}

	return nil
}

// IsLUKS reports whether device carries a LUKS header. A probe that fails
// for any reason counts as no header; a caller about to format still runs
// EnsureEmpty, which fails closed.
func (c *Cryptsetup) IsLUKS(ctx context.Context, device string) bool {
	return c.runProbe(ctx, "isLuks", device) == nil
}

// DataOffsetKiB reads from device's LUKS header where the encrypted data
// starts, which is what the header takes from the front of the device:
// the data segment's offset for LUKS2, the payload offset for LUKS1.
// That is HeaderKiB for a LUKS2 header Format writes, 2 MiB for a LUKS1
// one and 4 MiB for one some LUKS2 builds wrote. It reports false when
// the header cannot be read.
func (c *Cryptsetup) DataOffsetKiB(ctx context.Context, device string) (int64, bool) {
	out, err := c.exec.Run(ctx, "cryptsetup", "luksDump", device)
	if err != nil {
		return 0, false
	}

	return parseDataOffsetKiB(string(out))
}

// parseDataOffsetKiB reads the data offset out of `cryptsetup luksDump`:
// LUKS1 prints "Payload offset: <sectors>", LUKS2 an "offset: <n>
// [bytes]" line under its first data segment. The keyslot areas carry
// offsets of their own, so only the "Data segments" section is read.
func parseDataOffsetKiB(dump string) (int64, bool) {
	const (
		bytesPerKiB   = 1024
		sectorsPerKiB = 2
	)

	inSegments := false

	for line := range strings.SplitSeq(dump, "\n") {
		trimmed := strings.TrimSpace(line)

		if sectors, ok := strings.CutPrefix(trimmed, "Payload offset:"); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(sectors), 10, 64)

			return n / sectorsPerKiB, err == nil && n > 0
		}

		if line != "" && line[0] != ' ' && line[0] != '\t' {
			inSegments = strings.HasPrefix(trimmed, "Data segments:")

			continue
		}

		if !inSegments {
			continue
		}

		if offset, ok := strings.CutPrefix(trimmed, "offset:"); ok {
			n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(offset), " [bytes]"), 10, 64)

			return n / bytesPerKiB, err == nil && n > 0
		}
	}

	return 0, false
}

// IsActive reports whether the dm-crypt mapping dmName is active.
// `cryptsetup status` exits 0 and describes the mapping when it is, and
// exits non-zero when it is not. A probe that fails or prints nothing
// counts as not active: the caller then takes the path that opens the
// device, which answers ErrAlreadyOpen for a mapping that is active after
// all, so a wrong "no" costs a password read and never a format.
func (c *Cryptsetup) IsActive(ctx context.Context, dmName string) bool {
	out, err := c.exec.Run(ctx, "cryptsetup", "status", dmName)

	return err == nil && strings.TrimSpace(string(out)) != ""
}

// MapsDevice reports whether dmName is an active dm-crypt mapping whose
// one target encrypts device itself. It is how the satellite recognises
// a mapping another stack opened over one of its volumes, so it answers
// yes only on evidence: the live table must hold exactly one crypt
// target, and that target's device number must be device's. Anything
// else, a probe that fails included, is no.
//
// The table is read without --showkeys, so the volume key never leaves
// the kernel; the key field is not looked at.
func (c *Cryptsetup) MapsDevice(ctx context.Context, dmName, device string) bool {
	out, err := c.exec.Run(ctx, "dmsetup", "table", dmName)
	if err != nil {
		return false
	}

	// <start> <length> crypt <cipher> <key> <iv_offset> <major:minor> <offset> [<opts>]
	const (
		targetField = 2
		deviceField = 6
	)

	table := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(table) != 1 {
		return false
	}

	fields := strings.Fields(table[0])
	if len(fields) <= deviceField || fields[targetField] != "crypt" {
		return false
	}

	number, err := c.exec.Run(ctx, "lsblk", "--nodeps", "--noheadings", "--output", "MAJ:MIN", device)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(number)) == fields[deviceField]
}

// EnsureEmpty returns nil only for a block device blkid finds no
// signature on, and ErrDeviceHoldsData for one that carries any. It is
// the probe Format runs, for a caller that must commit to formatting,
// by storing the device's new password, before it formats.
func (c *Cryptsetup) EnsureEmpty(ctx context.Context, device string) error {
	signatures, err := c.probeSignatures(ctx, device)
	if err != nil {
		return err
	}

	if len(signatures) > 0 {
		return errors.Wrapf(ErrDeviceHoldsData, "%s carries %s", device, strings.Join(signatures, ", "))
	}

	return nil
}

// TestKey reports whether key opens one of device's key slots, without
// activating anything. A wrong key is (false, nil).
func (c *Cryptsetup) TestKey(ctx context.Context, device string, key []byte) (bool, error) {
	err := c.runWithKey(ctx, key, "luksOpen", "--test-passphrase", device, "--key-file", "-")
	if err == nil {
		return true, nil
	}

	if wrongKey(err) {
		return false, nil
	}

	return false, errors.Wrapf(err, "test a key on %s", device)
}

// cryptsetupWrongKey is the exit status cryptsetup gives when no key slot
// opens with the key it was handed ("No key available with this
// passphrase").
const cryptsetupWrongKey = 2

func wrongKey(err error) bool {
	var coded interface{ ExitCode() int }

	return errors.As(err, &coded) && coded.ExitCode() == cryptsetupWrongKey
}

// ErrDeviceHoldsData marks a device Format refused because it already
// carries a filesystem, DRBD metadata, a partition table or any other
// signature libblkid recognises.
var ErrDeviceHoldsData = errors.New("luks: device already holds data")

// ErrNoLUKSKey is a replica that carries no wrapped password for a
// volume.
var ErrNoLUKSKey = errors.New("luks: the replica carries no password for this volume")

// ErrLUKSKeyMissing is a device with a LUKS header and no password on its
// replica. It is neither opened nor formatted: formatting would destroy
// the data the header guards.
var ErrLUKSKeyMissing = errors.New("luks: the device has a LUKS header but its replica carries no password for it")

// ErrLUKSKeyMismatch is a device whose header the replica's password does
// not open. It is never re-formatted.
var ErrLUKSKeyMismatch = errors.New("luks: the replica's password does not open the device's LUKS header")

// ErrMasterKeyMismatch is a replica password wrapped under a master key
// other than the one the cluster unlocked.
var ErrMasterKeyMismatch = errors.New("luks: the replica's password is wrapped under another master key")

// ErrMasterKeyLocked is a cluster whose master key the passphrase Secret
// does not open, or that has none yet.
var ErrMasterKeyLocked = errors.New("luks: the master key is locked")

// ErrAlreadyOpen marks the cryptsetup EEXIST class — the dm-crypt
// mapper is already active for this device. Returned by Open wrapped
// via errors.Wrap so callers can both errors.Is-match the sentinel
// and surface the original cryptsetup message in logs.
//
// Bug 215: the reconciler used to detect this class via
// `strings.Contains(err.Error(), "already exists")` which silently
// missed translated cryptsetup output (e.g. de_DE "Gerät existiert
// bereits") and led to a luksFormat retry — a corruption-class
// outcome that wipes the key slots of an already-formatted device.
// Exposing the typed sentinel lets the reconciler classify the
// error by identity instead of by English substring, immune to
// locale drift even if storage.RealExec's LC_ALL=C guard is bypassed.
var ErrAlreadyOpen = errors.New("luks: device already open")

// Open unlocks device under the dm-crypt name `dmName`. The opened
// device shows up at /dev/mapper/<dmName>. When the mapper is
// already open (the everyday idempotent-reconcile path), the
// returned error wraps ErrAlreadyOpen so callers can errors.Is-match
// instead of substring-matching cryptsetup output.
func (c *Cryptsetup) Open(ctx context.Context, device, dmName string, key []byte) error {
	err := c.runWithKey(ctx, key, "luksOpen", device, dmName, "--key-file", "-")
	if err == nil {
		return nil
	}

	if wrongKey(err) {
		return errors.Wrapf(errors.WithSecondaryError(ErrLUKSKeyMismatch, err), "luksOpen %s -> %s", device, dmName)
	}

	if classifyAlreadyOpen(err) {
		return errors.Wrapf(
			errors.Mark(err, ErrAlreadyOpen),
			"luksOpen %s -> %s", device, dmName)
	}

	return errors.Wrapf(err, "luksOpen %s -> %s", device, dmName)
}

// classifyAlreadyOpen returns true when err's message matches any
// known cryptsetup EEXIST translation. The match is locale-tolerant
// because cryptsetup doesn't expose a structured error and we can't
// rely on its exit code through the storage.Exec abstraction (it
// folds exec.ExitError into a generic wrapped error). The needle
// list enumerates the cryptsetup "already exists" message in the
// locales the satellite is most likely to encounter; the exec layer
// already pins LC_ALL=C, so the English needle covers production
// and the non-English needles defend against env rewrites
// downstream of RealExec.
func classifyAlreadyOpen(err error) bool {
	if err == nil {
		return false
	}

	needles := []string{
		"already exists",    // C / en_US — the post-LC_ALL=C path
		"existiert bereits", // de_DE
		"existe déjà",       // fr_FR
		"уже существует",    // ru_RU
		"ya existe",         // es_ES
	}

	msg := err.Error()
	for _, needle := range needles {
		if strings.Contains(msg, needle) {
			return true
		}
	}

	return false
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

// blkidNothingFound is the exit status `blkid -p` returns when the
// device carries no signature at all. Every other non-zero status,
// including 8 (several signatures, ambivalent), is either data or a
// probe that could not tell.
//
// blkid also exits 2 when it cannot open the device. It usually says so
// on stderr with blkidErrorMarker, but not for every path: for one that
// does not exist it can exit 2 and print nothing. So exit 2 counts as
// empty only without the marker and for a path that stats as a block
// device; anything else is a probe that could not tell.
const blkidNothingFound = 2

// blkidErrorMarker prefixes blkid's own error lines on stderr, which
// storage.Exec folds into the returned error.
const blkidErrorMarker = "blkid: error:"

// probeSignatures lists the signatures on device with a low-level
// `blkid -p` probe, which reads the device itself rather than the
// cache and sees DRBD internal metadata (TYPE=drbd) as well as
// filesystems. A volume that held data under a stack without LUKS
// shows one or the other.
//
// It fails closed: a probe that errors for any reason other than
// finding nothing returns that error, so Format formats only a device
// it has seen to be empty.
func (c *Cryptsetup) probeSignatures(ctx context.Context, device string) ([]string, error) {
	out, err := c.exec.Run(ctx, "blkid", "-p", "-o", "export", device)
	if err != nil {
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) && coded.ExitCode() == blkidNothingFound &&
			!strings.Contains(err.Error(), blkidErrorMarker) {
			return nil, c.requireBlockDevice(device)
		}

		return nil, errors.Wrapf(err, "probe %s for existing data before luksFormat", device)
	}

	var signatures []string

	for line := range strings.SplitSeq(string(out), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok && (key == "TYPE" || key == "PTTYPE") {
			signatures = append(signatures, key+"="+value)
		}
	}

	if len(signatures) == 0 {
		// blkid reported a find without naming it; still a find.
		signatures = append(signatures, "an unnamed signature")
	}

	return signatures, nil
}

// requireBlockDevice stands between blkid's "nothing found" and
// luksFormat: blkid gives the same answer for a path it could not open.
func (c *Cryptsetup) requireBlockDevice(device string) error {
	ok, err := c.isBlockDevice(device)
	if err != nil {
		return errors.Wrapf(err, "probe %s for existing data before luksFormat", device)
	}

	if !ok {
		return errors.Wrapf(ErrNotABlockDevice, "probe %s for existing data before luksFormat", device)
	}

	return nil
}

// ErrNotABlockDevice marks a device path blkid found empty that is not
// a block device, so its answer says nothing about the volume.
var ErrNotABlockDevice = errors.New("luks: not a block device")

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
