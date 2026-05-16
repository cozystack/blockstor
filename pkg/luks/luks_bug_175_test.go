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

// Issue 175 (P1 SECURITY) regression tests: the previous
// implementation of runWithKey built a shell pipeline `printf %s %q
// | cryptsetup ...` and fed it to `sh -c`. fmt.Sprintf("%q", key)
// produces a Go-escaped double-quoted string — it does NOT escape
// shell command substitution (`$(...)`, backticks) or statement
// separators (`;`). A passphrase set via REST PATCH on a
// resource-definition therefore flows into `sh -c` and executes
// arbitrary commands as root on every satellite. These tests pin
// the fix: passphrase MUST arrive via cmd.Stdin and the cryptsetup
// invocation MUST NOT go through `sh -c`.
//
// The passphrase value used in these tests is a fixed sentinel
// `"<P1>"`; the real shell-metacharacter payloads are local
// constants below. We DO NOT log the payload on failure — only the
// recorded argv — to keep secret-shaped strings out of test output.
package luks_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/storage"
)

// payloadFormat is a shell-injection payload exercised against
// Format. If the cryptsetup wrapper still goes through `sh -c`, this
// would close the `printf %q` double-quoted string and run `rm -rf
// /`. We never compare against the literal in failure output — we
// just check argv shape — to keep secret-shaped material out of
// logs.
const payloadFormat = `"; rm -rf / #`

// payloadOpen exercises command-substitution: `$(...)` survives
// Go's %q quoting and is evaluated by `sh -c`.
const payloadOpen = `$(touch /tmp/pwned)`

// payloadResize covers the backtick alternative to $().
const payloadResize = "`touch /tmp/pwned`"

// secretSentinel is used in assertion messages so the actual payload
// never lands in test output.
const secretSentinel = "<P1>"

var errBug175NotLuks = errors.New("cryptsetup: not a LUKS device")

// assertNoShellPipeline fails if any recorded call went through
// `sh -c <pipeline>`. The pre-fix runWithKey called
//
//	fx.Run(ctx, "sh", "-c", pipeline)
//
// so this assertion catches the regression directly.
func assertNoShellPipeline(t *testing.T, fx *storage.FakeExec) {
	t.Helper()

	for i, call := range fx.Calls {
		if call.Name == "sh" {
			// Don't echo args — they contain the passphrase.
			t.Errorf("call[%d]: passphrase routed through `sh -c` (%s); "+
				"Bug 175 requires direct exec with stdin",
				i, secretSentinel)
		}
	}
}

// assertCryptsetupInvoked verifies cryptsetup ran directly with the
// expected subcommand AND that --key-file - is present (the stdin
// pathway). It also verifies the passphrase is NOT in argv.
func assertCryptsetupInvoked(
	t *testing.T,
	fx *storage.FakeExec,
	payload, subcommand string,
) {
	t.Helper()

	found := false
	for i, call := range fx.Calls {
		if call.Name != "cryptsetup" {
			continue
		}

		if len(call.Args) == 0 || call.Args[0] != subcommand {
			continue
		}

		found = true

		// Passphrase must NEVER show up in argv.
		for j, a := range call.Args {
			if strings.Contains(a, payload) {
				t.Errorf("call[%d].Args[%d]: passphrase (%s) "+
					"present in cryptsetup argv; must be on stdin",
					i, j, secretSentinel)
			}
		}

		// --key-file - tells cryptsetup to read the keyfile from
		// stdin. The pre-fix code did not pass this; it piped via
		// printf instead. The fix MUST include it.
		sawKeyFileStdin := false
		for j := 0; j+1 < len(call.Args); j++ {
			if call.Args[j] == "--key-file" && call.Args[j+1] == "-" {
				sawKeyFileStdin = true
				break
			}
		}

		if !sawKeyFileStdin {
			t.Errorf("call[%d]: cryptsetup %s argv missing `--key-file -`; "+
				"argv=%v", i, subcommand, call.Args)
		}
	}

	if !found {
		t.Errorf("no direct `cryptsetup %s` call recorded; calls=%v",
			subcommand, fx.CommandLines())
	}
}

// assertStdinDeliveredOnce verifies exactly one direct cryptsetup
// call received the passphrase via stdin. FakeExec records stdin per
// call via Stdins (mirrors Calls indexing). Asserts presence and
// content match without logging the actual payload.
func assertStdinDeliveredOnce(
	t *testing.T,
	fx *storage.FakeExec,
	payload string,
) {
	t.Helper()

	hits := 0

	for i, call := range fx.Calls {
		if call.Name != "cryptsetup" {
			continue
		}

		got := fx.StdinFor(i)
		if got == payload {
			hits++
		} else if got != "" {
			// Non-empty stdin that doesn't match payload — sign the
			// fix mangled or truncated the secret.
			t.Errorf("call[%d]: stdin present but content differs "+
				"from %s sentinel", i, secretSentinel)
		}
	}

	if hits == 0 {
		t.Errorf("passphrase (%s) was never delivered via stdin to "+
			"cryptsetup; recorded calls=%v",
			secretSentinel, fx.CommandLines())
	}
}

// TestBug175LUKSFormatPassphraseWithShellMetacharsSafe pins the fix
// on the Format path. A passphrase that would terminate the
// `printf %q` double-quoted string and inject a destructive command
// must not flow through `sh -c`.
func TestBug175LUKSFormatPassphraseWithShellMetacharsSafe(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup isLuks /dev/sda",
		storage.FakeResponse{Err: errBug175NotLuks})

	c := luks.NewCryptsetup(fx)

	err := c.Format(t.Context(), "/dev/sda", []byte(payloadFormat))
	if err != nil {
		t.Fatalf("Format: %v", err)
	}

	assertNoShellPipeline(t, fx)
	assertCryptsetupInvoked(t, fx, payloadFormat, "luksFormat")
	assertStdinDeliveredOnce(t, fx, payloadFormat)
}

// TestBug175LUKSOpenPassphraseWithShellMetacharsSafe pins the fix on
// the Open path with a command-substitution payload.
func TestBug175LUKSOpenPassphraseWithShellMetacharsSafe(t *testing.T) {
	fx := storage.NewFakeExec()

	c := luks.NewCryptsetup(fx)

	err := c.Open(t.Context(), "/dev/sda", "pvc-1-crypt", []byte(payloadOpen))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	assertNoShellPipeline(t, fx)
	assertCryptsetupInvoked(t, fx, payloadOpen, "luksOpen")
	assertStdinDeliveredOnce(t, fx, payloadOpen)
}

// TestBug175LUKSResizePassphraseWithShellMetacharsSafe pins the fix
// on the Resize path with a backtick payload.
func TestBug175LUKSResizePassphraseWithShellMetacharsSafe(t *testing.T) {
	fx := storage.NewFakeExec()

	c := luks.NewCryptsetup(fx)

	err := c.Resize(t.Context(), "pvc-grow-0-luks", []byte(payloadResize))
	if err != nil {
		t.Fatalf("Resize: %v", err)
	}

	assertNoShellPipeline(t, fx)
	assertCryptsetupInvoked(t, fx, payloadResize, "resize")
	assertStdinDeliveredOnce(t, fx, payloadResize)
}

// TestBug175LUKSPassphraseStdinNotArgv is a defence-in-depth check
// across Format+Open+Resize: the passphrase MUST never appear in any
// recorded argv element, regardless of payload content. We sweep
// every Calls entry for an exact-substring match.
func TestBug175LUKSPassphraseStdinNotArgv(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup isLuks /dev/sda",
		storage.FakeResponse{Err: errBug175NotLuks})

	c := luks.NewCryptsetup(fx)

	// Mix of three distinct injection shapes — if any single one
	// leaks via argv we want a localised failure.
	pass := []byte(payloadFormat + payloadOpen + payloadResize)

	if err := c.Format(t.Context(), "/dev/sda", pass); err != nil {
		t.Fatalf("Format: %v", err)
	}

	if err := c.Open(t.Context(), "/dev/sda", "pvc-1-crypt", pass); err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := c.Resize(t.Context(), "pvc-1-crypt", pass); err != nil {
		t.Fatalf("Resize: %v", err)
	}

	// No `sh -c` anywhere — fix forbids the shell pipeline entirely.
	assertNoShellPipeline(t, fx)

	// Sweep every recorded argv for the secret.
	for i, call := range fx.Calls {
		for j, a := range call.Args {
			if strings.Contains(a, string(pass)) {
				t.Errorf("call[%d].Args[%d]: passphrase (%s) present "+
					"in argv; must be on stdin", i, j, secretSentinel)
			}
		}
	}
}
