//go:build integration

// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/tests/integration/harness"
)

// `linstor encryption status` reads the `status` field of the reply as one
// object, the shape LINSTOR sends. An array of them, which the upstream
// OpenAPI declares, passes the machine-readable path untouched and fails
// the human one with a traceback, so the verb is driven without
// `--machine-readable`.
func TestEncryptionStatusThroughTheLinstorClient(t *testing.T) {
	stack := harness.StartStack(t)
	cli := &harness.CLI{URL: stack.RestURL}

	out := string(cli.Human(t, "encryption", "status"))
	if !strings.Contains(out, "has not been initialized yet") {
		t.Errorf("status before create-passphrase: got %q, want the unset message", out)
	}

	cli.Run(t, "encryption", "create-passphrase", "--passphrase", "supersecret-passphrase-1")

	out = string(cli.Human(t, "encryption", "status"))
	if !strings.Contains(out, "The controller is unlocked.") {
		t.Errorf("status after create-passphrase: got %q, want the unlocked message", out)
	}
}
