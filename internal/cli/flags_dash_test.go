// SPDX-License-Identifier: Apache-2.0

package cli

import "testing"

// A passphrase is free-form bytes, so a leading dash in one is a value and
// not a swallowed flag. The identifier guard that catches `resource list -n
// --faulty` rejects it, and the form that does work, `--passphrase=-s3cret`,
// is not discoverable from the refusal.
func TestOpaqueFlagValueMayLeadWithADash(t *testing.T) {
	parsed, err := parseFlags([]string{"--passphrase", "-s3cret"})
	if err != nil {
		t.Fatalf("a passphrase leading with a dash was refused: %v", err)
	}

	if got := parsed.Values["passphrase"]; got != "-s3cret" {
		t.Errorf("passphrase = %q, want %q", got, "-s3cret")
	}
}

// The guard still holds where the value is an identifier: a node name never
// starts with a dash, so this is a missing value and a swallowed flag.
func TestIdentifierFlagStillRefusesAFlagAsItsValue(t *testing.T) {
	_, err := parseFlags([]string{"-n", "--faulty"})
	if err == nil {
		t.Fatal("a flag was accepted as a node name, which yields an empty filter and exit 0")
	}
}
