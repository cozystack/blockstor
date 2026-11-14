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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/cozystack/blockstor/pkg/keyring"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

const fixtureDump = "../../pkg/linstormigrate/testdata/dump"

// The fixture's LINSTOR passphrase: -keyring-out writes a keyring it opens,
// the manifests carry the wrapped passwords, and neither the passphrase
// nor a password is in any output file.
func TestKeyringOutFromTheLinstorPassphrase(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	passFile := filepath.Join(dir, "pass")

	err := os.WriteFile(passFile, []byte("fixture-linstor-passphrase"), 0o600)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	out, ring := filepath.Join(dir, "manifests.yaml"), filepath.Join(dir, "keyring.yaml")

	err = convertToOutput(fixtureDump, out, "", &luksFlags{linstorPassFile: passFile, keyringOut: ring, namespace: "cozy-linstor"})
	if err != nil {
		t.Fatalf("convert: %v", err)
	}

	raw, err := os.ReadFile(ring)
	if err != nil {
		t.Fatalf("read keyring: %v", err)
	}

	var sec corev1.Secret

	err = yaml.Unmarshal(raw, &sec)
	if err != nil || sec.Name != passphrase.KeyringSecretName || sec.Namespace != "cozy-linstor" || sec.Type != passphrase.KeyringSecretType {
		t.Fatalf("keyring Secret %+v, %v", sec.ObjectMeta, err)
	}

	parsed, err := keyring.Parse(sec.Data[passphrase.KeyringDataKey])
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if _, err := parsed.Opens([]byte("fixture-linstor-passphrase")); err != nil {
		t.Fatalf("the LINSTOR passphrase does not open the keyring: %v", err)
	}

	manifests, err := os.ReadFile(out)
	if err != nil || !strings.Contains(string(manifests), "origin: LinstorImport") || !strings.Contains(string(manifests), parsed.KeyID) {
		t.Fatalf("manifests lack the imported keys: %v", err)
	}

	for _, leak := range []string{"fixture-linstor-passphrase", "fixtureVolumePassword"} {
		if strings.Contains(string(manifests)+string(raw), leak) {
			t.Fatalf("%q reached an output file", leak)
		}
	}
}

// A wrong passphrase writes nothing; inconsistent flags are refused.
func TestKeyringOutRefusals(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	passFile := filepath.Join(dir, "pass")

	err := os.WriteFile(passFile, []byte("fixture-linstor-passphrase\n"), 0o600)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	out, ring := filepath.Join(dir, "manifests.yaml"), filepath.Join(dir, "keyring.yaml")

	err = convertToOutput(fixtureDump, out, "", &luksFlags{linstorPassFile: passFile, keyringOut: ring})
	if err == nil {
		t.Fatal("a passphrase with a trailing newline the Secret does not have was accepted")
	}

	for _, path := range []string{out, ring} {
		if _, statErr := os.Stat(path); statErr == nil {
			t.Errorf("%s written after a refused passphrase", path)
		}
	}

	goodPass := filepath.Join(dir, "good")

	err = os.WriteFile(goodPass, []byte("fixture-linstor-passphrase"), 0o600)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	passFile = goodPass

	for name, flags := range map[string]luksFlags{
		"no keyring":       {linstorPassFile: passFile},
		"both keyrings":    {linstorPassFile: passFile, keyringOut: ring, keyringIn: ring, blockstorPassFile: passFile},
		"in without pass":  {linstorPassFile: passFile, keyringIn: ring},
		"out without pass": {keyringOut: ring},
	} {
		err := convertToOutput(fixtureDump, filepath.Join(dir, name), "", &flags)
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	err = convertToOutput(fixtureDump, filepath.Join(dir, "none"), "", &luksFlags{})
	if err == nil || !strings.Contains(err.Error(), "linstor-passphrase-file") {
		t.Errorf("a LUKS dump without the passphrase: %v", err)
	}
}
