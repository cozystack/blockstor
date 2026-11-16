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

package linstormigrate

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/keyring"
)

// fixtureLinstorPassphrase is the LINSTOR master passphrase the fixture
// dump's encrypted/* properties and LUKS volume passwords were made with
// (see luks_test.go for how they were computed).
const fixtureLinstorPassphrase = "fixture-linstor-passphrase"

// fixtureLUKS is the import options for the fixture dump, wrapping under a
// fresh blockstor master key.
func fixtureLUKS(t *testing.T) *LUKSImport {
	t.Helper()

	master, err := keyring.NewMasterKey()
	if err != nil {
		t.Fatalf("NewMasterKey: %v", err)
	}

	return &LUKSImport{LinstorPassphrase: []byte(fixtureLinstorPassphrase), MasterKey: master}
}

// convertForTest converts dump, importing LUKS passwords with the
// fixture's passphrase when it has any.
func convertForTest(t *testing.T, dump *Dump) (*Result, error) {
	t.Helper()

	var opts Options
	if len(dump.LayerLuksVolumes) > 0 {
		opts.LUKS = fixtureLUKS(t)
	}

	return ConvertWithOptions(dump, opts)
}
