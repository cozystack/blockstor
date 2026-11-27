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

package drbd_test

import (
	"encoding/base64"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// A generated secret has the shape LINSTOR's has: 15 random bytes in
// standard base64, so 20 characters with no padding.
func TestGenerateSharedSecretHasLINSTORsShape(t *testing.T) {
	t.Parallel()

	first, err := drbd.GenerateSharedSecret()
	if err != nil {
		t.Fatalf("GenerateSharedSecret: %v", err)
	}

	if len(first) != 20 {
		t.Errorf("length %d, want 20", len(first))
	}

	raw, err := base64.StdEncoding.DecodeString(first)
	if err != nil || len(raw) != 15 {
		t.Errorf("decodes to %d bytes, %v; want 15 bytes of standard base64", len(raw), err)
	}

	second, err := drbd.GenerateSharedSecret()
	if err != nil {
		t.Fatalf("GenerateSharedSecret: %v", err)
	}

	if first == second {
		t.Errorf("two calls returned the same secret")
	}
}

// A copied secret goes whatever case its key was written in, and nothing
// else does.
func TestDropSharedSecretFoldsCase(t *testing.T) {
	t.Parallel()

	props := map[string]string{
		"DrbdOptions/Net/shared-secret": "a",
		"drbdoptions/net/Shared-Secret": "b",
		drbd.CramHMACAlgProp:            drbd.DefaultCramHMACAlg,
	}

	drbd.DropSharedSecret(props)

	if len(props) != 1 || props[drbd.CramHMACAlgProp] != drbd.DefaultCramHMACAlg {
		t.Errorf("got %v, want only the digest left", props)
	}
}
