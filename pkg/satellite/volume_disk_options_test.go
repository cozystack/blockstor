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

package satellite

import (
	"strings"
	"testing"
)

// A volume's own disk options render inside that volume of the local
// host, where drbdadm attaches the device with them. block-size placed
// anywhere else either reaches every volume or none.
func TestAVolumesDiskOptionsRenderInsideItsVolume(t *testing.T) {
	t.Parallel()

	dr, devices := renderResFileFixture("pvc-vd")
	dr.Volumes[0].DiskOptions = map[string]string{"block-size": "4096"}

	body, err := buildResFile(dr, "n1", "10.0.0.1", devices, nil, nil)
	if err != nil {
		t.Fatalf("buildResFile: %v", err)
	}

	want := "      meta-disk internal;\n" +
		"      disk {\n" +
		"        block-size 4096;\n" +
		"      }\n" +
		"    }\n" +
		"  }\n" +
		"  on n2 {"
	if !strings.Contains(body, want) {
		t.Errorf("block-size must render inside n1's volume 0; got:\n%s", body)
	}
}
