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

package passphrase

import (
	"slices"
	"testing"
)

// Every passphrase key goes, in any case; the keys that held a value come
// back sorted for the log line, and an empty one is dropped silently.
func TestDropPlaintextPropsNamesWhatItDropped(t *testing.T) {
	t.Parallel()

	props := map[string]string{
		"drbdoptions/ENCRYPTPASSPHRASE": "a",
		PropKeyLegacy:                   "b",
		PropKeyVolume:                   "",
		"DrbdOptions/Net/max-buffers":   "8000",
	}

	got := DropPlaintextProps(props)

	if want := []string{PropKeyLegacy, "drbdoptions/ENCRYPTPASSPHRASE"}; !slices.Equal(got, want) {
		t.Errorf("dropped %v, want %v", got, want)
	}

	if len(props) != 1 || props["DrbdOptions/Net/max-buffers"] != "8000" {
		t.Errorf("props left %v, want only the unrelated key", props)
	}
}
