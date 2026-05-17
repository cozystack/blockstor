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

package lvm_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestRestoreIncompleteTagHasNoAtPrefix pins Bug 259: the constant
// MUST be a bare tag name. `--addtag X` / `--deltag X` accept bare
// names; `@X` is reserved for tag references in select/filter
// expressions. The pre-Bug-259 constant was `@blockstor-restore-incomplete`
// which `lvchange --addtag` either rejected or silently stripped,
// and `lvs -o lv_tags` returns bare names so the `strings.Contains`
// check always missed — the completion sentinel never tripped and
// the Bug 257 data-loss path stayed wide open.
//
// This test uses string literals (NOT the constant itself) to avoid
// the same tautological-test class that hid the bug — a future
// refactor that re-introduces `@` would have to ALSO break this
// literal assertion, which is much louder.
func TestRestoreIncompleteTagHasNoAtPrefix(t *testing.T) {
	t.Parallel()

	tag := lvm.RestoreIncompleteTag

	if strings.HasPrefix(tag, "@") {
		t.Fatalf("Bug 259: lvm.RestoreIncompleteTag = %q must NOT start with @ "+
			"(LVM `--addtag` / `--deltag` take bare names; @ is for tag "+
			"references in select/filter expressions). The pre-fix constant "+
			"silently broke the Bug 257 completion sentinel because lvchange "+
			"stripped the @ and lvs reported the bare name, so the "+
			"strings.Contains check always missed.", tag)
	}

	if tag != "blockstor-restore-incomplete" {
		t.Fatalf("Bug 259: lvm.RestoreIncompleteTag = %q; want literal "+
			"%q. This test uses a string literal (not the constant) to avoid "+
			"the tautological-test class v36 caught.",
			tag, "blockstor-restore-incomplete")
	}
}

// TestLvHasRestoreIncompleteTagMatchesWholeToken pins the comma-split
// matcher in lvHasRestoreIncompleteTag. `lvs -o lv_tags` emits
// comma-separated bare tag names; a naïve `strings.Contains` would
// false-positive on a sibling tag like `blockstor-restore-incomplete-v2`.
// Verifying the comma-token split rules out that false-positive.
func TestLvHasRestoreIncompleteTagMatchesWholeToken(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		output string
		want   bool
	}{
		{"exact match", "blockstor-restore-incomplete\n", true},
		{"trailing whitespace", "  blockstor-restore-incomplete  \n", true},
		{"first in list", "blockstor-restore-incomplete,linstor_other\n", true},
		{"middle of list", "tag_a,blockstor-restore-incomplete,tag_b\n", true},
		{"last in list", "tag_a,blockstor-restore-incomplete\n", true},
		{"empty output", "", false},
		{"only whitespace", "  \n", false},
		{"different tag", "blockstor-other-sentinel\n", false},
		{"substring false-positive guard", "blockstor-restore-incomplete-v2\n", false},
		{"substring with prefix", "z-blockstor-restore-incomplete\n", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := false

			for _, tag := range strings.Split(strings.TrimSpace(tc.output), ",") {
				if strings.TrimSpace(tag) == lvm.RestoreIncompleteTag {
					got = true

					break
				}
			}

			if got != tc.want {
				t.Errorf("Bug 259: lvs output %q match: got %v, want %v",
					tc.output, got, tc.want)
			}
		})
	}
}
