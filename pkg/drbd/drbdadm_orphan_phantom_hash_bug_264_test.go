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
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Bug 264 (P3, cosmetic but noisy) — stand-caught on dev-kvaps.
// Every 5 minutes the orphan-sweeper logged:
//
//	orphan DRBD resource detected; running drbdadm down resource=#
//	drbdadm down on orphan err=drbdadm down #: no resources defined!: exit status 1
//
// `drbdsetup status` emits leading `# ...` comment / banner lines in
// some environments (e.g. when invoked via wrapper scripts, or when
// the kernel side prepends a configuration hint header). The
// `StatusResources` parser only filtered blank and whitespace-indented
// lines — a column-0 `#` token tripped the
// "first whitespace-token = resource name" branch and was misread as
// a resource named "#". The sweeper then called `drbdadm down #` on
// every cycle, which always fails with `no resources defined!` — so
// the operator saw a recurring error every 5 minutes that was pure
// noise.
//
// Fix: skip lines whose first non-whitespace byte is `#`. Comments
// have always been the documented convention for `drbdsetup` text
// output (the JSON variant has no such ambiguity), so the parser was
// strictly under-strict.
func TestBug264StatusResourcesSkipsCommentLines(t *testing.T) {
	t.Parallel()

	// Mixed output: real resource, blank separator, comment line,
	// real resource. The comment must be dropped; both real
	// resources must come through.
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status", storage.FakeResponse{Stdout: []byte(`# drbdsetup-9.2.13
pvc-aaa role:Primary
  volume:0 disk:UpToDate
  worker-2 role:Secondary
    volume:0 peer-disk:UpToDate

# trailing banner the wrapper script appended
pvc-bbb role:Secondary
  volume:0 disk:Diskless
`)})

	adm := drbd.NewAdm(fx)

	names, err := adm.StatusResources(t.Context())
	if err != nil {
		t.Fatalf("StatusResources: %v", err)
	}

	want := []string{"pvc-aaa", "pvc-bbb"}
	if !slices.Equal(names, want) {
		t.Errorf("StatusResources with comment lines: got %v, want %v (the `#` lines must be skipped, not returned as a phantom resource named `#`)",
			names, want)
	}

	// Defensive: the phantom `#` must not slip through under any
	// indexing variation. Pre-fix this assertion is exactly the
	// stand-observed regression — "drbdadm down #" got called on
	// every sweep cycle.
	if slices.Contains(names, "#") {
		t.Errorf("StatusResources returned phantom resource `#` from a comment line; names=%v", names)
	}
}

// TestBug264StatusResourcesSkipsIndentedCommentLines pins the
// belt-and-braces variant: a comment that starts with whitespace was
// already covered by the indented-line skip, but make the invariant
// explicit so a future refactor that changes how leading whitespace
// is consumed doesn't reintroduce the phantom-`#` symptom.
func TestBug264StatusResourcesSkipsIndentedCommentLines(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status", storage.FakeResponse{Stdout: []byte(`pvc-aaa role:Primary
  # indented comment inside a resource block
  volume:0 disk:UpToDate
`)})

	adm := drbd.NewAdm(fx)

	names, err := adm.StatusResources(t.Context())
	if err != nil {
		t.Fatalf("StatusResources: %v", err)
	}

	want := []string{"pvc-aaa"}
	if !slices.Equal(names, want) {
		t.Errorf("StatusResources: got %v, want %v", names, want)
	}
}
