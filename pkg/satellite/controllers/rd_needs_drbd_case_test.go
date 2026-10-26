package controllers

import (
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// A stack stored in another case still names DRBD; reading it as "no DRBD"
// skipped the DRBD allocation while the apply path brought DRBD up.
func TestRDNeedsDRBDIgnoresCase(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{}
	rd.Spec.LayerStack = []string{"drbd", "storage"}

	if !rdNeedsDRBD(rd) {
		t.Fatal("a lowercase drbd,storage stack read as one without DRBD")
	}

	rd.Spec.LayerStack = []string{"storage"}
	if rdNeedsDRBD(rd) {
		t.Fatal("a storage-only stack read as one with DRBD")
	}
}
