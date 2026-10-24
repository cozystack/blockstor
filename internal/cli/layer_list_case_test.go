package cli

import (
	"slices"
	"testing"
)

// The CLI writes the CRDs directly, so it stores the stack in the same
// canonical case as the REST doors.
func TestParseLayerListStoresCanonicalCase(t *testing.T) {
	t.Parallel()

	got, err := parseLayerList("drbd,Luks,storage")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	want := []string{"DRBD", "LUKS", "STORAGE"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
