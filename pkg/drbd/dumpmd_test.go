package drbd

import (
	"slices"
	"testing"
)

func TestParseMetadataUUIDsReadsEverySlot(t *testing.T) {
	t.Parallel()

	dump := []byte(`# DRBD meta data dump
version "v09";
node-id 0;
current-uuid 0x9F00AA11BB22CC33; flags 0x00000011;
peer[0] {
    bitmap-uuid 0x0000000000000000;
}
peer[1] {
    bitmap-uuid 0x6e3a5d0b2c1f4a96;
}
history-uuids 0x1234567890ABCDEF 0x0000000000000000;
`)

	got := ParseMetadataUUIDs(dump)
	want := []string{"9F00AA11BB22CC33", "6E3A5D0B2C1F4A96", "1234567890ABCDEF"}

	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// The lowest bit marks a UUID issued while Primary and is cleared when the
// UUID moves into history.
func TestSameGenerationIgnoresThePrimaryBit(t *testing.T) {
	t.Parallel()

	if !SameGeneration("6E3A5D0B2C1F4A97", "6e3a5d0b2c1f4a96") {
		t.Error("a UUID and its history form are not the same generation")
	}

	if SameGeneration("6E3A5D0B2C1F4A97", "6E3A5D0B2C1F4A95") {
		t.Error("two generations that differ above the Primary bit compare equal")
	}
}
