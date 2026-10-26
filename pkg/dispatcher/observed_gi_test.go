package dispatcher

import (
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// The satellite tells its own crashed replica from a foreign superblock by
// the GI it last reported, so every desired volume carries it.
func TestBuildVolumesCarriesTheReportedGI(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{}
	rd.Spec.VolumeDefinitions = []blockstoriov1alpha1.ResourceDefinitionVolume{
		{VolumeNumber: 0, SizeKib: 4096},
		{VolumeNumber: 1, SizeKib: 4096},
	}

	target := &blockstoriov1alpha1.Resource{}
	target.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, CurrentGI: "ABCDEF0123456789"}}

	vols := buildVolumes(rd, target)
	if len(vols) != 2 {
		t.Fatalf("got %d volumes, want 2", len(vols))
	}

	if vols[0].GetObservedGI() != "ABCDEF0123456789" {
		t.Errorf("volume 0 carries %q, want the reported GI", vols[0].GetObservedGI())
	}

	if vols[1].GetObservedGI() != "" {
		t.Errorf("volume 1 never reported a GI and carries %q", vols[1].GetObservedGI())
	}
}
