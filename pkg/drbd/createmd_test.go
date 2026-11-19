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
	"errors"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

func TestCreateMDWithMaxPeersPassesTheCount(t *testing.T) {
	fx := storage.NewFakeExec()

	if err := drbd.NewAdm(fx).CreateMDWithMaxPeers(t.Context(), "pvc-1/0", 7); err != nil {
		t.Fatalf("CreateMDWithMaxPeers: %v", err)
	}

	want := "drbdadm create-md --force --max-peers=7 pvc-1/0"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// The peer-slot count is read with --force, so a superblock whose activity
// log was never applied, the state every snapshot of an attached volume
// carries, still answers.
func TestMetadataMaxPeersReadsAnUncleanSuperblock(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md --force pvc-1/0", storage.FakeResponse{Stdout: []byte(
		"# DRBD meta data dump\n" +
			"This_is_an_unclean_meta_data_dump._Don't_trust_the_bitmap.\n" +
			"# You should \"apply-al\" first, if you plan to restore this.\n\n" +
			"version \"v09\";\n\nmax-peers 7;\n# md_size_sect 552\nnode-id 2;\n",
	)})

	peers, err := drbd.NewAdm(fx).MetadataMaxPeers(t.Context(), "pvc-1/0")
	if err != nil || peers != 7 {
		t.Fatalf("MetadataMaxPeers = %d, %v; want 7", peers, err)
	}
}

func TestParseMetadataMaxPeersRefusesWhatItCannotRead(t *testing.T) {
	for name, dump := range map[string]string{
		"empty":        "",
		"v08":          "version \"v08\";\nuuid {\n    0x1; 0x0; 0x0; 0x0;\n    flags 0x1;\n}\n",
		"not a number": "version \"v09\";\nmax-peers seven;\n",
		"zero":         "version \"v09\";\nmax-peers 0;\n",
		"out of range": "version \"v09\";\nmax-peers 31;\n",
	} {
		t.Run(name, func(t *testing.T) {
			peers, err := drbd.ParseMetadataMaxPeers([]byte(dump))
			if !errors.Is(err, drbd.ErrNoMaxPeers) {
				t.Errorf("ParseMetadataMaxPeers = %d, %v; want ErrNoMaxPeers", peers, err)
			}
		})
	}
}
