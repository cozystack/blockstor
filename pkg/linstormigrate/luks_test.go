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

package linstormigrate

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/keyring"
)

// The vectors are LINSTOR's scheme computed outside Go, with Python's
// cryptography package: PBKDF2-HMAC-SHA1 (5000 iterations, 16 bytes) over
// fixtureLinstorPassphrase and salt 40 41 .. 4f, AES-128 in CFB with a
// 128-bit segment, IV first, LINSTOR's length padding to 32 bytes. The
// master key is 10 11 .. 1f. They match the fixture dump.
//
// TestLinstorJDKVectors checks the two JDK details against vectors made by
// a real JDK, from testdata/jdk/Vec.java.
const (
	katLinstorSalt       = "QEFCQ0RFRkdISUpLTE1OTw=="
	katLinstorMasterKey  = "YGFiY2RlZmdoaWprbG1ub5bVilcKH4i0OIPhTpxwW8Nd2jaxBs/yYutifx5pjM8E"
	katLinstorMasterHash = "fUwo1VHOQnfLAHVKzly/Bhr5lBOgsAwojcocYiwNm2eLuLZ+HZsOVctGnaFVzwxjiWqj8nnqfWRcrt9Vf3462Q=="
	katLinstorPasswordA  = "cHFyc3R1dnd4eXp7fH1+fy80DDtnOEBOEb0MkGfMu5+dQHXvyvr5aX79LVGZ5huD"
	katLinstorPasswordS  = "kJGSk5SVlpeYmZqbnJ2eny3pmt3o0WfLK2MMOgHbYA5tyzLnf17HClpEHLBoii1r"
)

func linstorProps(instance string) []PropsContainerRow {
	return []PropsContainerRow{
		{PropsInstance: instance, PropKey: linstorMasterKeyProp, PropValue: katLinstorMasterKey},
		{PropsInstance: instance, PropKey: linstorSaltProp, PropValue: katLinstorSalt},
		{PropsInstance: instance, PropKey: linstorHashProp, PropValue: katLinstorMasterHash},
	}
}

func seq(from, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(from + i)
	}

	return out
}

// LINSTOR's master key comes back from either props instance, and its
// volume passwords decrypt under it.
func TestLinstorMasterKeyMatchesKnownAnswer(t *testing.T) {
	t.Parallel()

	for _, instance := range []string{"/CTRL", "/CTRLCFG", "/ctrl"} {
		master, err := LinstorMasterKey(linstorProps(instance), []byte(fixtureLinstorPassphrase))
		if err != nil || !bytes.Equal(master, seq(0x10, 16)) {
			t.Fatalf("%s: master %x, %v", instance, master, err)
		}

		for encoded, want := range map[string]string{
			katLinstorPasswordA: "fixtureVolumePasswordAAAAAA=",
			katLinstorPasswordS: "fixtureSnapshotPasswordSS=",
		} {
			got, err := LinstorVolumePassword(master, encoded)
			if err != nil || string(got) != want {
				t.Fatalf("%s: password %q, %v; want %q", instance, got, err, want)
			}
		}
	}
}

// A wrong passphrase, a tampered hash and a missing property all stop the
// import; the passphrase is checked against the stored hash, not trusted.
func TestLinstorMasterKeyRefusals(t *testing.T) {
	t.Parallel()

	_, err := LinstorMasterKey(linstorProps("/CTRL"), []byte(fixtureLinstorPassphrase+"\n"))
	if !errors.Is(err, ErrLinstorWrongPassphrase) {
		t.Errorf("wrong passphrase: %v", err)
	}

	tampered := linstorProps("/CTRL")
	tampered[2].PropValue = strings.Replace(tampered[2].PropValue, "f", "g", 1)

	_, err = LinstorMasterKey(tampered, []byte(fixtureLinstorPassphrase))
	if !errors.Is(err, ErrLinstorWrongPassphrase) {
		t.Errorf("tampered hash: %v", err)
	}

	for i := range 3 {
		props := linstorProps("/CTRL")
		props = append(props[:i], props[i+1:]...)

		_, err = LinstorMasterKey(props, []byte(fixtureLinstorPassphrase))
		if !errors.Is(err, ErrLinstorNoMasterKey) {
			t.Errorf("without property %d: %v", i, err)
		}
	}

	_, err = LinstorMasterKey(linstorProps("/NODES/A"), []byte(fixtureLinstorPassphrase))
	if !errors.Is(err, ErrLinstorNoMasterKey) {
		t.Errorf("props on another instance: %v", err)
	}
}

func TestLinstorUnpad(t *testing.T) {
	t.Parallel()

	good := append([]byte{0, 3, 'a', 'b', 'c'}, make([]byte, 27)...)

	got, err := linstorUnpad(good)
	if err != nil || string(got) != "abc" {
		t.Fatalf("%q, %v", got, err)
	}

	for name, block := range map[string][]byte{
		"short":       {0},
		"not a block": good[:31],
		"zero length": append([]byte{0, 0}, make([]byte, 30)...),
		"too long":    append([]byte{0, 31}, make([]byte, 30)...),
	} {
		_, err := linstorUnpad(block)
		if !errors.Is(err, ErrLinstorBadPassword) {
			t.Errorf("%s: %v", name, err)
		}
	}

	_, err = linstorDecrypt(seq(0, 16), seq(0, 16))
	if !errors.Is(err, ErrLinstorBadPassword) {
		t.Errorf("IV only: %v", err)
	}
}

// ASCII reaches PBKDF2 unchanged; a byte above 0x7f is sign-extended to a
// char and UTF-8 encoded, as LINSTOR and the JDK do.
func TestJavaPassphraseBytes(t *testing.T) {
	t.Parallel()

	if got := javaPassphraseBytes([]byte("plain-ascii")); string(got) != "plain-ascii" {
		t.Errorf("ascii: %q", got)
	}

	// 0xc3 sign-extends to U+FFC3, three bytes in UTF-8.
	if got := javaPassphraseBytes([]byte{0xc3}); !bytes.Equal(got, []byte{0xef, 0xbf, 0x83}) {
		t.Errorf("0xc3: % x", got)
	}
}

// The converted replicas carry their own passwords, wrapped under the
// blockstor master key and marked LinstorImport, and the plaintext is in
// none of the output.
func TestConvertImportsTheReplicaPasswords(t *testing.T) {
	t.Parallel()

	dump, err := LoadDump(filepath.Join("testdata", "dump"))
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	luks := fixtureLUKS(t)

	res, err := ConvertWithOptions(dump, Options{LUKS: luks})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	got := map[string]string{}

	for i := range res.Resources {
		for _, vol := range res.Resources[i].Spec.Volumes {
			if vol.LUKSKey == nil {
				continue
			}

			if vol.LUKSKey.Origin != "LinstorImport" {
				t.Errorf("%s: origin %s", res.Resources[i].Name, vol.LUKSKey.Origin)
			}

			pw, err := luks.MasterKey.UnwrapVolumePassword(keyring.WrappedKey{KeyID: vol.LUKSKey.KeyID, Wrapped: vol.LUKSKey.Wrapped})
			if err != nil {
				t.Fatalf("%s: unwrap: %v", res.Resources[i].Name, err)
			}

			got[res.Resources[i].Name] = string(pw)
		}
	}

	want := map[string]string{"pvc-vol3.node-a": "fixtureVolumePasswordAAAAAA=", "pvc-vol3.node-b": "fixtureVolumePasswordBBBBBB="}
	if len(got) != len(want) {
		t.Fatalf("imported %v, want %v", got, want)
	}

	for name, pw := range want {
		if got[name] != pw {
			t.Errorf("%s: %q, want %q", name, got[name], pw)
		}
	}

	var manifests, report bytes.Buffer

	_ = WriteManifests(&manifests, res)
	_ = WriteReport(&report, res)

	for _, pw := range want {
		if strings.Contains(manifests.String()+report.String(), pw) {
			t.Fatal("a plaintext password reached the output")
		}
	}
}

// A dump with LUKS volumes is refused without the LINSTOR passphrase, and
// with a wrong one, before anything is converted.
func TestConvertRefusesLUKSWithoutThePassphrase(t *testing.T) {
	t.Parallel()

	dump, err := LoadDump(filepath.Join("testdata", "dump"))
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	res, err := Convert(dump)
	if !errors.Is(err, ErrLUKSNeedsPassphrase) || res != nil {
		t.Errorf("no passphrase: %v", err)
	}

	wrong := fixtureLUKS(t)
	wrong.LinstorPassphrase = []byte("wrong")

	res, err = ConvertWithOptions(dump, Options{LUKS: wrong})
	if !errors.Is(err, ErrLinstorWrongPassphrase) || res != nil {
		t.Errorf("wrong passphrase: %v", err)
	}
}

// A given passphrase is checked even when the dump has no LUKS volume: the
// keyring made from it is what the cluster will unlock with. With no LINSTOR
// master key to check it against, it is refused rather than trusted.
func TestConvertChecksThePassphraseWithoutLUKSVolumes(t *testing.T) {
	t.Parallel()

	dump, err := LoadDump(filepath.Join("testdata", "dump"))
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	dump.LayerLuksVolumes = nil

	res, err := ConvertWithOptions(dump, Options{LUKS: fixtureLUKS(t)})
	if err != nil || res == nil {
		t.Fatalf("right passphrase: %v", err)
	}

	wrong := fixtureLUKS(t)
	wrong.LinstorPassphrase = []byte("wrong")

	res, err = ConvertWithOptions(dump, Options{LUKS: wrong})
	if !errors.Is(err, ErrLinstorWrongPassphrase) || res != nil {
		t.Errorf("wrong passphrase: %v", err)
	}

	dump.PropsContainers = nil

	res, err = ConvertWithOptions(dump, Options{LUKS: fixtureLUKS(t)})
	if !errors.Is(err, ErrLinstorPassphraseUnchecked) || res != nil {
		t.Errorf("nothing to check against: %v", err)
	}

	res, err = Convert(dump)
	if err != nil || res == nil {
		t.Errorf("no passphrase and no LUKS volume: %v", err)
	}
}

// A snapshot's LUKS rows become its luksKeys, per node, under the node's
// CR name.
func TestConvertImportsTheSnapshotPasswords(t *testing.T) {
	t.Parallel()

	dump, err := LoadDump(filepath.Join("testdata", "dump"))
	if err != nil {
		t.Fatalf("LoadDump: %v", err)
	}

	snapRD, snapName := "", ""

	for _, rd := range dump.ResourceDefinitions {
		if rd.SnapshotName != "" && rd.ResourceFlags&snapDfnFlagSuccessful != 0 && rd.ResourceFlags&snapDfnFlagFailedDeployment == 0 {
			snapRD, snapName = rd.ResourceName, rd.SnapshotName

			break
		}
	}

	if snapName == "" {
		t.Skip("the fixture has no successful snapshot")
	}

	node := ""

	for _, r := range dump.Resources {
		if r.ResourceName == snapRD && r.SnapshotName == snapName {
			node = r.NodeName

			break
		}
	}

	dump.LayerResourceIDs = append(dump.LayerResourceIDs, LayerResourceIDRow{
		LayerResourceID: 900, LayerResourceKind: "LUKS", NodeName: node, ResourceName: snapRD, SnapshotName: snapName,
	})
	dump.LayerLuksVolumes = append(dump.LayerLuksVolumes, LayerLuksVolumeRow{LayerResourceID: 900, VlmNr: 0, EncryptedPassword: katLinstorPasswordS})

	luks := fixtureLUKS(t)

	res, err := ConvertWithOptions(dump, Options{LUKS: luks})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.Snapshots) == 0 || len(res.Snapshots[0].Spec.LUKSKeys) != 1 {
		t.Fatalf("snapshots %+v", res.Snapshots)
	}

	entry := res.Snapshots[0].Spec.LUKSKeys[0]

	pw, err := luks.MasterKey.UnwrapVolumePassword(keyring.WrappedKey{KeyID: entry.KeyID, Wrapped: entry.Wrapped})
	if err != nil || string(pw) != "fixtureSnapshotPasswordSS=" || entry.NodeName != strings.ToLower(node) {
		t.Fatalf("snapshot key on %s: %q, %v", entry.NodeName, pw, err)
	}

	for i := range res.Resources {
		if strings.EqualFold(res.Resources[i].Spec.NodeName, node) && strings.EqualFold(res.Resources[i].Spec.ResourceDefinitionName, snapRD) {
			for _, vol := range res.Resources[i].Spec.Volumes {
				if vol.LUKSKey != nil {
					t.Fatalf("replica %s got the snapshot's password", res.Resources[i].Name)
				}
			}
		}
	}

	if strings.Contains(res.Warnings[0], "fixtureSnapshotPassword") {
		t.Fatal("the snapshot password reached the report")
	}
}

// Vectors made by JDK 22 (SunJCE) running LINSTOR's own steps:
// JclSymmetricKeyCipher.getInstanceWithPassword (each passphrase byte cast
// to a char, PBKDF2WithHmacSHA1, 5000 iterations, 128 bits),
// AES/CFB/NoPadding with the IV first, LengthPaddingImpl's 2-byte length
// and fill to 32, and SHA-512 of the padded master key block. The second
// passphrase is Cyrillic with a check mark and an accent, so every byte
// above 0x7f goes through the JDK's sign extension and UTF-8 encoding. The
// generator is testdata/jdk/Vec.java.
func TestLinstorJDKVectors(t *testing.T) {
	t.Parallel()

	const (
		salt   = "QEFCQ0RFRkdISUpLTE1OTw=="
		hash   = "qshMTUH0xqSrUKtaVAGxbntuAssqTECV8qJsXvngjVHIU7szigyLshQFcH8HND2QpuV5MQKkjdYwsHPiYISh6A=="
		volume = "cHFyc3R1dnd4eXp7fH1+fy81ADZ0GlpQAYYGrHPSrbi527tB02AGKZHAqrVB2kmt"
	)

	for pass, masterKey := range map[string]string{
		"jdk-ascii-passphrase": "YGFiY2RlZmdoaWprbG1ub5o2fVoMmVatM4UrxBWQ644lNy2C+fMMwlL7rgvH6cp3",
		"пароль ключ✓ é":       "YGFiY2RlZmdoaWprbG1ub8IprxpI0plt10ur+9T8roYPvP4EHNynAqjQ5aMAJf1C",
	} {
		props := []PropsContainerRow{
			{PropsInstance: "/CTRL", PropKey: linstorMasterKeyProp, PropValue: masterKey},
			{PropsInstance: "/CTRL", PropKey: linstorSaltProp, PropValue: salt},
			{PropsInstance: "/CTRL", PropKey: linstorHashProp, PropValue: hash},
		}

		master, err := LinstorMasterKey(props, []byte(pass))
		if err != nil || !bytes.Equal(master, seq(0x10, 16)) {
			t.Fatalf("%q: master %x, %v", pass, master, err)
		}

		got, err := LinstorVolumePassword(master, volume)
		if err != nil || string(got) != "jdkVolumePassword0123456789A=" {
			t.Fatalf("%q: volume password %q, %v", pass, got, err)
		}

		_, err = LinstorMasterKey(props, []byte(pass+"x"))
		if !errors.Is(err, ErrLinstorWrongPassphrase) {
			t.Fatalf("%q: a wrong passphrase: %v", pass, err)
		}
	}
}
