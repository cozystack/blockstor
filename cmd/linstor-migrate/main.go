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

// Command linstor-migrate converts a LINSTOR k8s-backend database dump
// into blockstor CRD manifests.
//
// Input is a directory of `kubectl get <plural>.<version>.<group> -ojson`
// dumps of the `*.internal.linstor.linbit.com` CRDs LINSTOR uses as its
// database when running with the k8s connector, each read at its CRD's
// storage version (the migration runbook has the command). A plain
// `kubectl get <crd>` reads the oldest version the CRD serves, where
// the API server prunes columns such as tcp_port_list, and a dump read
// that way is refused.
//
// Output is a multi-document YAML stream of blockstor
// `blockstor.org/v1alpha1` objects in apply order (nodes,
// storage pools, resource groups, resource definitions, resources,
// snapshots) on stdout (or -out <file>), with the migration report —
// row counts plus every skipped row / dropped flag bit — on stderr.
//
// A dump with LUKS volumes needs the LINSTOR master passphrase, the value
// of MASTER_PASSPHRASE in the piraeus Secret, from a file: each volume's
// password is decrypted the way LINSTOR does it and re-wrapped under a
// blockstor master key. -keyring-out writes a new keyring Secret whose
// one slot is the LINSTOR passphrase itself, so the Secret piraeus used
// keeps unlocking the cluster; -keyring-in wraps under the keyring of a
// blockstor cluster that already exists.
//
// Usage:
//
//	linstor-migrate -in /path/to/dump [-out manifests.yaml] \
//	  [-linstor-passphrase-file pass -keyring-out keyring.yaml | -keyring-in keyring.json -blockstor-passphrase-file pass]
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/cozystack/blockstor/pkg/keyring"
	"github.com/cozystack/blockstor/pkg/linstormigrate"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// luksFlags are the flags of the LUKS password import.
type luksFlags struct {
	linstorPassFile, keyringOut, keyringIn, blockstorPassFile, namespace string

	// ring is the keyring -keyring-out writes, set by load.
	ring *keyring.Keyring
}

// keyringFileMode keeps the keyring manifest to its owner: it holds no
// key in the clear, but it is what a passphrase guess is tried against.
const keyringFileMode = 0o600

var errLUKSFlags = errors.New(
	"-linstor-passphrase-file needs exactly one of -keyring-out, or -keyring-in with -blockstor-passphrase-file")

func main() {
	os.Exit(run())
}

func run() int {
	var (
		inDir     = flag.String("in", "", "directory with *.internal.linstor.linbit.com.json table dumps (required)")
		outPath   = flag.String("out", "-", "write manifests to this file ('-' = stdout)")
		portsPath = flag.String("drbd-ports", "", "optional file of LIVE DRBD ports, '<rd-name> <node> <port>' per replica or '<rd-name> <port>' per definition (see runbook); cross-checks the database's ports against the running mesh endpoints, which win, so adoption doesn't reconnect")
		luks      luksFlags
	)

	flag.StringVar(&luks.linstorPassFile, "linstor-passphrase-file", "",
		"file holding the LINSTOR master passphrase, byte for byte ('-' = stdin); needed when the dump has LUKS volumes")
	flag.StringVar(&luks.keyringOut, "keyring-out", "",
		"write a new keyring Secret manifest here whose one slot is the LINSTOR master passphrase")
	flag.StringVar(&luks.keyringIn, "keyring-in", "",
		"keyring.json of an existing blockstor cluster to wrap the passwords under, instead of -keyring-out")
	flag.StringVar(&luks.blockstorPassFile, "blockstor-passphrase-file", "",
		"file holding a passphrase that opens -keyring-in")
	flag.StringVar(&luks.namespace, "keyring-namespace", "blockstor-system",
		"namespace of the keyring Secret -keyring-out writes: blockstor's own")

	flag.Parse()

	if *inDir == "" {
		fmt.Fprintln(os.Stderr, "error: -in <dump-dir> is required")
		flag.Usage()

		return 2
	}

	err := convertToOutput(*inDir, *outPath, *portsPath, &luks)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)

		return 1
	}

	return 0
}

// convertToOutput runs the whole load → convert → write pipeline,
// writing manifests to outPath and the migration report to stderr.
func convertToOutput(inDir, outPath, portsPath string, luks *luksFlags) error {
	opts, err := buildOptions(portsPath)
	if err != nil {
		return err
	}

	err = luks.load(&opts)
	if err != nil {
		return err
	}

	if opts.LUKS != nil {
		defer opts.LUKS.MasterKey.Destroy()
		defer clear(opts.LUKS.LinstorPassphrase)
	}

	dump, err := linstormigrate.LoadDump(inDir)
	if err != nil {
		return fmt.Errorf("load dump: %w", err)
	}

	result, err := linstormigrate.ConvertWithOptions(dump, opts)
	if err != nil {
		return fmt.Errorf("convert: %w", err)
	}

	out, closeOut, err := openOutput(outPath)
	if err != nil {
		return err
	}

	err = linstormigrate.WriteManifests(out, result)
	if err != nil {
		_ = closeOut()

		return fmt.Errorf("write manifests: %w", err)
	}

	// Close BEFORE reporting success: a deferred close would run after
	// the exit code is fixed, so an ENOSPC-style flush failure would
	// leave a truncated manifest behind while the process still exited
	// 0 — and a `linstor-migrate … && kubectl apply -f …` pipeline would
	// apply the truncated stream as if it were complete.
	err = closeOut()
	if err != nil {
		return err
	}

	// Written only after the conversion verified the LINSTOR passphrase
	// and imported every password.
	if luks.ring != nil {
		err = writeKeyring(luks.keyringOut, luks.namespace, luks.ring)
		if err != nil {
			return err
		}
	}

	err = linstormigrate.WriteReport(os.Stderr, result)
	if err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	return nil
}

// load reads the passphrase files and the keyring the flags name and puts
// the import options into opts; with -keyring-out it keeps the new keyring
// in f.ring for writing once the conversion succeeded.
func (f *luksFlags) load(opts *linstormigrate.Options) error {
	if f.linstorPassFile == "" {
		if f.keyringOut != "" || f.keyringIn != "" || f.blockstorPassFile != "" {
			return errLUKSFlags
		}

		return nil
	}

	if (f.keyringOut == "") == (f.keyringIn == "") || (f.keyringIn != "") != (f.blockstorPassFile != "") {
		return errLUKSFlags
	}

	linstorPass, err := readSecretFile(f.linstorPassFile)
	if err != nil {
		return err
	}

	var master *keyring.MasterKey

	if f.keyringOut != "" {
		// The LINSTOR passphrase opens the new keyring, so the piraeus
		// Secret unlocks blockstor unchanged.
		f.ring, master, err = keyring.New(linstorPass, passphrase.SlotNameSecret, keyring.OriginSecret, time.Now())
		if err != nil {
			err = fmt.Errorf("create the keyring: %w", err)
		}
	} else {
		master, err = unlockKeyringFile(f.keyringIn, f.blockstorPassFile)
	}

	if err != nil {
		clear(linstorPass)

		return err
	}

	opts.LUKS = &linstormigrate.LUKSImport{LinstorPassphrase: linstorPass, MasterKey: master}

	return nil
}

func unlockKeyringFile(keyringPath, passPath string) (*keyring.MasterKey, error) {
	data, err := os.ReadFile(keyringPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", keyringPath, err)
	}

	ring, err := keyring.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", keyringPath, err)
	}

	pass, err := readSecretFile(passPath)
	if err != nil {
		return nil, err
	}

	defer clear(pass)

	master, _, err := ring.Unlock(pass)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", keyringPath, err)
	}

	return master, nil
}

// readSecretFile reads a passphrase byte for byte: a trailing newline is
// part of it, as it is in the Secret.
func readSecretFile(path string) ([]byte, error) {
	if path == "-" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("read the passphrase from stdin: %w", err)
		}

		return data, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	return data, nil
}

// writeKeyring writes the keyring Secret manifest: wrapped material only.
func writeKeyring(path, namespace string, ring *keyring.Keyring) error {
	data, err := ring.Marshal()
	if err != nil {
		return fmt.Errorf("encode the keyring: %w", err)
	}

	manifest, err := yaml.Marshal(&corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: passphrase.KeyringSecretName, Namespace: namespace},
		Type:       passphrase.KeyringSecretType,
		Data:       map[string][]byte{passphrase.KeyringDataKey: data},
	})
	if err != nil {
		return fmt.Errorf("encode the keyring Secret: %w", err)
	}

	err = os.WriteFile(path, manifest, keyringFileMode)
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	return nil
}

// buildOptions loads the optional live DRBD port map from portsPath
// (see linstormigrate.ParseDRBDPorts for the format). Empty path
// yields empty options.
func buildOptions(portsPath string) (linstormigrate.Options, error) {
	if portsPath == "" {
		return linstormigrate.Options{}, nil
	}

	data, err := os.ReadFile(portsPath)
	if err != nil {
		return linstormigrate.Options{}, fmt.Errorf("read %s: %w", portsPath, err)
	}

	ports, err := linstormigrate.ParseDRBDPorts(string(data))
	if err != nil {
		return linstormigrate.Options{}, fmt.Errorf("%s: %w", portsPath, err)
	}

	return linstormigrate.Options{DRBDPorts: ports.ByDefinition, ReplicaDRBDPorts: ports.ByReplica}, nil
}

// openOutput resolves the -out flag: "-" streams to stdout (no-op
// closer), anything else creates the file. The returned closer reports
// its error so the caller can fail the run on a truncated write rather
// than exiting 0 with a partial manifest on disk.
func openOutput(path string) (io.Writer, func() error, error) {
	if path == "-" {
		return os.Stdout, func() error { return nil }, nil
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("create %s: %w", path, err)
	}

	closeFile := func() error {
		closeErr := file.Close()
		if closeErr != nil {
			return fmt.Errorf("close %s: %w", path, closeErr)
		}

		return nil
	}

	return file, closeFile, nil
}
