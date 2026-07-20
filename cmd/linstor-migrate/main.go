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
// Input is a directory of `kubectl get <table> -ojson` dumps of the
// `*.internal.linstor.linbit.com` CRDs LINSTOR uses as its database
// when running with the k8s connector:
//
//	kubectl get crds | grep -o ".*.internal.linstor.linbit.com" | \
//	  xargs -I{} sh -c "kubectl get {} -ojson > {}.json"
//
// Output is a multi-document YAML stream of blockstor
// `blockstor.cozystack.io/v1alpha1` objects in apply order (nodes,
// storage pools, resource groups, resource definitions, resources,
// snapshots) on stdout (or -out <file>), with the migration report —
// row counts plus every skipped row / dropped flag bit — on stderr.
//
// Usage:
//
//	linstor-migrate -in /path/to/dump [-out manifests.yaml]
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/cozystack/blockstor/pkg/linstormigrate"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		inDir     = flag.String("in", "", "directory with *.internal.linstor.linbit.com.json table dumps (required)")
		outPath   = flag.String("out", "-", "write manifests to this file ('-' = stdout)")
		portsPath = flag.String("drbd-ports", "", "optional '<rd-name> <port>' file of LIVE DRBD ports (see runbook); preserves the running mesh endpoint so adoption doesn't reconnect")
	)

	flag.Parse()

	if *inDir == "" {
		fmt.Fprintln(os.Stderr, "error: -in <dump-dir> is required")
		flag.Usage()

		return 2
	}

	err := convertToOutput(*inDir, *outPath, *portsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)

		return 1
	}

	return 0
}

// convertToOutput runs the whole load → convert → write pipeline,
// writing manifests to outPath and the migration report to stderr.
func convertToOutput(inDir, outPath, portsPath string) error {
	opts, err := buildOptions(portsPath)
	if err != nil {
		return err
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

	err = linstormigrate.WriteReport(os.Stderr, result)
	if err != nil {
		return fmt.Errorf("write report: %w", err)
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

	return linstormigrate.Options{DRBDPorts: ports}, nil
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
