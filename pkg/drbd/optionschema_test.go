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
	"os"
	"path/filepath"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// xmlHelp reads the trimmed `drbdsetup xml-help <command>` excerpts in
// testdata, which keep the shape drbdsetup prints (print_command_usage
// and the describe_xml functions of drbd-utils).
func xmlHelp(t *testing.T) [][]byte {
	t.Helper()

	docs := make([][]byte, 0, len(drbd.DiskSectionCommands()))

	for _, cmd := range drbd.DiskSectionCommands() {
		doc, err := os.ReadFile(filepath.Join("testdata", "xml-help-"+cmd+".xml"))
		if err != nil {
			t.Fatal(err)
		}

		docs = append(docs, doc)
	}

	return docs
}

func diskSchema(t *testing.T) *drbd.OptionSchema {
	t.Helper()

	schema, err := drbd.ParseOptionSchema(xmlHelp(t)...)
	if err != nil {
		t.Fatalf("ParseOptionSchema: %v", err)
	}

	return schema
}

// The schema accepts what drbdadm's disk-section parser accepts and
// refuses what it refuses, so only an option that would fail the whole
// .res is dropped from it.
func TestOptionSchemaChecksLikeDrbdadm(t *testing.T) {
	t.Parallel()

	schema := diskSchema(t)

	cases := []struct {
		name, value string
		ok          bool
	}{
		{"block-size", "4096", true},           // a device option, from new-minor
		{"block-size", "8192", false},          // above its range
		{"block-size", "4k", true},             // a K suffix is 4096
		{"discard-granularity", "65536", true}, // device option
		{"rs-discard-granularity", "1M", true}, // M suffix is 1048576, the max
		{"rs-discard-granularity", "2M", false},
		{"rs-discard-granularity", "lots", false},
		{"c-max-rate", "250M", true}, // peer-device option, counted in KiB
		{"c-max-rate", "100", false}, // below its minimum
		{"resync-rate", "1s", true},  // one sector rounds up to 1 KiB
		{"disk-barrier", "no", true},
		{"disk-barrier", "maybe", false},
		{"on-io-error", "detach", true},
		{"on-io-error", "explode", false},
		{"al-extents", "100000", true},  // clamped by drbdadm, not refused
		{"resync-after", "rsc/0", true}, // checked after the parse
		{"set-defaults", "yes", false},  // a drbdsetup switch, no .res keyword
		{"some-future-option", "1", false},
	}

	for _, tc := range cases {
		err := schema.Check(tc.name, tc.value)
		if tc.ok && err != nil {
			t.Errorf("Check(%s, %s) = %v, want accepted", tc.name, tc.value, err)
		}

		if !tc.ok && !errors.Is(err, drbd.ErrOptionRejected) {
			t.Errorf("Check(%s, %s) = %v, want ErrOptionRejected", tc.name, tc.value, err)
		}
	}
}

// A schema that could not be read accepts everything, so a node whose
// drbdsetup cannot describe its options renders what it did before.
func TestNilOptionSchemaAcceptsEverything(t *testing.T) {
	t.Parallel()

	var schema *drbd.OptionSchema

	if err := schema.Check("some-future-option", "1"); err != nil {
		t.Errorf("nil schema refused an option: %v", err)
	}

	if _, err := drbd.ParseOptionSchema([]byte{}); err == nil {
		t.Error("an empty xml-help parsed into a schema that would refuse every option")
	}
}

// DiskOptionSchema asks drbdsetup once and keeps the answer, but a
// failure is not kept: the next render asks again.
func TestAdmDiskOptionSchemaIsReadOnceFromDrbdsetup(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if _, err := adm.DiskOptionSchema(t.Context()); err == nil {
		t.Fatal("DiskOptionSchema with a silent drbdsetup succeeded")
	}

	for i, cmd := range drbd.DiskSectionCommands() {
		fx.Expect("drbdsetup xml-help "+cmd, storage.FakeResponse{Stdout: xmlHelp(t)[i]})
	}

	for range 2 {
		schema, err := adm.DiskOptionSchema(t.Context())
		if err != nil {
			t.Fatalf("DiskOptionSchema: %v", err)
		}

		if err := schema.Check("block-size", "4096"); err != nil {
			t.Errorf("schema from drbdsetup refuses block-size: %v", err)
		}
	}

	asked := 0

	for _, line := range fx.CommandLines() {
		if line == "drbdsetup xml-help new-minor" {
			asked++
		}
	}

	// Once for the try whose empty answers failed to parse, once for the
	// success, and never again.
	if asked != 2 {
		t.Errorf("drbdsetup xml-help new-minor asked %d times, want 2 (failure retried, success kept)", asked)
	}
}
