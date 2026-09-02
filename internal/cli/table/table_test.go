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

package table_test

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cozystack/blockstor/internal/cli/color"
	"github.com/cozystack/blockstor/internal/cli/table"
)

var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// resourceListTable mirrors the shape of `resource list`: seven
// columns, one healthy row and one broken row.
func resourceListTable() *metav1.Table {
	return &metav1.Table{
		ColumnDefinitions: []metav1.TableColumnDefinition{
			{Name: "ResourceName"},
			{Name: "Node"},
			{Name: "Port"},
			{Name: "Usage"},
			{Name: "Conns"},
			{Name: "State"},
			{Name: "CreatedOn"},
		},
		Rows: []metav1.TableRow{
			{Cells: []any{"pvc-abc", "node-1", int64(7001), "InUse", "Ok", "UpToDate", "2026-07-16 10:00:00"}},
			{Cells: []any{"pvc-abc", "node-2", int64(7001), "Unused", "Ok", "Inconsistent", "2026-07-16 10:00:01"}},
		},
	}
}

// The shell harnesses in this repository parse the table with
// `awk -F'|'` and read FIXED field indexes: `$5` is Usage and `$7` is
// State on a `resource list` row (tests/e2e/cli-matrix). That pins two
// things at once — the column order, and the fact that a row starts
// with the separator, which is what shifts the first data column into
// field 2. If this test fails, those scripts silently read the wrong
// column.
func TestRowFieldPositionsMatchHarnessContract(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := table.Render(&buf, resourceListTable(), table.Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	row := findRow(t, buf.String(), "node-1")

	fields := strings.Split(row, "|")
	if len(fields) < 8 {
		t.Fatalf("row %q split into %d fields, want at least 8 (leading separator + 7 columns)", row, len(fields))
	}

	// awk fields are 1-based and Go's split is 0-based, so awk `$N` is
	// fields[N-1]. The leading separator makes fields[0] empty, which
	// is exactly what shifts ResourceName into `$2` and lands Usage on
	// `$5` and State on `$7`.
	if got := strings.TrimSpace(fields[0]); got != "" {
		t.Errorf("field[0] = %q, want empty — the row must start with the separator", got)
	}

	if got := strings.TrimSpace(fields[4]); got != "InUse" {
		t.Errorf("awk $5 (fields[4]) = %q, want the Usage cell %q", got, "InUse")
	}

	if got := strings.TrimSpace(fields[6]); got != "UpToDate" {
		t.Errorf("awk $7 (fields[6]) = %q, want the State cell %q", got, "UpToDate")
	}
}

// Colour must never change the layout. Rendering the same table with
// and without colour and stripping the escapes has to produce
// byte-identical output — if width accounting counted escape bytes,
// every coloured row would be short by nine characters and the awk
// field positions above would still pass while the table looked broken.
func TestColourDoesNotDisturbLayout(t *testing.T) {
	t.Parallel()

	var plain, painted bytes.Buffer

	err := table.Render(&plain, resourceListTable(), table.Options{})
	if err != nil {
		t.Fatalf("Render plain: %v", err)
	}

	err = table.Render(&painted, resourceListTable(), table.Options{
		Color:        color.New(true),
		StateColumns: []string{"State"},
	})
	if err != nil {
		t.Fatalf("Render painted: %v", err)
	}

	if !strings.Contains(painted.String(), "\x1b[") {
		t.Fatal("painted render contains no escapes — the State column was not coloured")
	}

	stripped := ansiRE.ReplaceAllString(painted.String(), "")
	if stripped != plain.String() {
		t.Errorf("stripping colour did not reproduce the plain render:\n--- plain ---\n%s\n--- stripped ---\n%s",
			plain.String(), stripped)
	}
}

// A default (uncoloured) render must be free of escapes entirely: the
// harnesses that do NOT strip ANSI grep this output directly.
func TestPlainRenderHasNoEscapes(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := table.Render(&buf, resourceListTable(), table.Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	if strings.Contains(buf.String(), "\x1b") {
		t.Errorf("plain render leaked an escape sequence:\n%s", buf.String())
	}
}

// Only the state-ish columns are painted, and each cell is painted
// according to its own value — a broken replica is red on the same
// table where a healthy one is green.
func TestPaintsPerCellSemantics(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := table.Render(&buf, resourceListTable(), table.Options{
		Color:        color.New(true),
		StateColumns: []string{"State"},
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	healthy := findRow(t, buf.String(), "node-1")
	if !strings.Contains(healthy, "\x1b[32mUpToDate\x1b[0m") {
		t.Errorf("UpToDate not painted green: %q", healthy)
	}

	broken := findRow(t, buf.String(), "node-2")
	if !strings.Contains(broken, "\x1b[31mInconsistent\x1b[0m") {
		t.Errorf("Inconsistent not painted red: %q", broken)
	}

	// The node column is not a state column and must stay unpainted.
	if strings.Contains(healthy, "\x1b[32mnode-1") {
		t.Errorf("non-state column was painted: %q", healthy)
	}
}

// Column widths follow the widest cell, headers included, and cells are
// left-aligned with a single space of padding either side — the shape
// the parity tooling normalises border runs against.
func TestLayout(t *testing.T) {
	t.Parallel()

	tbl := &metav1.Table{
		ColumnDefinitions: []metav1.TableColumnDefinition{{Name: "A"}, {Name: "LongHeader"}},
		Rows: []metav1.TableRow{
			{Cells: []any{"a-very-long-cell-value", "x"}},
			{Cells: []any{"b", "y"}},
		},
	}

	var buf bytes.Buffer

	err := table.Render(&buf, tbl, table.Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) < 5 {
		t.Fatalf("want border/header/separator/2 rows, got %d lines:\n%s", len(lines), buf.String())
	}

	// Every line is the same visual width.
	width := len(lines[0])
	for i, line := range lines {
		if len(line) != width {
			t.Errorf("line %d width = %d, want %d:\n%s", i, len(line), width, buf.String())
		}
	}

	if !strings.HasPrefix(lines[0], "+") || !strings.Contains(lines[0], "-") {
		t.Errorf("top border = %q, want a +---+ rule", lines[0])
	}

	if !strings.Contains(lines[2], "=") {
		t.Errorf("header separator = %q, want an === rule", lines[2])
	}

	if !strings.Contains(lines[1], "| A ") {
		t.Errorf("header row = %q, want single-space padding", lines[1])
	}
}

// Cell values arrive from the Kubernetes Table API as typed `any`
// values; every one must render the way an operator expects, and a
// missing value must be the upstream's empty marker rather than Go's
// "&lt;nil&gt;".
func TestCellFormatting(t *testing.T) {
	t.Parallel()

	tbl := &metav1.Table{
		ColumnDefinitions: []metav1.TableColumnDefinition{
			{Name: "Str"}, {Name: "Int"}, {Name: "Bool"}, {Name: "Nil"}, {Name: "Slice"},
		},
		Rows: []metav1.TableRow{
			{Cells: []any{"s", int64(42), true, nil, []any{"DRBD", "STORAGE"}}},
		},
	}

	var buf bytes.Buffer

	err := table.Render(&buf, tbl, table.Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	row := findRow(t, buf.String(), "| s ")
	for _, want := range []string{"42", "True", "DRBD,STORAGE"} {
		if !strings.Contains(row, want) {
			t.Errorf("row %q missing %q", row, want)
		}
	}

	if strings.Contains(row, "<nil>") {
		t.Errorf("nil cell rendered as Go's <nil>: %q", row)
	}
}

// An empty result still prints its header, so a script that greps for a
// column name does not mistake "no rows" for "wrong command".
func TestEmptyTableStillPrintsHeader(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := table.Render(&buf, &metav1.Table{
		ColumnDefinitions: []metav1.TableColumnDefinition{{Name: "ResourceName"}, {Name: "State"}},
	}, table.Options{})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	if !strings.Contains(buf.String(), "ResourceName") {
		t.Errorf("empty table dropped its header:\n%s", buf.String())
	}
}

func findRow(t *testing.T, out, needle string) string {
	t.Helper()

	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}

	t.Fatalf("no row containing %q in:\n%s", needle, out)

	return ""
}
