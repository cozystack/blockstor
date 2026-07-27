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

// Package table renders a Kubernetes `metav1.Table` as the box-drawn
// text table operators expect from the storage CLI.
//
// `metav1.Table` is the single intermediate representation for every
// view: tables served by the API server (from the CRDs' printer
// columns) and tables assembled client-side from store DTOs both land
// here, so there is exactly one place where layout, padding and colour
// are decided.
//
// The layout is a contract, not a preference. Shell in this repository
// parses these tables with `awk -F'|'` at fixed field indexes, so a row
// begins with the separator and the column order is fixed by the view
// that built the table.
package table

import (
	"fmt"
	"io"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cozystack/blockstor/internal/cli/color"
)

// Options tunes a render.
type Options struct {
	// Color paints state cells. The zero value never paints.
	Color color.Writer

	// StateColumns names the columns whose cells carry a state string
	// and should be classified and painted. Matching is exact and
	// case-sensitive, mirroring the column names the views declare.
	StateColumns []string
}

// Render writes the table.
func Render(w io.Writer, tbl *metav1.Table, opts Options) error {
	if tbl == nil {
		return nil
	}

	headers := make([]string, 0, len(tbl.ColumnDefinitions))
	for i := range tbl.ColumnDefinitions {
		headers = append(headers, tbl.ColumnDefinitions[i].Name)
	}

	if len(headers) == 0 {
		return nil
	}

	// Cell text is computed once: widths are measured on the PLAIN
	// text and colour is applied afterwards, so escape bytes never
	// enter the width arithmetic and a painted table lines up exactly
	// like an unpainted one.
	rows := make([][]string, 0, len(tbl.Rows))
	for i := range tbl.Rows {
		rows = append(rows, formatRow(tbl.Rows[i].Cells, len(headers)))
	}

	widths := columnWidths(headers, rows)
	painted := opts.stateColumnSet()

	var buf strings.Builder

	buf.WriteString(rule(widths, '-'))
	buf.WriteString(line(headers, headers, widths, nil, opts.Color))
	buf.WriteString(rule(widths, '='))

	for _, row := range rows {
		buf.WriteString(line(row, headers, widths, painted, opts.Color))
	}

	buf.WriteString(rule(widths, '-'))

	_, err := io.WriteString(w, buf.String())
	if err != nil {
		return fmt.Errorf("write table: %w", err)
	}

	return nil
}

// stateColumnSet indexes the columns to paint by name.
func (o Options) stateColumnSet() map[string]struct{} {
	if len(o.StateColumns) == 0 || !o.Color.Enabled() {
		return nil
	}

	set := make(map[string]struct{}, len(o.StateColumns))
	for _, name := range o.StateColumns {
		set[name] = struct{}{}
	}

	return set
}

// formatRow renders each cell, padding the row out to the header count
// so a short row cannot shift the field positions the harnesses read.
func formatRow(cells []any, columns int) []string {
	out := make([]string, columns)

	for i := range out {
		if i < len(cells) {
			out[i] = formatCell(cells[i])
		}
	}

	return out
}

// formatCell renders one Table cell. The Kubernetes Table API delivers
// cells as untyped values, and the empty marker is a dash rather than
// Go's `<nil>`.
func formatCell(v any) string {
	switch value := v.(type) {
	case nil:
		return ""
	case string:
		return value
	case bool:
		if value {
			return "True"
		}

		return "False"
	case []any:
		parts := make([]string, 0, len(value))
		for _, item := range value {
			parts = append(parts, formatCell(item))
		}

		return strings.Join(parts, ",")
	case []string:
		return strings.Join(value, ",")
	default:
		return fmt.Sprintf("%v", value)
	}
}

// columnWidths measures the widest plain cell per column.
func columnWidths(headers []string, rows [][]string) []int {
	widths := make([]int, len(headers))

	for i, h := range headers {
		widths[i] = displayWidth(h)
	}

	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && displayWidth(cell) > widths[i] {
				widths[i] = displayWidth(cell)
			}
		}
	}

	return widths
}

// displayWidth counts runes, not bytes: a multi-byte name must not
// widen its column by its UTF-8 length.
func displayWidth(s string) int {
	return len([]rune(s))
}

// rule draws a horizontal border out of the given fill character.
func rule(widths []int, fill byte) string {
	var b strings.Builder

	b.WriteByte('+')

	for _, w := range widths {
		b.WriteString(strings.Repeat(string(fill), w+2))
		b.WriteByte('+')
	}

	b.WriteByte('\n')

	return b.String()
}

// line draws one row. Padding is computed from the PLAIN text and the
// colour is wrapped around the value only, so the escapes sit outside
// the measured region and a painted table lines up with an unpainted
// one byte for byte once the escapes are stripped.
func line(cells, headers []string, widths []int, painted map[string]struct{}, paint color.Writer) string {
	var b strings.Builder

	b.WriteByte('|')

	for i, cell := range cells {
		if i >= len(widths) {
			break
		}

		value := cell

		if painted != nil && i < len(headers) {
			if _, ok := painted[headers[i]]; ok {
				value = paint.PaintState(cell)
			}
		}

		b.WriteByte(' ')
		b.WriteString(value)
		b.WriteString(strings.Repeat(" ", widths[i]-displayWidth(cell)))
		b.WriteString(" |")
	}

	b.WriteByte('\n')

	return b.String()
}
