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

package drbd

import (
	"encoding/xml"
	"math/bits"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// ErrOptionRejected is the class of a disk option this node's drbd-utils
// would refuse to parse, by name or by value.
var ErrOptionRejected = errors.New("drbd-utils rejects option")

// DiskSectionCommands are the drbdsetup commands whose xml-help together
// describe what drbdadm accepts inside a `disk { }` section: the disk
// options proper, the device options (block-size, discard-granularity,
// applied at new-minor) and the peer-device options, which drbdadm's
// parser routes out of the disk section on its own
// (insert_pd_options_delegate in drbdadm_parser.c).
func DiskSectionCommands() []string {
	return []string{"disk-options", "new-minor", "peer-device-options"}
}

// SectionCommands are the drbdsetup commands whose xml-help together
// describe what drbdadm accepts inside one `.res` section, the ones
// LINSTOR's satellite builds its whitelist from
// (WhitelistPropsReconfigurator): new-peer for net, resource-options for
// the resource options block, and DiskSectionCommands for a disk section,
// which also holds the peer-device options. A section xml-help does not
// describe (handlers) has none.
func SectionCommands(section string) []string {
	switch section {
	case SectionNet:
		return []string{"new-peer"}
	case SectionOptions:
		return []string{"resource-options"}
	case SectionDisk, SectionPeerDevice:
		return DiskSectionCommands()
	default:
		return nil
	}
}

// HandlerNames are the handlers a `handlers { }` section may name, the
// list LINSTOR whitelists (WhitelistProps.overrideDrbdProperties). drbdsetup
// has no xml-help for them: drbdadm alone runs them.
func HandlerNames() []string {
	return []string{
		"after-resync-target",
		"before-resync-target",
		"before-resync-source",
		"out-of-sync",
		"quorum-lost",
		"fence-peer",
		"unfence-peer",
		"initial-split-brain",
		"local-io-error",
		"pri-lost",
		"pri-lost-after-sb",
		"pri-on-incon-degr",
		"split-brain",
	}
}

// Option names the schema treats specially; see notInResFile and
// valueUnchecked.
const (
	optSetDefaults = "set-defaults"
	optALExtents   = "al-extents"
	optResyncAfter = "resync-after"
)

// Shifts new_strtoll applies for a unit: KiB and 512-byte sectors.
const (
	shiftKiB    = 10
	shiftMiB    = 20
	shiftGiB    = 30
	shiftSector = 9
)

// OptionSchema is what one node's drbd-utils accepts for a set of
// options, read from `drbdsetup xml-help <command>`. It is the list
// LINSTOR's satellite builds its whitelist from when it starts, and it
// lets the satellite drop an option this drbd-utils would refuse
// instead of writing a .res that drbdadm cannot parse at all.
//
// A nil schema accepts everything: a node whose drbdsetup could not be
// asked renders what it rendered before.
type OptionSchema struct {
	options map[string]optionSpec
}

// optionSpec is one <option> of xml-help.
type optionSpec struct {
	Name       string   `xml:"name,attr"`
	Type       string   `xml:"type,attr"`
	Min        string   `xml:"min"`
	Max        string   `xml:"max"`
	UnitPrefix string   `xml:"unit_prefix"`
	Handlers   []string `xml:"handler"`
	Symbols    []string `xml:"symbol"`
}

type xmlHelpCommand struct {
	Options []optionSpec `xml:"option"`
}

// notInResFile says whether xml-help lists name as a command-line
// switch, not configuration: drbdsetup prints set-defaults for every
// command that takes it, and drbdadm's parser knows no such keyword.
func notInResFile(name string) bool {
	return name == optSetDefaults
}

// valueUnchecked says whether drbdadm leaves the option's value unchecked
// while it parses, so a value outside what xml-help states is no reason
// to drop it. al-extents is clamped to its range silently
// (implicit_clamp), and resync-after names a resource and volume and is
// checked only after the whole configuration is read
// (checked_in_postparse), both in config_flags.c.
func valueUnchecked(name string) bool {
	return name == optALExtents || name == optResyncAfter
}

// ParseOptionSchema reads the output of one or more `drbdsetup xml-help`
// runs, each a <command> element, into one schema.
func ParseOptionSchema(docs ...[]byte) (*OptionSchema, error) {
	schema := &OptionSchema{options: map[string]optionSpec{}}

	for _, doc := range docs {
		var cmd xmlHelpCommand

		err := xml.Unmarshal(doc, &cmd)
		if err != nil {
			return nil, errors.Wrap(err, "parse drbdsetup xml-help")
		}

		for i := range cmd.Options {
			opt := &cmd.Options[i]
			if opt.Name == "" || notInResFile(opt.Name) {
				continue
			}

			schema.options[opt.Name] = *opt
		}
	}

	if len(schema.options) == 0 {
		return nil, errors.New("drbdsetup xml-help lists no options")
	}

	return schema, nil
}

// Check reports whether drbdadm would accept `name value;` in a disk
// section, following the checks drbd-utils runs while it parses
// (the field classes' check functions in config_flags.c). A value it
// cannot judge is accepted: the schema is there to drop what drbdadm
// would certainly refuse, not to second-guess what it would take.
func (s *OptionSchema) Check(name, value string) error {
	if s == nil {
		return nil
	}

	spec, ok := s.options[name]
	if !ok {
		return errors.Wrapf(ErrOptionRejected, "%s is not an option this drbd-utils knows", name)
	}

	if valueUnchecked(name) || spec.valueOK(value) {
		return nil
	}

	return errors.Wrapf(ErrOptionRejected, "%s does not take %q", name, value)
}

// valueOK applies the check of the option's field class.
func (o *optionSpec) valueOK(value string) bool {
	switch o.Type {
	case "boolean":
		return value == wireYes || value == wireNo
	case "handler":
		// drbdsetup prints case-sensitive and case-insensitive enums
		// alike, so the looser comparison keeps both.
		return slices.ContainsFunc(o.Handlers, func(h string) bool { return strings.EqualFold(h, value) })
	case "numeric":
		return o.numericOK(value, o.unit())
	case "numeric-or-symbol":
		if slices.Contains(o.Symbols, value) {
			return true
		}

		return o.numericOK(value, '1')
	default:
		// string, flag, and any type a later drbd-utils adds.
		return true
	}
}

// unit is the option's default unit, '1' when xml-help gives none.
func (o *optionSpec) unit() byte {
	if o.UnitPrefix == "" {
		return '1'
	}

	return o.UnitPrefix[0]
}

// numericOK parses value as drbd-utils does and checks it against the
// option's range. A range xml-help does not state is not checked.
func (o *optionSpec) numericOK(value string, unit byte) bool {
	n, ok := parseDrbdNumber(value, unit)
	if !ok {
		return false
	}

	lo, err := strconv.ParseInt(o.Min, 10, 64)
	if err == nil && lo > 0 && n < uint64(lo) {
		return false
	}

	hi, err := strconv.ParseInt(o.Max, 10, 64)
	if err == nil && hi >= 0 && n > uint64(hi) {
		return false
	}

	return true
}

// parseDrbdNumber is new_strtoll from drbd-utils' shared_tool.c: an
// unsigned decimal with an optional K, M, G or s (sectors) suffix,
// converted into the option's default unit, rounding up when the value
// is given in a finer unit than the option counts in.
func parseDrbdNumber(value string, unit byte) (uint64, bool) {
	from, ok := unitShift(unit)
	if !ok {
		return 0, false
	}

	digits := strings.TrimRight(value, "KkMmGgs")
	suffix := value[len(digits):]

	if digits == "" || len(suffix) > 1 {
		return 0, false
	}

	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}

	if suffix == "" {
		return n, true
	}

	to, _ := unitShift(suffix[0])
	shift := to - from

	if shift < 0 {
		return (n + (1 << -shift) - 1) >> -shift, true
	}

	if bits.LeadingZeros64(n) < shift {
		return 0, false
	}

	return n << shift, true
}

// unitShift is the power of two a unit letter stands for, as
// new_strtoll reads it. A default unit is only ever 1, K or s; a
// suffix only ever K, M, G or s, which TrimRight already guarantees.
func unitShift(unit byte) (int, bool) {
	switch unit {
	case '1', 0:
		return 0, true
	case 'K', 'k':
		return shiftKiB, true
	case 'M', 'm':
		return shiftMiB, true
	case 'G', 'g':
		return shiftGiB, true
	case 's':
		return shiftSector, true
	default:
		return 0, false
	}
}
