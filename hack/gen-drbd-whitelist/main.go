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

// Command gen-drbd-whitelist writes pkg/propwhitelist/drbdoptions.json, the
// DrbdOptions part of LINSTOR's property whitelist, from a checkout of
// linstor-common: the submodule of linstor-server that LINSTOR's own build
// generates GeneratedPropertyRules.java from.
//
// Usage:
//
//	go run ./hack/gen-drbd-whitelist <linstor-common dir> <source label> > pkg/propwhitelist/drbdoptions.json
//
// Only keys under DrbdOptions/ are kept, and only for the objects blockstor
// serves; the drbd-proxy objects are left out because blockstor has no proxy.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

const drbdPrefix = "DrbdOptions/"

// objects are the LINSTOR whitelist objects the output carries.
//
//nolint:gochecknoglobals // static list of the objects kept
var objects = []string{
	"controller",
	"node",
	"node-conn",
	"resource",
	"resource-definition",
	"rsc-conn",
	"storagepool",
	"storagepool-definition",
	"volume",
	"volume-definition",
}

// rule is one property rule, the fields of linstor-common's JSON that decide
// whether a value is valid. Key is a string in drbdoptions.json and a list of
// consts names in properties.json, so it is decoded separately.
type rule struct {
	Type   string   `json:"type"`
	Values []string `json:"values,omitempty"`
	Min    *bound   `json:"min,omitempty"`
	Max    *bound   `json:"max,omitempty"`
	Unit   string   `json:"unit,omitempty"`
	Regex  string   `json:"value,omitempty"`
}

// bound is a range limit. linstor-common writes most as numbers and a few
// as strings; both decode to the number, and it is written as a number.
type bound int64

// UnmarshalJSON accepts the number and the string form.
func (b *bound) UnmarshalJSON(raw []byte) error {
	text := strings.Trim(string(raw), `"`)

	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("parse bound %s: %w", raw, err)
	}

	*b = bound(n)

	return nil
}

type sourceFile struct {
	Properties map[string]json.RawMessage `json:"properties"`
	Objects    map[string][]string        `json:"objects"`
}

type output struct {
	Source  string              `json:"source"`
	Rules   map[string]rule     `json:"rules"`
	Objects map[string][]string `json:"objects"`
}

var errUsage = errors.New("usage: gen-drbd-whitelist <linstor-common dir> <source label>")

func main() {
	err := run(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 2 {
		return errUsage
	}

	dir := args[0]

	consts, err := readConsts(filepath.Join(dir, "consts.json"))
	if err != nil {
		return err
	}

	out := output{Source: args[1], Rules: map[string]rule{}, Objects: map[string][]string{}}

	for _, name := range []string{"properties.json", "drbdoptions.json"} {
		err = merge(&out, filepath.Join(dir, name), consts)
		if err != nil {
			return err
		}
	}

	for obj, keys := range out.Objects {
		slices.Sort(keys)
		out.Objects[obj] = slices.Compact(keys)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	err = enc.Encode(out)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}

// readConsts maps every string constant of consts.json to its value, which
// is what properties.json spells a key with.
func readConsts(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // a path the operator passes
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var entries []struct {
		Name  string `json:"name"`
		Value any    `json:"value"`
	}

	err = json.Unmarshal(raw, &entries)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	consts := map[string]string{}

	for _, entry := range entries {
		if s, ok := entry.Value.(string); ok {
			consts[entry.Name] = s
		}
	}

	return consts, nil
}

func merge(out *output, path string, consts map[string]string) error {
	raw, err := os.ReadFile(path) //nolint:gosec // a path the operator passes
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	var src sourceFile

	err = json.Unmarshal(raw, &src)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}

	for _, obj := range objects {
		for _, name := range src.Objects[obj] {
			key, propRule, err := decodeProperty(src.Properties[name], consts)
			if err != nil {
				return fmt.Errorf("%s: property %s: %w", path, name, err)
			}

			if !strings.HasPrefix(key, drbdPrefix) {
				continue
			}

			if seen, ok := out.Rules[key]; ok && !reflect.DeepEqual(seen, propRule) {
				return fmt.Errorf("%s: %w: %s", path, errTwoRules, key)
			}

			out.Rules[key] = propRule
			out.Objects[obj] = append(out.Objects[obj], key)
		}
	}

	return nil
}

var (
	errNoProperty = errors.New("listed for an object but not defined")
	errTwoRules   = errors.New("two different rules for one key")
)

func decodeProperty(raw json.RawMessage, consts map[string]string) (string, rule, error) {
	if raw == nil {
		return "", rule{}, errNoProperty
	}

	var withKey struct {
		Key json.RawMessage `json:"key"`
	}

	err := json.Unmarshal(raw, &withKey)
	if err != nil {
		return "", rule{}, fmt.Errorf("parse: %w", err)
	}

	key, err := decodeKey(withKey.Key, consts)
	if err != nil {
		return "", rule{}, err
	}

	var propRule rule

	err = json.Unmarshal(raw, &propRule)
	if err != nil {
		return "", rule{}, fmt.Errorf("parse rule: %w", err)
	}

	// LINSTOR's generator sorts the symbols (genproperties.py), and the
	// error message shows them in that order.
	slices.Sort(propRule.Values)

	return key, propRule, nil
}

var errUnknownConst = errors.New("unknown constant")

// decodeKey reads a key spelled either literally or as a list of consts
// names joined with "/", the two forms linstor-common uses.
func decodeKey(raw json.RawMessage, consts map[string]string) (string, error) {
	var literal string

	err := json.Unmarshal(raw, &literal)
	if err == nil {
		return literal, nil
	}

	var parts []string

	err = json.Unmarshal(raw, &parts)
	if err != nil {
		return "", fmt.Errorf("parse key: %w", err)
	}

	resolved := make([]string, 0, len(parts))

	for _, part := range parts {
		value, ok := consts[part]
		if !ok {
			return "", fmt.Errorf("%w: %s", errUnknownConst, part)
		}

		resolved = append(resolved, value)
	}

	return strings.Join(resolved, "/"), nil
}
