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

// Package propwhitelist holds the DrbdOptions part of LINSTOR's property
// whitelist and checks a property against it.
//
// LINSTOR refuses to store a property its whitelist does not list for the
// object, and a listed one whose value its rule refuses (CtrlPropsHelper.
// fillProperties, WhitelistProps.isAllowed). blockstor used to store any
// DrbdOptions key verbatim. The satellite then wrote it into the .res, and
// drbdadm refused the whole file: a typo such as DrbdOptions/AutoSnapshot/Keep
// on a definition left every replica of it unable to apply anything until the
// key was deleted, and the mistake showed only as Warning events on the nodes.
//
// The rules are LINSTOR's own, generated from the linstor-common checkout
// linstor-server v1.33.1 builds from, by hack/gen-drbd-whitelist. Only keys
// under DrbdOptions/ are checked; every other namespace is left as it was.
package propwhitelist

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// Prefix is the namespace this package judges. A key outside it is never
// refused here.
const Prefix = "DrbdOptions/"

// Object names a LINSTOR whitelist object: which keys an object of that
// kind accepts. The values are linstor-common's own names.
type Object string

// The objects blockstor serves properties on. A resource group is judged as
// a resource definition and a volume group as a volume definition, as
// LINSTOR does (CtrlRscGrpApiCallHandler, CtrlVlmGrpApiCallHandler).
const (
	Controller            Object = "controller"
	Node                  Object = "node"
	NodeConnection        Object = "node-conn"
	Resource              Object = "resource"
	ResourceDefinition    Object = "resource-definition"
	ResourceConnection    Object = "rsc-conn"
	StoragePool           Object = "storagepool"
	StoragePoolDefinition Object = "storagepool-definition"
	Volume                Object = "volume"
	VolumeDefinition      Object = "volume-definition"
)

//go:embed drbdoptions.json
var catalogueJSON []byte

// Property rule types, as linstor-common names them.
const (
	typeBoolean          = "boolean"
	typeBooleanTrueFalse = "boolean_true_false"
	typeRange            = "range"
	typeLong             = "long"
	typeSymbol           = "symbol"
	typeNumericOrSymbol  = "numeric-or-symbol"
	typeString           = "string"
	typeRegex            = "regex"
)

// rule is one key's rule, the fields linstor-common gives it.
type rule struct {
	Type   string   `json:"type"`
	Values []string `json:"values,omitempty"`
	Min    *int64   `json:"min,omitempty"`
	Max    *int64   `json:"max,omitempty"`
	Unit   string   `json:"unit,omitempty"`
	Regex  string   `json:"value,omitempty"`
}

type catalogue struct {
	Source  string              `json:"source"`
	Rules   map[string]rule     `json:"rules"`
	Objects map[string][]string `json:"objects"`
}

//nolint:gochecknoglobals // the embedded catalogue, parsed once
var loaded = sync.OnceValue(func() *catalogue {
	var cat catalogue

	err := json.Unmarshal(catalogueJSON, &cat)
	if err != nil {
		// The file is generated and embedded at build time; a parse
		// failure is a broken build, which the tests catch.
		panic(fmt.Sprintf("propwhitelist: embedded catalogue: %v", err))
	}

	return &cat
})

// Error is a property LINSTOR would refuse. Known says whether the key is on
// the object's whitelist, so the refusal is about the value; otherwise the
// key itself is not whitelisted.
type Error struct {
	Key   string
	Value string
	Known bool
	// Detail is LINSTOR's description of what the rule accepts, set when
	// Known.
	Detail string
}

// Error implements error.
func (e *Error) Error() string {
	if e.Known {
		return fmt.Sprintf("the value '%s' is not valid for the key '%s': %s", e.Value, e.Key, e.Detail)
	}

	return fmt.Sprintf("the key '%s' is not whitelisted", e.Key)
}

// Check reports whether LINSTOR would store key=value on obj. A key outside
// DrbdOptions/ is always accepted; so is any key on an object kind the
// catalogue does not know.
func Check(obj Object, key, value string) error {
	if !strings.HasPrefix(key, Prefix) {
		return nil
	}

	cat := loaded()

	keys, ok := cat.Objects[string(obj)]
	if !ok && !knownObject(obj) {
		return nil
	}

	keyRule, ok := ruleFor(cat, keys, obj, key)
	if !ok {
		return &Error{Key: key, Value: value}
	}

	if keyRule.valid(value) {
		return nil
	}

	return &Error{Key: key, Value: value, Known: true, Detail: keyRule.errorMsg()}
}

// ruleFor finds key's rule on obj: LINSTOR's when its whitelist lists the
// key for the object, otherwise blockstor's own when it reads the key there.
func ruleFor(cat *catalogue, keys []string, obj Object, key string) (rule, bool) {
	if slices.Contains(keys, key) {
		return cat.Rules[key], true
	}

	return ownRuleFor(obj, key)
}

// CheckAll checks every key=value of props on obj, in key order, and returns
// the first refusal, the one LINSTOR's request would fail on.
func CheckAll(obj Object, props map[string]string) error {
	keys := make([]string, 0, len(props))

	for key := range props {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	for _, key := range keys {
		err := Check(obj, key, props[key])
		if err != nil {
			return err
		}
	}

	return nil
}

// knownObject says whether obj is one of the objects this package names.
// An object no key is listed for (a storage-pool definition) still refuses
// every DrbdOptions key, as LINSTOR's empty whitelist for it does.
func knownObject(obj Object) bool {
	switch obj {
	case Controller, Node, NodeConnection, Resource, ResourceDefinition, ResourceConnection,
		StoragePool, StoragePoolDefinition, Volume, VolumeDefinition:
		return true
	}

	return false
}

var (
	booleanPattern          = regexp.MustCompile(`^(?i)(?:yes|no)$`)
	booleanTrueFalsePattern = regexp.MustCompile(`^(?i)(?:true|false|yes|no)$`)
)

// valid applies the rule's check, the one the matching LINSTOR Property class
// runs (BooleanProperty, RangeProperty, NumericOrSymbolProperty, ...).
func (r *rule) valid(value string) bool {
	switch r.Type {
	case typeBoolean:
		return booleanPattern.MatchString(value)
	case typeBooleanTrueFalse:
		return booleanTrueFalsePattern.MatchString(value)
	case typeRange:
		return r.inRange(value)
	case typeLong:
		_, err := strconv.ParseInt(value, 10, 64)

		return err == nil
	case typeSymbol:
		return slices.Contains(r.Values, value)
	case typeNumericOrSymbol:
		_, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return r.inRange(value)
		}

		return slices.Contains(r.Values, value)
	case typeRegex:
		matched, err := regexp.MatchString(`^(?:`+r.Regex+`)$`, value)

		return err == nil && matched
	default:
		// string, and any type a later catalogue adds: LINSTOR's
		// StringProperty accepts every value.
		return true
	}
}

func (r *rule) inRange(value string) bool {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false
	}

	if r.Min != nil && n < *r.Min {
		return false
	}

	return r.Max == nil || n <= *r.Max
}

// errorMsg is the rule's description, as LINSTOR's getErrorMsg words it.
func (r *rule) errorMsg() string {
	switch r.Type {
	case typeBoolean:
		return "This value must be either 'yes' or 'no'."
	case typeBooleanTrueFalse:
		return "This value must be either 'yes', 'no', 'true' or 'false'."
	case typeLong:
		return r.withUnit("This value has to be of type Long.")
	case typeRange:
		return r.hasToMatch(r.rangeText())
	case typeSymbol:
		return r.hasToMatch(r.symbolsText())
	case typeNumericOrSymbol:
		return r.hasToMatch(r.rangeText() + " or " + r.symbolsText())
	case typeRegex:
		return r.hasToMatch(r.Regex)
	case typeString:
		return ""
	default:
		return ""
	}
}

func (r *rule) hasToMatch(what string) string {
	if r.Unit == "" {
		return "This value has to match " + what + "."
	}

	// LINSTOR's own wording, double space included.
	return "This value  has to match " + what + " " + r.Unit + "."
}

func (r *rule) withUnit(msg string) string {
	if r.Unit == "" {
		return msg
	}

	return "This value contains " + r.Unit + " and has to be of type Long."
}

func (r *rule) rangeText() string {
	var low, high int64

	if r.Min != nil {
		low = *r.Min
	}

	if r.Max != nil {
		high = *r.Max
	}

	return fmt.Sprintf("(%d - %d)", low, high)
}

func (r *rule) symbolsText() string {
	return "(?:" + strings.Join(r.Values, "|") + ")"
}
