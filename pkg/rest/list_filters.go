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

package rest

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/cozystack/blockstor/pkg/naming"
)

// regexMetaChars are the characters that make LINSTOR read a filter as a
// regular expression rather than as a plain name (RegexMatcher.isRegex). A
// dot is not one of them: names may contain dots, and `a.b` stays the name.
const regexMetaChars = `\+*?[](){}^$|`

// isFilterRegex reports whether LINSTOR reads filter as a regular expression.
func isFilterRegex(filter string) bool {
	return strings.ContainsAny(filter, regexMetaChars)
}

// compileFilter compiles a filter LINSTOR reads as a regular expression,
// anchored at both ends: Java's Matcher.matches() matches the whole input.
// A filter that is not a valid expression is answered as nil, which the
// callers read as a filter that matches nothing but its literal self.
func compileFilter(filter string, caseInsensitive bool) *regexp.Regexp {
	flags := ""
	if caseInsensitive {
		flags = "(?i)"
	}

	re, err := regexp.Compile(flags + `^(?:` + filter + `)$`)
	if err != nil {
		return nil
	}

	return re
}

// nameMatches reports whether candidate is matched by one name filter: a
// regular expression matched case-insensitively against the whole name, as
// LINSTOR does since REST 1.28, or a plain name compared the way the store
// keys names.
func nameMatches(filter, candidate string) bool {
	if isFilterRegex(filter) {
		if re := compileFilter(filter, true); re != nil {
			return re.MatchString(candidate)
		}
	}

	return naming.Same(filter, candidate)
}

// propsQuery returns the `props` filters of a list request. They are taken
// as sent, one per repeated key, and never split on commas the way the name
// filters are: a property value, and a regular expression, may hold one.
func propsQuery(r *http.Request) []string {
	return r.URL.Query()["props"]
}

// matchProps reports whether props satisfies every filter, LINSTOR's
// ReadOnlyProps.contains: a filter is a key, which matches when the
// property is set, or `key=value`, which matches when it has that value.
// Either part may be a regular expression matched against the whole key or
// value, case-sensitively; a regular expression key matches when any
// property whose key it matches also satisfies the value part.
func matchProps(filters []string, props map[string]string) bool {
	for _, filter := range filters {
		if !matchPropFilter(filter, props) {
			return false
		}
	}

	return true
}

func matchPropFilter(filter string, props map[string]string) bool {
	keyFilter, valueFilter, hasValue := strings.Cut(filter, "=")

	valueOK := func(value string) bool {
		switch {
		case !hasValue:
			return true
		case isFilterRegex(valueFilter):
			if re := compileFilter(valueFilter, false); re != nil {
				return re.MatchString(value)
			}

			return value == valueFilter
		default:
			return value == valueFilter
		}
	}

	if isFilterRegex(keyFilter) {
		if re := compileFilter(keyFilter, false); re != nil {
			for key, value := range props {
				if re.MatchString(key) && valueOK(value) {
					return true
				}
			}

			return false
		}
	}

	value, ok := props[keyFilter]

	return ok && valueOK(value)
}
