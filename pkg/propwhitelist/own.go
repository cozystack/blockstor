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

package propwhitelist

import "slices"

// ownRule is a DrbdOptions key blockstor itself reads although LINSTOR's
// whitelist does not list it, with the objects it is read from.
type ownRule struct {
	objects []Object
	rule    rule
}

// ownRules are blockstor's own DrbdOptions keys. They sit outside the
// generated catalogue, so regenerating it cannot drop them, and each is
// accepted only on the objects its reader looks at.
//
// They are documented as the way to set what they set, the GET output of a
// definition carries AutoAddQuorumTiebreaker back to clients that write the
// props they read, and the e2e suites set it through REST. Refusing them
// would turn every one of those writes into a 400.
//
//nolint:gochecknoglobals // a fixed table, read only
var ownRules = map[string]ownRule{
	// The per-definition and per-group switch for the auto tiebreaker
	// (isAutoTieBreakerEnabled, transcoded to spec.drbdOptions.resource.
	// autoTieBreaker). A group is judged as a definition.
	"DrbdOptions/AutoAddQuorumTiebreaker": {
		objects: []Object{Controller, ResourceDefinition},
		rule:    rule{Type: typeBooleanTrueFalse},
	},
	// The per-node DRBD TCP port and minor ranges (portRangeForNode,
	// intersectMinorRange), written as min-max as drbd.ParseRange reads
	// them.
	"DrbdOptions/TcpPortRange": {
		objects: []Object{Node},
		rule:    rule{Type: typeRegex, Regex: `[0-9]+-[0-9]+`},
	},
	"DrbdOptions/MinorNrRange": {
		objects: []Object{Node},
		rule:    rule{Type: typeRegex, Regex: `[0-9]+-[0-9]+`},
	},
}

// ownRuleFor returns the rule of one of blockstor's own keys on obj.
func ownRuleFor(obj Object, key string) (rule, bool) {
	own, ok := ownRules[key]
	if !ok || !slices.Contains(own.objects, obj) {
		return rule{}, false
	}

	return own.rule, true
}
