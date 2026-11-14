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

package linstormigrate

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrBadPortLine is returned when a DRBD port-map line is malformed.
var ErrBadPortLine = errors.New("malformed DRBD port line")

// ErrConflictingPort is returned when a DRBD port map gives one
// definition, or one replica, two different ports.
var ErrConflictingPort = errors.New("conflicting DRBD port")

// maxTCPPort is the inclusive upper bound of a valid TCP port.
const maxTCPPort = 65535

// DRBDPortMap is a live DRBD port map. LINSTOR 1.33 can give each node
// its own port for one definition, so a port is either a definition's,
// shared by every replica, or one replica's.
type DRBDPortMap struct {
	// ByDefinition maps a definition name (lowercased) to the port
	// every replica of it listens on.
	ByDefinition map[string]int32
	// ByReplica maps a definition name and a node name (both
	// lowercased) to the port that replica listens on.
	ByReplica map[string]map[string]int32
}

// ParseDRBDPorts parses a live DRBD port map. A line is either
// `<rd-name> <node> <port>`, the port one replica listens on, or
// `<rd-name> <port>`, a port every replica of the definition shares.
// Fields are whitespace-separated, blank lines and `#` comments are
// ignored, and names are lowercased so lookups are case-insensitive
// against LINSTOR's uppercase keys.
//
// Capture the per-replica form on every storage node, from the
// kernel's own view (see the migration runbook):
//
//	drbdsetup show | awk -v node="$(hostname)" '
//	  /^resource / { gsub(/"/, "", $2); rd = $2 }
//	  /_this_host ipv4/ && rd != "" { split($3, a, ":"); sub(/;$/, "", a[2]); print rd, node, a[2]; rd = "" }'
//
// One definition given two ports, one replica given two ports, or a
// definition given in both forms is refused: on a cluster whose nodes
// use different ports for one definition, a definition-wide map built
// by concatenating the nodes' lists is exactly that, and keeping one
// of the values would hand a replica another node's port.
func ParseDRBDPorts(content string) (DRBDPortMap, error) {
	ports := DRBDPortMap{ByDefinition: map[string]int32{}, ByReplica: map[string]map[string]int32{}}

	for i, raw := range strings.Split(content, "\n") {
		text := strings.TrimSpace(raw)
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}

		err := ports.add(strings.Fields(text), i+1, text)
		if err != nil {
			return DRBDPortMap{}, err
		}
	}

	return ports, nil
}

func (m DRBDPortMap) add(fields []string, line int, text string) error {
	if len(fields) != 2 && len(fields) != 3 {
		return fmt.Errorf("%w at line %d: want '<rd-name> <node> <port>' or '<rd-name> <port>', got %q", ErrBadPortLine, line, text)
	}

	raw := fields[len(fields)-1]

	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || parsed <= 0 || parsed > maxTCPPort {
		return fmt.Errorf("%w at line %d: invalid port %q", ErrBadPortLine, line, raw)
	}

	port := int32(parsed)

	rd := strings.ToLower(fields[0])

	if len(fields) == 2 {
		if _, perReplica := m.ByReplica[rd]; perReplica {
			return fmt.Errorf("%w at line %d: %s has per-replica ports, and a definition-wide one too", ErrConflictingPort, line, fields[0])
		}

		if prev, ok := m.ByDefinition[rd]; ok && prev != port {
			return fmt.Errorf("%w at line %d: %s listens on %d and %d; give each replica its own line, '<rd-name> <node> <port>'",
				ErrConflictingPort, line, fields[0], prev, port)
		}

		m.ByDefinition[rd] = port

		return nil
	}

	if _, shared := m.ByDefinition[rd]; shared {
		return fmt.Errorf("%w at line %d: %s has a definition-wide port, and per-replica ones too", ErrConflictingPort, line, fields[0])
	}

	node := strings.ToLower(fields[1])

	if m.ByReplica[rd] == nil {
		m.ByReplica[rd] = map[string]int32{}
	}

	if prev, ok := m.ByReplica[rd][node]; ok && prev != port {
		return fmt.Errorf("%w at line %d: the replica of %s on %s listens on %d and %d", ErrConflictingPort, line, fields[0], fields[1], prev, port)
	}

	m.ByReplica[rd][node] = port

	return nil
}
