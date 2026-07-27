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

// maxTCPPort is the inclusive upper bound of a valid TCP port.
const maxTCPPort = 65535

// ParseDRBDPorts parses a live DRBD port map: one `<rd-name> <port>`
// per line (whitespace-separated), blank lines and `#` comments
// ignored. Keys are lowercased so lookups are case-insensitive against
// LINSTOR's uppercase resource names. Capture it from the running
// cluster before cutover so adoption preserves each mesh's endpoint —
// e.g. per satellite pod:
//
//	for f in /var/lib/linstor.d/*.res; do
//	  rd=$(basename "$f" .res)
//	  port=$(grep -oE 'address[^;]*:[0-9]+' "$f" | grep -oE '[0-9]+$' | head -1)
//	  [ -n "$port" ] && echo "$rd $port"
//	done
//
// (union across nodes; every replica of an RD shares one port).
func ParseDRBDPorts(content string) (map[string]int32, error) {
	ports := map[string]int32{}

	for i, raw := range strings.Split(content, "\n") {
		text := strings.TrimSpace(raw)
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}

		fields := strings.Fields(text)
		if len(fields) != 2 {
			return nil, fmt.Errorf("%w at line %d: want '<rd-name> <port>', got %q", ErrBadPortLine, i+1, text)
		}

		port, err := strconv.ParseInt(fields[1], 10, 32)
		if err != nil || port <= 0 || port > maxTCPPort {
			return nil, fmt.Errorf("%w at line %d: invalid port %q", ErrBadPortLine, i+1, fields[1])
		}

		ports[strings.ToLower(fields[0])] = int32(port)
	}

	return ports, nil
}
