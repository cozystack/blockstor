#!/usr/bin/env bash
#
# state.sh — cluster-state cache for the fuzzer.
#
# Generates a single JSON document per settle cycle holding everything
# the verb generators need to evaluate preconditions:
#
#   {
#     "rds": [{"name": "fuzz-1-aa", "has_vd": true,
#              "replicas": [{"node": "w1", "diskful": true,
#                            "diskstate": "UpToDate"}]}],
#     "nodes": ["w1", "w2", "w3"],
#     "sps": [{"node": "w1", "name": "stand"}, ...],
#     "snaps": [{"rd": "fuzz-1-aa", "name": "fuzz-1-aa-snap-0"}],
#     "rgs":   [{"name": "DfltRscGrp", "place_count": 2}]
#   }
#
# Sourced by operator-fuzz.sh and all verb files. Caller must have
# already sourced lib.sh.
#
# Two entry points:
#
#   refresh_cluster_state         re-runs `linstor ... -o json` once and
#                                 fills $CLUSTER_STATE_JSON (a string)
#   cluster_state                 echoes the current cached JSON

CLUSTER_STATE_JSON=""

# Refresh — collect everything in one Python pass so a single state
# refresh does at most 4 CLI calls.
refresh_cluster_state() {
    local rd_json sp_json n_json snap_json rg_json
    rd_json=$(linstor_cli --output-fmt=json resource list 2>/dev/null || echo "[]")
    sp_json=$(linstor_cli --output-fmt=json storage-pool list 2>/dev/null || echo "[]")
    n_json=$(linstor_cli --output-fmt=json node list 2>/dev/null || echo "[]")
    snap_json=$(linstor_cli --output-fmt=json snapshot list 2>/dev/null || echo "[]")
    rg_json=$(linstor_cli --output-fmt=json resource-group list 2>/dev/null || echo "[]")

    CLUSTER_STATE_JSON=$(python3 - "$rd_json" "$sp_json" "$n_json" "$snap_json" "$rg_json" <<'EOF'
import json, sys

def unwrap(s):
    try:
        d = json.loads(s)
    except Exception:
        return []
    while isinstance(d, list) and d and isinstance(d[0], list):
        d = d[0]
    return d if isinstance(d, list) else []

resources  = unwrap(sys.argv[1])
sps        = unwrap(sys.argv[2])
nodes_raw  = unwrap(sys.argv[3])
snaps      = unwrap(sys.argv[4])
rgs        = unwrap(sys.argv[5])

# Build RD-keyed view from `resource list` rows.
rds = {}
for r in resources:
    name = r.get('name') or r.get('resource_name') or r.get('rsc_name')
    node = r.get('node_name') or r.get('nodeName')
    if not name or not node:
        continue
    rd = rds.setdefault(name, {'name': name, 'has_vd': False, 'replicas': []})
    vols = r.get('volumes') or []
    has_vd = len(vols) > 0
    if has_vd:
        rd['has_vd'] = True
    diskstate = ''
    diskful = True
    if vols:
        diskstate = (vols[0].get('state') or {}).get('disk_state', '') if isinstance(vols[0].get('state'), dict) else vols[0].get('disk_state', '')
    flags = r.get('flags') or []
    if 'DISKLESS' in flags or 'TIE_BREAKER' in flags:
        diskful = False
    rd['replicas'].append({
        'node': node,
        'diskful': diskful,
        'diskstate': diskstate,
        'flags': flags,
    })

# Storage pools per node.
sp_list = []
for sp in sps:
    sp_list.append({
        'node': sp.get('node_name') or sp.get('nodeName') or '',
        'name': sp.get('storage_pool_name') or sp.get('name') or '',
        'provider': sp.get('provider_kind') or sp.get('providerKind') or '',
    })

# Online nodes.
nodes = []
for n in nodes_raw:
    if (n.get('connection_status') or n.get('connectionStatus') or '').upper() != 'OFFLINE':
        nodes.append(n.get('name') or '')
nodes = [x for x in nodes if x]

# Snapshots flat list.
snap_list = []
for s in snaps:
    snap_list.append({
        'rd': s.get('resource_name') or s.get('rsc_name') or '',
        'name': s.get('name') or '',
    })

# Resource groups.
rg_list = []
for g in rgs:
    sd = g.get('select_filter') or {}
    rg_list.append({
        'name': g.get('name') or '',
        'place_count': sd.get('place_count') or 0,
    })

print(json.dumps({
    'rds': list(rds.values()),
    'nodes': nodes,
    'sps': sp_list,
    'snaps': snap_list,
    'rgs': rg_list,
}))
EOF
)
}

cluster_state() {
    echo "$CLUSTER_STATE_JSON"
}

# Convenience helpers for verb files — query the cached JSON without
# re-shelling out. Each emits one item per line.

state_fuzz_rds() {
    # Only the RDs the fuzzer itself owns (FUZZ_PREFIX-namespaced).
    local prefix=${FUZZ_PREFIX:-fuzz}
    printf '%s' "$CLUSTER_STATE_JSON" | FUZZ_PREFIX="$prefix" python3 -c "
import json, sys, os
prefix = os.environ['FUZZ_PREFIX']
d = json.loads(sys.stdin.read() or '{}')
for r in d.get('rds', []):
    if r['name'].startswith(prefix):
        print(json.dumps(r))
"
}

state_nodes() {
    printf '%s' "$CLUSTER_STATE_JSON" | python3 -c "
import json, sys
d = json.loads(sys.stdin.read() or '{}')
for n in d.get('nodes', []): print(n)
"
}

state_sps_for_node() {
    local node=$1
    printf '%s' "$CLUSTER_STATE_JSON" | NODE="$node" python3 -c "
import json, sys, os
node = os.environ['NODE']
d = json.loads(sys.stdin.read() or '{}')
for sp in d.get('sps', []):
    if sp['node'] == node: print(sp['name'])
"
}

state_snaps() {
    printf '%s' "$CLUSTER_STATE_JSON" | python3 -c "
import json, sys
d = json.loads(sys.stdin.read() or '{}')
for s in d.get('snaps', []):
    print(json.dumps(s))
"
}
