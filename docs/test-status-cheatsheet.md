# Test author cheatsheet: kubectl get vs drbdsetup status

This document maps every common `drbdsetup` / `drbdadm` bypass pattern used in `tests/e2e/*.sh` to its preferred **k8s-native** equivalent through the Resource / ResourceDefinition / Node CRD `Status` subresource.

**Why this exists**. Phase 11.5.a audit found 34 of 60 e2e tests bypass `Resource.Status` and instead `kubectl exec satellite-pod -- drbdsetup status <rd>` to parse the kernel directly. Each bypass:

- couples the test to the satellite pod lifecycle (which is exactly what some recovery tests are trying to perturb),
- hides what the test actually asserts behind a brittle `grep ... | awk` chain,
- duplicates the satellite's events2 observer — the Status subresource already carries the answer, the test just isn't reading it.

Of the 34 bypasses, ~25 have a k8s-native equivalent **today** (the satellite-side events2 observer populates `Resource.Status` from `drbdsetup events2`, so jsonpath returns the same string the test was about to grep). The Phase 11.5.b P0 work (`Status.Role` + `Status.Suspended`) landed in commit `a077afcf2` — observer now emits both fields. The remaining ~2 gaps sit on the P1 roadmap (`Connections[*].PeerDrbdNodeId`, `Connections[*].PeerVolumes[*].PeerDiskState`).

The audit's `docs/e2e-audit.md` already gates Tier-4 scripts on "must touch a real DRBD kernel". This cheatsheet refines that gate: **only the kernel-truth assertion at the end of a recovery test needs `drbdsetup`** — every prelude, wait-loop, and intermediate check should be a k8s-native read. The conversion is mechanical once the jsonpath is known; this doc supplies the jsonpaths.

Conventions used below:

- `$rd` is the ResourceDefinition name (e.g. `e2e-pvc-abc`).
- `$node` is the worker node name (e.g. `worker-1`); the Resource CRD is `${rd}.${node}`.
- `$peer` is another worker node name from the perspective of `$node`.
- `$vol` is the volume number (almost always `0` for single-volume RDs).
- `NS=blockstor-system` (matches lib.sh).
- All Resource CRDs are cluster-scoped — no `-n` flag.

## Table of contents

- [Disk state per local volume](#disk-state-per-local-volume)
- [Connection state per peer](#connection-state-per-peer)
- [Replication state per peer](#replication-state-per-peer)
- [Role per local replica](#role-per-local-replica)
- [I/O suspension](#io-suspension)
- [Quorum per volume](#quorum-per-volume)
- [Out-of-sync KiB / sync progress](#out-of-sync-kib--sync-progress)
- [DRBD IDs (TCP port, minor, node-id)](#drbd-ids-tcp-port-minor-node-id)
- [Peer's node-id (P1 pending)](#peers-node-id-p1-pending)
- [Peer's disk view (P1 pending)](#peers-disk-view-p1-pending)
- [Resource presence](#resource-presence)
- [Multi-node disk-state count](#multi-node-disk-state-count)
- [Convenience helpers for `lib.sh`](#convenience-helpers-for-libsh)
- [What is NOT exposed yet](#what-is-not-exposed-yet)
- [Conversion playbook](#conversion-playbook)

---

## Disk state per local volume

Reads the per-volume local kernel disk state: `UpToDate`, `Inconsistent`, `Outdated`, `Diskless`, `Failed`, `Negotiating`, `Attaching`, `Detaching`. This is by far the most-bypassed field — ~17 tests parse it from `drbdsetup status`.

**Bypass pattern**:

```bash
on_node "$node" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1
# → "  disk:UpToDate"
```

**k8s-native equivalent**:

```bash
kubectl get resource "${rd}.${node}" \
  -o jsonpath='{.status.volumes[?(@.volumeNumber=='${vol}')].diskState}'
# → "UpToDate"
```

For convenience (jsonpath's `?()` filter is fragile when piping shell vars; jq is easier):

```bash
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --argjson v "${vol}" '.status.volumes[]? | select(.volumeNumber==$v) | .diskState'
```

**Sample tests that should convert**: `tests/e2e/backing-device-fail.sh:99,106`, `tests/e2e/disk-replace-internal-metadata.sh:221,236,281,302`, `tests/e2e/disk-replace-external-metadata.sh:491,515`, `tests/e2e/lifecycle-toggle-retry.sh:222,255,281,308`, `tests/e2e/lifecycle-toggle-migrate.sh:223`, `tests/e2e/network-partition.sh:79-81`, `tests/e2e/quorum-loss-recovery.sh:161-163,350`, `tests/e2e/recovery-bitmap-drop.sh:225,263,323,343`, `tests/e2e/recovery-deleting-convert.sh:145`, `tests/e2e/recovery-node-id-mismatch.sh:143-145,359-361`, `tests/e2e/recovery-setgi-per-peer.sh:160`, `tests/e2e/split-brain-recovery.sh:258-259`, `tests/e2e/state-standalone-partition.sh:245-246`, `tests/e2e/storage-error-injection.sh:245,256`, `tests/e2e/toggle-disk.sh:94,114`.

**Kernel-truth exception**: the FINAL assertion at the end of a recovery test (after `drbdadm attach` + initial-sync wait) MAY still read `drbdsetup status` once — to prove the kernel itself sees `UpToDate`, not just the satellite's events2 observer. The wait-loops and intermediate state-machine probes should not.

---

## Connection state per peer

DRBD's per-peer connection-machine state: `Connected`, `Connecting`, `StandAlone`, `Disconnecting`, `Unconnected`, `Timeout`, `BrokenPipe`, `NetworkFailure`, `ProtocolError`, `TearDown`, `WFConnection`.

**Bypass pattern**:

```bash
on_node "$node" drbdsetup status "$RD" --verbose 2>/dev/null \
  | grep -oE 'connection:[A-Za-z]+' | head -1 | cut -d: -f2
# → "Connected"
```

**k8s-native equivalent**:

```bash
# As string (full kernel state):
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --arg p "${peer}" '.status.connections[]? | select(.peerNodeName==$p) | .message'

# As bool (derived "Connected"=true, anything-else=false):
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --arg p "${peer}" '.status.connections[]? | select(.peerNodeName==$p) | .connected'
```

**Notes**:

- `.status.connections[].message` is the full kernel string verbatim. Use this when the test cares about transitional states (`Connecting`, `BrokenPipe`).
- `.status.connections[].connected` is the derived bool. Use this when the test only cares about "Connected vs not".
- Filter on `peerNodeName` to disambiguate — a 3-replica RD has 2 connections per replica.

**Sample tests**: `tests/e2e/network-partition.sh:183,193` (`grep -q "connection:Connecting"`), `tests/e2e/recovery-discard-my-data.sh:160-161,167`, `tests/e2e/recovery-down-reverses.sh:136-137`, `tests/e2e/recovery-stuck-synctarget.sh:96-97`, `tests/e2e/split-brain-recovery.sh:260-262`, `tests/e2e/state-standalone-partition.sh:127-128` (counts `peer-disk:UpToDate` — see Peer disk view below).

---

## Replication state per peer

DRBD-9 replication state machine per peer-device: `Established`, `SyncSource`, `SyncTarget`, `PausedSyncS`, `PausedSyncT`, `VerifyS`, `VerifyT`, `Ahead`, `Behind`, `Off`, `WFBitMapS`, `WFBitMapT`, `WFSyncUUID`, `StartingSyncS`, `StartingSyncT`.

**Bypass pattern**:

```bash
on_node "$node" drbdsetup status "$RD" --verbose 2>/dev/null \
  | grep -E 'replication:Established' | head -1
```

**k8s-native equivalent**:

```bash
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --arg p "${peer}" '.status.connections[]? | select(.peerNodeName==$p) | .replicationState'
# → "Established"
```

**Sample tests**: `tests/e2e/recovery-stuck-synctarget.sh:338-340`, `tests/e2e/recovery-stuck-synctarget-down-up.sh:431-433`, `tests/e2e/state-auto-resync.sh:118,214,282`, `tests/e2e/state-inconsistent-mid-sync.sh:151,240`.

---

## Role per local replica

DRBD-9 role on this node: `Primary` (open for write), `Secondary` (replication-only), `Unknown` (transient — kernel just attached). Per-replica, in contrast to the cluster-wide `Status.InUse` bool.

**Bypass pattern** (used in ~12 tests):

```bash
on_node "$node" drbdsetup status "$RD" | grep "role:" | head -1
# → "  role:Primary"
```

**k8s-native equivalent** (shipped in commit `a077afcf2`, Phase 11.5.b P0):

```bash
kubectl get resource "${rd}.${node}" -o jsonpath='{.status.role}'
# → "Primary"
```

Schema field is `ResourceStatus.Role` (`api/v1alpha1/resource_types.go:218`). The observer parses the `role:` token from `drbdsetup events2` resource frames and stamps it onto the Status subresource on every change.

For "is anyone Primary across the whole RD?" (cluster-wide), the older `Status.InUse` bool is still the right field:

```bash
kubectl get resource "${rd}.${node}" -o jsonpath='{.status.inUse}'
# → "true" / "false"
```

`Status.Role` and `Status.InUse` answer different questions: Role is THIS replica's role, InUse is "any replica in the RD reporting Primary". A 3-replica RD has one Role=Primary + two Role=Secondary, and all three carry InUse=true.

**Sample tests** (all convertible NOW): `tests/e2e/disk-replace-internal-metadata.sh:175,326`, `tests/e2e/quorum-loss-recovery.sh:254,273,290,372`, `tests/e2e/recovery-discard-my-data.sh:112,221`, `tests/e2e/recovery-inconsistent-blocking.sh:431`, `tests/e2e/recovery-primary-force.sh:113-114,147-148,166-167`, `tests/e2e/replica-add-no-resync.sh:121`, `tests/e2e/split-brain-recovery.sh:128,251`, `tests/e2e/two-primaries-live-migration.sh:79-80,95`.

---

## I/O suspension

DRBD-9 I/O suspension reason: `No` (I/O serves normally), `Quorum` (quorum lost, `on-no-quorum=suspend-io` is blocking the queue), `User` (`drbdadm suspend-io` from the operator), `NoData`, `Fencing`.

**Bypass pattern**:

```bash
status=$(on_node "$node" drbdsetup status "$RD" 2>/dev/null)
if echo "$status" | grep -qiE "quorum:no|suspended|may_promote:no"; then
    echo "I/O is suspended"
fi
```

**k8s-native equivalent** (shipped in commit `a077afcf2`, Phase 11.5.b P0):

```bash
kubectl get resource "${rd}.${node}" -o jsonpath='{.status.suspended}'
# → "" (= No) / "Quorum" / "User" / "NoData" / "Fencing"
```

Schema field is `ResourceStatus.Suspended` (`api/v1alpha1/resource_types.go:232`). Observer parses the `suspended:` token from `drbdsetup events2`. Pair with `Status.Volumes[].Quorum` (next section) for the quorum-loss vs operator-suspend distinction.

Note: the bypass pattern conflates three distinct DRBD signals (`quorum:no`, `suspended:*`, `may_promote:no`). After conversion, tests should pick the precise field for the assertion — `Status.Volumes[].Quorum` for "kernel lost quorum on this volume", `Status.Suspended` for "kernel suspended I/O and here's why", and (after P2 lands) `Status.MayPromote` for "kernel will refuse a promotion request".

**Sample tests** (all convertible NOW): `tests/e2e/network-partition.sh:128,136,185,195`, `tests/e2e/quorum-loss-recovery.sh:236`, `tests/e2e/recovery-quorum-persistence.sh:339`, `tests/e2e/recovery-suspended-quorum.sh:194`.

---

## Quorum per volume

DRBD-9 per-volume kernel quorum from the `quorum:yes|no` field in events2 device frames.

**Bypass pattern**:

```bash
status=$(on_node "$node" drbdsetup status "$RD" 2>/dev/null)
if echo "$status" | grep -q "quorum:no"; then
    echo "lost quorum"
fi
```

**k8s-native equivalent**:

```bash
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --argjson v "${vol}" '.status.volumes[]? | select(.volumeNumber==$v) | .quorum'
# → "true" (= has quorum) / "false"
```

**Notes**: per-volume, not per-resource. The node-wide `drbd.linbit.com/lost-quorum` k8s taint is coarser — it answers "does ANY volume on this node lack quorum" rather than "does THIS volume lack quorum". For the latter, read `Resource.Status.Volumes[].Quorum`.

**Sample tests**: `tests/e2e/network-partition.sh:184,194`, `tests/e2e/quorum-loss-recovery.sh:236`, `tests/e2e/recovery-quorum-persistence.sh:339`, `tests/e2e/recovery-suspended-quorum.sh:194`.

---

## Out-of-sync KiB / sync progress

How many KiB this volume is behind any peer (worst case across all peers).

**Bypass pattern**: tests today wait on `disk:UpToDate` rather than parsing `out-of-sync`. But the field IS surfaced.

**k8s-native equivalent**:

```bash
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --argjson v "${vol}" '.status.volumes[]? | select(.volumeNumber==$v) | .outOfSyncKib'
# → "0" (fully in sync) / "65536" (64 MiB behind)
```

Combined with `ResourceDefinition.Spec.VolumeDefinitions[].SizeKib`, callers compute sync-progress percentage. Useful for "wait until sync finishes" loops that today poll `disk:UpToDate` repeatedly — `outOfSyncKib==0` is the cleaner predicate.

---

## DRBD IDs (TCP port, minor, node-id)

These are the kernel-allocated identifiers that the satellite stamps into the `.res` file.

**Bypass pattern**: tests usually `grep` the `/etc/drbd.d/${RD}.res` file on the satellite or parse `drbdsetup status --verbose`.

**k8s-native equivalents** (Resource-scope, per-replica):

```bash
# Local DRBD-9 node-id (0..15):
kubectl get resource "${rd}.${node}" -o jsonpath='{.status.drbdNodeId}'

# TCP port (this replica's listen port — different replicas of the
# same RD can use different ports because each lives on a different node):
kubectl get resource "${rd}.${node}" -o jsonpath='{.status.drbdPort}'

# /dev/drbd<N> minor on this node:
kubectl get resource "${rd}.${node}" -o jsonpath='{.status.drbdMinor}'

# DRBD device path is derivable: /dev/drbd<minor>
# Or read it from the volume:
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --argjson v "${vol}" '.status.volumes[]? | select(.volumeNumber==$v) | .devicePath'
```

**k8s-native equivalents** (RD-scope, cluster-wide for the whole RD):

```bash
# Cluster-wide RD port + minor (every replica inherits these):
kubectl get resourcedefinition "${rd}" -o jsonpath='{.status.drbdPort}'
kubectl get resourcedefinition "${rd}" -o jsonpath='{.status.drbdMinor}'
```

The RD-scope and Resource-scope ports/minors are equal by construction (Bug 266 + Bug 268: cluster-wide allocation, every Resource inherits the parent RD's value). Prefer RD-scope when the test just wants "what port is this RD using"; prefer Resource-scope when the test wants the canonical per-replica copy.

**Sample tests** to convert away from `.res` parsing or `--verbose` grep: `tests/e2e/device_for_rd` helper in `lib.sh:71-74` (uses `grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${rd}.res`); should be `kubectl get resource ${rd}.${node} -o jsonpath='{.status.volumes[0].devicePath}'` plus a fallback to constructing from `drbdMinor`.

---

## Peer's node-id (P1 pending)

The DRBD-9 node-id assigned to a PEER replica, as seen from this node's perspective. Tests use this for `drbdadm new-current-uuid --discard-my-data --peer-node-id=N` and `drbdmeta set-gi --node-id=N`.

**Bypass pattern**:

```bash
peer_node_id=$(on_node "$N1" drbdsetup status "$RD" --verbose 2>/dev/null \
    | grep -E "^[[:space:]]+${N2}[[:space:]]+node-id:" \
    | grep -oE 'node-id:[0-9]+' | head -1 | cut -d: -f2)
```

**Current workaround** (read peer's OWN Resource CRD):

```bash
# The peer's local Status.DrbdNodeID is the same number the local
# node sees in --verbose output. Read it from the peer's Resource:
kubectl get resource "${rd}.${peer}" -o jsonpath='{.status.drbdNodeId}'
```

This works because the DRBD-9 node-id is globally unique within an RD — every peer sees the same ID for `$peer`. The `drbdsetup status --verbose` output on `$N1` reports `$N2`'s node-id, which equals `Resource(${rd}.${N2}).Status.DRBDNodeID`. So tests do NOT need a new field — they need to read the OTHER Resource CRD.

**Future k8s-native equivalent** (after Phase 11.5.b P1, more explicit):

```bash
# Same answer, but as a denormalised field on the local Resource,
# so tests don't need to know the peer's CRD name:
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --arg p "${peer}" '.status.connections[]? | select(.peerNodeName==$p) | .peerDrbdNodeId'
```

**Sample tests** that should switch to "read peer's CRD" today: `tests/e2e/recovery-discard-my-data.sh:132-142,195-202`, `tests/e2e/recovery-stuck-synctarget.sh:146-148`, `tests/e2e/recovery-stuck-synctarget-down-up.sh:217-219`, `tests/e2e/state-inconsistent-mid-sync.sh:506-509`.

---

## Peer's disk view (P1 pending)

What disk state DOES THIS REPLICA see THE PEER reporting? Distinct from "what does the peer say about itself" (= peer Resource CRD's `.status.volumes[].diskState`). The peer-disk view is what local DRBD sees over the wire; under a network partition the two can diverge.

**Bypass pattern**:

```bash
on_node "$node" drbdsetup status "$RD" 2>/dev/null | grep -c "peer-disk:UpToDate"
```

**Current workaround**: read the OTHER replica's own status:

```bash
kubectl get resource "${rd}.${peer}" -o json \
  | jq -r --argjson v "${vol}" '.status.volumes[]? | select(.volumeNumber==$v) | .diskState'
```

This works **except under partition**: when `$node` cannot reach `$peer`, the satellite on `$peer` may keep stamping `UpToDate` on its own Resource (it's UpToDate from its own perspective), while `$node`'s DRBD kernel reports the peer as `Outdated` or `DUnknown`. Tests that specifically exercise partition semantics (`tests/e2e/network-partition.sh`, `tests/e2e/state-standalone-partition.sh`) need the local kernel's view of the peer, not the peer's view of itself.

**Future k8s-native equivalent** (after Phase 11.5.b P1):

```bash
kubectl get resource "${rd}.${node}" -o json \
  | jq -r --arg p "${peer}" --argjson v "${vol}" \
    '.status.connections[]?
     | select(.peerNodeName==$p)
     | .peerVolumes[]? | select(.volumeNumber==$v) | .peerDiskState'
```

The schema doesn't expose `Connections[].PeerVolumes[].PeerDiskState` today — that's the Phase 11.5.b P1 addition. Until then, peer-disk-view tests are genuinely kernel-bound.

**Sample tests**: `tests/e2e/network-partition.sh:181,191` (`peer-disk:UpToDate` count), `tests/e2e/state-standalone-partition.sh:127-128` (3-node partition view).

---

## Resource presence

"Does this RD exist on this node?" — used by recovery tests after `linstor r d` to assert cleanup completed.

**Bypass pattern**:

```bash
out=$(on_node "$node" drbdsetup status "$RD" 2>&1 || true)
if [[ "$out" == *"No currently configured DRBD found"* ]]; then
    echo "gone"
fi
```

**k8s-native equivalent**:

```bash
if ! kubectl get resource "${rd}.${node}" >/dev/null 2>&1; then
    echo "gone"
fi
```

Note: `kubectl get` returning NotFound on the Resource CRD is the CONTRACT contract. The satellite then `drbdsetup down`s the kernel resource as part of finalizer cleanup. Tests that want to assert the kernel is also clean (no leak) MAY additionally read `drbdsetup status` — that is a legitimate Tier-4 kernel-truth assertion. But the precondition "the Resource CRD must be gone first" is k8s-native.

**Sample tests**: `tests/e2e/recovery-suspended-quorum.sh:264-266`, `lib.sh:wait_cluster_idle:162-170` (currently does both — could fold the CRD check into a separate pre-step).

---

## Multi-node disk-state count

"How many of the N replicas are UpToDate?" — common in quorum and convergence wait-loops.

**Bypass pattern**:

```bash
s1=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
s2=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
s3=$(on_node "$N3" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
total=$((s1 + s2 + s3))
```

**k8s-native equivalent**:

```bash
# Count UpToDate replicas across all Resources of this RD in one round-trip:
uptodate=$(kubectl get resources -o json \
  | jq -r --arg rd "${rd}" '
      [.items[] | select(.spec.resourceDefinitionName==$rd)
                | .status.volumes[]? | select(.diskState=="UpToDate")]
      | length')
```

**Sample tests**: `tests/e2e/network-partition.sh:79-81`, `tests/e2e/quorum-loss-recovery.sh:161-163`, `tests/e2e/recovery-node-id-mismatch.sh:143-145,359-361`.

---

## Convenience helpers for `lib.sh`

Add a `# k8s-native readers` section to `tests/e2e/lib.sh`. Suggested API surface:

```bash
# ---- k8s-native readers (preferred) ----

# $1=rd, $2=node, $3=volNum (default 0)
status_disk_state() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --argjson v "${3:-0}" \
            '.status.volumes[]? | select(.volumeNumber==$v) | .diskState // ""'
}

# $1=rd, $2=node, $3=peer
status_connection_state() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --arg p "${3}" \
            '.status.connections[]? | select(.peerNodeName==$p) | .message // ""'
}

# $1=rd, $2=node, $3=peer — derived bool "true"/"false"
status_connected() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --arg p "${3}" \
            '.status.connections[]? | select(.peerNodeName==$p) | .connected // false'
}

# $1=rd, $2=node, $3=peer
status_replication_state() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --arg p "${3}" \
            '.status.connections[]? | select(.peerNodeName==$p) | .replicationState // ""'
}

# $1=rd, $2=node, $3=volNum (default 0) — "true"/"false"
status_quorum() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --argjson v "${3:-0}" \
            '.status.volumes[]? | select(.volumeNumber==$v) | .quorum // false'
}

# $1=rd, $2=node — int / empty if not yet allocated
status_drbd_port() {
    kubectl get resource "${1}.${2}" -o jsonpath='{.status.drbdPort}' 2>/dev/null
}
status_drbd_minor() {
    kubectl get resource "${1}.${2}" -o jsonpath='{.status.drbdMinor}' 2>/dev/null
}
status_drbd_node_id() {
    kubectl get resource "${1}.${2}" -o jsonpath='{.status.drbdNodeId}' 2>/dev/null
}

# $1=rd, $2=node, $3=volNum (default 0) — /dev/drbdN
status_device_path() {
    kubectl get resource "${1}.${2}" -o json 2>/dev/null \
        | jq -r --argjson v "${3:-0}" \
            '.status.volumes[]? | select(.volumeNumber==$v) | .devicePath // ""'
}

# $1=rd, $2=node — "true"/"false"
status_resource_exists() {
    if kubectl get resource "${1}.${2}" >/dev/null 2>&1; then
        echo true
    else
        echo false
    fi
}

# $1=rd — count UpToDate replicas across the whole RD
status_uptodate_count() {
    kubectl get resources -o json 2>/dev/null \
        | jq -r --arg rd "${1}" '
            [.items[] | select(.spec.resourceDefinitionName==$rd)
                      | .status.volumes[]? | select(.diskState=="UpToDate")]
            | length'
}

# $1=rd, $2=node — Status.Role landed in a077afcf2 (Phase 11.5.b P0)
status_role() {
    kubectl get resource "${1}.${2}" -o jsonpath='{.status.role}' 2>/dev/null
}

# $1=rd, $2=node — Status.Suspended landed in a077afcf2 (Phase 11.5.b P0)
# Returns "" for No, or "Quorum"/"User"/"NoData"/"Fencing".
status_suspended() {
    kubectl get resource "${1}.${2}" -o jsonpath='{.status.suspended}' 2>/dev/null
}

# ---- peer-side workaround (until Phase 11.5.b P1) ----

# $1=rd, $2=peer — read peer's OWN Resource CRD for its node-id.
# Globally unique within an RD, so same answer as drbdsetup status --verbose
# on any local node would report for this peer.
peer_node_id() {
    kubectl get resource "${1}.${2}" -o jsonpath='{.status.drbdNodeId}' 2>/dev/null
}

# ---- kernel-truth fallback (Tier-4 final assertions only) ----

# Recovery tests' FINAL assertion may still want to prove the kernel
# itself reached the target state, not just the observer's snapshot.
# Use sparingly; intermediate wait-loops should use status_* helpers.
kernel_disk_state() {
    on_node "$2" drbdsetup status "$1" 2>/dev/null \
        | grep -oE 'disk:[A-Za-z]+' | head -1 | cut -d: -f2
}
```

The `jq` pipeline is preferred over raw `kubectl -o jsonpath='{?(@...)}'` because:

1. `kubectl` jsonpath has buggy escape behaviour around quoted filter expressions — the same query that works in `jq` fails silently in `kubectl jsonpath` depending on the kubectl version.
2. `jq` returns `""` cleanly when the field is missing, so callers can `[[ -z "$state" ]] && retry` without grep gymnastics.
3. Defaulting via `// ""` / `// false` short-circuits the "field not set yet" race during Resource creation.

`jq` is already in the satellite image (we use it elsewhere in `lib.sh`-adjacent tooling) and in the QEMU stand. If a test environment lacks `jq`, the `kubectl jsonpath` form documented above each helper works for the simple cases (everything except the `?()` filtered ones).

---

## What is NOT exposed yet

Phase 11.5.b backlog. P0 already landed; P1/P2 still need schema additions:

- **`Status.Role`** — **SHIPPED** in commit `a077afcf2` (Phase 11.5.b P0). Schema: `resource_types.go:218`. Observer: `pkg/satellite/controllers/observer.go`.
- **`Status.Suspended`** — **SHIPPED** in commit `a077afcf2` (Phase 11.5.b P0). Schema: `resource_types.go:232`.
- **`Status.MayPromote`** (P2, not in schema). Tests use `may_promote:no` as a quorum-suspension probe today; with Suspended shipped MayPromote is largely redundant — defer until a concrete test needs it.
- **`Connections[*].PeerDrbdNodeId`** (P1, not in schema). The workaround "read the peer's own Resource CRD's `Status.DRBDNodeID`" returns the same answer today.
- **`Connections[*].PeerVolumes[*].PeerDiskState`** (P1, not in schema). Distinct from "the peer's own diskState" — this is the LOCAL kernel's VIEW of the peer's disk, which can diverge under partition. Needed by `network-partition.sh` and `state-standalone-partition.sh`.
- **`Connections[*].PeerVolumes[*].OutOfSyncKib`** (P2, not in schema). Per-peer (vs worst-case) sync-progress.

The `kernel_role` / `kernel_suspended` fallback helpers in the `lib.sh` section below are now **vestigial** — keep them out of new tests. They are documented only as a reference for what the `drbdsetup` parse used to look like.

---

## Conversion playbook

When converting a bypass test to k8s-native reads:

1. **Identify the field**. Grep the script for `drbdsetup` and `drbdadm` calls. Classify each by table-of-contents section above.
2. **Replace wait-loops first**. The biggest win is replacing `while sleep 2; do on_node ... drbdsetup status; done` loops with `kubectl wait`-style polls of `Resource.Status`. The loop body shrinks from ~5 lines of pipeline to one line of `jq`.
3. **Keep ONE kernel-truth assertion at the end**. Tier-4 recovery tests SHOULD prove the kernel itself reached the target state, not just the satellite's events2 observer. Leave the final `drbdsetup status` assertion alone; remove the intermediate ones.
4. **Don't fold tests gated by P0/P1**. Tests that grep `role:` or `suspended:` or `peer-disk:` stay on `drbdsetup` until the corresponding Status field lands. The audit doc tracks which.
5. **Watch for the "peer's own Resource" trick**. Several patterns that LOOK like they need a new schema field can be solved by reading the OTHER replica's CRD — see the `peer_node_id` helper above. When in doubt, ask: "is this DRBD-9 field globally unique across the RD?" — if yes, the peer's own Status row carries the same answer.
6. **`kubectl get -o json | jq` over `-o jsonpath`** for anything with a filter. See the convenience-helpers section.

Estimated conversion impact (from Phase 11.5.a audit, refined here, with P0 already shipped):

| Bucket | Tests | Convertible today | After P1 | Genuinely kernel-bound |
|---|---|---|---|---|
| disk-state wait-loops | ~17 | ~17 | — | 0 |
| connection / replication state | ~6 | ~6 | — | 0 |
| role probes | ~12 | ~12 (P0 shipped) | — | 0 |
| suspended / quorum probes | ~4 | ~4 (P0 shipped) | — | 0 |
| peer-disk view under partition | ~2 | 0 (need P1) | ~2 | 0 |
| peer node-id resolution | ~4 | ~4 (peer-CRD trick) | (cleaner) | 0 |
| kernel-truth final assertions | ~5 | n/a — keep as `drbdsetup` | — | 5 |

Net: with P0 shipped (`a077afcf2`), **~37 of 34 distinct bypass call-sites can be converted today** (some tests have multiple bypasses); ~2 remain blocked on P1 (peer-disk view under partition); ~5 stay on `drbdsetup` by design (they ARE the kernel-truth assertion). The total exceeds 34 because several scripts duplicate the same pattern across pre/post phases.
