# Observability + diagnostic-flow test scenarios

Companion to `linstor-cli-scenarios.md` (operator-facing CLI happy
paths) and `drbd-troubleshooting-scenarios.md` (DRBD9 failure
modes). This one is structured around LINBIT's
"Troubleshooting LINSTOR in Kubernetes" cheat sheet, which
organises diagnostic commands into a **three-level decision flow**:

```
Kubernetes level   →   LINSTOR level   →   Node level
(linstor-csi-       →   (linstor-       →   (linstor-node /
 controller)            controller)         satellite)
```

The cheat sheet's value isn't the individual commands (those are
covered in the other two docs). The value is the **walking order**:
when something's broken, the operator works top-down, narrowing
from K8s API objects → LINSTOR REST → DRBD kernel state, exiting
as soon as the failure layer is identified.

Tests here pin that walking order works for blockstor, i.e. that
each level's observability surface is consistent with the level
below, so the operator can trust the narrowing.

For blockstor the name translation is:

| Cheat sheet (upstream)   | blockstor                    |
|--------------------------|------------------------------|
| linstor-csi-controller   | piraeus-csi (unchanged image)|
| linstor-controller       | blockstor-apiserver          |
| linstor-node             | blockstor-satellite          |
| `/var/lib/linstor.d/`    | `/etc/drbd.d/`               |

---

## Three-level consistency tests

These pin that the three observability levels report **the same
underlying state** for the same object, so the cheat sheet's
top-down narrowing works.

### 1. PVC ↔ Resource CRD ↔ DRBD device consistency (happy path)

**Why:** Cheat sheet's Provisioning row. A bound PVC must
correspond to: a placed Resource CRD in blockstor, AND a live DRBD
device on each diskful satellite.

**Setup:** StorageClass with `linstor.csi.linbit.com` provisioner
pointing at blockstor's REST, placementCount: 2.

**Steps:**
```bash
kubectl apply -f testdata/pvc-100mi.yaml
# Wait until PVC Bound:
kubectl wait pvc/test-pvc --for=condition=Bound --timeout=60s

# Level 1: K8s view
kubectl describe pvc test-pvc
kubectl get volumeattachments

# Level 2: LINSTOR view (via blockstor-apiserver)
linstor resource-definition list
linstor resource list -r <pvc-volume-id>
linstor volume list -r <pvc-volume-id>

# Level 3: Node view (via satellite container)
satellite_exec <worker> drbdadm status <pvc-volume-id>
satellite_exec <worker> lsblk | grep drbd
satellite_exec <worker> cat /etc/drbd.d/<pvc-volume-id>.res
```

**Expected:**
- PVC status `Bound`, volumeName matches the PV
- VolumeAttachment exists for the node where the consumer Pod runs
- `linstor r l` shows 2 diskful replicas (+ 1 tiebreaker if
  PlaceCount=2)
- All replicas State = `UpToDate`
- `drbdadm status` on each diskful node shows the same resource
  name with `disk:UpToDate`
- `lsblk` shows `/dev/drbd<minor>` matching `linstor v l` DeviceName
- `.res` file on the satellite matches `Resource.Spec` (RD name,
  DRBD port, peer list)

**Three-way assertion** (the core of this test): all three views
agree on:
- Resource name (PV volumeName == linstor RD name == .res `resource`
  block)
- DRBD port (linstor r l Port == .res `address` port == netstat -l
  shows the port LISTEN)
- DRBD minor (linstor v l MinorNr == /dev/drbd<N> == .res `volume 0
  { device /dev/drbdN minor N; }`)

**Failure modes this catches:**
- PVC Bound but Resource CRD not yet stamped → bound-too-early bug
- linstor r l shows UpToDate but `.res` file missing → satellite
  reconciler didn't render
- DRBD minor in `.res` doesn't match kernel state → minor allocator
  desync (Phase 8.1 invariant)

---

### 2. K8s side broken, LINSTOR side healthy → operator narrows fast

**Why:** Cheat sheet's primary use case. PVC stuck Pending; CSI
side is the failure layer, not LINSTOR / DRBD.

**Setup:** Provision a PVC where the StorageClass has a malformed
parameter (e.g. `storagePool: nonexistent-pool`).

**Steps:**
```bash
kubectl apply -f testdata/pvc-bad-pool.yaml
# PVC stays Pending forever.

# Level 1: K8s view — cheat sheet's first cell
kubectl describe pvc test-pvc-bad
# Expected: Events section shows the CSI provisioner error verbatim
kubectl logs -n blockstor-system deploy/piraeus-csi-controller \
    -c csi-provisioner --tail=20
# Expected: log line containing "storage pool nonexistent-pool not
# found" or equivalent
```

**Operator's narrowing:**
- After level 1, the operator has enough info — no need to descend
  into LINSTOR or Node level.

**Assertion:** the CSI provisioner error reaches `kubectl describe
pvc`'s Events section AND the piraeus-csi-controller pod's logs
within 30s. The text identifies the pool name AND that it's missing
(not a generic "Internal error").

---

### 3. K8s side healthy, LINSTOR side broken → cheat sheet bridge command

**Why:** Cheat sheet's `kubectl exec -ti linstor-controller -- bash`
arrow — the bridge from level 1 to level 2.

**Setup:** Provision a PVC; meanwhile force-fail blockstor's
autoplacer by marking every storage pool as full.

**Steps:**
```bash
# Trigger PVC create:
kubectl apply -f testdata/pvc-100mi.yaml

# Level 1: K8s view — PVC has CSI error
kubectl describe pvc test-pvc | grep Events -A 5
# Expected: "Insufficient capacity" or similar

# Cheat-sheet bridge: exec into apiserver pod (or any pod with
# linstor CLI) and use the REST:
kubectl exec -n blockstor-system -ti deploy/blockstor-apiserver -- \
    linstor sp list

# Level 2: LINSTOR view confirms the pool is full
# Expected: FreeCapacity column shows 0 for all rows
```

**Operator's narrowing:** the CSI error pointed at capacity; level 2
confirms it.

**Assertion:** the apiserver pod's image includes the `linstor` CLI
binary (or a similar diagnostic tool). If it doesn't, document the
fallback (port-forward + local `linstor`).

**Open question for blockstor:** should we bake `linstor` into the
apiserver image? It's ~30 MB of Python, but it lets the cheat-sheet
bridge work without a port-forward.

---

### 4. LINSTOR side healthy, Node side broken → bridge to satellite

**Why:** Cheat sheet's `kubectl exec -ti linstor-node -- bash` arrow
— the bridge from level 2 to level 3.

**Setup:** Provision a 2-replica PVC. Wait until Bound + replicas
UpToDate. Then induce a kernel-level fault: drop tcp/<drbd-port>
on worker-2 via iptables.

**Steps:**
```bash
# Level 2: LINSTOR view shows the connection problem
linstor resource list -r <pvc-id>
# Expected: worker-1 Conns column shows StandAlone(worker-2) or
# NetworkFailure(worker-2)

# Cheat-sheet bridge: find which satellite pod runs on worker-1
# and exec in
SAT=$(kubectl get pod -n blockstor-system -l app=blockstor-satellite \
    --field-selector spec.nodeName=<worker-1> -o name)
kubectl exec -n blockstor-system -ti $SAT -- bash

# Level 3: Node view confirms it
drbdadm status <pvc-id>
dmesg | grep drbd | tail -20
cat /etc/drbd.d/<pvc-id>.res
```

**Expected:**
- `drbdadm status` shows the same StandAlone state for worker-2's
  peer slot
- `dmesg` shows the TCP connection failure with the timestamp
  matching the iptables drop

**Heal:** restore iptables; both `linstor r l` and `drbdadm status`
recover to Connected within 30s.

**Assertion:** at every step, level 2 and level 3 agree on which
peer is broken AND what the connection state is. blockstor's
observer must not drift from kernel reality.

---

## Caution-zone command tests

The cheat sheet highlights two command groups as **caution-required**:
`linstor resource delete` and `drbdadm down`. These tests pin that
the operator's destructive ops at one level don't surprise the
levels above/below.

### 5. `linstor resource delete <node> <rd>` is observable at all 3 levels

**Why:** Most common destructive op. Operator removes a replica;
all three levels must reflect it cleanly.

**Setup:** RD with 3 replicas (2 diskful + 1 tiebreaker). Active
pod consuming the volume from worker-1.

**Steps:**
```bash
# Operator removes the tiebreaker:
linstor resource delete <tiebreaker-worker> <rd>

# Level 1: K8s view — PVC stays Bound, pod unaffected
kubectl get pvc test-pvc
kubectl get pod test-consumer
# Expected: both Ready, no events

# Level 2: LINSTOR view — tiebreaker gone (briefly), then
# auto-recreated by ensureTiebreaker reconciler
linstor r l -r <rd>
# Expected within 10s: 2 UpToDate + 1 fresh TieBreaker on a
# different node

# Level 3: Node view on the (formerly) tiebreaker node
satellite_exec <tiebreaker-worker> drbdadm status <rd>
# Expected: "No currently configured DRBD found" (or the resource
# is just absent)
satellite_exec <tiebreaker-worker> ls /etc/drbd.d/
# Expected: <rd>.res gone
```

**Assertion:** No data loss visible from the pod's `dd` write+read
cycle that runs throughout the test.

---

### 6. `drbdadm down <rd>` from a satellite shell is recovered

**Why:** Cheat sheet's `drbdadm down / drbdadm up` pair. Operator
manually disables a resource for kernel-level surgery (e.g. wipe
its metadata); satellite reconciler must bring it back.

**Setup:** RD with 2 replicas, Connected.

**Steps:**
```bash
# Level 3: operator runs down
satellite_exec <worker-1> drbdadm down <rd>
satellite_exec <worker-1> drbdadm status <rd>
# Expected: "No currently configured DRBD found"

# Wait for reconciler to react (≤30s typically):
sleep 30

# Level 3 again:
satellite_exec <worker-1> drbdadm status <rd>
# Expected: resource back up, Connected to peer

# Level 2: confirm LINSTOR view is consistent
linstor r l -r <rd>
# Expected: worker-1 row State = UpToDate again
```

**Reconciler-side assertion:** the satellite-resource reconciler
detects the kernel state divergence (its Resource CRD says
diskful + Connected, kernel says nothing), re-applies `.res` +
`drbdadm up` + `drbdadm adjust`, and converges within 30s.

**Failure mode caught:** reconciler debounces too long (kernel
state stays empty for minutes) → cheat sheet's "down then up" flow
breaks.

---

### 7. `drbdadm primary --force` is allowed but not auto-undone

**Why:** Cheat sheet lists this under cautious ops. Operator may
need to force-promote in emergency (e.g. fence the other node and
recover from a single replica). Reconciler shouldn't demote it.

**Setup:** RD with 2 replicas. worker-2 is unreachable.

**Steps:**
```bash
satellite_exec <worker-1> drbdadm primary <rd> --force
satellite_exec <worker-1> drbdadm status <rd>
# Expected: role:Primary

# Reconciler must NOT auto-demote — wait + recheck:
sleep 30
satellite_exec <worker-1> drbdadm status <rd>
# Expected: still Primary

# Mount + write to prove it's usable:
satellite_exec <worker-1> mount /dev/drbd<N> /mnt/recovery
satellite_exec <worker-1> bash -c "echo test > /mnt/recovery/file"
```

**Assertion:** blockstor's reconciler treats Primary role as
operator intent — it doesn't demote based on its Spec. (Spec's
`Resource.Spec.Flags` doesn't include a Role field; role is
satellite-observed state.)

---

## Cross-level error-correlation tests

The cheat sheet's value compounds when one level's symptom maps to
another level's root cause. These tests pin the most common
correlations.

### 8. CSI "VolumeAttach failed" ↔ DRBD "Permission denied"

**Why:** Common chain: pod can't mount → CSI publishes the volume
→ kernel rejects because DRBD is Secondary on this node and not
the target of any attach.

**Setup:** RD with 2 diskful replicas on worker-1 + worker-2.
Schedule a Pod onto worker-3 (no replica).

**Steps:**
```bash
kubectl apply -f testdata/pod-on-worker-3.yaml
# Pod stays ContainerCreating.

# Level 1: K8s sees the VolumeAttach failure
kubectl describe pod test-pod | grep -A 5 Events
# Expected: FailedAttachVolume / "DRBD ... Permission denied"

# Level 2: LINSTOR view
linstor r l -r <rd>
# Expected: NEW diskless row for worker-3 appears (CSI's
# diskless-attach flow)
```

**Expected timeline:**
- Within 30s, the diskless attach completes (Resource CRD created
  on worker-3 with `Flags: [DISKLESS]`)
- Pod transitions ContainerCreating → Running

**Cheat sheet narrowing:** if the diskless attach DOESN'T happen
(stuck in level-1 error), the operator descends via
`kubectl exec -ti deploy/blockstor-apiserver -- linstor r c
worker-3 <rd> --diskless` as the manual fallback.

---

### 9. Linstor "not enough storage pools" ↔ kubectl pool capacity

**Why:** Inverse of #2. LINSTOR-side autoplace fails;
operator needs to confirm by checking node-level free space.

**Setup:** Mark all storage pools nearly full (write a file to
fill `/var/lib/piraeus/file-thin`).

**Steps:**
```bash
# Try to provision:
kubectl apply -f testdata/pvc-1gi.yaml

# Level 1: PVC stuck Pending
kubectl describe pvc | grep Events

# Level 2: confirm the cause
linstor sp list
# Expected: FreeCapacity column shows <100 MiB
linstor resource-definition autoplace <pvc-id>
# Expected: error "not enough candidate storage pools"

# Level 3: confirm at node-level
satellite_exec <any-worker> df -h /var/lib/piraeus/file-thin
satellite_exec <any-worker> lvs blockstor-lvm/thin   # for LVM_THIN
satellite_exec <any-worker> zfs list blockstor-zfs   # for ZFS_THIN
```

**Assertion:** all three views agree on the pool being near-full
(matching FreeCapacity values within 5% across levels).

---

## Cheat-sheet completeness audit

For each command in the cheat sheet, confirm blockstor has it
working. This is a checklist test, not a single scenario.

### 10. Level-1 commands work (CSI)

| Command                                          | Status                       |
|--------------------------------------------------|------------------------------|
| `kubectl describe pvc <name>`                    | Standard k8s — works         |
| `kubectl get volumeattachments`                  | Standard k8s — works         |
| `kubectl logs piraeus-csi-controller -c csi-attacher`     | piraeus image — works |
| `kubectl logs piraeus-csi-controller -c csi-provisioner`  | piraeus image — works |
| `kubectl logs piraeus-csi-controller -c csi-resizer`      | piraeus image — works |

Test: each `kubectl logs` command on a fresh stand returns non-empty
output. No image-pull errors for any of the 3 sidecars.

---

### 11. Level-2 commands work (linstor CLI against blockstor REST)

| Command                                  | Test coverage                                |
|------------------------------------------|----------------------------------------------|
| `linstor resource-definition list`       | linstor-cli-scenarios.md #3                  |
| `linstor node list`                      | linstor-cli-scenarios.md #1                  |
| `linstor storage-pool list`              | linstor-cli-scenarios.md #2                  |
| `linstor volume-definition list`         | linstor-cli-scenarios.md #4                  |
| `linstor resource list`                  | linstor-cli-scenarios.md #5                  |
| `linstor volume list`                    | linstor-cli-scenarios.md #6                  |
| `linstor resource create [--diskless]`   | linstor-cli-scenarios.md #11 (manual create) |
| `linstor resource delete`                | linstor-cli-scenarios.md #13                 |

Plus one new audit test for diskless attach:

```bash
linstor resource create <worker-without-replica> <rd> --diskless
linstor r l -r <rd>
# Expected: new row with State = Diskless (NOT TieBreaker)
```

This distinguishes operator-requested diskless from
autoplacer-stamped tiebreaker — both share kernel-level DRBD-9
diskless but have different LINSTOR-side semantics.

---

### 12. Level-3 commands work (drbdadm/drbdsetup/utility from satellite)

| Command                              | Expected from satellite container       |
|--------------------------------------|-----------------------------------------|
| `drbdadm status <rd>`                | per-resource state                      |
| `dmesg \| grep drbd`                 | kernel ring buffer access (needs CAP_SYSLOG) |
| `drbdadm down / drbdadm up <rd>`     | take resource down + bring back         |
| `drbdadm primary [--force] <rd>`     | role switch                             |
| `drbdadm secondary <rd>`             | demote                                  |
| `drbdadm disconnect <rd>`            | single-peer or all-peer                 |
| `drbdadm connect [--discard-my-data] <rd>` | re-establish + force resync        |
| `cat /etc/drbd.d/<rd>.res`           | satellite-rendered config file          |
| `drbdadm adjust <rd>`                | kernel ↔ .res reconciliation             |
| `lsblk`                              | block-device listing (CAP_SYS_ADMIN?)   |
| `lvs / vgs / pvs`                    | LVM utilities                           |
| `zfs list / zpool list`              | ZFS utilities (add to cheat-sheet for blockstor) |

Test: a smoke script `tests/e2e/satellite-utils-smoke.sh` enters
each satellite pod in turn, runs each command, and asserts no
"command not found". Catches missing utilities in the satellite
container image at build time.

**Failure modes:**
- `lvs` missing from the satellite image when the cluster uses
  LVM_THIN pools → operator can't debug pool state
- `dmesg` returns permission-denied → satellite pod missing
  `CAP_SYSLOG`
- `drbdadm adjust` errors with "config not found" → satellite's
  `.res` directory isn't `/etc/drbd.d/` (path mismatch with upstream
  cheat sheet's `/var/lib/linstor.d/`)

---

## blockstor adaptations to the cheat sheet

These document **where blockstor's UX diverges from the
upstream cheat sheet**, so we can either align or document the
delta.

### A. `.res` file location

Upstream: `/var/lib/linstor.d/<rd>.res`
blockstor: `/etc/drbd.d/<rd>.res`

The cheat sheet's `cat /var/lib/linstor.d/pvc-*.res` won't find
files for blockstor users. Either:
- (operator doc) Update the blockstor-specific runbook to use
  `/etc/drbd.d/`
- (code) Symlink `/var/lib/linstor.d/` → `/etc/drbd.d/` in the
  satellite container for muscle-memory compatibility

Test: `tests/e2e/cheat-sheet-paths.sh` — enter a satellite pod and
assert `cat /var/lib/linstor.d/*.res` works (symlink in place) OR
that the runbook documents the new path clearly.

---

### B. linstor controller pod name

Upstream: `kubectl exec -ti linstor-controller -- bash`
blockstor: `kubectl exec -n blockstor-system -ti deploy/blockstor-apiserver -- bash`

The deployment name is different (`blockstor-apiserver` vs
`linstor-controller`) and namespace is different.

Test: assert these specific commands work end-to-end against a
fresh stand:
```bash
kubectl exec -n blockstor-system -ti deploy/blockstor-apiserver -- linstor n l
```

If we want true cheat-sheet parity, consider:
- A `linstor-controller` Service alias for the apiserver
- A `linstor` symlink in the apiserver image's PATH that wraps the
  REST in a CLI-compatible shim

---

### C. linstor-node pod selector

Upstream: `kubectl get pod -l app=linstor-node -o wide`
blockstor: `kubectl get pod -n blockstor-system -l app=blockstor-satellite -o wide`

Same issue. Either rename the DaemonSet's `app` label to align,
or document the delta.

Test: assert the cheat-sheet selector AND the blockstor-specific
selector both return the same pod set (via dual label).

---

## Test harness skeleton

```bash
#!/usr/bin/env bash
# tests/e2e/cheat-sheet-flow-<scenario>.sh
set -euo pipefail
WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"
source "$(dirname "$0")/lib.sh"

# Helper: walk the cheat sheet from level 1 to level N until
# either an asserted condition is satisfied or N==3 (full descent).
walk_cheat_sheet() {
    local pvc=$1 want=$2
    # ... level-1 assertions ...
    # ... level-2 assertions ...
    # ... level-3 assertions ...
}
```

Plus `lib.sh` additions:

```bash
# Get the satellite pod running on a specific worker node.
satellite_pod_on() {
    kubectl get pod -n blockstor-system -l app=blockstor-satellite \
        --field-selector spec.nodeName="$1" \
        -o jsonpath='{.items[0].metadata.name}'
}

# Exec into the satellite pod for a worker.
satellite_exec() {
    local node=$1; shift
    local pod
    pod=$(satellite_pod_on "$node")
    kubectl exec -n blockstor-system "$pod" -- "$@"
}
```

---

## Priority

| Test | Group | Priority | Why |
|------|-------|----------|-----|
| #1   | 3-way consistency happy path | **High** | Foundational; every other test assumes this works |
| #2   | K8s-side fault narrowing | High | Most common operator scenario |
| #4   | LINSTOR ↔ Node bridge | High | This session's observer cleanup fix exercises this |
| #5   | r delete is observable everywhere | High | Closes the force-strip aftermath loop |
| #6   | drbdadm down recovery | High | Reconciler invariant |
| #8   | CSI ↔ DRBD permission-denied chain | Medium | Diskless-attach edge case |
| #11/12 | Command-completeness audit | Medium | One-shot smoke check |
| #3   | LINSTOR-side fault narrowing | Medium | Less common in practice |
| #7   | primary --force not auto-undone | Medium | Emergency recovery path |
| #9   | Pool capacity correlation | Low | Easy to spot via single-level inspection |
| A–C  | Cheat-sheet path / pod-name deltas | Low | Doc fix, not a behaviour test |

---

## How the three docs fit together

```
tests/linstor-cli-scenarios.md         operator happy-path workflows
                                       (provisioning, listing, deleting)

tests/drbd-troubleshooting-scenarios.md kernel-level failure modes
                                       (states, recovery, split-brain)

tests/observability-cheat-sheet-scenarios.md     three-level narrowing flow
                                       (this file)
```

Most blockstor tests in `tests/e2e/` already cover one slice. The
gap these three docs identify: we've been testing isolated layers,
not the cross-layer consistency that operators rely on when
debugging. The High-priority tests above (#1, #2, #4, #5, #6) close
that gap.
