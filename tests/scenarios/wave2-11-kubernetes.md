# Wave 2 — Group 11 — Kubernetes integration (Day2 ops)

K8s-specific Day2 scenarios: external controller pointer, HA fast
failover, RWX PVC (NFS+drbd-reactor), Affinity Controller for PV node
affinity, K8s evacuate via operator label, set DRBD options via
node-connection from K8s, K8s VolumeSnapshot CRUD.

Wave1's `03-networking.md` covers the host-network ↔ container-network
DRBD switch (wave2-03 mirrors this). Wave2-02 covers StorageClass
locality / zone parameters.

[Group index in README.md](README.md).

---

## Controller deployment models

### 11.W01 `externalController.url` points operator to bare-metal controller — O

- **Priority:** —  **Target:** —  **Complexity:** —
- **Source:** linstor-kubernetes.adoc §"Operator v2 deployment with an external LINSTOR controller" (lines 1441-1538) via tests/scenarios/day2-k8s-external-controller.md

**Out of scope for blockstor.** Cozystack runs blockstor in-cluster — the entire reason blockstor exists is replacing the bare-metal controller. Pin in `out-of-scope.md`. Test stance: piraeus CR with `externalController.url` is allowed (operator wiring) but blockstor's apiserver is unaffected.

### 11.W02 HA Controller for fast pod failover (taint within ~30s) — P

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"Fast workload failover using the high availability controller" (lines 1383-1421) via tests/scenarios/day2-k8s-ha-controller-fast-failover.md

`linstor-ha-controller` adds `node.kubernetes.io/out-of-service` taint within seconds (instead of K8s default 5min `tolerationSeconds`). Independent of the blockstor apiserver; operates on K8s scheduling.

**E2E:** install ha-controller via helm; power off worker-3; affected Pods rescheduled to surviving workers within ~30s; PVCs that had a replica on worker-3 still have N-1 UpToDate replicas via blockstor's tiebreaker + auto-place.

## Workload patterns

### 11.W03 RWX PVC via NFS + drbd-reactor — T

- **Priority:** P2  **Target:** e2e  **Complexity:** H (verify implementation first)
- **Source:** linstor-kubernetes.adoc §"ReadWriteMany volume access" (lines 2635-2696) via tests/scenarios/day2-k8s-rwx-pvc.md

Operator-level integration: piraeus + drbd-reactor manages NFS export over a DRBD PV. blockstor's job: provision the underlying DRBD resource per SC's `autoPlace ≥ 2`. **Pre-3-node clusters:** SC must explicitly set `DrbdOptions/Resource/quorum: majority` (otherwise defaults `off` and risks split-brain).

**E2E:** SC + RWX PVC; two Pods on different nodes simultaneously mount + write — both succeed via NFS. Performance is sub-RWO (expected). Cross-listed with wave2-07 monitoring.

### 11.W04 LINSTOR Affinity Controller updates PV node affinity — P

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"LINSTOR affinity controller" (lines 2697-2718) via tests/scenarios/day2-k8s-affinity-controller-deploy.md

Without this, PV node-affinity is set ONCE at creation; later replica moves don't update the PV → Pods can't reschedule to nodes that now have replicas. Affinity Controller watches LINSTOR moves + rewrites PV affinity.

**E2E:** install via helm; evacuate worker-3 (per wave1 4.20); affected PVs' `nodeAffinity` updated to reflect new replica nodes; a Pod referencing the PV schedules onto the new node.

### 11.W05 K8s evacuate via operator label + `linstorcluster` affinity — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"Deleting a LINSTOR node in Kubernetes" (lines 2976-3026) via tests/scenarios/day2-node-delete-via-operator-label.md

Operator-mediated flow: `kubectl label node <name> marked-for-deletion=` + edit `linstorclusters.linstorcluster.spec.nodeAffinity` to exclude the label → operator internally calls `linstor node evacuate` → waits for `EvacuationCompleted` condition → safe to `kubectl delete node`.

**E2E:** label + edit CR; assert condition transitions; final `linstor node list` doesn't contain the node; PVCs that had replicas on the node still have replica count restored.

### 11.W06 K8s evacuate via cordon + drain + `linstor node evacuate` — S

- **Priority:** P1  **Target:** e2e  **Complexity:** M
- **Source:** linstor-kubernetes.adoc §"Evacuating a node in Kubernetes" (lines 2949-2974) via tests/scenarios/day2-node-evacuate-kubernetes.md

Manual flow (vs 11.W05 operator-mediated). Sequence: `kubectl cordon` → `kubectl drain --ignore-daemonsets` → `linstor node evacuate` → `linstor node delete`. Tunable order: evacuate-then-drain avoids pod pause when a Pod's only replica was on the drained node.

## DRBD options from K8s

### 11.W07 Set DRBD node-connection options via kubectl-exec or CRD — P

- **Priority:** P2  **Target:** e2e  **Complexity:** L
- **Source:** linstor-kubernetes.adoc §"Setting DRBD options on a LINSTOR node connection in Kubernetes" (lines 1882-1925) via tests/scenarios/day2-k8s-set-drbd-options-via-node-connection.md

`kubectl -n linbit-sds exec deploy/blockstor-apiserver -- linstor node-connection drbd-peer-options --max-buffers 8192 worker-1 worker-2` — same surface as wave2-05 5.W03, plus declarative `LinstorClusterConfiguration` CRD path if exposed by the Operator. Hierarchy: RD > RG > resource-connection > node-connection > controller.

## K8s VolumeSnapshot CRUD

### 11.W08 CSI `VolumeSnapshot` create — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** linstor-kubernetes.adoc §"Working with snapshots" (lines 2214-2335), §"Creating a snapshot" (lines 2270-2302) via tests/scenarios/day2-snapshot-create-kubernetes.md

`VolumeSnapshotClass` driver=`linstor.csi.linbit.com` + `VolumeSnapshot` referencing PVC. csi-snapshotter calls blockstor's `CreateSnapshot` (wave1 1.13). Wait for `readyToUse=true`.

**E2E:** apply CRDs; PVC with snapshot-capable SP; `kubectl wait volumesnapshot ... --for=jsonpath='{.status.readyToUse}'=true`; `linstor s l` shows `State=Successful`.

### 11.W09 CSI `VolumeSnapshot` restore into new PVC — S

- **Priority:** P0  **Target:** e2e  **Complexity:** L
- **Source:** linstor-kubernetes.adoc §"Restoring a snapshot" (lines 2338-2386) via tests/scenarios/day2-snapshot-restore-kubernetes.md

Cross-listed with wave1 4.14. PVC `dataSource` referencing `VolumeSnapshot` → CSI calls blockstor's `snapshot resource restore` (wave2-08 8.W03) under the hood. New PVC must be ≥ snapshot size. Snapshot must already be `readyToUse=true`.

**E2E:** scale-down deploy → delete old PVC (snapshot persists) → recreate PVC with `dataSource: VolumeSnapshot` → scale-up; data hash matches pre-snapshot.

---

## Group summary

| Tag | Count |
|-----|------:|
| P0 e2e | 2 |
| P1 e2e | 3 |
| P2 (any) | 2 |
| T (verify first) | 1 |
| O (out of scope) | 1 |
