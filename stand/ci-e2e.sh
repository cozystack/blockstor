#!/usr/bin/env bash
#
# usage: ci-e2e.sh LANE LANES SCENARIO [SCENARIO ...]
#
# CI entry point for the real QEMU/Talos e2e suite. Provisions a Talos
# cluster on a KVM-capable GitHub Actions runner (the cozystack
# oracle-vm pool exposes /dev/kvm, same as the dev stand), deploys
# blockstor, and runs THIS lane's share of the scenario list.
#
# Relationship to the dev-stand flow: this is the same pipeline a
# maintainer runs by hand —
#     make build-images && make up NAME=<n> && make blockstor NAME=<n>
#     && make pools NAME=<n> && stand/run-scenarios-only.sh <n> <scen...>
# — wrapped for an ephemeral CI runner. The differences vs the dev
# stand are deliberate and limited to the runner environment:
#   1. We do NOT run stand/setup-host.sh: its `mkfs.xfs` on "the largest
#      unused block device" is unsafe on a shared CI runner. We install
#      only the qemu userspace + the FORWARD iptables allow the Talos
#      qemu provisioner needs.
#   2. .work lives on the runner's own disk (no NVMe symlink) — a CI
#      runner has a single disk and the cluster is torn down at the end.
#   3. VM disks are sized down (EXTRA_DISK_SIZE_MB) to fit a CI runner.
#   4. A throwaway localhost:5000 registry is started for
#      build-images.sh; the cluster pulls it via the bridge gateway
#      exactly as on the dev stand (see stand/up.sh registry mirror).
#
# Scenario sharding: the full scenario list is passed as arguments and
# this lane runs every LANES-th entry starting at index (LANE-1). To
# widen coverage, add lanes to the workflow matrix and bump LANES — the
# round-robin here then hands each lane its 1/N share automatically; no
# change to this script is required.
set -uo pipefail

LANE=${1:?LANE required (1-based)}
LANES=${2:?LANES required (total lane count)}
shift 2
ALL_SCENARIOS=("$@")

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"
# run-scenarios-only.sh hardcodes `cd ~/blockstor` (its dev-stand home);
# point it at this checkout instead — on a CI runner the repo lives in
# $GITHUB_WORKSPACE, not ~/blockstor.
export BS_REPO="$REPO_ROOT"

# No explicit scenarios → discover the WHOLE suite via `make e2e-list`
# (= ls tests/e2e/*.sh). That listing is the single source of truth: a
# new test file is picked up automatically and sharded onto a lane —
# nothing in the workflow needs updating when a scenario is added. Pass
# an explicit list only to scope a subset (dev/debug).
if [ ${#ALL_SCENARIOS[@]} -eq 0 ]; then
    mapfile -t ALL_SCENARIOS < <(make -s e2e-list)
fi
[ ${#ALL_SCENARIOS[@]} -gt 0 ] || { echo "FATAL: make e2e-list returned no scenarios" >&2; exit 2; }

STAND="ci-lane${LANE}"
REGISTRY_CONTAINER="blockstor-ci-registry"

# CI VM sizing (env-overridable). TWO extra disks per worker are
# REQUIRED: `make pools TYPE=both` puts a ZFS pool on the first and an
# LVM thin pool on the second; with only one, LVM device-discovery fell
# through to /dev/zram0 (not a valid PV) → `make pools` exit 5 → no
# StoragePools → every scenario aborted (first oracle run, 790b4b3a5).
# Each disk MUST be >= ~16 GiB: install-pools.sh's LVM thin pool is 1 GiB
# meta + 13 GiB data = 14 GiB, so an 8 GiB disk made `lvcreate -L 13G
# thin` fail (no space), leaving an orphan thin_meta that wedged the
# re-run with "thin_meta already exists" (exit 5). Match the dev stand's
# 16 GiB — disks are sparse, so the real footprint stays tiny until
# written. Still 3 workers (scenarios need 2 diskful + a diskless
# tiebreaker, or 3-way placement).
export EXTRA_DISKS=${EXTRA_DISKS:-2}
export EXTRA_DISK_SIZE_MB=${EXTRA_DISK_SIZE_MB:-16384}
CI_WORKERS=${CI_WORKERS:-3}

log() { echo ">> [$STAND] $*"; }

# ── teardown ────────────────────────────────────────────────────────
cleanup() {
    local rc=$?
    log "teardown (exit $rc)"
    make down NAME="$STAND" >/dev/null 2>&1 || true
    docker rm -f "$REGISTRY_CONTAINER" >/dev/null 2>&1 || true
    return $rc
}
trap cleanup EXIT

# ── 0. KVM sanity ───────────────────────────────────────────────────
if [ ! -e /dev/kvm ]; then
    echo "::error::/dev/kvm is missing — this runner has no (nested) virtualization; the qemu provisioner cannot run" >&2
    exit 1
fi
log "KVM ok: $(ls -l /dev/kvm)"

# ── 1. qemu userspace + provisioner networking ──────────────────────
# Only the bits stand/setup-host.sh installs that the qemu provisioner
# needs — no disk formatting, no go/helm (CI provides those separately).
if ! command -v qemu-system-x86_64 >/dev/null 2>&1; then
    log "installing qemu userspace"
    sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        qemu-kvm qemu-system-x86 qemu-utils ovmf \
        bridge-utils dnsmasq-base iproute2 socat conntrack ipset
fi

# The `linstor` CLI (python-linstor) for scenarios that drive the
# controller through the real client (`LCTL=(linstor --controllers ...)`,
# e.g. recovery-node-id-mismatch, recovery-inconsistent-blocking). The dev
# stand gets it from stand/setup-host.sh, which we deliberately skip here —
# so `linstor` is only present on a runner that happens to have a leftover
# copy. On a fresh ephemeral oracle runner it is absent and those scenarios
# die with `linstor: command not found` (non-deterministic per runner).
# Install it explicitly, pinned to match the workflow's other jobs
# (linstor_client.VERSION the integration harness asserts on).
if ! command -v linstor >/dev/null 2>&1; then
    log "installing linstor CLI (python-linstor)"
    python3 -m pip install --break-system-packages --upgrade \
        python-linstor==1.27.1 argcomplete
    python3 -m pip install --break-system-packages --no-deps \
        https://github.com/LINBIT/linstor-client/archive/refs/tags/v1.27.1.tar.gz
fi

# Ubuntu cloud images ship a catch-all REJECT in FORWARD; the talos
# qemu bridges (talos<hash>) need traffic forwarded. Mirrors setup-host.sh.
sudo iptables -P FORWARD ACCEPT 2>/dev/null || true
sudo iptables -D FORWARD -j REJECT --reject-with icmp-host-prohibited 2>/dev/null || true

# ── 2. throwaway local registry for build-images.sh ─────────────────
if ! docker ps --format '{{.Names}}' | grep -qx "$REGISTRY_CONTAINER"; then
    log "starting localhost:5000 registry"
    docker rm -f "$REGISTRY_CONTAINER" >/dev/null 2>&1 || true
    docker run -d -p 5000:5000 --name "$REGISTRY_CONTAINER" registry:2 >/dev/null
fi

# ── 3. build + push blockstor images (pins digests in .work/_factory) ─
log "building images"
make build-images

# ── 4. provision the Talos+QEMU cluster ─────────────────────────────
log "provisioning cluster (workers=$CI_WORKERS, extra-disks=$EXTRA_DISKS x ${EXTRA_DISK_SIZE_MB}MB)"
make up NAME="$STAND" WORKERS="$CI_WORKERS"

# ── 5. deploy blockstor + storage pools ─────────────────────────────
log "installing blockstor"
make blockstor NAME="$STAND"
log "provisioning pools"
make pools NAME="$STAND" TYPE=both

# ── 6. this lane's scenario shard (round-robin over LANES) ──────────
shard=()
for i in "${!ALL_SCENARIOS[@]}"; do
    if [ $(( i % LANES )) -eq $(( LANE - 1 )) ]; then
        shard+=("${ALL_SCENARIOS[$i]}")
    fi
done
[ ${#shard[@]} -gt 0 ] || { log "no scenarios for this lane — nothing to do"; exit 0; }
log "scenarios for this lane: ${shard[*]}"

bash stand/run-scenarios-only.sh "$STAND" "${shard[@]}" || true

# ── 7. verdict ──────────────────────────────────────────────────────
RESULTS="/tmp/e2e-$STAND.results"
echo "================ results ($STAND) ================"
cat "$RESULTS" 2>/dev/null || { echo "FATAL: results file $RESULTS missing"; exit 1; }
echo "=================================================="

reds=$(grep -cE '^(FAIL|TIMEOUT) ' "$RESULTS" 2>/dev/null || echo 0)
if grep -q '^FATAL' "$RESULTS" 2>/dev/null; then
    echo "::error::stand $STAND never became ready — see results above"
    exit 1
fi
if [ "$reds" -gt 0 ]; then
    echo "::error::$reds scenario(s) failed on lane $LANE"
    # Surface the tail of each failing scenario's log for quick triage.
    grep -E '^(FAIL|TIMEOUT) ' "$RESULTS" | awk '{print $2}' | while read -r sc; do
        sc_log="/tmp/e2e-$STAND-${sc//\//__}.log"
        echo "----- last 25 lines of $sc_log -----"
        tail -25 "$sc_log" 2>/dev/null || echo "(no log)"
    done
    exit 1
fi
log "all ${#shard[@]} scenario(s) passed"
