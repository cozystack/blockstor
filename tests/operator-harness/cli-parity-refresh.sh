#!/usr/bin/env bash
#
# cli-parity-refresh.sh — re-runnable CLI parity diff between blockstor (BS)
# REST and an upstream LINSTOR (UP) reference controller.
#
# Replaces the one-shot docs/cli-parity-audit-2026-05-14.md with a script
# that emits the same Markdown table from a live run. Designed to be wired
# into nightly CI on the stand so any new behavioural delta between BS
# and upstream linstor surfaces immediately — instead of two weeks later
# when an operator hits it by hand.
#
# Usage:
#
#   BS_URL=http://127.0.0.1:3370 \
#   UP_URL=http://127.0.0.1:3371 \
#   tests/operator-harness/cli-parity-refresh.sh /tmp/cli-parity
#
# Arguments:
#
#   $1   — work_dir (mandatory). Every command's stdout/stderr/exit/JSON
#          dump lands under $work_dir/raw/<NN>-<slug>.{bs,up}.{out,err,code,json}
#          so a human can re-inspect on the dev box without re-running.
#
# Environment knobs:
#
#   BS_URL                   blockstor controller URL          (required)
#   UP_URL                   upstream linstor controller URL   (required)
#   BS_PROFILE               linstor CLI profile name for BS   (default: bs)
#   UP_PROFILE               linstor CLI profile name for UP   (default: up)
#   PARITY_PREFIX            prefix for ephemeral test objects (default: parity)
#   PARITY_NODE              real satellite node used in seed  (default: discover via "n l")
#   PARITY_SP                storage pool name on BS           (default: stand)
#   PARITY_UP_SP             storage pool name on UP           (default: pool)
#   KNOWN_DELTAS_FILE        accept-list path                  (default: docs/cli-parity-known-deltas.md)
#   REPORT_FILE              report output path                (default: $work_dir/cli-parity-<date>.md)
#   SKIP_SEED                if non-empty, do not seed objects (assume caller did)
#   SKIP_TEARDOWN            if non-empty, do not delete seeds (operator debug)
#
# Exit codes:
#
#   0   — every non-PARITY row in the generated report is whitelisted in
#         docs/cli-parity-known-deltas.md (or there are no non-PARITY rows).
#   1   — at least one non-PARITY row is NOT whitelisted. The report path
#         is printed to stderr; CI MUST fail.
#   2   — usage / config error (missing flags, controller unreachable,
#         seed step failed before any compare ran).
#
# How rows are tagged (must match docs/cli-parity-audit-*.md legend):
#
#   PARITY          identical exit + equivalent semantic stdout/JSON.
#   WIRE_SHAPE      JSON has different schema (missing/extra keys); CLI
#                   table may still render but information is lost.
#   ERROR_TEXT      exit code matches but message/structure differs.
#   MISSING_FEATURE BS returns OK but skips a real side-effect upstream
#                   performs (or returns empty list where upstream has
#                   data).
#   CLI_BUG         both sides fail identically because the CLI itself
#                   is wrong (argparse, etc.) — counted as PARITY for
#                   gating purposes.
#
# This script DOES NOT execute side-effectful flows itself (no autoplace
# loops, no snapshot waits). It compares response shapes. The replay/*
# YAMLs are the place for "behaviour" assertions over time.

set -euo pipefail

# ----------------------------------------------------------------------
# argument & env validation
# ----------------------------------------------------------------------

WORK_DIR=${1:?usage: cli-parity-refresh.sh <work_dir>}
mkdir -p "$WORK_DIR/raw"

: "${BS_URL:?BS_URL required (e.g. http://127.0.0.1:3370)}"
: "${UP_URL:?UP_URL required (e.g. http://127.0.0.1:3371)}"
BS_PROFILE=${BS_PROFILE:-bs}
UP_PROFILE=${UP_PROFILE:-up}
PARITY_PREFIX=${PARITY_PREFIX:-parity}
PARITY_SP=${PARITY_SP:-stand}
PARITY_UP_SP=${PARITY_UP_SP:-pool}
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
KNOWN_DELTAS_FILE=${KNOWN_DELTAS_FILE:-$REPO_ROOT/docs/cli-parity-known-deltas.md}
REPORT_FILE=${REPORT_FILE:-$WORK_DIR/cli-parity-$(date +%Y-%m-%d).md}

if ! command -v linstor >/dev/null 2>&1; then
    echo "FATAL: linstor CLI not on PATH (apt install linstor-client)" >&2
    exit 2
fi

if [[ ! -f "$KNOWN_DELTAS_FILE" ]]; then
    echo "FATAL: known-deltas whitelist not found: $KNOWN_DELTAS_FILE" >&2
    echo "       create it (see docs/cli-parity-known-deltas.md template)" >&2
    exit 2
fi

# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------

bs() { linstor --controllers "$BS_URL" "$@"; }
up() { linstor --controllers "$UP_URL" "$@"; }

# slugify a command for the filename. "n l --pastable" -> "n-l--pastable"
slugify() {
    local s=$1
    s=${s// /-}
    s=${s//\//_}
    s=${s//\"/}
    s=${s//\'/}
    echo "$s"
}

# Run one CLI invocation against one side, capture exit / stdout / stderr,
# and (best-effort) capture the JSON wire shape via --output-fmt=json.
# Side = "bs" or "up". Index is the row number (zero-padded).
# Args after side/index are passed verbatim to the linstor CLI.
capture() {
    local side=$1 idx=$2 slug=$3
    shift 3
    local base="$WORK_DIR/raw/${idx}-${slug}.${side}"
    local rc=0
    if [[ "$side" == "bs" ]]; then
        bs "$@" >"$base.out" 2>"$base.err" || rc=$?
        # Best-effort JSON dump. linstor returns 0 on commands that do
        # not implement --output-fmt=json; we accept whatever lands.
        bs --output-fmt=json "$@" >"$base.json" 2>/dev/null || true
    else
        up "$@" >"$base.out" 2>"$base.err" || rc=$?
        up --output-fmt=json "$@" >"$base.json" 2>/dev/null || true
    fi
    echo "$rc" >"$base.code"
}

# Compare two capture artefacts. Emit (to stdout):
#   <tag>\t<short reason>
# tag ∈ {PARITY, WIRE_SHAPE, ERROR_TEXT, MISSING_FEATURE, CLI_BUG}
classify() {
    local idx=$1 slug=$2
    local bs_out="$WORK_DIR/raw/${idx}-${slug}.bs.out"
    local up_out="$WORK_DIR/raw/${idx}-${slug}.up.out"
    local bs_err="$WORK_DIR/raw/${idx}-${slug}.bs.err"
    local up_err="$WORK_DIR/raw/${idx}-${slug}.up.err"
    local bs_code="$WORK_DIR/raw/${idx}-${slug}.bs.code"
    local up_code="$WORK_DIR/raw/${idx}-${slug}.up.code"
    local bs_json="$WORK_DIR/raw/${idx}-${slug}.bs.json"
    local up_json="$WORK_DIR/raw/${idx}-${slug}.up.json"

    local bs_rc up_rc
    bs_rc=$(cat "$bs_code" 2>/dev/null || echo "?")
    up_rc=$(cat "$up_code" 2>/dev/null || echo "?")

    # Crash on python-linstor parse error → ALWAYS a WIRE_SHAPE failure.
    if grep -qiE 'xml.*ParseError|Traceback|HTTPConnectionPool' "$bs_err" 2>/dev/null; then
        printf '%s\t%s\n' "WIRE_SHAPE" "python-linstor crashed parsing BS response"
        return
    fi

    # Both sides fail identically — CLI bug, not a controller delta.
    if [[ "$bs_rc" != "0" && "$bs_rc" == "$up_rc" ]]; then
        printf '%s\t%s\n' "CLI_BUG" "both sides exit $bs_rc identically"
        return
    fi

    # Exit codes diverge → ERROR_TEXT (or MISSING_FEATURE if one side empty).
    if [[ "$bs_rc" != "$up_rc" ]]; then
        printf '%s\t%s\n' "ERROR_TEXT" "exit codes differ: bs=$bs_rc up=$up_rc"
        return
    fi

    # Same exit code, both succeeded. Now compare wire shape.
    # 1. JSON top-level key sets:
    if [[ -s "$bs_json" && -s "$up_json" ]] && command -v jq >/dev/null 2>&1; then
        local bs_keys up_keys
        bs_keys=$(jq -r 'if type=="array" then (.[0] // {}) else . end | keys_unsorted // [] | join(",")' "$bs_json" 2>/dev/null || echo "")
        up_keys=$(jq -r 'if type=="array" then (.[0] // {}) else . end | keys_unsorted // [] | join(",")' "$up_json" 2>/dev/null || echo "")
        if [[ -n "$bs_keys" && -n "$up_keys" && "$bs_keys" != "$up_keys" ]]; then
            printf '%s\t%s\n' "WIRE_SHAPE" "JSON keys differ (bs:[$bs_keys] up:[$up_keys])"
            return
        fi
    fi

    # 2. Empty-vs-non-empty stdout: classic MISSING_FEATURE.
    local bs_lines up_lines
    bs_lines=$(wc -l <"$bs_out" 2>/dev/null || echo 0)
    up_lines=$(wc -l <"$up_out" 2>/dev/null || echo 0)
    if [[ "$bs_lines" -lt 2 && "$up_lines" -gt 2 ]]; then
        printf '%s\t%s\n' "MISSING_FEATURE" "bs returned empty list, up returned $up_lines lines"
        return
    fi

    # 3. Plain stdout diff (modulo whitespace).
    if ! diff -q -B -w "$bs_out" "$up_out" >/dev/null 2>&1; then
        printf '%s\t%s\n' "WIRE_SHAPE" "stdout differs (see diff $bs_out vs $up_out)"
        return
    fi

    printf '%s\t%s\n' "PARITY" "identical exit + stdout"
}

# ----------------------------------------------------------------------
# command catalogue
# ----------------------------------------------------------------------
#
# Format: one entry per line: "<id>|<cmd...>". <id> is a stable index
# (zero-padded) so a row can be referenced from known-deltas.md. <cmd>
# is the argv as it would be passed to `linstor`.
#
# Keep this list aligned with the original docs/cli-parity-audit-2026-05-14.md
# numbering so historical rows stay traceable.

read -r -d '' COMMANDS <<'EOF' || true
01|n l
02|n l -p
03|sp l
04|sp l --show-props StorDriver/*
05|rg l
06|rd l
07|rd l --resource-definitions PARITY_RD
08|r l
09|r l --faulty
10|vd l
11|vd l --resource-definitions PARITY_RD
12|v l
13|v l --resources PARITY_RD
14|s l
16|ps l
17|err l
18|controller version
19|controller list-properties
20|rg l --pastable
21|advise r
22|advise rd
30|sp c X dup --provider-kind LVM_THIN
31|rd c ""
32|r c PARITY_RD --auto-place 99
33|s d PARITY_RD nonexistent-snap
40|n c PARITY_FAKE 10.99.99.99 --node-type Satellite
42|r d PARITY_RD PARITY_NONEXISTENT_NODE
50|node info
51|resource-connection list PARITY_RD
52|exos defaultUser
53|backup l
54|schedule l
55|key-value-store list
EOF

# ----------------------------------------------------------------------
# seed
# ----------------------------------------------------------------------

seed() {
    [[ -n "${SKIP_SEED:-}" ]] && return 0

    echo ">> seeding ephemeral objects on BS and UP (prefix=$PARITY_PREFIX)"

    bs resource-group create "${PARITY_PREFIX}-rg" --place-count 2 --storage-pool "$PARITY_SP" >/dev/null 2>&1 || true
    up resource-group create "${PARITY_PREFIX}-rg" --place-count 2 --storage-pool "$PARITY_UP_SP" >/dev/null 2>&1 || true

    bs resource-definition create "${PARITY_PREFIX}-rd" --resource-group "${PARITY_PREFIX}-rg" >/dev/null 2>&1 || true
    up resource-definition create "${PARITY_PREFIX}-rd" --resource-group "${PARITY_PREFIX}-rg" >/dev/null 2>&1 || true

    bs volume-definition create "${PARITY_PREFIX}-rd" 16M >/dev/null 2>&1 || true
    up volume-definition create "${PARITY_PREFIX}-rd" 16M >/dev/null 2>&1 || true
}

teardown() {
    [[ -n "${SKIP_TEARDOWN:-}" ]] && return 0
    echo ">> tearing down ephemeral objects"
    bs resource-definition delete "${PARITY_PREFIX}-rd" >/dev/null 2>&1 || true
    up resource-definition delete "${PARITY_PREFIX}-rd" >/dev/null 2>&1 || true
    bs resource-group delete   "${PARITY_PREFIX}-rg" >/dev/null 2>&1 || true
    up resource-group delete   "${PARITY_PREFIX}-rg" >/dev/null 2>&1 || true
    bs node delete "${PARITY_PREFIX}-fake" >/dev/null 2>&1 || true
    up node delete "${PARITY_PREFIX}-fake" >/dev/null 2>&1 || true
}
trap teardown EXIT

# Sanity: both controllers must be reachable before we do any work.
if ! bs node list >/dev/null 2>&1; then
    echo "FATAL: BS controller $BS_URL not reachable via linstor CLI" >&2
    exit 2
fi
if ! up node list >/dev/null 2>&1; then
    echo "FATAL: UP controller $UP_URL not reachable via linstor CLI" >&2
    exit 2
fi

seed

# ----------------------------------------------------------------------
# run + classify
# ----------------------------------------------------------------------

# Build report header.
{
    echo "# CLI parity report (auto-generated)"
    echo
    echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "- BS controller: $BS_URL"
    echo "- UP controller: $UP_URL"
    echo "- Raw artefacts: $WORK_DIR/raw/"
    echo
    echo "Tag legend (matches docs/cli-parity-audit-*.md):"
    echo "PARITY / WIRE_SHAPE / ERROR_TEXT / MISSING_FEATURE / CLI_BUG."
    echo
    echo "| # | Command | UP exit | BS exit | Tag | Notes |"
    echo "|---|---------|---------|---------|-----|-------|"
} >"$REPORT_FILE"

NEW_DELTAS=()

while IFS= read -r entry; do
    [[ -z "$entry" ]] && continue
    idx=${entry%%|*}
    raw_cmd=${entry#*|}

    # Substitute placeholders.
    cmd=${raw_cmd//PARITY_RD/${PARITY_PREFIX}-rd}
    cmd=${cmd//PARITY_FAKE/${PARITY_PREFIX}-fake}
    cmd=${cmd//PARITY_NONEXISTENT_NODE/nonexistent-node}

    slug=$(slugify "$cmd")

    # shellcheck disable=SC2086
    capture bs "$idx" "$slug" $cmd
    # shellcheck disable=SC2086
    capture up "$idx" "$slug" $cmd

    bs_rc=$(cat "$WORK_DIR/raw/${idx}-${slug}.bs.code" 2>/dev/null || echo "?")
    up_rc=$(cat "$WORK_DIR/raw/${idx}-${slug}.up.code" 2>/dev/null || echo "?")

    result=$(classify "$idx" "$slug")
    tag=${result%%$'\t'*}
    notes=${result#*$'\t'}

    printf '| %s | `%s` | %s | %s | **%s** | %s |\n' \
        "$idx" "$cmd" "$up_rc" "$bs_rc" "$tag" "$notes" >>"$REPORT_FILE"

    if [[ "$tag" != "PARITY" && "$tag" != "CLI_BUG" ]]; then
        # Check whitelist. We accept a match if the row id appears in
        # known-deltas.md OR an exact (command,tag) row appears.
        if ! grep -qE "^\| *${idx} *\|" "$KNOWN_DELTAS_FILE" \
            && ! grep -qE "\`${cmd//\//\\/}\`.*\| *${tag} *\|" "$KNOWN_DELTAS_FILE"; then
            NEW_DELTAS+=("$idx $cmd → $tag: $notes")
        fi
    fi
done <<<"$COMMANDS"

# ----------------------------------------------------------------------
# verdict
# ----------------------------------------------------------------------

{
    echo
    if [[ ${#NEW_DELTAS[@]} -eq 0 ]]; then
        echo "## Verdict"
        echo
        echo "All non-PARITY rows accounted for in $KNOWN_DELTAS_FILE."
    else
        echo "## Verdict — NEW DELTAS DETECTED"
        echo
        echo "The following rows tagged non-PARITY are NOT whitelisted in"
        echo "$KNOWN_DELTAS_FILE. Either fix the underlying BS behaviour"
        echo "or add an explicit row to the whitelist with justification."
        echo
        for d in "${NEW_DELTAS[@]}"; do
            echo "- $d"
        done
    fi
} >>"$REPORT_FILE"

echo
echo "Report: $REPORT_FILE"

if [[ ${#NEW_DELTAS[@]} -gt 0 ]]; then
    echo "FAIL: ${#NEW_DELTAS[@]} new non-PARITY delta(s) — see $REPORT_FILE" >&2
    exit 1
fi

echo "OK: every delta is whitelisted."
