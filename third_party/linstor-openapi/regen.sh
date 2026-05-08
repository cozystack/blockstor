#!/usr/bin/env bash
#
# Regenerate pkg/api/openapi/types.gen.go from upstream LINSTOR's
# rest_v1_openapi.yaml. The upstream spec uses the same identifier
# for parameters and schemas (e.g. NetInterface, Node, StoragePool),
# which oapi-codegen rejects with "duplicate typename". We rename
# every entry under components.parameters to <Name>Param and patch
# all $ref's that point at them, then run oapi-codegen.
#
# Run from the repo root:  third_party/linstor-openapi/regen.sh
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

UPSTREAM_URL=${UPSTREAM_URL:-https://raw.githubusercontent.com/LINBIT/linstor-server/master/docs/rest_v1_openapi.yaml}
SPEC=third_party/linstor-openapi/rest_v1_openapi.yaml
CONFIG=third_party/linstor-openapi/oapi-codegen.yaml
OUT=pkg/api/openapi/types.gen.go

command -v yq           >/dev/null || { echo "need yq (v4)"; exit 1; }
command -v oapi-codegen >/dev/null || command -v "$(go env GOPATH)/bin/oapi-codegen" >/dev/null \
    || { echo "need oapi-codegen on PATH or in GOPATH/bin"; exit 1; }

OAPI=$(command -v oapi-codegen || echo "$(go env GOPATH)/bin/oapi-codegen")

echo ">> downloading $UPSTREAM_URL"
curl -fsSL "$UPSTREAM_URL" -o "$SPEC"

echo ">> renaming components.parameters keys to *Param + updating refs"
yq -i '.components.parameters |= with_entries(.key += "Param")' "$SPEC"
sed -E -i.bak "s|(components/parameters/)([A-Za-z]+)'|\1\2Param'|g" "$SPEC"
rm -f "$SPEC.bak"

echo ">> running oapi-codegen → $OUT"
mkdir -p "$(dirname "$OUT")"
"$OAPI" -config "$CONFIG" -o "$OUT" "$SPEC"

echo ">> done; commit $SPEC + $OUT together"
