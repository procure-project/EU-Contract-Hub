#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
#  export_dashboards.sh
#  Exports every OpenSearch Dashboards saved object (dashboards,
#  visualizations, index patterns, saved searches...) from every
#  tenant into dashboards/<tenant>.ndjson, so they can live in git.
#
#  Run on the server, from the repo root:   ./export_dashboards.sh
#  Restore a file:   curl -k -u user:pass -X POST \
#     "$DASHBOARDS_URL/api/saved_objects/_import?overwrite=true" \
#     -H "osd-xsrf: true" -H "securitytenant: <tenant>" \
#     --form file=@dashboards/<tenant>.ndjson
#
#  Settings (from .env or the environment):
#    OPENSEARCH_USERNAME / OPENSEARCH_PASSWORD   (required)
#    OPENSEARCH_URL    default https://localhost:9200
#    DASHBOARDS_URL    default https://localhost:5601
#    TENANTS           space-separated list; default: ask OpenSearch
# ──────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -f .env ]]; then
    set -a; source .env; set +a
fi

: "${OPENSEARCH_USERNAME:?Set OPENSEARCH_USERNAME in .env}"
: "${OPENSEARCH_PASSWORD:?Set OPENSEARCH_PASSWORD in .env}"
OPENSEARCH_URL="${OPENSEARCH_URL:-https://localhost:9200}"
DASHBOARDS_URL="${DASHBOARDS_URL:-https://localhost:5601}"
OUT_DIR="$SCRIPT_DIR/dashboards"
AUTH="$OPENSEARCH_USERNAME:$OPENSEARCH_PASSWORD"

# Every saved-object type OpenSearch Dashboards can export.
TYPES='["config","url","index-pattern","query","search","visualization","visualization-visbuilder","augment-vis","dashboard","observability-panel","observability-visualization","observability-savedquery"]'

if [[ -z "${TENANTS:-}" ]]; then
    # Custom tenants come from the security plugin; global and private always exist.
    custom=$(curl -sk -u "$AUTH" "$OPENSEARCH_URL/_plugins/_security/api/tenants" \
        | python3 -c 'import json,sys
try:
    print(" ".join(k for k in json.load(sys.stdin) if k != "global_tenant"))
except Exception:
    pass')
    TENANTS="global __user__ $custom"
fi

mkdir -p "$OUT_DIR"
for tenant in $TENANTS; do
    name="$tenant"; [[ "$tenant" == "__user__" ]] && name="private_${OPENSEARCH_USERNAME}"
    out="$OUT_DIR/$name.ndjson"
    echo "Exporting tenant '$tenant' -> $out"

    # Ask for each type separately: an export request fails as a whole if one
    # type is not installed on this Dashboards version.
    : > "$out.tmp"
    for type in $(python3 -c "import json; print(' '.join(json.loads('$TYPES')))"); do
        code=$(curl -sk -u "$AUTH" -o "$out.part" -w '%{http_code}' \
            -X POST "$DASHBOARDS_URL/api/saved_objects/_export" \
            -H "osd-xsrf: true" -H "Content-Type: application/json" \
            -H "securitytenant: $tenant" \
            -d "{\"type\":[\"$type\"],\"includeReferencesDeep\":true,\"excludeExportDetails\":true}")
        if [[ "$code" == "200" ]]; then
            cat "$out.part" >> "$out.tmp"; echo >> "$out.tmp"
        elif [[ "$code" == "401" || "$code" == "403" ]]; then
            echo "  ! HTTP $code for tenant '$tenant': check credentials / tenant access" >&2
            break
        fi
        # Other codes (400 for unknown types) are skipped on purpose.
    done
    rm -f "$out.part"

    # Drop blank lines and duplicates (references are pulled in by several types),
    # then sort so git diffs stay small between exports.
    python3 - "$out.tmp" "$out" <<'PY'
import json, sys
seen = {}
for line in open(sys.argv[1]):
    line = line.strip()
    if not line:
        continue
    obj = json.loads(line)
    seen[(obj.get("type"), obj.get("id"))] = obj
with open(sys.argv[2], "w") as f:
    for key in sorted(seen, key=lambda k: (str(k[0]), str(k[1]))):
        f.write(json.dumps(seen[key], sort_keys=True, ensure_ascii=False) + "\n")
counts = {}
for t, _ in seen:
    counts[t] = counts.get(t, 0) + 1
print("  " + (", ".join(f"{n} {t}" for t, n in sorted(counts.items())) or "nothing exported"))
PY
    rm -f "$out.tmp"
    [[ -s "$out" ]] || rm -f "$out"
done

echo "Done. Review and commit:  git add dashboards && git commit -m 'Export dashboards'"
