#!/usr/bin/env bash
set -uo pipefail

# Delete SonarQube projects concurrently.
# Usage: ./delete.sh [-t token] [-h host] [-p parallel] [--dry-run]
# -t token       Sonar token (env SONAR_TOKEN fallback)
# -h host        Sonar host (default: http://localhost:9001)
# -p parallel    Number of parallel jobs (default: 8)
# --dry-run     Print actions but don't call delete

SONAR_HOST=${SONAR_HOST:-http://localhost:9001}
PARALLEL=${PARALLEL:-8}
PAGE_SIZE=${PAGE_SIZE:-500}
DRY_RUN=0
TOKEN=${SONAR_TOKEN:-}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--token)      TOKEN="$2"; shift 2;;
        -h|--host)       SONAR_HOST="$2"; shift 2;;
        -p|--parallel)   PARALLEL="$2"; shift 2;;
        -s|--page-size)  PAGE_SIZE="$2"; shift 2;;
        --dry-run)       DRY_RUN=1; shift;;
        -?|--help)       cat <<'EOF'
-s, --page-size  Page size for API (default 500)
Usage: delete.sh [-t token] [-h host] [-p parallel] [--dry-run]

Deletes all projects returned by `api/projects/search` concurrently.
Options:
    -t, --token      Sonar token (or set SONAR_TOKEN env var)
    -h, --host       Sonar host URL (default http://localhost:9001)
    -p, --parallel   Number of concurrent jobs (default 8)
    --dry-run        Print what would be done without deleting
EOF
                        exit 0;;
        *)               echo "Unknown arg: $1" >&2; exit 1;;
    esac
done

if [[ -z "$TOKEN" ]]; then
    echo "Error: Sonar token not provided. Use -t or set SONAR_TOKEN." >&2
    exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "Error: 'jq' is required but not found. Install jq and try again." >&2
    exit 3
fi

if ! command -v curl >/dev/null 2>&1; then
    echo "Error: 'curl' is required but not found. Install curl and try again." >&2
    exit 4
fi

if ! command -v parallel >/dev/null 2>&1; then
    echo "Error: 'parallel' is required but not found. Install GNU parallel and try again." >&2
    exit 5
fi

echo "Fetching project list from $SONAR_HOST (page size = $PAGE_SIZE)..."
PROJECTS=""
PAGE=1
while true; do
    echo "  Page $PAGE..."
    RESP=$(curl -s -u "$TOKEN:" "$SONAR_HOST/api/projects/search?ps=$PAGE_SIZE&p=$PAGE")
    KEYS=$(echo "$RESP" | jq -r '.components[]?.key // empty')
    if [[ -z "$KEYS" ]]; then
        break
    fi
    PROJECTS="$PROJECTS\n$KEYS"
    COUNT_PAGE=$(echo "$KEYS" | sed '/^$/d' | wc -l)
    if [[ "$COUNT_PAGE" -lt "$PAGE_SIZE" ]]; then
        break
    fi
    PAGE=$((PAGE + 1))
done
PROJECTS=$(echo -e "$PROJECTS" | sed '/^$/d')

if [[ -z "$PROJECTS" ]]; then
    echo "No projects found or failed to fetch." >&2
    exit 0
fi

TOTAL_PROJECTS=$(echo "$PROJECTS" | sed '/^$/d' | wc -l)
echo "Found $TOTAL_PROJECTS projects. Parallelism = $PARALLEL. Dry run = $DRY_RUN"

if ! [[ "$PARALLEL" =~ ^[0-9]+$ ]] || [[ "$PARALLEL" -le 0 ]]; then
    echo "Parallelism must be a positive integer." >&2
    exit 6
fi

if [[ $DRY_RUN -eq 1 ]]; then
    echo "--dry-run enabled. Here are the delete endpoints that would be called:"
    echo "$PROJECTS" | while IFS= read -r k; do
        echo "$SONAR_HOST/api/projects/delete?project=$k"
    done
    exit 0
fi

echo "Using GNU parallel (jobs=$PARALLEL)"
echo "$PROJECTS" | sed '/^$/d' | parallel -j "$PARALLEL" --bar --line-buffer \
    'curl -s --retry 3 --retry-delay 1 -u "'$TOKEN':" -X POST "'$SONAR_HOST'/api/projects/delete?project={}" >/dev/null && echo "Deleted {}" || echo "Failed {}"'

echo "Done."
