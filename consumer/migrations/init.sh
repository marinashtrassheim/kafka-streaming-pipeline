#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-default}"
MIGRATIONS_TABLE="schema_migrations"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

ch_query() {
    clickhouse-client \
        --host "$CLICKHOUSE_HOST" \
        --port "$CLICKHOUSE_PORT" \
        --database "$CLICKHOUSE_DB" \
        --query "$1"
}

wait_for_clickhouse() {
    log_info "Waiting for ClickHouse at ${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}..."
    local attempts=0
    until ch_query "SELECT 1" >/dev/null 2>&1; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 60 ]; then
            log_error "ClickHouse is not reachable after 60 attempts"
            exit 1
        fi
        sleep 1
    done
    log_info "ClickHouse is ready"
}

file_checksum() {
    md5sum "$1" | awk '{print $1}'
}

create_migrations_table() {
    log_info "Ensuring migrations tracking table exists..."
    ch_query "
        CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
            version String,
            applied_at DateTime64(3) DEFAULT now(),
            migration_name String,
            checksum String,
            success UInt8 DEFAULT 1
        ) ENGINE = MergeTree
        ORDER BY (version, applied_at)
    "
}

get_migration_state() {
    local version=$1
    ch_query "
        SELECT checksum, success
        FROM ${MIGRATIONS_TABLE}
        WHERE version = '${version}'
        ORDER BY applied_at DESC
        LIMIT 1
        FORMAT TabSeparated
    " 2>/dev/null || true
}

record_migration() {
    local version=$1
    local name=$2
    local checksum=$3
    local success=$4

    ch_query "
        INSERT INTO ${MIGRATIONS_TABLE}
            (version, migration_name, checksum, success)
        VALUES ('${version}', '${name}', '${checksum}', ${success})
    "
}

run_migration() {
    local version=$1
    local name=$2
    local file=$3
    local checksum
    checksum="$(file_checksum "$file")"

    log_info "Checking migration ${version} - ${name}"

    local state
    state="$(get_migration_state "$version")"

    if [ -n "$state" ]; then
        local applied_checksum applied_success
        applied_checksum="$(echo "$state" | awk -F'\t' '{print $1}')"
        applied_success="$(echo "$state" | awk -F'\t' '{print $2}')"

        if [ "$applied_success" = "1" ]; then
            if [ "$applied_checksum" = "$checksum" ]; then
                log_warn "Migration ${version} already applied with matching checksum, skipping"
                return 0
            fi

            log_error "Migration ${version} was modified after apply"
            log_error "  recorded checksum: ${applied_checksum}"
            log_error "  current checksum:  ${checksum}"
            log_error "Create a new migration file instead of editing ${file}"
            exit 1
        fi

        log_warn "Migration ${version} previously failed, retrying..."
    fi

    log_info "Applying migration ${version} - ${name}"
    if clickhouse-client \
        --host "$CLICKHOUSE_HOST" \
        --port "$CLICKHOUSE_PORT" \
        --database "$CLICKHOUSE_DB" \
        --queries-file "$file"; then
        record_migration "$version" "$name" "$checksum" 1
        log_info "Completed migration ${version}"
    else
        record_migration "$version" "$name" "$checksum" 0
        log_error "Failed migration ${version}"
        exit 1
    fi
}

run_up() {
    wait_for_clickhouse
    create_migrations_table

    log_info "Running pending migrations from ${SCRIPT_DIR}..."
    shopt -s nullglob
    local migration_files=( [0-9][0-9][0-9]_*.sql )
    shopt -u nullglob

    if [ "${#migration_files[@]}" -eq 0 ]; then
        log_warn "No migration files found"
        return 0
    fi

    for migration in "${migration_files[@]}"; do
        local version="${migration%%_*}"
        local name="${migration#*_}"
        name="${name%.sql}"
        run_migration "$version" "$name" "$migration"
    done

    log_info "All migrations completed"
}

show_status() {
    wait_for_clickhouse
    create_migrations_table

    log_info "Migration status:"
    ch_query "
        SELECT
            version,
            migration_name,
            checksum,
            success,
            applied_at
        FROM ${MIGRATIONS_TABLE}
        ORDER BY version, applied_at
        FORMAT PrettyCompact
    "
}

show_help() {
    cat <<EOF
Usage: $0 [COMMAND]

Commands:
  up      Run pending migrations (default)
  status  Show applied migrations
  help    Show this help

Environment:
  CLICKHOUSE_HOST  default: clickhouse
  CLICKHOUSE_PORT  default: 9000
  CLICKHOUSE_DB    default: default
EOF
}

case "${1:-up}" in
    up) run_up ;;
    status) show_status ;;
    help|--help|-h) show_help ;;
    *)
        log_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac
