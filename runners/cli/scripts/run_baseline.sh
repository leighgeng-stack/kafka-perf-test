#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLI_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${CLI_ROOT}/.." && pwd)"
ARTIFACT_ROOT="${REPO_ROOT}/artifacts"

DEFAULT_DURATION=600
DEFAULT_MSG_SIZE=1024
DEFAULT_WARMUP=120
DEFAULT_REPLICATION=3
DEFAULT_SINGLE_PARTITIONS=12
DEFAULT_MULTI_PARTITIONS=12
DEFAULT_APP_PARTITIONS=12

CONSUMER_CONFIG_TEMPLATE="${CLI_ROOT}/config/baseline-consumer.properties"

usage() {
  cat <<'EOF'
Usage: run_baseline.sh --bootstrap <brokers> [options]

Options:
  --bootstrap <servers>   Comma-separated bootstrap servers (host1:9092,...)
  --phase <name>          Phase to run (single|multi|app|all). Default: all.
  --duration <seconds>    Steady-state duration per phase. Default: 600.
  --message-size <bytes>  Message payload size. Default: 1024.
  --client-config <file>  Extra producer/consumer properties (e.g., SASL).
  --command-config <file> Admin client config file for topic commands.
  --replication-factor N  Topic replication factor (default 3).
  --single-partitions N   Override single phase partition count (default 12).
  --multi-partitions N    Override multi phase partition count (default 12).
  --app-partitions N      Override app phase partition count (default 12).
  --dry-run               Print commands without executing.
  -h, --help              Show this message.

Artifacts (producer/consumer logs) are placed under artifacts/<timestamp>/<phase>.
EOF
}

DRY_RUN=false
PHASE_FILTER="all"
DURATION=${DEFAULT_DURATION}
MESSAGE_SIZE=${DEFAULT_MSG_SIZE}
BOOTSTRAP=""
CLIENT_CONFIG=""
COMMAND_CONFIG=""
REPLICATION_FACTOR=${DEFAULT_REPLICATION}
SINGLE_PARTITIONS=${DEFAULT_SINGLE_PARTITIONS}
MULTI_PARTITIONS=${DEFAULT_MULTI_PARTITIONS}
APP_PARTITIONS=${DEFAULT_APP_PARTITIONS}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap)
      BOOTSTRAP="$2"; shift 2 ;;
    --phase)
      PHASE_FILTER="$2"; shift 2 ;;
    --duration)
      DURATION="$2"; shift 2 ;;
    --message-size)
      MESSAGE_SIZE="$2"; shift 2 ;;
    --client-config)
      CLIENT_CONFIG="$2"; shift 2 ;;
    --command-config)
      COMMAND_CONFIG="$2"; shift 2 ;;
    --replication-factor)
      REPLICATION_FACTOR="$2"; shift 2 ;;
    --single-partitions)
      SINGLE_PARTITIONS="$2"; shift 2 ;;
    --multi-partitions)
      MULTI_PARTITIONS="$2"; shift 2 ;;
    --app-partitions)
      APP_PARTITIONS="$2"; shift 2 ;;
    --dry-run)
      DRY_RUN=true; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage; exit 1 ;;
  esac
done

if [[ -z "${BOOTSTRAP}" ]]; then
  BOOTSTRAP="${BOOTSTRAP_SERVERS:-}"
fi

if [[ -z "${BOOTSTRAP}" ]]; then
  echo "Error: bootstrap servers not provided. Use --bootstrap or BOOTSTRAP_SERVERS env." >&2
  exit 1
fi

if [[ -n "${CLIENT_CONFIG}" && ! -f "${CLIENT_CONFIG}" ]]; then
  echo "Error: client config file not found: ${CLIENT_CONFIG}" >&2
  exit 1
fi

if [[ -n "${COMMAND_CONFIG}" && ! -f "${COMMAND_CONFIG}" ]]; then
  echo "Error: command config file not found: ${COMMAND_CONFIG}" >&2
  exit 1
fi

validate_positive_int() {
  local value="$1"
  local label="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -lt 1 ]]; then
    echo "Error: ${label} must be a positive integer." >&2
    exit 1
  fi
}

validate_positive_int "${REPLICATION_FACTOR}" "replication factor"
validate_positive_int "${SINGLE_PARTITIONS}" "single phase partitions"
validate_positive_int "${MULTI_PARTITIONS}" "multi phase partitions"
validate_positive_int "${APP_PARTITIONS}" "app phase partitions"

mkdir -p "${ARTIFACT_ROOT}"

timestamp() { date +"%Y%m%d_%H%M%S"; }
log() {
  # Portable ISO-8601 timestamp (works on GNU date and BSD date/macOS)
  if command -v date >/dev/null 2>&1; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"
  else
    echo "[timestamp-unavailable] $*"
  fi
}

run_cmd() {
  if $DRY_RUN; then
    echo "[dry-run] $*"
  else
    eval "$@"
  fi
}

SUMMARY_ROWS=()

create_topic() {
  local topic="$1"
  local partitions="$2"
  local min_isr=2
  if [[ "${REPLICATION_FACTOR}" -lt 2 ]]; then
    min_isr=1
  fi
  log "Creating topic ${topic} (partitions=${partitions}, replication=${REPLICATION_FACTOR}, minISR=${min_isr})"
  local base_cmd="kafka-topics.sh --bootstrap-server \"${BOOTSTRAP}\""
  if [[ -n "${COMMAND_CONFIG}" ]]; then
    base_cmd+=" --command-config \"${COMMAND_CONFIG}\""
  fi
  run_cmd "${base_cmd} --delete --topic \"${topic}\" >/dev/null 2>&1 || true"
  run_cmd "${base_cmd} --create --topic \"${topic}\" --partitions ${partitions} \
    --replication-factor ${REPLICATION_FACTOR} --config min.insync.replicas=${min_isr} --if-not-exists"
}

calc_records() {
  local target_tps="$1"
  echo $(( (DURATION + DEFAULT_WARMUP) * target_tps ))
}

phase_definition() {
  local phase="$1"
  case "$phase" in
    single)
      echo "topic=baseline-1p partitions=${SINGLE_PARTITIONS} threads=1 throughput=20000"
      ;;
    multi)
      echo "topic=baseline-12p partitions=${MULTI_PARTITIONS} threads=6 throughput=240000"
      ;;
    app)
      echo "topic=baseline-app partitions=${APP_PARTITIONS} threads=6 throughput=240000"
      ;;
    *)
      echo "Unknown phase ${phase}" >&2
      exit 1
      ;;
  esac
}

run_phase() {
  local name="$1"
  eval "$(phase_definition "${name}")"
  local phase_dir="${ARTIFACT_ROOT}/$(timestamp)_${name}"
  mkdir -p "${phase_dir}"

  create_topic "${topic}" "${partitions}"

  local total_records
  total_records=$(calc_records "${throughput}")

  local tmp_consumer_config
  tmp_consumer_config="$(mktemp)"
  cp "${CONSUMER_CONFIG_TEMPLATE}" "${tmp_consumer_config}"
  if [[ -n "${CLIENT_CONFIG}" ]]; then
    cat "${CLIENT_CONFIG}" >> "${tmp_consumer_config}"
  fi
  printf 'bootstrap.servers=%s\n' "${BOOTSTRAP}" >> "${tmp_consumer_config}"

  local producer_cmd="kafka-producer-perf-test.sh \
    --topic ${topic} \
    --num-records ${total_records} \
    --throughput ${throughput} \
    --record-size ${MESSAGE_SIZE} \
    ${CLIENT_CONFIG:+--producer.config ${CLIENT_CONFIG}} \
    --producer-props \
      bootstrap.servers=${BOOTSTRAP} \
      acks=all \
      compression.type=snappy \
      linger.ms=5 \
      batch.size=16384 \
      retries=5 \
      enable.idempotence=true"

  local consumer_cmd="kafka-consumer-perf-test.sh \
    --broker-list ${BOOTSTRAP} \
    --topic ${topic} \
    --messages ${total_records} \
    --threads ${threads} \
    --fetch-size 1048576 \
    --consumer.config ${tmp_consumer_config}"

  log "Running producer for phase ${name} (records=${total_records}; first ${DEFAULT_WARMUP}s are warm-up)"
  run_cmd "${producer_cmd} | tee \"${phase_dir}/producer.log\""

  log "Running consumer for phase ${name}"
  run_cmd "${consumer_cmd} | tee \"${phase_dir}/consumer.log\""

  log "Phase ${name} complete. Artifacts at ${phase_dir}"
  rm -f "${tmp_consumer_config}"

  generate_summary "${phase_dir}" "${name}"
}

generate_summary() {
  local phase_dir="$1"
  local phase_name="$2"
  local summary_json

  summary_json="$(
    python3 - "${phase_dir}/producer.log" "${phase_dir}/consumer.log" "${phase_name}" <<'PY'
import sys, json, re, pathlib

producer_path, consumer_path, phase_name = sys.argv[1:]

def parse_producer(path):
    line = None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for l in f:
                if 'records sent' in l:
                    line = l.strip()
    except FileNotFoundError:
        return {"raw_line": None}
    if not line:
        return {"raw_line": None}
    pattern = re.compile(
        r'([\d,\.]+)\s+records sent,\s+([\d,\.]+)\s+records/sec\s+\(([\d,\.]+)\s+MB/sec\),\s+([\d,\.]+)\s+ms avg latency,\s+([\d,\.]+)\s+ms max latency'
    )
    m = pattern.search(line)
    if not m:
        return {"raw_line": line}
    records_sent, records_sec, mb_sec, avg_lat, max_lat = m.groups()
    return {
        "records_sent": float(records_sent.replace(',', '')),
        "records_per_sec": float(records_sec.replace(',', '')),
        "mb_per_sec": float(mb_sec.replace(',', '')),
        "latency_avg_ms": float(avg_lat.replace(',', '')),
        "latency_max_ms": float(max_lat.replace(',', '')),
        "raw_line": line,
    }

def parse_consumer(path):
    header = None
    last_row = None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                if ': ' in line:
                    line = line.split(': ', 1)[1]
                if ',' not in line:
                    continue
                parts = [p.strip() for p in line.split(',')]
                if any('start.time' in p for p in parts):
                    header = [h.strip() for h in parts]
                    continue
                if header and len(parts) == len(header):
                    last_row = parts
                else:
                    last_row = parts
    except FileNotFoundError:
        return {"raw_line": None}
    if not last_row:
        return {"raw_line": None}
    if header and len(header) == len(last_row):
        data = {}
        for key, value in zip(header, last_row):
            safe_key = key.lower().replace('.', '_')
            try:
                data[safe_key] = float(value)
            except ValueError:
                data[safe_key] = value
        data["raw_line"] = ','.join(last_row)
        return data
    return {"raw_line": ','.join(last_row)}

summary = {
    "phase": phase_name,
    "producer": parse_producer(producer_path),
    "consumer": parse_consumer(consumer_path),
}

print(json.dumps(summary))
PY
  )"

  if [[ -z "${summary_json}" ]]; then
    log "Warning: unable to build summary for phase ${phase_name}"
    return
  fi

  printf '%s\n' "${summary_json}" > "${phase_dir}/summary.json"
  log "Summary for phase ${phase_name}: ${phase_dir}/summary.json"

  if command -v jq >/dev/null 2>&1; then
    local prod_rps prod_mb cons_rps cons_latency
    prod_rps=$(echo "${summary_json}" | jq -r '.producer.records_per_sec // empty')
    prod_mb=$(echo "${summary_json}" | jq -r '.producer.mb_per_sec // empty')
    cons_rps=$(echo "${summary_json}" | jq -r '.consumer.records_per_sec // empty')
    cons_latency=$(echo "${summary_json}" | jq -r '.consumer.fetch_latency_avg_ms // .consumer.latency_avg_ms // empty')
    SUMMARY_ROWS+=("${phase_name}|${prod_rps:-n/a}|${prod_mb:-n/a}|${cons_rps:-n/a}|${cons_latency:-n/a}")
  fi
}

PHASES=(single multi app)

for phase in "${PHASES[@]}"; do
  if [[ "${PHASE_FILTER}" != "all" && "${PHASE_FILTER}" != "${phase}" ]]; then
    continue
  fi
  run_phase "${phase}"
done

if [[ ${#SUMMARY_ROWS[@]} -gt 0 ]]; then
  summary_table="${ARTIFACT_ROOT}/summary_latest.md"
  {
    echo "| Phase | Producer rec/s | Producer MB/s | Consumer rec/s | Consumer latency (ms) |"
    echo "|-------|----------------|---------------|-----------------|-----------------------|"
    for row in "${SUMMARY_ROWS[@]}"; do
      IFS='|' read -r phase_name prod_rps prod_mb cons_rps cons_lat <<<"${row}"
      echo "| ${phase_name} | ${prod_rps} | ${prod_mb} | ${cons_rps} | ${cons_lat} |"
    done
  } > "${summary_table}"
  log "Aggregated summary written to ${summary_table}"
fi

