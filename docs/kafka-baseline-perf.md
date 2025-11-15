## Kafka Baseline Load Testing Design Document

**Document purpose**  
Codifies the reusable baseline methodology for measuring raw Kafka cluster throughput and latency, detached from application code. Implementation details (Docker tooling, scripts, configs) live in the repository root; operational instructions are summarized in `README.md`.

This document instantiates the reusable baseline design with Apache Kafka’s native benchmarking utilities: `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh`. It keeps Kafka isolated from any business logic so you can establish raw cluster ceilings before layering application code.

---

### 1. Guiding Principles

| Rule | Implementation Detail |
|------|-----------------------|
| **Isolate Kafka** | Run all tests from jump-hosts that have direct broker access; disable REST/DB/Kafka Connect dependencies. |
| **One topic per run** | Re-create topics between phases to avoid cross-test data. |
| **Simple producer → consumer loop** | Use perf-test CLI pair for pure Kafka throughput & latency signals. |
| **Controlled variables** | Fix payload size (1 KB JSON), compression (snappy), acks (all), batching (`batch.size=16384`, `linger.ms=5`). |
| **Steady-state** | 2–3 min warm-up, then capture 10 min of metrics per phase. |

---

### 2. Environment Checklist

* Kafka binaries available (e.g., `/opt/kafka/bin` in `PATH`).
* `KAFKA_BROKERS` export, e.g. `export KAFKA_BROKERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"`.
* Replication factor ≥3, `min.insync.replicas=2`.
* Dedicated monitoring for broker CPU, network, disk, and GC.

---

### 3. Standard Message Template

The perf-test producer will generate random bytes, but for auditability tie runs to the canonical JSON payload (~1 KB):

```
{"id":12345,"ts":1731700000000,"data":"<900 x chars>"}
```

When scripting custom producers later, reuse that format to keep metrics comparable.

---

### 4. Topic Lifecycle Commands

```bash
BIN_DIR=/opt/kafka/bin
TOPIC=baseline-1p            # change per phase
PARTITIONS=1                 # change per phase

$BIN_DIR/kafka-topics.sh \
  --bootstrap-server "$KAFKA_BROKERS" \
  --create \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --if-not-exists
```

Between phases, delete and re-create to reset offsets:

```bash
$BIN_DIR/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" --delete --topic "$TOPIC"
```

---

### 5. Producer Baseline (`kafka-producer-perf-test.sh`)

Core flags (common to every phase):

```bash
COMMON_PRODUCER_FLAGS="\
  --throughput -1 \
  --record-size 1024 \
  --producer-props \
    bootstrap.servers=$KAFKA_BROKERS \
    acks=all \
    compression.type=snappy \
    linger.ms=5 \
    batch.size=16384 \
    retries=5 \
    enable.idempotence=true"
```

Add run-specific values:

```bash
NUM_RECORDS=$((10 * 60 * 20000))   # e.g., target 20k msg/s for 10 min
TOPIC=baseline-1p

$BIN_DIR/kafka-producer-perf-test.sh \
  --topic "$TOPIC" \
  --num-records "$NUM_RECORDS" \
  $COMMON_PRODUCER_FLAGS
```

*Throughput (`records/sec`, `MB/sec`) prints every 10 s—persist stdout for later analysis.*

---

### 6. Consumer Baseline (`kafka-consumer-perf-test.sh`)

The consumer perf tool reports fetch latency stats (avg/max/percentiles) and commit lag.

```bash
COMMON_CONSUMER_FLAGS="\
  --broker-list $KAFKA_BROKERS \
  --fetch-size 1048576 \
  --messages $NUM_RECORDS \
  --threads 6 \
  --consumer.config /etc/kafka/baseline-consumer.properties"

# baseline-consumer.properties
# bootstrap.servers=<same as producer>
# enable.auto.commit=false
# auto.offset.reset=earliest
# max.poll.records=500
# group.id=baseline-perf

$BIN_DIR/kafka-consumer-perf-test.sh \
  --topic "$TOPIC" \
  $COMMON_CONSUMER_FLAGS
```

For latency histograms beyond what the CLI prints, run periodic `kafka-consumer-groups.sh --describe --group baseline-perf --bootstrap-server $KAFKA_BROKERS` to capture lag and calculate end-to-end latency using producer timestamps.

---

### 7. Sequential Test Phases

| Phase | Topic | Partitions | Producer Command | Consumer Command | Notes |
|-------|-------|------------|------------------|------------------|-------|
| **Single Partition** | `baseline-1p` | 1 | Run perf-test with `PARTITIONS=1`, `--threads 1` if CPU limited. | Consume with `--threads 1`. | Determine per-partition ceiling. |
| **Multi-Partition** | `baseline-12p` | 12 | Keep same producer config; increase `NUM_RECORDS` proportionally. | `--threads 6`–12. | Expect near-linear scaling; watch broker network. |
| **App Consumer** | `baseline-app` | 6–12 | Still use perf producer to fill topic quickly. | Replace perf consumer with real app consumer or wrapper script to compare latency vs baseline. | Validates app stack overhead. |

Each phase: warm-up 2 min, then capture 10 min steady state metrics (producer stdout, consumer stdout, broker telemetry).

---

### 8. Metrics Collection

| Metric | Source | Command / Tooling |
|--------|--------|-------------------|
| `throughput_msg_sec`, `throughput_mb_sec` | Producer perf CLI | Parse log lines (`records/sec`, `MB/sec`). |
| `latency_avg/max/50/95/99` | Consumer perf CLI | Use final summary block; supplement with custom scripts if needed. |
| `consumer_lag` | `kafka-consumer-groups.sh` | `watch -n 10 ... --describe`. |
| Broker CPU, disk, net, GC | Prometheus/JMX | Tag runs with `test_name=baseline-*`. |

Store raw outputs under `artifacts/<timestamp>/<phase>/`.

Each phase also records a machine-readable `summary.json` (parsed from producer/consumer logs) and a global `artifacts/summary_latest.md` table so you can review throughput/latency per phase without spinning up visualization tooling.

---

### 9. Storage Planning (1 KB Payload Reference)

With default timings (2 min warm-up + 10 min steady state) and phase throughputs:

| Phase | Target Throughput | Messages / phase | Data per replica (1 KB) | Disk @ RF=1 | Disk @ RF=3 |
|-------|-------------------|------------------|-------------------------|-------------|-------------|
| Single (`baseline-1p`) | 20 k msg/s | ~14.4 M | ~14 GB | ~14 GB | ~43 GB |
| Multi (`baseline-12p`) | 240 k msg/s | ~172.8 M | ~172 GB | ~172 GB | ~516 GB |
| App (`baseline-app`) | 240 k msg/s | ~172.8 M | ~172 GB | ~172 GB | ~516 GB |
| **Total** | — | — | — | **~360 GB** | **~1.08 TB** |

Plan broker storage accordingly. If disk is limited, shorten `--duration`, lower phase throughputs, or run fewer phases locally while reserving the full workload for capacity environments. Adjust retention policies if you need the data to expire quickly after each run.

---

### 9. Operational Runbook (Summary)

Operational procedures (Docker Compose setup, script flags, artifact handling) are covered in `README.md`. At a glance:

- Use the tooling container (`docker-compose.yml`) to avoid managing Kafka CLI locally.
- Run `runners/cli/scripts/run_baseline.sh` with `--phase` / `--duration` / `--message-size` overrides as needed. For single-node Compose smoke tests, prefer `--phase single --replication-factor 1` (one partition, ISR 1). When targeting the Strimzi-managed cluster, switch back to `--phase all --replication-factor 3` so multi-partition topics match production expectations.
- Execute inside Kubernetes by building/pushing the repo image (`runners/cli/docker/Dockerfile`) and installing `charts/kafka-perf-runner`, which launches a Job running the same script inside the `kafka-perf-test` (or chosen) namespace. Alternatively, build `runners/spring/docker/Dockerfile` and deploy `charts/kafka-perf-spring` to exercise the Spring Kafka workload.
- Adjust replication factor per environment via `--replication-factor` (default 3; set to 1 for local single-broker Compose runs). The script auto-sets `min.insync.replicas`.
- Supply optional `--client-config` and `--command-config` files for secure clusters; the script merges them with `runners/cli/config/baseline-consumer.properties` before launching the benchmark clients.
- Artifacts are persisted under `artifacts/<timestamp>_<phase>/producer.log|consumer.log`.

Refer to the README for exact commands and customization examples; the remainder of this design doc focuses on the why and what to measure.

---

### 10. Automation Template (YAML)

```yaml
test_name: kafka_baseline
phases:
  - name: single_partition
    topic: baseline-1p
    partitions: 1
    threads: 1
    duration: 600
    target_throughput: 20000
  - name: multi_partition
    topic: baseline-12p
    partitions: 12
    threads: 6
    duration: 600
    target_throughput: 240000
  - name: app_consumer
    topic: baseline-app
    partitions: 12
    threads: 6
    duration: 600
    target_throughput: 240000

producer:
  record_size: 1024
  compression: snappy
  acks: all
  batch_size: 16384
  linger_ms: 5
  retries: 5

consumer:
  max_poll_records: 500
  auto_commit: false
  offset_reset: earliest
```

Feed this YAML into automation (Ansible, Jenkins, custom Python) to drive the perf-test binaries with predictable parameters.

---

### 11. Reusable Prompt

> Generate a Kafka baseline load test with N partitions, 1 KB JSON messages (`id`, `ts`, `data`), snappy compression, `acks=all`, 10 min runtime, report throughput every 10 s, and compute p50/p95/p99 latency using consumer timestamps.

Keep this document as the single source of truth (“golden template”) to unblock quick code generation, CI/CD capacity guardrails, and regression testing.


