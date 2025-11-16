## Kafka Baseline Performance Toolkit

This repo bundles two runnable implementations of the baseline design in `docs/kafka-baseline-perf.md`:

1. **CLI runner** – wraps Apache Kafka’s native `kafka-producer-perf-test.sh` + `kafka-consumer-perf-test.sh`.
2. **Spring runner** – a Spring Kafka workload that mirrors the same phases/end-to-end metrics but closer to real application consumers/producers.

---

### Repository Layout

| Path | Purpose |
|------|---------|
| `runners/cli/scripts/run_baseline.sh` | Orchestrates topic creation, producer & consumer perf tests, and artifacts per phase. |
| `runners/cli/config/baseline-consumer.properties` | Base consumer config appended to each CLI run; override with `--client-config` for secure clusters. |
| `runners/cli/docker/` | CLI runner image (Temurin JRE + Kafka CLI + scripts). |
| `runners/spring/` | Spring Kafka app (Gradle/JDK21). Message flow lives in `BaselineProducer` (sends JSON payloads) and `BaselineConsumer` (polls, tracks latency). |
| `docker-compose.yml` | Runs the tooling container with the repo mounted into `/workspace`. |
| `charts/kafka-perf-runner/` | Helm chart for the CLI runner job. |
| `charts/kafka-perf-spring/` | Helm chart for the Spring runner job. |
| `docs/kafka-baseline-perf.md` | Full design document (principles, metrics, automation template, storage planning). |
| `artifacts/` | Output logs per run (`<timestamp>_<phase>/.../summary.json`) plus `summary_latest*.md`. |

---

### Prerequisites

- Docker + Docker Compose Plugin.
- Access to a Kafka cluster (bootstrap servers, credentials if secured).
- Optional: local Kafka CLI binaries if you want to run `runners/cli/scripts/run_baseline.sh` directly on your host.
- JDK 21 (the Docker images use Eclipse Temurin 21 for both runners).

---

### Quick Start (Docker Compose)

#### A. Launch local Kafka stack (optional)

Need a sandbox cluster? The Compose file now bundles the official **Apache Kafka 3.8.0** container running in single-node KRaft mode:

```bash
docker compose up -d kafka
```

Host apps can reach it via `localhost:9092`. Containers in the Compose network should use `kafka:9094` (the internal listener). Override `KAFKA_CLUSTER_ID` in `.env` if you need a specific cluster UUID; the default (`4L6g3nShT-eMCtK--X86sw`) works for local testing. Tear down with `docker compose down`.

#### B. Build runner images

```bash
# Build both runner images
docker compose build perf-runner spring-runner

# CLI runner: local single-node sanity check (1 partition, replication 1)
BOOTSTRAP_SERVERS="kafka:9094" \
  docker compose run --rm perf-runner \
  ./runners/cli/scripts/run_baseline.sh --phase single --replication-factor 1

# CLI runner: full baseline against real clusters (multi partitions, replication 3)
BOOTSTRAP_SERVERS="broker1:9092,broker2:9092,broker3:9092" \
  docker compose run --rm perf-runner \
  ./runners/cli/scripts/run_baseline.sh --phase all --replication-factor 3

# Spring runner: app-style workload vs. local broker
docker compose run --rm spring-runner \
  --baseline.bootstrap-servers=kafka:9094 \
  --baseline.duration-seconds=180 \
  --baseline.replication-factor=1
```

When using the local broker, keep `BOOTSTRAP_SERVERS=kafka:9094` inside Compose (or `localhost:9092` when running the script directly on your host). Override the variable with your real bootstrap list to target external clusters.

Artifacts are written under `artifacts/` on your host. Remove the container afterward (`docker compose down`); the logs remain in the workspace.

---

### Usage At A Glance

- **CLI runner (Kafka perf scripts)**
  - Local validation: `docker compose run --rm perf-runner ./runners/cli/scripts/run_baseline.sh --bootstrap kafka:9094 --phase single --replication-factor 1`
  - Full baseline vs Strimzi: same command with `--phase all --replication-factor 3` and `BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092`
  - Kubernetes/CI: build via `runners/cli/docker/Dockerfile`, install `charts/kafka-perf-runner`
- **Spring runner (app-style workload)**
  - Local validation: `docker compose run --rm spring-runner --baseline.bootstrap-servers=kafka:9094 --baseline.duration-seconds=300 --baseline.replication-factor=1`
  - Kubernetes/CI: build via `runners/spring/docker/Dockerfile`, install `charts/kafka-perf-spring`

Pick the runner that matches your environment, then collect artifacts either from the local `artifacts/` directory or from the Kubernetes PVC/logs.

---

### Documentation
- Run locally to verify the workflow: `docs/running-locally.md`
- Run in Kubernetes with Helm: `docs/kubernetes-helm.md`
- Simulate multi-topic workloads (multiple CLI jobs): `docs/multi-topic-workloads.md`
- Baseline design and methodology: `docs/kafka-baseline-perf.md`

### Script Options

```bash
./runners/cli/scripts/run_baseline.sh --bootstrap host1:9092[,host2:9092,...] [flags]

Flags:
  --phase single|multi|app|all  # default all
  --duration <seconds>          # steady-state window (default 600)
  --message-size <bytes>        # default 1024
  --client-config <path>        # extra producer/consumer props (SASL/SSL)
  --command-config <path>       # admin (topic) client config
  --replication-factor <n>      # topic replication factor (default 3)
  --dry-run                     # print commands only
```

For Docker Compose examples, see `docs/running-locally.md`. Set `BOOTSTRAP_SERVERS` env var to avoid passing `--bootstrap` explicitly. When using Compose, mount any custom config files under the repo so the container can read them (they appear under `/workspace/...`).

---

### Customizing Runs

- **Security**: create `runners/cli/config/secure.properties` with SASL/SSL settings and pass `--client-config runners/cli/config/secure.properties`. Use the same file for `--command-config` if topic admin needs identical credentials.
- **Per-phase tweaks**: use `--single-partitions`, `--multi-partitions`, or `--app-partitions` when running the CLI, or edit `phase_definition()` inside `runners/cli/scripts/run_baseline.sh` to change threads/throughput defaults.
- **Message size**: `--message-size` updates `--record-size` while keeping the JSON template in the design doc consistent.
- **Replication factor**: use `--replication-factor 1` for local single-broker Compose runs (default 3 for production clusters); `min.insync.replicas` auto-adjusts accordingly. Pair this with `--phase single` (1 partition) to quickly verify the workflow before pointing at multi-broker Strimzi clusters.
- **Dry runs**: `--dry-run` plus `docker compose run ...` offers a safe preview before touching production clusters.
- **Spring runner overrides**: pass standard Spring Boot arguments (e.g., `--baseline.duration-seconds=300`) or supply `SPRING_APPLICATION_JSON`/Helm values to alter phases, replication, message size, and artifacts directory.

#### Partition defaults & overrides

Both runners now default each phase to **12 partitions** (single, multi, and app).

- **CLI runner (local command line)** – override per phase with the new flags:
  ```bash
  docker compose run --rm perf-runner \
    ./runners/cli/scripts/run_baseline.sh \
    --bootstrap kafka:9094 \
    --phase single \
    --single-partitions 24
  ```
- **CLI runner (Helm chart)** – pass the same flags via `runner.extraArgs`:
  ```yaml
  runner:
    extraArgs:
      - --single-partitions
      - "24"
  ```
- **Spring runner (CLI/Compose)** – supply Spring Boot properties inline:
  ```bash
  docker compose run --rm spring-runner \
    --baseline.bootstrap-servers=kafka:9094 \
    --baseline.phases[0].name=single \
    --baseline.phases[0].topic=baseline-1p \
    --baseline.phases[0].partitions=24
  ```
- **Spring runner (application.yml / Helm values)** – define the `baseline.phases` array:
  ```yaml
  baseline:
    phases:
      - name: single
        topic: baseline-1p
        partitions: 24
        target-throughput: 20000
        consumer-threads: 1
  ```
  Mount the file into `/workspace/config/application.yml` and run with `--spring.config.additional-location=file:/workspace/config/`, or set the same values through the Helm chart:
  ```yaml
  baseline:
    phases:
      - name: single
        topic: baseline-1p
        partitions: 24
        targetThroughput: 20000
        consumerThreads: 1
  ```

### Interpreting Results

- Throughput (`records/sec`, `MB/sec`) prints every 10 s in `producer.log`.
- Consumer latency summaries (avg, max, 50/95/99) land at the end of `consumer.log`.
- Run `kafka-consumer-groups.sh --describe --group baseline-perf --bootstrap-server <...>` from the container to capture lag snapshots during the test.
- Each phase now emits `summary.json` containing parsed producer/consumer stats; once all selected phases finish, the script writes an aggregated Markdown table to `artifacts/summary_latest.md` for quick CLI reporting.
- The Spring runner mirrors the JSON summaries (directories suffixed with `_spring`) and logs consumer latency stats directly from the application.

For the rationale behind each phase and the metrics to track, see `docs/kafka-baseline-perf.md`.

---

### CLI Runner Sequence Diagrams

The following Mermaid sequence diagrams illustrate how the CLI runner orchestrates each phase. All phases follow the same high-level flow: create topic → run producer perf → run consumer perf → emit artifacts (logs and summaries). Defaults: partitions per phase = 12; consumer threads = 1 (single) or 6 (multi/app).

#### Phase: single

```mermaid
sequenceDiagram
    autonumber
    actor Operator
    participant Script as run_baseline.sh
    participant Admin as kafka-topics.sh
    participant Producer as kafka-producer-perf-test.sh
    participant Consumer as kafka-consumer-perf-test.sh
    participant Artifacts as artifacts/

    Operator->>Script: Start phase "single" (bootstrap, duration, message-size, RF, etc.)
    Script->>Admin: Create topic baseline-1p (partitions=12 default, replicationFactor)
    Admin-->>Script: Topic ready

    Note over Producer,Consumer: Warmup then steady-state windows
    Script->>Producer: Start producer (targetThroughput, recordSize, acks=all, idempotence=true)
    Producer-->>Artifacts: producer.log (10s throughput, latency metrics)

    Script->>Consumer: Start consumer (threads=1, expectedRecords)
    Consumer-->>Artifacts: consumer.log (throughput, fetch/latency stats)

    Producer-->>Script: Exit with summary stats
    Consumer-->>Script: Exit with summary stats
    Script->>Artifacts: Write single/summary.json
    Script->>Artifacts: Update summary_latest.md (aggregate)
```

#### Phase: multi

```mermaid
sequenceDiagram
    autonumber
    actor Operator
    participant Script as run_baseline.sh
    participant Admin as kafka-topics.sh
    participant Producer as kafka-producer-perf-test.sh
    participant Consumer as kafka-consumer-perf-test.sh
    participant Artifacts as artifacts/

    Operator->>Script: Start phase "multi"
    Script->>Admin: Create topic baseline-12p (partitions=12 default, replicationFactor)
    Admin-->>Script: Topic ready

    Note over Producer,Consumer: Warmup then steady-state windows
    Script->>Producer: Start producer (parallel partitions, compression=snappy, linger/batch tuned)
    Producer-->>Artifacts: producer.log

    Script->>Consumer: Start consumer (threads=6, group.id=baseline-perf)
    Consumer-->>Artifacts: consumer.log

    Producer-->>Script: Exit with summary stats
    Consumer-->>Script: Exit with summary stats
    Script->>Artifacts: Write multi/summary.json
    Script->>Artifacts: Update summary_latest.md (aggregate)
```

#### Phase: app

```mermaid
sequenceDiagram
    autonumber
    actor Operator
    participant Script as run_baseline.sh
    participant Admin as kafka-topics.sh
    participant Producer as kafka-producer-perf-test.sh
    participant Consumer as kafka-consumer-perf-test.sh
    participant Artifacts as artifacts/

    Operator->>Script: Start phase "app"
    Script->>Admin: Create topic baseline-app (partitions=12 default, replicationFactor)
    Admin-->>Script: Topic ready

    Note over Producer,Consumer: Warmup then steady-state windows
    Script->>Producer: Start producer (app-like tuning, same durable semantics)
    Producer-->>Artifacts: producer.log

    Script->>Consumer: Start consumer (threads=6, expectedRecords)
    Consumer-->>Artifacts: consumer.log

    Producer-->>Script: Exit with summary stats
    Consumer-->>Script: Exit with summary stats
    Script->>Artifacts: Write app/summary.json
    Script->>Artifacts: Update summary_latest.md (aggregate)
```

---

### Baseline Design and Rationale

This toolkit provides a repeatable, “good enough” baseline of Kafka cluster throughput and consumer latency using two complementary runners:

- CLI runner: wraps Kafka’s native `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh`.
- Spring runner: mirrors the workload with application-style producers/consumers.

Core ideas:
- Phased workload shape
  - single: one topic sized for single-partition throughput and latency characterization.
  - multi: a multi-partition topic to exercise parallelism and typical production fan-out.
  - app: application-like topic with similar parallelism to multi but different tuning.
- Topics per phase
  - single → `baseline-1p`
  - multi → `baseline-12p`
  - app → `baseline-app`
  - Default partitions per phase are 12; override with flags or Helm values to match real clusters.
- Time windows
  - Warmup (default 120s) lets producers/consumers stabilize and caches/JIT warm up.
  - Steady state (default 600s) is long enough to average short spikes/dips.
- Producer settings
  - acks=all, idempotence=true to represent durable, production-typical semantics.
  - modest batching/linger and snappy compression for realistic efficiency without hiding broker issues.
- Consumer settings
  - `kafka-consumer-perf-test.sh` runs with a fixed thread count (1 for single; 6 for multi/app).
  - `group.id=baseline-perf`, `auto.offset.reset=earliest`, `enable.auto.commit=false`.
  - The consumer reads exactly the expected message count and exits; output is parsed for summaries.

Why this produces a solid baseline:
- Repeatable: all parameters (partitions, replication, message size, duration, throughput) are explicit and versioned; both runners generate phase-scoped artifacts and a combined summary.
- Representative: durable producer semantics and realistic batching/compression exercise broker I/O, replication, and controller behavior similar to production.
- Comparable: identical phases across environments (local Docker/Compose, Kubernetes, managed Kafka) allow apples-to-apples comparisons over time and across clusters.
- Sensitive to bottlenecks: multi-partition phases surface partition/replica placement, ISR/replication throughput, and disk/network limits; single-partition highlights broker/core latency.
- Safe to tune: knobs (e.g., `--single-partitions`, `--multi-partitions`, `--message-size`, `--replication-factor`) allow adapting the baseline to cluster size without invalidating comparisons.

Collected outputs:
- Producer: records/sec, MB/sec, avg/max latency (from Kafka perf producer).
- Consumer: throughput and fetch/latency stats (from perf consumer).
- Summaries: per-phase `summary.json` and an aggregated `summary_latest.md` for quick review.

Use the CLI runner for fast, portable checks and the Spring runner when you want end-to-end behavior close to application services. Running both provides confidence that broker-side capacity translates to app-observable performance.

### Deploying in Kubernetes with Helm (CI)

The repository includes a `.gitlab-ci.yml` that automates building and pushing images to Nexus, and optionally deploying via Helm. Required GitLab CI/CD variables:

- `CI_REGISTRY_HOSTED` - Your Nexus registry URL (e.g., `registry.example.com`)
- `CI_REGISTRY_USER` - Nexus username
- `CI_REGISTRY_PASSWORD` - Nexus password
- `CI_REGISTRY_USER_EMAIL` - Email for registry secret
- `CI_REGISTRY_HUB` - Base registry for pulling CI images (e.g., `docker.io`)
- `CI_WORKER_TAG` - GitLab runner tag
- `KUBE_CONFIG_FILE` - Kubernetes config file path (for deploy stage)
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka bootstrap servers (for deploy stage)
- `TARGET_PLATFORM` - Docker build platform (default `linux/amd64`; set to your cluster arch)

The pipeline builds both runners and pushes them to `$CI_REGISTRY_HOSTED/kafka-perf/kafka-perf-runner` and `$CI_REGISTRY_HOSTED/kafka-perf/kafka-perf-spring` when you create a version tag matching `v*.*.*` (e.g., `v1.0.0`).

Note: The CI uses Docker Buildx to build architecture-specific images (via `TARGET_PLATFORM`). If your cluster nodes are `arm64`, set `TARGET_PLATFORM=linux/arm64` in your CI/CD variables.
#### Using CLI Script

Alternatively, you can use the provided script to build and push images manually:

```bash
export NEXUS_REGISTRY="registry.example.com"
export NEXUS_USER="your-username"
export NEXUS_PASSWORD="your-password"
export VERSION_TAG="v1.0.0"  # optional, defaults to "latest"
./scripts/push-to-nexus.sh
```

Or manually with Docker commands:

```bash
# Login to Nexus
echo "$NEXUS_PASSWORD" | docker login -u "$NEXUS_USER" "$NEXUS_REGISTRY" --password-stdin

# Build and push CLI runner
docker build -f runners/cli/docker/Dockerfile . \
  -t $NEXUS_REGISTRY/kafka-perf/kafka-perf-runner:v1.0.0 \
  -t $NEXUS_REGISTRY/kafka-perf/kafka-perf-runner:latest
docker push $NEXUS_REGISTRY/kafka-perf/kafka-perf-runner:v1.0.0
docker push $NEXUS_REGISTRY/kafka-perf/kafka-perf-runner:latest

# Build and push Spring runner
docker build -f runners/spring/docker/Dockerfile . \
  -t $NEXUS_REGISTRY/kafka-perf/kafka-perf-spring:v1.0.0 \
  -t $NEXUS_REGISTRY/kafka-perf/kafka-perf-spring:latest
docker push $NEXUS_REGISTRY/kafka-perf/kafka-perf-spring:v1.0.0
docker push $NEXUS_REGISTRY/kafka-perf/kafka-perf-spring:latest
```

For full Kubernetes/Helm usage, see `docs/kubernetes-helm.md`.

For concurrent multi-size jobs, see `docs/multi-topic-workloads.md`.

