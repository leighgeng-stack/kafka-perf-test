## Run Locally to Verify the Workflow

This guide shows how to build the runner images and validate the workflow against a local Kafka (Compose) or your own cluster.

### Start a local Kafka (optional)

```bash
docker compose up -d kafka
```

Host: `localhost:9092`  •  Containers: `kafka:9094`

### Build runner images

```bash
docker compose build perf-runner spring-runner
```

### Quick local runs

CLI runner (script) single phase:
```bash
docker compose run --rm perf-runner \
  ./runners/cli/scripts/run_baseline.sh --bootstrap kafka:9094 --phase single --replication-factor 1
```

CLI runner (all phases):
```bash
docker compose run --rm perf-runner \
  ./runners/cli/scripts/run_baseline.sh --bootstrap kafka:9094 --phase all --replication-factor 1
```

Spring runner single phase:
```bash
docker compose run --rm spring-runner \
  --baseline.bootstrap-servers=kafka:9094 \
  --baseline.phases[0].name=single \
  --baseline.replication-factor=1
```

Spring runner (all phases):
```bash
docker compose run --rm spring-runner \
  --baseline.bootstrap-servers=kafka:9094 \
  --baseline.replication-factor=1
```

Artifacts are written under `artifacts/`.

### Notes
- You can set `BOOTSTRAP_SERVERS` to avoid passing `--bootstrap` explicitly when invoking the CLI runner inside Compose.
- To clean up the local Kafka container after testing: `docker compose down`. Artifacts remain on your host under the repository’s `artifacts/` directory.


