## Run in Kubernetes with Helm

This guide shows how to build images, install suspended Jobs, trigger runs, collect artifacts, and re-run.

### Build and push images
```bash
docker build -t registry.example.com/kafka-perf-runner:latest -f runners/cli/docker/Dockerfile .
docker push registry.example.com/kafka-perf-runner:latest
```

### Install the CLI runner (suspended)
```bash
helm install kafka-baseline-cli charts/kafka-perf-runner \
  --namespace kafka-perf-test --create-namespace \
  --set image.repository=registry.example.com/kafka-perf-runner \
  --set image.tag=latest \
  --set bootstrapServers=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --set runner.phase=all \
  --set suspend=true
```

If your registry is private, create a pull secret and wire `imagePullSecrets`.

### Trigger a one-shot run
```bash
kubectl -n kafka-perf-test patch job kafka-baseline-cli-kafka-perf-runner -p '{"spec":{"suspend":false}}'
kubectl -n kafka-perf-test logs job/kafka-baseline-cli-kafka-perf-runner -f
```

### Re-run cleanly
```bash
kubectl -n kafka-perf-test delete job kafka-baseline-cli || true
helm upgrade --install kafka-baseline-cli charts/kafka-perf-runner -n kafka-perf-test --reuse-values
kubectl -n kafka-perf-test patch job kafka-baseline-cli -p '{"spec":{"suspend":false}}'
```

### Collect artifacts
- Logs: `kubectl logs job/kafka-baseline-cli-kafka-perf-runner`
- Copy artifacts: `kubectl cp kafka-perf-test/<pod>:/workspace/artifacts ./artifacts-k8s`
- Use `--set artifacts.pvcName=<pvc>` to persist under `/workspace/artifacts`

### Common overrides
- `--set runner.duration=600`
- `--set runner.messageSize=1024`
- `--set runner.replicationFactor=3`
- `--set runner.extraArgs[0]=--single-partitions --set runner.extraArgs[1]=24`

Tip: To have the Job start immediately on install, set `--set suspend=false` (default).

If using a private registry, add:
```bash
--set imagePullSecrets[0].name=nxregsecret
```


