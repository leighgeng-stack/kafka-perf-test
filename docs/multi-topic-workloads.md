## Simulating Multi-Topic Workloads (multiple CLI Jobs with different sizes)

Run several CLI runner Jobs in parallel with different message sizes to approximate multiple services/topics.

Note: the current CLI script targets fixed topic names per phase (`baseline-1p`, `baseline-12p`, `baseline-app`). Concurrent Jobs will share those topics, which is fine to stress brokers with mixed payloads. If you need distinct topics, use the Spring runner with per-phase topic names or fork the script.

### Example: three concurrent “multi” phase jobs
```bash
NEXUS_REGISTRY="registry.example.com"
BOOTSTRAP="my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
NAMESPACE="kafka-perf-test"

helm install kafka-multi-small charts/kafka-perf-runner \
  -n "$NAMESPACE" --create-namespace \
  --set image.repository=$NEXUS_REGISTRY/kafka-perf/kafka-perf-runner \
  --set image.tag=latest \
  --set bootstrapServers="$BOOTSTRAP" \
  --set runner.phase=multi \
  --set runner.messageSize=256 \
  --set runner.duration=600 \
  --set suspend=true

helm install kafka-multi-medium charts/kafka-perf-runner \
  -n "$NAMESPACE" \
  --set image.repository=$NEXUS_REGISTRY/kafka-perf/kafka-perf-runner \
  --set image.tag=latest \
  --set bootstrapServers="$BOOTSTRAP" \
  --set runner.phase=multi \
  --set runner.messageSize=4096 \
  --set runner.duration=600 \
  --set suspend=true

helm install kafka-multi-large charts/kafka-perf-runner \
  -n "$NAMESPACE" \
  --set image.repository=$NEXUS_REGISTRY/kafka-perf/kafka-perf-runner \
  --set image.tag=latest \
  --set bootstrapServers="$BOOTSTRAP" \
  --set runner.phase=multi \
  --set runner.messageSize=1048576 \
  --set runner.duration=600 \
  --set suspend=true

kubectl -n "$NAMESPACE" patch job kafka-multi-small-kafka-perf-runner -p '{"spec":{"suspend":false}}'
kubectl -n "$NAMESPACE" patch job kafka-multi-medium-kafka-perf-runner -p '{"spec":{"suspend":false}}'
kubectl -n "$NAMESPACE" patch job kafka-multi-large-kafka-perf-runner -p '{"spec":{"suspend":false}}'
```

Optional: tail logs in parallel terminals:
```bash
kubectl -n "$NAMESPACE" logs job/kafka-multi-small-kafka-perf-runner -f
kubectl -n "$NAMESPACE" logs job/kafka-multi-medium-kafka-perf-runner -f
kubectl -n "$NAMESPACE" logs job/kafka-multi-large-kafka-perf-runner -f
```

### Tips
- Balance total load using `runner.messageSize`, `runner.duration`, and `runner.extraArgs` (e.g., `--multi-partitions`).
- Persist outputs with `--set artifacts.pvcName=<pvc>`.
- Re-run: delete Jobs and reinstall, or `helm upgrade --install ... --reuse-values` then unsuspend.

### Specify consumer group.id per job (CLI runner)
The CLI runner’s consumer group is controlled via the consumer config file. To set a distinct `group.id` for each concurrent job, provide a small override file via a Kubernetes Secret and wire it through the chart’s `clientConfigSecret`.

Example secrets (one per job):
```bash
kubectl -n "$NAMESPACE" create secret generic kafka-multi-small-consumer \
  --from-literal=client.properties='group.id=baseline-perf-small'
kubectl -n "$NAMESPACE" create secret generic kafka-multi-medium-consumer \
  --from-literal=client.properties='group.id=baseline-perf-medium'
kubectl -n "$NAMESPACE" create secret generic kafka-multi-large-consumer \
  --from-literal=client.properties='group.id=baseline-perf-large'
```

Re-install the jobs referencing those secrets (note: the chart mounts the secret at `/config/client/client.properties` and passes `--client-config` automatically when set):
```bash
helm install kafka-multi-small charts/kafka-perf-runner \
  -n "$NAMESPACE" --create-namespace \
  --set image.repository=$NEXUS_REGISTRY/kafka-perf/kafka-perf-runner \
  --set image.tag=latest \
  --set bootstrapServers="$BOOTSTRAP" \
  --set runner.phase=multi \
  --set runner.messageSize=256 \
  --set runner.duration=600 \
  --set clientConfigSecret=kafka-multi-small-consumer \
  --set clientConfigKey=client.properties \
  --set suspend=true

helm install kafka-multi-medium charts/kafka-perf-runner \
  -n "$NAMESPACE" \
  --set image.repository=$NEXUS_REGISTRY/kafka-perf/kafka-perf-runner \
  --set image.tag=latest \
  --set bootstrapServers="$BOOTSTRAP" \
  --set runner.phase=multi \
  --set runner.messageSize=4096 \
  --set runner.duration=600 \
  --set clientConfigSecret=kafka-multi-medium-consumer \
  --set clientConfigKey=client.properties \
  --set suspend=true

helm install kafka-multi-large charts/kafka-perf-runner \
  -n "$NAMESPACE" \
  --set image.repository=$NEXUS_REGISTRY/kafka-perf/kafka-perf-runner \
  --set image.tag=latest \
  --set bootstrapServers="$BOOTSTRAP" \
  --set runner.phase=multi \
  --set runner.messageSize=1048576 \
  --set runner.duration=600 \
  --set clientConfigSecret=kafka-multi-large-consumer \
  --set clientConfigKey=client.properties \
  --set suspend=true
```

The script merges this file with the base `baseline-consumer.properties`, so your `group.id` override takes effect while preserving other defaults.

### Spring runner: mimic many consumer pods (same group)
To simulate “many pods in the same consumer group” with Spring Kafka:
- Make all consumer threads share the same `group.id` and let partitions be divided among them, or
- Use `@KafkaListener(concurrency=N, groupId=...)` which handles group membership and partition assignment for you, or
- Run multiple Spring Jobs/Pods with the same `groupId`.


