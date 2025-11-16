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


