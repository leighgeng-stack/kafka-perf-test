package ca.quarksys.kafkaperf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class BaselineRunner {

    private static final Logger log = LoggerFactory.getLogger(BaselineRunner.class);

    private final BaselineProperties properties;
    private final BaselineProducer baselineProducer;
    private final BaselineConsumer baselineConsumer;
    private final ObjectMapper objectMapper;

    public BaselineRunner(BaselineProperties properties,
                          BaselineProducer baselineProducer,
                          BaselineConsumer baselineConsumer,
                          ObjectMapper objectMapper) {
        this.properties = properties;
        this.baselineProducer = baselineProducer;
        this.baselineConsumer = baselineConsumer;
        this.objectMapper = objectMapper;
    }

    public void execute() throws Exception {
        Path artifactsRoot = Path.of(properties.getArtifactsDir());
        Files.createDirectories(artifactsRoot);
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers()
        ))) {
            for (PhaseConfig phase : properties.getPhases()) {
                runPhase(admin, artifactsRoot, phase);
            }
        }
    }

    private void runPhase(AdminClient admin, Path artifactsRoot, PhaseConfig phase) throws Exception {
        short replication = properties.getReplicationFactor();
        short minIsr = (short) Math.min(replication, 2);
        recreateTopic(admin, phase, replication, minIsr);

        long warmupMillis = TimeUnit.SECONDS.toMillis(properties.getWarmupSeconds());
        long measurementMillis = TimeUnit.SECONDS.toMillis(properties.getDurationSeconds());
        long totalMillis = warmupMillis + measurementMillis;

        Instant phaseStart = Instant.now();
        Instant warmupEnd = phaseStart.plusMillis(warmupMillis);
        Instant phaseEnd = phaseStart.plusMillis(totalMillis);

        AtomicBoolean producerDone = new AtomicBoolean(false);
        int consumerThreads = Math.max(1, phase.getConsumerThreads());
        ExecutorService executor = Executors.newFixedThreadPool(consumerThreads + 1);
        try {
            Future<ProducerStats> producerFuture = executor.submit(() ->
                    baselineProducer.run(phase, warmupEnd, phaseEnd));
            Future<ConsumerStats> consumerFuture = executor.submit(() ->
                    baselineConsumer.run(phase, warmupEnd, phaseEnd, producerDone));

            ProducerStats producerStats = producerFuture.get();
            producerDone.set(true);
            ConsumerStats consumerStats = consumerFuture.get();

            PhaseSummary summary = new PhaseSummary(
                    phase.getName(),
                    producerStats.totalSent(),
                    producerStats.measuredSent(),
                    producerStats.throughputPerSecond(),
                    consumerStats.consumed(),
                    consumerStats.latencyTracker().snapshot()
            );

            writeSummary(artifactsRoot, phase, summary);
            log.info("Phase {} complete: produced={} measured={} throughput={} msg/s, consumed={} latency={}",
                    phase.getName(),
                    summary.producedTotal(),
                    summary.producedMeasured(),
                    String.format("%.2f", summary.producerThroughputPerSec()),
                    summary.consumed(),
                    summary.latencyStats());
        } finally {
            executor.shutdownNow();
        }
    }

    private void recreateTopic(AdminClient admin, PhaseConfig phase, short replication, short minIsr) throws Exception {
        try {
            admin.deleteTopics(Collections.singletonList(phase.getTopic())).all().get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
        Map<String, String> configs = new HashMap<>();
        configs.put("min.insync.replicas", String.valueOf(Math.max(1, Math.min(minIsr, replication))));
        NewTopic topic = new NewTopic(phase.getTopic(), phase.getPartitions(), replication)
                .configs(configs);
        admin.createTopics(Collections.singletonList(topic)).all().get(10, TimeUnit.SECONDS);
        log.info("Created topic {} partitions={} rf={}", phase.getTopic(), phase.getPartitions(), replication);
    }

    private void writeSummary(Path artifactsRoot, PhaseConfig phase, PhaseSummary summary) throws IOException {
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());
        Path phaseDir = artifactsRoot.resolve(timestamp + "_" + phase.getName() + "_spring");
        Files.createDirectories(phaseDir);
        Path summaryPath = phaseDir.resolve("summary.json");
        Files.writeString(summaryPath, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(summary));
    }

}

