package ca.quarksys.kafkaperf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class BaselineConsumer {

    private static final Logger log = LoggerFactory.getLogger(BaselineConsumer.class);

    private final ConsumerFactory<String, String> consumerFactory;
    private final ObjectMapper objectMapper;

    public BaselineConsumer(ConsumerFactory<String, String> consumerFactory,
                            ObjectMapper objectMapper) {
        this.consumerFactory = consumerFactory;
        this.objectMapper = objectMapper;
    }

    public ConsumerStats run(PhaseConfig phase,
                             Instant warmupEnd,
                             Instant phaseEnd,
                             AtomicBoolean producerDone) throws Exception {
        LatencyTracker latencyTracker = new LatencyTracker();
        AtomicLong consumed = new AtomicLong();
        int threads = Math.max(1, phase.getConsumerThreads());
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<java.util.concurrent.Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                tasks.add(() -> {
                    consumeLoop(phase, warmupEnd, phaseEnd, producerDone, latencyTracker, consumed);
                    return null;
                });
            }
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
        }
        log.debug("Consumer phase {} finished: records={} latency={}",
                phase.getName(), consumed.get(), latencyTracker.snapshot());
        return new ConsumerStats(consumed.get(), latencyTracker);
    }

    private void consumeLoop(PhaseConfig phase,
                             Instant warmupEnd,
                             Instant phaseEnd,
                             AtomicBoolean producerDone,
                             LatencyTracker latencyTracker,
                             AtomicLong consumed) {
        String groupId = "baseline-spring-" + phase.getName() + "-" + UUID.randomUUID();
        try (Consumer<String, String> consumer = consumerFactory.createConsumer(groupId, "baseline", null)) {
            consumer.subscribe(Collections.singletonList(phase.getTopic()));
            while (Instant.now().isBefore(phaseEnd.plusSeconds(30)) || !producerDone.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(record -> {
                    consumed.incrementAndGet();
                    processRecord(record.value(), warmupEnd, latencyTracker);
                });
                consumer.commitAsync();
                if (producerDone.get() && records.isEmpty()
                        && Instant.now().isAfter(phaseEnd.plusSeconds(5))) {
                    break;
                }
            }
        }
    }

    private void processRecord(String payload, Instant warmupEnd, LatencyTracker tracker) {
        try {
            BaselineMessage message = objectMapper.readValue(payload, BaselineMessage.class);
            Instant producedAt = Instant.ofEpochMilli(message.ts());
            if (producedAt.isBefore(warmupEnd)) {
                return;
            }
            long latencyMs = Instant.now().toEpochMilli() - message.ts();
            tracker.record(latencyMs);
        } catch (Exception e) {
            log.warn("Failed to parse message for latency tracking", e);
        }
    }
}

