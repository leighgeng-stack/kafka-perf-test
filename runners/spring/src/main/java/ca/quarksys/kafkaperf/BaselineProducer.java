package ca.quarksys.kafkaperf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
public class BaselineProducer {

    private static final Logger log = LoggerFactory.getLogger(BaselineProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final BaselineProperties properties;

    public BaselineProducer(KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper,
                            BaselineProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public ProducerStats run(PhaseConfig phase, Instant warmupEnd, Instant phaseEnd) throws Exception {
        long intervalNanos = phase.getTargetThroughput() > 0
                ? 1_000_000_000L / phase.getTargetThroughput()
                : 0;
        long producedTotal = 0;
        long producedMeasured = 0;
        long nextTick = System.nanoTime();
        long messageId = 0;

        String payloadData = "x".repeat(Math.max(1, properties.getMessageSize() - 50));

        while (Instant.now().isBefore(phaseEnd)) {
            long now = System.currentTimeMillis();
            messageId++;
            BaselineMessage payload = new BaselineMessage(messageId, now, payloadData);
            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(phase.getTopic(), json);
            producedTotal++;
            if (!Instant.ofEpochMilli(now).isBefore(warmupEnd)) {
                producedMeasured++;
            }
            if (intervalNanos > 0) {
                nextTick += intervalNanos;
                long sleepNanos = nextTick - System.nanoTime();
                if (sleepNanos > 0) {
                    TimeUnit.NANOSECONDS.sleep(sleepNanos);
                }
            }
        }
        kafkaTemplate.flush();

        double throughput = producedMeasured / (double) properties.getDurationSeconds();
        log.debug("Producer phase {} finished: total={}, measured={}, throughput={} msg/s",
                phase.getName(), producedTotal, producedMeasured, throughput);
        return new ProducerStats(producedTotal, producedMeasured, throughput);
    }
}

