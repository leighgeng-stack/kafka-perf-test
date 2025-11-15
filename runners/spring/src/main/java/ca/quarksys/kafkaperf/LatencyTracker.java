package ca.quarksys.kafkaperf;

import org.HdrHistogram.Histogram;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class LatencyTracker {

    private final Histogram histogram = new Histogram(3_600_000, 3); // up to 1 hour latency (ms)
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong max = new AtomicLong();

    public void record(long latencyMs) {
        if (latencyMs < 0) {
            return;
        }
        synchronized (histogram) {
            histogram.recordValue(Math.min(latencyMs, 3_600_000));
        }
        count.incrementAndGet();
        max.updateAndGet(prev -> Math.max(prev, latencyMs));
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> stats = new HashMap<>();
        if (count.get() == 0) {
            stats.put("count", 0);
            return stats;
        }
        synchronized (histogram) {
            stats.put("count", count.get());
            stats.put("p50_ms", histogram.getValueAtPercentile(50));
            stats.put("p95_ms", histogram.getValueAtPercentile(95));
            stats.put("p99_ms", histogram.getValueAtPercentile(99));
        }
        stats.put("max_ms", max.get());
        return stats;
    }
}

