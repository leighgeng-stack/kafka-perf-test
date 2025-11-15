package ca.quarksys.kafkaperf;

import java.util.Map;

public record PhaseSummary(
        String phase,
        long producedTotal,
        long producedMeasured,
        double producerThroughputPerSec,
        long consumed,
        Map<String, Object> latencyStats
) {
}

