package ca.quarksys.kafkaperf;

public record ProducerStats(long totalSent, long measuredSent, double throughputPerSecond) {
}

