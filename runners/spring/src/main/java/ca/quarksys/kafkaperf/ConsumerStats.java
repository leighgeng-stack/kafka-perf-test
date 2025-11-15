package ca.quarksys.kafkaperf;

public record ConsumerStats(long consumed, LatencyTracker latencyTracker) {
}

