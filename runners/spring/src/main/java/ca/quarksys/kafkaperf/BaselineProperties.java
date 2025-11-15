package ca.quarksys.kafkaperf;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ConfigurationProperties(prefix = "baseline")
public class BaselineProperties {

    private String bootstrapServers = Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS"))
            .orElse("localhost:9092");
    private String artifactsDir = Optional.ofNullable(System.getenv("ARTIFACTS_DIR"))
            .orElse("/workspace/artifacts");
    private int warmupSeconds = 120;
    private int durationSeconds = 600;
    private int messageSize = 1024;
    private short replicationFactor = 3;
    private List<PhaseConfig> phases = defaultPhases();

    private List<PhaseConfig> defaultPhases() {
        List<PhaseConfig> defaults = new ArrayList<>();
        defaults.add(new PhaseConfig("single", "baseline-1p", 12, 20_000, 1));
        defaults.add(new PhaseConfig("multi", "baseline-12p", 12, 240_000, 6));
        defaults.add(new PhaseConfig("app", "baseline-app", 12, 240_000, 6));
        return defaults;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getArtifactsDir() {
        return artifactsDir;
    }

    public void setArtifactsDir(String artifactsDir) {
        this.artifactsDir = artifactsDir;
    }

    public int getWarmupSeconds() {
        return warmupSeconds;
    }

    public void setWarmupSeconds(int warmupSeconds) {
        this.warmupSeconds = warmupSeconds;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(int durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public List<PhaseConfig> getPhases() {
        return phases;
    }

    public void setPhases(List<PhaseConfig> phases) {
        if (phases != null && !phases.isEmpty()) {
            this.phases = phases;
        }
    }
}

