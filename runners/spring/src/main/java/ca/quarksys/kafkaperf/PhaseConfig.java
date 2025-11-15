package ca.quarksys.kafkaperf;

public class PhaseConfig {
    private String name;
    private String topic;
    private int partitions;
    private int targetThroughput;
    private int consumerThreads;

    public PhaseConfig() {
    }

    public PhaseConfig(String name, String topic, int partitions, int targetThroughput, int consumerThreads) {
        this.name = name;
        this.topic = topic;
        this.partitions = partitions;
        this.targetThroughput = targetThroughput;
        this.consumerThreads = consumerThreads;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getTargetThroughput() {
        return targetThroughput;
    }

    public void setTargetThroughput(int targetThroughput) {
        this.targetThroughput = targetThroughput;
    }

    public int getConsumerThreads() {
        return consumerThreads;
    }

    public void setConsumerThreads(int consumerThreads) {
        this.consumerThreads = consumerThreads;
    }
}

