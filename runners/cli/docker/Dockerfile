ARG SCALA_VERSION=2.13
ARG KAFKA_VERSION=3.8.0

FROM eclipse-temurin:21-jre AS kafka-tools

ARG SCALA_VERSION
ARG KAFKA_VERSION

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl jq ca-certificates python3 \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o /tmp/kafka.tgz \
 && tar -xzf /tmp/kafka.tgz -C /opt \
 && mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
 && rm /tmp/kafka.tgz

ENV PATH="/opt/kafka/bin:${PATH}"
WORKDIR /workspace/runners/cli
COPY . /workspace
RUN chmod +x /workspace/runners/cli/scripts/run_baseline.sh
CMD ["sleep", "infinity"]

