plugins {
    id("org.springframework.boot") version "3.3.0"
    id("io.spring.dependency-management") version "1.1.5"
    java
}

group = "ca.quarksys"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("org.apache.kafka:kafka-clients:3.8.0")
    implementation("org.hdrhistogram:HdrHistogram:2.1.12")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

