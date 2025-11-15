FROM gradle:8.7-jdk21 AS build
WORKDIR /build
COPY runners/spring/ .
RUN gradle -q bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /workspace
RUN mkdir -p /opt/app
COPY --from=build /build/build/libs/*.jar /opt/app/app.jar
ENV SPRING_PROFILES_ACTIVE=default
ENTRYPOINT ["java","-jar","/opt/app/app.jar"]

