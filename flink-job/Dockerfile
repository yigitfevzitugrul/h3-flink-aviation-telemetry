# syntax=docker/dockerfile:1
# 1) Fat jar'ı derle (lokalde Maven kurulu olması gerekmez)
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package

# 2) Jar'ı çalışan Flink cluster'ına gönderen küçük imaj
FROM flink:1.20.3-java17
COPY --from=build /src/target/telemetry-job.jar /opt/flink/usrlib/telemetry-job.jar
COPY submit.sh /submit.sh
RUN chmod +x /submit.sh
ENTRYPOINT ["/submit.sh"]
