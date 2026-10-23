#!/usr/bin/env bash
# JobManager hazır olana kadar bekler, çalışan eski sürümü durdurur ve yeni jar'ı gönderir.
# Kafka offset'leri checkpoint'lerde commit edildiği için yeni job kaldığı yerden devam eder.
set -euo pipefail

JM="${JOBMANAGER:-jobmanager:8081}"
JOB_NAME="aero-telemetry"

echo "JobManager bekleniyor ($JM)..."
until /opt/flink/bin/flink list -m "$JM" > /dev/null 2>&1; do sleep 3; done

for id in $(/opt/flink/bin/flink list -r -m "$JM" 2>/dev/null | grep ": $JOB_NAME (RUNNING)" | awk -F' : ' '{print $2}'); do
  echo "Çalışan job durduruluyor: $id"
  /opt/flink/bin/flink cancel -m "$JM" "$id"
done

echo "Job gönderiliyor..."
exec /opt/flink/bin/flink run -d -m "$JM" /opt/flink/usrlib/telemetry-job.jar
