#!/usr/bin/env bash
# JobManager hazır olana kadar bekler; job zaten çalışıyorsa tekrar göndermez.
set -euo pipefail

JM="${JOBMANAGER:-jobmanager:8081}"
JOB_NAME="aero-telemetry"

echo "JobManager bekleniyor ($JM)..."
until /opt/flink/bin/flink list -m "$JM" > /dev/null 2>&1; do sleep 3; done

if /opt/flink/bin/flink list -r -m "$JM" 2>/dev/null | grep -q ": $JOB_NAME (RUNNING)"; then
  echo "'$JOB_NAME' zaten çalışıyor, gönderim atlandı."
  exit 0
fi

echo "Job gönderiliyor..."
exec /opt/flink/bin/flink run -d -m "$JM" /opt/flink/usrlib/telemetry-job.jar
