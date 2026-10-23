package com.aerotelemetry;

import com.aerotelemetry.agg.CellAggregator;
import com.aerotelemetry.detect.AlertCooldown;
import com.aerotelemetry.detect.AnomalyDetector;
import com.aerotelemetry.detect.ProximityDetector;
import com.aerotelemetry.detect.ProximityFanOut;
import com.aerotelemetry.h3.H3Enricher;
import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.model.Alert;
import com.aerotelemetry.model.CellStats;
import com.aerotelemetry.serde.ParsePositions;
import com.aerotelemetry.sink.JdbcSinks;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

/**
 * Gerçek zamanlı havacılık telemetrisi pipeline'ı.
 *
 * <pre>
 * Kafka -> JSON parse (bozuklar side output) -> watermark -> H3 zenginleştirme
 *   ├─ keyBy(res-5 hücre) -> 1 dk tumbling window            -> PostgreSQL cell_traffic
 *   ├─ keyBy(icao24) -> AnomalyDetector                      ─┐
 *   └─ gridDisk fan-out -> keyBy(res-7 hücre) -> Proximity    ─┴> PostgreSQL alerts
 * </pre>
 */
public class TelemetryJob {

    static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(10);
    static final Duration SOURCE_IDLENESS = Duration.ofMinutes(1);
    static final Duration TRAFFIC_WINDOW = Duration.ofMinutes(1);
    static final Duration PROXIMITY_COOLDOWN = Duration.ofMinutes(5);

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
        String topic = env("KAFKA_TOPIC", "aircraft-positions");
        String groupId = env("KAFKA_GROUP_ID", "aero-telemetry");
        JdbcConnectionOptions db = JdbcSinks.connection(
                env("JDBC_URL", "jdbc:postgresql://postgres:5432/telemetry"),
                env("POSTGRES_USER", "telemetry"),
                env("POSTGRES_PASSWORD", "telemetry"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Kaynak paralelliği = partisyon sayısı (3). Parse operatörü kaynağa zincirlendiği için
        // watermark'lar fiilen partisyon başına üretilir.
        SingleOutputStreamOperator<AircraftPosition> parsed = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-positions")
                .uid("kafka-source")
                .setParallelism(3)
                .process(new ParsePositions())
                .uid("parse-json")
                .name("parse-json")
                .setParallelism(3);

        DataStream<AircraftPosition> positions = parsed
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<AircraftPosition>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner((p, ts) -> p.eventTime)
                        .withIdleness(SOURCE_IDLENESS))
                .uid("watermarks")
                .name("event-time-watermarks")
                .setParallelism(3)
                .map(new H3Enricher())
                .uid("h3-enrich")
                .name("h3-enrich")
                .setParallelism(3);

        parsed.getSideOutput(ParsePositions.INVALID).print("INVALID").name("invalid-sink");

        // 1) H3 res-5 hücresi başına dakikalık trafik yoğunluğu
        DataStream<CellStats> cellTraffic = positions
                .keyBy(p -> p.h3Region)
                .window(TumblingEventTimeWindows.of(TRAFFIC_WINDOW))
                .aggregate(new CellAggregator(), new CellAggregator.AttachWindow())
                .uid("cell-traffic-window")
                .name("cell-traffic-1m");

        cellTraffic.sinkTo(JdbcSinks.cellTraffic(db)).uid("cell-traffic-sink").name("pg-cell-traffic");

        // 2) Uçak bazlı anomaliler (keyed state)
        DataStream<Alert> anomalies = positions
                .keyBy(p -> p.icao24)
                .process(new AnomalyDetector())
                .uid("anomaly-detector")
                .name("anomaly-detector");

        // 3) Yakın uçuşlar: konumu kendi ve komşu H3 hücrelerine dağıt, hücre bazında karşılaştır,
        //    birlikte uçan çiftlerin tekrarlayan uyarılarını çift bazında bastır
        DataStream<Alert> proximity = positions
                .flatMap(new ProximityFanOut())
                .uid("proximity-fanout")
                .name("proximity-fanout")
                .keyBy(probe -> probe.targetCell)
                .process(new ProximityDetector())
                .uid("proximity-detector")
                .name("proximity-detector")
                .keyBy(a -> a.icao24 + "|" + a.otherIcao24)
                .process(new AlertCooldown(PROXIMITY_COOLDOWN))
                .uid("proximity-cooldown")
                .name("proximity-cooldown");

        anomalies.union(proximity)
                .sinkTo(JdbcSinks.alerts(db))
                .uid("alerts-sink")
                .name("pg-alerts");

        env.execute("aero-telemetry");
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }
}
