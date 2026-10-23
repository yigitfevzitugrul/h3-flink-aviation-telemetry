package com.aerotelemetry;

import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.serde.ParsePositions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

/**
 * Gerçek zamanlı havacılık telemetrisi pipeline'ı.
 *
 * <pre>
 * Kafka (aircraft-positions) -> JSON parse (bozuklar side output) -> event-time watermark -> ...
 * </pre>
 */
public class TelemetryJob {

    static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(10);
    static final Duration SOURCE_IDLENESS = Duration.ofMinutes(1);

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
        String topic = env("KAFKA_TOPIC", "aircraft-positions");
        String groupId = env("KAFKA_GROUP_ID", "aero-telemetry");

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
                .setParallelism(3)
                .process(new ParsePositions())
                .name("parse-json")
                .setParallelism(3);

        DataStream<AircraftPosition> positions = parsed
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<AircraftPosition>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner((p, ts) -> p.eventTime)
                        .withIdleness(SOURCE_IDLENESS))
                .name("event-time-watermarks")
                .setParallelism(3);

        parsed.getSideOutput(ParsePositions.INVALID).print("INVALID").name("invalid-sink");
        positions.print("POS").name("debug-print");

        env.execute("aero-telemetry");
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }
}
