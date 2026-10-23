package com.aerotelemetry.serde;

import com.aerotelemetry.model.AircraftPosition;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Ham JSON'u ayrıştırır. Bozuk kayıtlar job'ı düşürmek yerine
 * {@link #INVALID} side output'una gider ve bir metrikte sayılır.
 */
public class ParsePositions extends ProcessFunction<String, AircraftPosition> {

    public static final OutputTag<String> INVALID = new OutputTag<>("invalid-records") {};

    private transient Counter invalidRecords;

    @Override
    public void open(OpenContext openContext) {
        invalidRecords = getRuntimeContext().getMetricGroup().counter("invalidRecords");
    }

    @Override
    public void processElement(String value, Context ctx, Collector<AircraftPosition> out) {
        try {
            out.collect(PositionParser.parse(value));
        } catch (PositionParser.InvalidRecordException e) {
            invalidRecords.inc();
            ctx.output(INVALID, e.getMessage() + " | " + value);
        }
    }
}
