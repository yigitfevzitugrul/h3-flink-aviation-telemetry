package com.aerotelemetry.detect;

import com.aerotelemetry.model.Alert;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Aynı anahtar (ör. uçak çifti) için bekleme süresi dolmadan gelen tekrar uyarıları bastırır.
 * Birlikte uçan iki uçak her hücre geçişinde yeniden yakalanır; bu operatör onları tek uyarıya indirir.
 */
public class AlertCooldown extends KeyedProcessFunction<String, Alert, Alert> {

    private final long cooldownMs;
    private transient ValueState<Long> lastEmitted;

    public AlertCooldown(Duration cooldown) {
        this.cooldownMs = cooldown.toMillis();
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("last-emitted", Long.class);
        desc.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMillis(cooldownMs).multipliedBy(2)).build());
        lastEmitted = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Alert alert, Context ctx, Collector<Alert> out) throws Exception {
        Long last = lastEmitted.value();
        if (last == null || alert.eventTime - last >= cooldownMs) {
            lastEmitted.update(alert.eventTime);
            out.collect(alert);
        }
    }
}
