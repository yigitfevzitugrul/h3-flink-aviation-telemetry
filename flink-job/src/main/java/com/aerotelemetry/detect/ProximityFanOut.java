package com.aerotelemetry.detect;

import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.model.CellProbe;
import com.uber.h3core.H3Core;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * Her havadaki konumu kendi yakınlık hücresine ve 6 komşusuna ({@code gridDisk(cell, 1)}) dağıtır.
 * Böylece hücre sınırının iki yanındaki uçaklar da aynı key altında karşılaştırılabilir.
 */
public class ProximityFanOut extends RichFlatMapFunction<AircraftPosition, CellProbe> {

    static final double MIN_ALTITUDE_M = 500.0;   // havalimanı çevresindeki yer trafiğini hariç tut

    private transient H3Core h3;

    @Override
    public void open(OpenContext openContext) throws IOException {
        h3 = H3Core.newInstance();
    }

    @Override
    public void flatMap(AircraftPosition p, Collector<CellProbe> out) {
        if (p.onGround || p.baroAltitude == null || p.baroAltitude < MIN_ALTITUDE_M) {
            return;
        }
        for (String cell : h3.gridDisk(p.h3Proximity, 1)) {
            out.collect(new CellProbe(cell, cell.equals(p.h3Proximity), p));
        }
    }
}
