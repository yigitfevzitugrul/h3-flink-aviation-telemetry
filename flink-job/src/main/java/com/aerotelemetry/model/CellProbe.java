package com.aerotelemetry.model;

/**
 * Yakınlık tespiti için bir konumun bir hedef H3 hücresine gönderilen kopyası.
 * {@code home} true ise konum bu hücrenin içindedir; false ise komşu hücreden gelen bir sorgudur.
 */
public class CellProbe {

    public String targetCell;
    public boolean home;
    public AircraftPosition position;

    public CellProbe() {}

    public CellProbe(String targetCell, boolean home, AircraftPosition position) {
        this.targetCell = targetCell;
        this.home = home;
        this.position = position;
    }
}
