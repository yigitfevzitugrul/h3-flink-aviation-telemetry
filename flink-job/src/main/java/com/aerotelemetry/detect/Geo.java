package com.aerotelemetry.detect;

/** Küçük coğrafi yardımcılar. */
final class Geo {

    private static final double EARTH_RADIUS_M = 6_371_000.0;

    private Geo() {}

    /** İki nokta arasındaki büyük daire mesafesi (metre). */
    static double haversineM(double lat1, double lon1, double lat2, double lon2) {
        double p1 = Math.toRadians(lat1);
        double p2 = Math.toRadians(lat2);
        double dp = p2 - p1;
        double dl = Math.toRadians(lon2 - lon1);
        double a = Math.pow(Math.sin(dp / 2), 2) + Math.cos(p1) * Math.cos(p2) * Math.pow(Math.sin(dl / 2), 2);
        return 2 * EARTH_RADIUS_M * Math.asin(Math.sqrt(a));
    }

    static double metersToFeet(double m) {
        return m * 3.28084;
    }
}
