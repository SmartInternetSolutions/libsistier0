package com.sis.util;

public class MathHelper {
	/**
	 * TODO earth is an ellipsoid not a sphere
	 */
	public static Double[] fromLatLongToCartesian(double lat, double lng) {
		final double earthRadius = 6371.f;

		double clat = java.lang.Math.cos(lat);
		double clng = java.lang.Math.cos(lng);
		double slat = java.lang.Math.sin(lat);
		double slng = java.lang.Math.sin(lng);
		
		return new Double[] {earthRadius * clat * clng, earthRadius * clat * slng, earthRadius * slat};
	}
}
