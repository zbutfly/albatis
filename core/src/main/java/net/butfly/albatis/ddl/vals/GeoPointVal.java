package net.butfly.albatis.ddl.vals;

import java.util.Arrays;

public final class GeoPointVal {
	public static final ValType TYPE = ValType.GEO;
	private static final double MIN_LAT = -90;
	private static final double MAX_LAT = 90;
	private static final double MIN_LON = -180;
	private static final double MAX_LON = 180;

	public final double longitude, latitude;

	public GeoPointVal(double[] latAndLon) {
		super();
		double[] ll = valid(latAndLon);
		this.latitude = ll[0];
		this.longitude = ll[1];
	}

	public GeoPointVal(double latitude, double longitude) {
		this(new double[] { latitude, longitude });
	}

	public GeoPointVal(String geo) {
		this(parse(geo));
	}

	@Override
	public String toString() {
		return latitude + "," + longitude;
	}

	public static double[] parse(String geo) {
		String[] fields = geo.split(",", 2);
		if (fields.length != 2) throw new IllegalArgumentException("Geo coordinate point should be \"lat,lon\", but \"" + geo + "\"");
		return new double[] { Double.parseDouble(fields[0]), Double.parseDouble(fields[1]) };
	}

	public static double[] valid(double[] latAndLon) {
		if (latAndLon.length != 2) throw new IllegalArgumentException("Geo point should be \"lat,lon\", but " + Arrays.asList(latAndLon));
		if (latAndLon[0] >= MIN_LAT && latAndLon[0] <= MAX_LAT && latAndLon[1] >= MIN_LON && latAndLon[1] <= MAX_LON) return latAndLon;
		else if (latAndLon[1] >= MIN_LAT && latAndLon[1] <= MAX_LAT && latAndLon[0] >= MIN_LON && latAndLon[0] <= MAX_LON)
			return new double[] { latAndLon[1], latAndLon[0] };
		else throw new IllegalArgumentException("Geo value invalid:  \"" + latAndLon[0] + "," + latAndLon[1] + "\"");
	}
}
