package com.hzcominfo.dataggr.uniquery.es5.test;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilders;

public class GeoTest {

	public static void main(String[] args) {
		// geo_distance(field,lat,lon,d);
		//http://172.16.17.11/ cidev  test_hzwa TEST_HZWA_WA_SOURCE_FJ_1001
		QueryBuilders.geoDistanceQuery("field")  
	    .point(Double.valueOf("lat"), Double.valueOf("lon"))                                 
	    .distance(Double.valueOf("distance"), DistanceUnit.KILOMETERS).geoDistance(GeoDistance.PLANE);
	}
}