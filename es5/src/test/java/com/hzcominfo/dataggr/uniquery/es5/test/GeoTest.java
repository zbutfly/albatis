package com.hzcominfo.dataggr.uniquery.es5.test;

import com.hzcominfo.dataggr.uniquery.Client;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;

import net.butfly.albacore.io.URISpec;

public class GeoTest {

	public static void main(String[] args) throws Exception {
		/*geo_distance(field,x,y,d);
		geo_box(field,top,left,bottom,right);
		geo_polygon(field,x1,y1,x2,y2...xn,yn,x1,y1)*/

		String uri = "elasticsearch://cidev@172.16.17.11:39300";
		URISpec uriSpec = new URISpec(uri);
		Client conn = new Client(uriSpec);
		String sql = "select LOCATION from test_hzwa.TEST_HZWA_WA_SOURCE_FJ_1001 where geo_distance(LOCATION,30.287797,120.052425,0.5)"; // 圆形
//		sql = "select LOCATION from test_hzwa.TEST_HZWA_WA_SOURCE_FJ_1001 where geo_box(LOCATION,130, 0, 0, 40)"; // 矩形
//		sql = "select LOCATION from test_hzwa.TEST_HZWA_WA_SOURCE_FJ_1001 where geo_polygon(LOCATION,50,140, 50,0, 20,0, 20,100, 0,100, 0,140, 50,140)"; // 多边形
		ResultSet r = conn.execute(sql);
		System.out.println(r);
		conn.close();
	}
}