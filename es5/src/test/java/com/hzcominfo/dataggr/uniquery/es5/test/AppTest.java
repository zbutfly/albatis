package com.hzcominfo.dataggr.uniquery.es5.test;

import com.hzcominfo.dataggr.uniquery.Client;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;

import net.butfly.albacore.io.URISpec;

public class AppTest {

	public static void main(String[] args) throws Exception {
		//建立连接
//		String uri = "es://pseudo-elasticsearch@172.16.27.232:9300";
		String uri = "elasticsearch://hzcominfo@172.30.10.31:39300";
		URISpec uriSpec = new URISpec(uri);
		Client conn = new Client(uriSpec);
		//执行查询
//		String sql = "select * from uniquery.sell";
//		String sql = "SELECT * FROM track_dev.ES_ORA_GAZHK_LGY_NB WHERE ZJHM_s.keyword = '33082518240610278X'";
//		String sql = "select count(*) from track_dev.ES_ORA_GAZHK_LGY_NB WHERE ZJHM_s.keyword = '33082518240610278X'";
		String sql = "select * from test_wj_tag.PERSON where target.keyword='36243018630709904X' and public_tags.\"user\"= '09876' limit 1";
		ResultSet r = conn.execute(sql);
		List<Map<String, Object>> result = r.getResults();
		System.out.println(r.getTotal());
		
		// dynamic test
		/*String sql = "select * from uniquery.sell where name=? and sell = ?";
		Object[] params = {"xx", "200"}; 
		List<Map<String, Object>> result = conn.execute(sql, params);
		System.out.println(result);*/
		
		/*String sql = "select a.b from uniquery.sell where name not in (?, ?)";
		Object[] params = {"xx", "yy"}; 
		List<Map<String, Object>> result = conn.execute(sql, params);
		System.out.println(result);*/
		
		// facet
		/*String sql = "select name, years, sum(sell), max(sell) from uniquery.sell group by name.keyword, years";
		Object[] params = {}; 
		List<Map<String, Object>> result = conn.execute(sql, params);
		System.out.println(result);*/
		
		//multi facet test
		/*String sql = "select * from uniquery.sell";
		String[] facets = {"name.keyword", "years"};
		List<List<Map<String, Object>>> result = conn.execute(sql, facets, "");
		System.out.println(result);*/
		
		// count test
		/*String sql = "select count(*) from uniquery.sell";
		ResultSet r = conn.execute(sql);
		System.out.println(r.getTotal());*/
		
		// geo func search
		String sql = "SELECT * FROM test_hzwa.WA_SOURCE_FJ_1001_TEST_JZ WHERE MAC_s.keyword = 'E0-3E-44-04-00-00' ORDER BY CAPTURE_TIME_s.keyword ASC"; // 圆形
//		sql = "select LOCATION from test_hzwa.TEST_HZWA_WA_SOURCE_FJ_1001 where geo_box(LOCATION,130, 0, 0, 40)"; // 矩形
//		sql = "select LOCATION from test_hzwa.TEST_HZWA_WA_SOURCE_FJ_1001 where geo_polygon(LOCATION,50,140, 50,0, 20,0, 20,100, 0,100, 0,140, 50,140)"; // 多边形
		ResultSet r = conn.execute(sql);
		System.out.println(r);*/
		
		// nested having
		/*String sql = "select * from subject_person.person  \n"
+ "where LG.LGDM_LGMC_FORMAT_s = '横塘村十组旅馆'  \n"
+ "group by SFZH_s, LG.ZKLSH_s   \n"
+ "having count(LG.ZKLSH_s) >= 3";*/
//		String sql = "SELECT * FROM test_phga_search.dpc_solr_czrk where query_all('chenw')";
		
		//test
//		System.setProperty("uniquery.default.aggsize", "15");
		String sql = "SELECT COUNT(XM_s.keyword) as cnt FROM test_phga_search.PH_ACU_QYYRXX GROUP BY XM_s.keyword"; 
		ResultSet r = conn.execute(sql);
		System.out.println(r);
		
		//释放连接
		conn.close();
	}
}
