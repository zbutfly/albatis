package com.hzcominfo.dataggr.uniquery.es5.test;

import java.util.List;
import java.util.Map;

import com.hzcominfo.dataggr.uniquery.Client;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;

import net.butfly.albacore.io.URISpec;

public class AppTest {

	public static void main(String[] args) throws Exception {
		//建立连接
//		String uri = "es://pseudo-elasticsearch@172.16.27.232:9300";
		String uri = "elasticsearch://cidev@172.16.17.11:39300";
		URISpec uriSpec = new URISpec(uri);
		Client conn = new Client(uriSpec);
		Client conn1 = new Client(uriSpec);
		Client conn2 = new Client(uriSpec);
		//执行查询
//		String sql = "select * from uniquery.sell";
		String sql = "SELECT * FROM track_dev.ES_ORA_GAZHK_LGY_NB WHERE ZJHM_s.keyword = '33082518240610278X'";
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
		long result = conn.execute(sql);
		System.out.println(result);*/
		
		//释放连接
		conn.close();
	}
}
