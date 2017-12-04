package com.hzcominfo.dataggr.uniquery.es5.test;

import java.util.List;
import java.util.Map;

import com.hzcominfo.dataggr.uniquery.Client;

import net.butfly.albacore.io.URISpec;

public class AppTest {

	public static void main(String[] args) throws Exception {
		//建立连接
		String uri = "es://pseudo-elasticsearch@172.16.16.232:9300";
		URISpec uriSpec = new URISpec(uri);
		Client conn = new Client(uriSpec);
		//执行查询
		/*String sql = "select * from uniquery.sellinfo";
		List<Map<String, Object>> result = conn.execute(sql);
		System.out.println(result);*/
		
		// dynamic test
		/*String sql = "select * from uniquery.sellinfo where name=? and sell = ?";
		Object[] params = {"xx", "200"}; 
		List<Map<String, Object>> result = conn.execute(sql, params);
		System.out.println(result);*/
		
		String sql = "select uniquery.sellinfo.a.b from uniquery.sellinfo where name not in (?, ?)";
		Object[] params = {"xx", "yy"}; 
		List<Map<String, Object>> result = conn.execute(sql, params);
		System.out.println(result);
		
		
		//multi facet test
		/*String sql = "select * from tdl_c1";
		String[] facets = {"XB_FORMAT_s", "MZ_FORMAT_s", "MZ_i"};
		List<List<Map<String, Object>>> result = conn.execute(sql, facets, "");
		System.out.println(result);*/
		
		//释放连接
		conn.close();
	}
}
