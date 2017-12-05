package com.hzcominfo.dataggr.uniquery.test;

import java.util.List;
import java.util.Map;

import com.hzcominfo.dataggr.uniquery.Client;

import net.butfly.albacore.io.URISpec;

public class AppTest {

	public static void main(String[] args) {
		//建立连接
		String uri = "solr:http://172.16.17.11:10180/solr/tdl_c1";
		URISpec uriSpec = new URISpec(uri);
		Client conn = new Client(uriSpec);
		
		//执行查询
		String sql = "select * from tdl_c1";
		List<Map<String, Object>> result = conn.execute(sql, "");
		System.out.println(result);
//				Map r1 = conn.execute(sql, "");
//				List<Map> r2 = conn.execute(sql, "");
//				long r3 = conn.execute(sql, "");
		
		//释放连接
		try {
			conn.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}