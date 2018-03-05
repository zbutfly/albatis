package com.hzcominfo.dataggr.uniquery.mongo.test;

import java.util.List;
import java.util.Map;

import com.hzcominfo.dataggr.uniquery.Client;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;

import net.butfly.albacore.io.URISpec;

public class AppTest {

	public static void main(String[] args) throws Exception {
		//建立连接
		String uri = "mongodb://yjdb:yjdb1234@172.30.10.101:22001/yjdb";
		URISpec uriSpec = new URISpec(uri);
		Client conn = new Client(uriSpec);
		//执行查询
		/*String sql = "select _id, CERTIFICATE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB";
		ResultSet r = conn.execute(sql);
		List<Map<String, Object>> result = r.getResults();
		System.out.println(r.getTotal());*/
		
		// dynamic test
		/*String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME = ? or SERVICE_NAME = ?";
		Object[] params = {"丁丁网吧", "仁和网吧"}; 
		ResultSet r = conn.execute(sql, params);
		List<Map<String, Object>> result = r.getResults();
		System.out.println(r.getTotal());*/
		
		// group
		String sql = "SELECT MIN(NL), MAX(NL) as max_nl, COUNT(*) as cnt FROM GROUP_TEST_COLLECTION GROUP BY XB, MZ";
		Object[] params = {}; 
		ResultSet r = conn.execute(sql, params);
		List<Map<String, Object>> result = r.getResults();
		System.out.println(r.getTotal());
		
		//释放连接
		conn.close();
	}
}
