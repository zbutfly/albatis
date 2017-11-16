package com.hzcominfo.dataggr.uniquery.test;

import java.util.List;
import java.util.Map;

import com.hzcominfo.dataggr.uniquery.Client;
import com.hzcominfo.dataggr.uniquery.ClientManager;

import net.butfly.albacore.io.URISpec;

public class AppTest {

	public static void main(String[] args) {
		//建立连接
		String uri = "";
		URISpec uriSpec = new URISpec(uri);
		Client client = ClientManager.getConnection(uriSpec);
		
		//执行查询
		String sql = "";
		Map r1 = client.execute(sql, "");
		List<Map> r2 = client.execute(sql, "");
		long r3 = client.execute(sql, "");
		
		//释放连接
		try {
			client.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}