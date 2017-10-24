package net.butfly.albatis.kudu.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.kudu.KuduAsyncConnection;
import net.butfly.albatis.kudu.KuduConnection;

public class KuduConnectionTest {
	@Test
	public void test() {
		String kuduUri = "kudu://10.60.70.234:7051";
		try (@SuppressWarnings("rawtypes")
		KuduConnection connection = new KuduAsyncConnection(new URISpec(kuduUri));) {
			// KuduOutput output = new KuduOutput("", new URISpec(kuduUri), "");
			Map<String, Object> o = new HashMap<>();
			o.put("id", 123456789);
			o.put("name", "123456789");
			Message result = new Message("my_first_table", o);
			Assert.assertNotNull(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}