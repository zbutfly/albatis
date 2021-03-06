package net.butfly.albatis.kudu.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.kudu.KuduConnectionAsync;
import net.butfly.albatis.kudu.KuduConnBase;

public class KuduConnectionTest {
	@Test
	public void test() {
		String kuduUri = "kudu://10.60.70.234:7051";
		try (@SuppressWarnings("rawtypes")
		KuduConnBase connection = new KuduConnectionAsync(new URISpec(kuduUri));) {
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