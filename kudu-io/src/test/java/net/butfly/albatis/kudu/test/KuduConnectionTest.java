package net.butfly.albatis.kudu.test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import net.butfly.albatis.kudu.KuduConnection;
import net.butfly.albatis.kudu.KuduResult;

/**
 * @Author Naturn
 *
 * @Date 2017年3月20日-下午5:27:11
 *
 * @Version 1.0.0
 *
 * @Email juddersky@gmail.com
 */

public class KuduConnectionTest {

	@Test
	public void test() {

		String kuduUri = "kudu://10.60.70.234:7051";
		try (KuduConnection connection = new KuduConnection(kuduUri, Collections.emptyMap());) {

			// KuduOutput output = new KuduOutput("", new URISpec(kuduUri), "");
			Map<String, Object> o = new HashMap<>();
			o.put("id", 123456789);
			o.put("name", "123456789");
			KuduResult result = new KuduResult(o, "my_first_table");
			Assert.assertNotNull(result.forWrite());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
